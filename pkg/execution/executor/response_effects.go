package executor

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/meta"
	"github.com/inngest/inngest/pkg/tracing/metadata"
	"github.com/inngest/inngest/pkg/tracing/metadata/extractors"
	"golang.org/x/sync/errgroup"
)

// goResponseEffects binds the runner to the current executor, not to a new
// service or durable owner. These are concrete Go calls, not an effect VM/ABI.
type goResponseEffects struct {
	executor *executor
}

// responseStepBinding retains the real RunContext (including shared metadata
// accounting and capacity release), with the branch-local opcode and edge. It
// also serves direct HandleGenerator/checkpoint calls without staging a batch.
type responseStepBinding struct {
	runCtx execution.RunContext
	gen    state.GeneratorOpcode
	edge   queue.PayloadEdge
}

type responseSaveRequest struct {
	runID  sv2.ID
	stepID string
	output []byte
}

// Preserve both store observations. Error identity/wrapping and hasPending are
// authoritative; successful and permanent-failure continuations accept different
// errors, and neither may substitute a previous pending read.
type responseSaveResult struct {
	hasPending bool
	err        error
}

func (g goResponseEffects) now() time.Time { return g.executor.now() }

func (g goResponseEffects) updateMetadata(ctx context.Context, id sv2.ID, update sv2.MutableConfig) error {
	return g.executor.smv2.UpdateMetadata(ctx, id, update)
}

func (g goResponseEffects) savePending(ctx context.Context, id sv2.ID, ids []string) error {
	return g.executor.smv2.SavePending(ctx, id, ids)
}

func (g goResponseEffects) saveStep(ctx context.Context, request responseSaveRequest) responseSaveResult {
	pending, err := g.executor.smv2.SaveStep(ctx, request.runID, request.stepID, request.output)
	return responseSaveResult{hasPending: pending, err: err}
}

func (g goResponseEffects) runGroup(ctx context.Context, i *runInstance, group OpcodeGroup) error {
	e := g.executor
	eg := errgroup.Group{}
	for _, op := range group.Opcodes {
		if op == nil {
			// This is clearly an error.
			if e.log != nil {
				e.log.Error("error handling generator", "error", "nil generator returned")
			}
			continue
		}
		copied := *op
		runInstanceCopy := *i
		if group.ShouldStartHistoryGroup {
			// Give each opcode its own group ID, since we want to track each
			// parallel step individually.
			runInstanceCopy.item.GroupID = uuid.New().String()
		}
		eg.Go(func() error {
			defer func() {
				if r := recover(); r != nil {
					e.log.Error(
						"panic in handleGenerator",
						"error", r,
						"stack", string(debug.Stack()),
					)
				}
			}()
			return e.handleGeneratorOp(ctx, &runInstanceCopy, copied, group)
		})
	}
	return eg.Wait()
}

func (g goResponseEffects) prepareStep(ctx context.Context, step responseStepBinding) (context.Context, responseSaveRequest, error) {
	e, runCtx, gen := g.executor, step.runCtx, step.gen
	// Prepare output and trace metadata before constructing the save request.
	output, err := gen.Output()
	if err != nil {
		return ctx, responseSaveRequest{}, err
	}

	if err := e.validateStateSize(len(output), *runCtx.Metadata()); err != nil {
		return ctx, responseSaveRequest{}, err
	}

	// Calculate step duration in milliseconds
	stepDurationMs := gen.Timing.B / 1_000_000

	// Extract AI metadata from step output before saving so the cumulative
	// metadata size delta is accurate when persisted alongside the step.
	extraMetadata := metadata.WithWarnings(extractors.ExtractAIOutputMetadata(
		[]byte(output),
		stepDurationMs,
	))

	e.emitStepSpan(ctx, runCtx, &gen, extraMetadata, nil)

	ctx, request := step.saveRequest(ctx, output)
	return ctx, request, nil
}

func (g goResponseEffects) prepareFailedStep(ctx context.Context, step responseStepBinding) (context.Context, responseSaveRequest, error) {
	e, runCtx, gen := g.executor, step.runCtx, step.gen
	// Materialize the error envelope before tracing and metadata accounting.
	output, err := gen.Output()
	if err != nil {
		return ctx, responseSaveRequest{}, err
	}

	e.emitStepSpan(ctx, runCtx, &gen, nil, nil)

	ctx, request := step.saveRequest(ctx, output)
	return ctx, request, nil
}

func (step responseStepBinding) saveRequest(ctx context.Context, output string) (context.Context, responseSaveRequest) {
	// Tracing/AI metadata preparation has already run. All siblings retain the
	// shared RunMetrics accounting; each swap claims only its own current delta.
	if delta := step.runCtx.Metadata().Metrics.SwapMetadataSizeDelta(); delta > 0 {
		ctx = state.WithMetadataSizeDelta(ctx, delta)
	}
	return ctx, responseSaveRequest{runID: step.runCtx.Metadata().ID, stepID: step.gen.ID, output: []byte(output)}
}

func (g goResponseEffects) logExistingStepOutput(ctx context.Context, step responseStepBinding, err error) {
	g.executor.log.Warn("step output already persisted; keeping existing output", "error", err, "run_id", step.runCtx.Metadata().ID.RunID, "step_id", step.gen.ID)
}

func (g goResponseEffects) stepFinished(ctx context.Context, step responseStepBinding) {
	g.executor.RunStepRunFinishedLifecycle(ctx, *step.runCtx.Metadata(), step.runCtx.LifecycleItem(), step.edge.Edge, step.gen, g.executor.now())
}

func (g goResponseEffects) stepFailed(ctx context.Context, step responseStepBinding) {
	g.executor.RunStepRunFailedLifecycle(ctx, *step.runCtx.Metadata(), step.runCtx.LifecycleItem(), step.edge.Edge, step.gen, enums.StepStatusFailed, step.runCtx.AttemptCount(), g.executor.now())
}

func (g goResponseEffects) releaseCapacity(ctx context.Context, step responseStepBinding) {
	if err := step.runCtx.ReleaseCapacityLease(); err != nil {
		logger.StdlibLogger(ctx).ReportError(err, "could not release capacity lease early")
	}
}

func (g goResponseEffects) enqueueStep(ctx context.Context, step responseStepBinding, hasPendingSteps bool) error {
	// Allocate only after the accepted save/lifecycle/release, even when the
	// discovery recipe will suppress enqueue because there are pending steps.
	groupID := uuid.New().String()
	return g.executor.maybeEnqueueDiscoveryStep(
		state.WithGroupID(ctx, groupID),
		step.runCtx, step.gen, step.edge, groupID, hasPendingSteps,
		step.runCtx.LifecycleItem().ParallelCoalesceKey,
	)
}

func (g goResponseEffects) enqueueFailedStep(ctx context.Context, step responseStepBinding, hasPendingSteps bool) error {
	e, runCtx, gen, edge := g.executor, step.runCtx, step.gen, step.edge
	// Because this is a final step error that was handled gracefully, enqueue
	// another attempt to the function with a new edge type.
	nextEdge := inngest.Edge{
		Outgoing: gen.ID,             // Going from the current step
		Incoming: edge.Edge.Incoming, // And re-calling the incoming function in a loop
	}
	groupID := uuid.New().String()
	ctx = state.WithGroupID(ctx, groupID)

	// This is the discovery step to find what happens after we error
	jobID := fmt.Sprintf("%s-%s-failure", runCtx.Metadata().IdempotencyKey(), gen.ID)
	now := e.now()
	nextItem := queue.Item{
		JobID:                 &jobID,
		WorkspaceID:           runCtx.Metadata().ID.Tenant.EnvID,
		GroupID:               groupID,
		Kind:                  queue.KindEdgeError,
		Identifier:            sv2.V1FromMetadata(*runCtx.Metadata()),
		PriorityFactor:        runCtx.PriorityFactor(),
		CustomConcurrencyKeys: runCtx.ConcurrencyKeys(),
		Semaphores:            stepSemaphores(*runCtx.Metadata()),
		Attempt:               0,
		MaxAttempts:           runCtx.MaxAttempts(),
		Payload:               queue.PayloadEdge{Edge: nextEdge},
		Metadata:              make(map[string]any),
		ParallelMode:          gen.ParallelMode(),
	}

	if shouldEnqueueDiscovery(hasPendingSteps, runCtx.ParallelMode()) {
		if err := e.maybeResetForceStepPlan(ctx, runCtx.Metadata()); err != nil {
			return fmt.Errorf("error resetting force step plan: %w", err)
		}
		lifecycleItem := runCtx.LifecycleItem()
		metadata := runCtx.Metadata()
		span, err := e.tracerProvider.CreateDroppableSpan(
			ctx,
			meta.SpanNameStepDiscovery,
			&tracing.CreateSpanOptions{
				Carriers:    []map[string]any{nextItem.Metadata},
				FollowsFrom: tracing.SpanRefFromQueueItem(&lifecycleItem),
				Debug:       &tracing.SpanDebugData{Location: "executor.handleStepFailed"},
				Metadata:    runCtx.Metadata(),
				QueueItem:   &nextItem,
				Parent:      tracing.RunSpanRefFromMetadata(metadata),
			},
		)
		if err != nil {
			// return fmt.Errorf("error creating span for next step after
			// StepError: %w", err)
			e.log.Debug("error creating span for next step after StepFailed", "error", err)
		}

		err = e.queue.Enqueue(ctx, nextItem, now, queue.EnqueueOpts{})
		if errors.Is(err, queue.ErrQueueItemExists) {
			span.Drop()
			return nil
		}
		if err != nil {
			span.Drop()
			logger.StdlibLogger(ctx).Error("error scheduling step failure queue item", "error", err)
			return retryableEnqueueError(err)
		}

		_ = span.Send()
	}

	for _, l := range e.lifecycles {
		go l.OnStepScheduled(ctx, *runCtx.Metadata(), nextItem, nil)
	}
	syncNow := e.now()
	execution.SafelyInvokeSyncListeners(ctx, e.log, e.syncLifecycles, "OnStepScheduled", func(sl execution.SyncLifecycleListener) {
		sl.OnStepScheduled(ctx, *runCtx.Metadata(), nextItem, nil, syncNow)
	})

	return nil
}
