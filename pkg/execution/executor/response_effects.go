package executor

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/telemetry/metrics"
	"github.com/inngest/inngest/pkg/tracing/metadata"
	"github.com/inngest/inngest/pkg/tracing/metadata/extractors"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	g.lifecycleAt(ctx, step, enums.StepStatusCompleted, 0, g.now())
}

func (g goResponseEffects) stepFailed(ctx context.Context, step responseStepBinding) {
	g.lifecycleAt(ctx, step, enums.StepStatusFailed, step.runCtx.AttemptCount(), g.now())
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

// Activation occurs after history binding and edge validation, including None.
// StepError marks the incoming span before a local permanent-failure conversion.
func (g goResponseEffects) activateBranch(ctx context.Context, gen state.GeneratorOpcode) {
	metrics.IncrExecutorHandleGeneratorCount(ctx, gen.Op.String(), metrics.CounterOpt{PkgName: pkgName})
	if gen.Op == enums.OpcodeStepError {
		trace.SpanFromContext(ctx).SetStatus(codes.Error, gen.Error.Name)
	}
}
func (g goResponseEffects) prepareRetrySpan(ctx context.Context, step responseStepBinding) {
	gen := step.gen
	g.executor.emitStepSpan(ctx, step.runCtx, &gen, nil, nil)
}

// The caller owns the clock read. This effect must never sample it again.
func (g goResponseEffects) lifecycleAt(ctx context.Context, step responseStepBinding, status enums.StepStatus, attempt int, at time.Time) {
	e := g.executor
	if status == enums.StepStatusCompleted {
		e.RunStepRunFinishedLifecycle(ctx, *step.runCtx.Metadata(), step.runCtx.LifecycleItem(), step.edge.Edge, step.gen, at)
	} else {
		e.RunStepRunFailedLifecycle(ctx, *step.runCtx.Metadata(), step.runCtx.LifecycleItem(), step.edge.Edge, step.gen, status, attempt, at)
	}
}
