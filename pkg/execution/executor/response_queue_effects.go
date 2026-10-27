package executor

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/telemetry/metrics"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/meta"
)

// responseQueueBinding keeps the materialized carrier maps, context and span
// together through enqueue and both listener paths. Async acknowledgement means
// launch; the callbacks themselves retain this binding until they return.
type responseQueueBinding struct {
	ctx  context.Context
	item queue.Item
	at   time.Time
	span *tracing.DroppableSpan
}

func (g goResponseEffects) enqueue(b *responseQueueBinding) error {
	return g.executor.queue.Enqueue(b.ctx, b.item, b.at, queue.EnqueueOpts{})
}
func (g goResponseEffects) finishSpan(b *responseQueueBinding, send bool) {
	if send {
		_ = b.span.Send()
	} else {
		b.span.Drop()
	}
}
func (g goResponseEffects) launchScheduled(b *responseQueueBinding, step responseStepBinding, name *string, listener execution.LifecycleListener) {
	// Match a direct go call: bind the receiver and shallow argument values
	// before launch, while checkpoint handlers still own this RunContext.
	callback := listener.OnStepScheduled
	ctx, md, item := b.ctx, *step.runCtx.Metadata(), b.item
	go func() {
		defer runtime.KeepAlive(b)
		callback(ctx, md, item, name)
	}()
}

func (g goResponseEffects) syncScheduled(b *responseQueueBinding, step responseStepBinding, name *string, at time.Time) {
	e := g.executor
	execution.SafelyInvokeSyncListeners(b.ctx, e.log, e.syncLifecycles, "OnStepScheduled", func(sl execution.SyncLifecycleListener) {
		sl.OnStepScheduled(b.ctx, *step.runCtx.Metadata(), b.item, name, at)
	})
}
func (g goResponseEffects) scheduled(b *responseQueueBinding, step responseStepBinding, name *string) {
	for _, listener := range g.executor.lifecycles {
		g.launchScheduled(b, step, name, listener)
	}
	g.syncScheduled(b, step, name, g.now())
}

func (g goResponseEffects) discoveryBinding(ctx context.Context, step responseStepBinding, groupID string, at time.Time, coalesceKey *string, failed bool) *responseQueueBinding {
	runCtx, gen, edge := step.runCtx, step.gen, step.edge
	nextEdge := inngest.Edge{Outgoing: gen.ID, Incoming: edge.Edge.Incoming}
	var jobID string
	if failed {
		jobID = fmt.Sprintf("%s-%s-failure", runCtx.Metadata().IdempotencyKey(), gen.ID)
	} else if coalesceKey != nil && *coalesceKey != "" && gen.ParallelMode() != enums.ParallelModeRace {
		jobID = fmt.Sprintf("%s-%s-discover", runCtx.Metadata().ID.RunID, *coalesceKey)
	} else {
		jobID = fmt.Sprintf("%s-%s", runCtx.Metadata().IdempotencyKey(), gen.ID)
	}
	kind := queue.KindEdge
	if failed {
		kind = queue.KindEdgeError
	}
	return &responseQueueBinding{ctx: ctx, at: at, item: queue.Item{
		JobID: &jobID, WorkspaceID: runCtx.Metadata().ID.Tenant.EnvID, GroupID: groupID,
		Kind: kind, Identifier: sv2.V1FromMetadata(*runCtx.Metadata()), PriorityFactor: runCtx.PriorityFactor(),
		CustomConcurrencyKeys: runCtx.ConcurrencyKeys(), Semaphores: stepSemaphores(*runCtx.Metadata()),
		Attempt: 0, MaxAttempts: runCtx.MaxAttempts(), Payload: queue.PayloadEdge{Edge: nextEdge},
		Metadata: make(map[string]any), ParallelMode: gen.ParallelMode(),
	}}
}

func (g goResponseEffects) prepareDiscovery(b *responseQueueBinding, step responseStepBinding, failed bool) {
	e, runCtx := g.executor, step.runCtx
	lifecycleItem := runCtx.LifecycleItem()
	md := runCtx.Metadata()
	location := "executor.maybeEnqueueDiscoveryStep"
	if failed {
		location = "executor.handleStepFailed"
	}
	span, err := e.tracerProvider.CreateDroppableSpan(b.ctx, meta.SpanNameStepDiscovery, &tracing.CreateSpanOptions{
		Carriers: []map[string]any{b.item.Metadata}, FollowsFrom: tracing.SpanRefFromQueueItem(&lifecycleItem),
		Debug: &tracing.SpanDebugData{Location: location}, Metadata: md,
		Parent: tracing.RunSpanRefFromMetadata(md), QueueItem: &b.item,
	})
	b.span = span
	if err != nil {
		if failed {
			e.log.Debug("error creating span for next step after StepFailed", "error", err)
		} else {
			e.log.Debug("error creating span for next step after Step", "error", err)
		}
	}
}

func (g goResponseEffects) plannedBinding(ctx context.Context, step responseStepBinding, groupID string, adjustedStartTime time.Time, coalesceKey string) *responseQueueBinding {
	runCtx, gen, edge := step.runCtx, step.gen, step.edge
	nextEdge := inngest.Edge{
		// Planned generator IDs are the same as the actual OpcodeStep IDs.
		// We can't set edge.Edge.Outgoing here because the step hasn't yet ran.
		//
		// We do, though, want to store the incoming step ID name _without_ overriding
		// the actual DAG step, though.
		// Run the same action.
		IncomingGeneratorStep:     gen.ID,
		IncomingGeneratorStepName: gen.Name,
		Outgoing:                  edge.Edge.Outgoing,
		Incoming:                  edge.Edge.Incoming,
	}
	// prefer DisplayName if available
	if gen.DisplayName != nil {
		nextEdge.IncomingGeneratorStepName = *gen.DisplayName
	}

	// Re-enqueue the exact same edge to run now.
	jobID := fmt.Sprintf("%s-%s", runCtx.Metadata().IdempotencyKey(), gen.ID+"-plan")
	// NOTE: we fudge the time to be slightly in the past so that ultra-low-latency step executions don't return
	// with the same timestamp as the step, which can cause issues with span ordering for rollup.
	nextItem := queue.Item{
		JobID:                 &jobID,
		GroupID:               groupID, // Ensure we correlate future jobs with this group ID, eg. started/failed.
		WorkspaceID:           runCtx.Metadata().ID.Tenant.EnvID,
		Kind:                  queue.KindEdge,
		Identifier:            sv2.V1FromMetadata(*runCtx.Metadata()),
		PriorityFactor:        runCtx.PriorityFactor(),
		CustomConcurrencyKeys: runCtx.ConcurrencyKeys(),
		Attempt:               0,
		MaxAttempts:           runCtx.MaxAttempts(),
		Payload: queue.PayloadEdge{
			Edge: nextEdge,
		},
		Metadata:     make(map[string]any),
		ParallelMode: gen.ParallelMode(),
	}
	if coalesceKey != "" {
		ck := coalesceKey
		nextItem.ParallelCoalesceKey = &ck
	}

	return &responseQueueBinding{ctx: ctx, item: nextItem, at: adjustedStartTime}
}

func (g goResponseEffects) preparePlannedDiscovery(b *responseQueueBinding, step responseStepBinding) {
	e, runCtx := g.executor, step.runCtx
	md := runCtx.Metadata()
	lifecycleItem := runCtx.LifecycleItem()

	_, err := e.tracerProvider.CreateSpan(
		b.ctx,
		meta.SpanNameStepDiscovery,
		&tracing.CreateSpanOptions{
			Debug:       &tracing.SpanDebugData{Location: "executor.handleGeneratorStepPlanned"},
			Carriers:    []map[string]any{b.item.Metadata},
			Metadata:    md,
			FollowsFrom: tracing.SpanRefFromQueueItem(&lifecycleItem),
			Parent:      runCtx.RootSpan(),
			StartTime:   b.at,
			QueueItem:   &b.item,
		},
	)
	if err != nil {
		e.log.Debug("error creating span discovery step after sleep", "error", err)
	}

}

func (g goResponseEffects) preparePlannedSpan(b *responseQueueBinding, step responseStepBinding, handledAt time.Time) {
	e, runCtx, gen := g.executor, step.runCtx, step.gen

	attrs := tracing.GeneratorAttrs(&gen)
	// The planned step is queued the moment this opcode is handled (its own
	// queue item's EnqueuedAt is only stamped at Enqueue, below). The queued
	// segment still covers any wait for a concurrency slot: handling time →
	// execution start. See OpcodeGroup.HandledAt, which avoids a stale
	// queuedAt inherited from the request.
	{
		tracing.AddTimingAttrs(attrs, handledAt, handledAt, time.Time{}, time.Time{})
	}

	span, err := e.tracerProvider.CreateDroppableSpan(
		b.ctx,
		meta.SpanNameStep,
		&tracing.CreateSpanOptions{
			DynamicSpanIDOverride: tracing.DeterministicSpanConfig(tracing.FinalizedStepDynamicSeed(gen.ID)).SpanID.String(),
			Debug:                 &tracing.SpanDebugData{Location: "executor.handleGeneratorStepPlanned"},
			Metadata:              runCtx.Metadata(),
			QueueItem:             &b.item,
			Parent:                runCtx.RootSpan(),
			StartTime:             b.at,
			Attributes:            attrs,
		},
	)
	if err != nil {
		// return fmt.Errorf("error creating span for next step after
		// StepPlanned: %w", err)
		e.log.Debug("error creating span for next step after StepPlanned", "error", err)
	}

	b.span = span
}

func (e *executor) maybeEnqueueDiscoveryStep(ctx context.Context, runCtx execution.RunContext, gen state.GeneratorOpcode, edge queue.PayloadEdge, groupID string, hasPendingSteps bool, coalesceKey *string) error {
	g := goResponseEffects{executor: e}
	step := responseStepBinding{runCtx: runCtx, gen: gen, edge: edge}
	b := g.discoveryBinding(ctx, step, groupID, e.now(), coalesceKey, false)
	if shouldEnqueueDiscovery(hasPendingSteps, gen.ParallelMode()) {
		if err := e.maybeResetForceStepPlan(ctx, runCtx.Metadata()); err != nil {
			return fmt.Errorf("error resetting force step plan: %w", err)
		}
		g.prepareDiscovery(b, step, false)
		err := g.enqueue(b)
		if err != nil {
			g.finishSpan(b, false)
			if errors.Is(err, queue.ErrQueueItemExists) {
				if coalesceKey != nil && gen.ParallelMode() != enums.ParallelModeRace {
					e.log.Debug("discovery enqueue deduped via coalesce key", "run_id", runCtx.Metadata().ID.RunID.String(), "job_id", *b.item.JobID, "coalesce_key", *coalesceKey, "parallel_mode", gen.ParallelMode())
					metrics.IncrDiscoveryCoalesceDedupCount(ctx, metrics.CounterOpt{PkgName: pkgName})
				}
				return nil
			}
			logger.StdlibLogger(ctx).Error("error scheduling step queue item", "error", err)
			return retryableEnqueueError(err)
		}
		g.finishSpan(b, true)
	}
	g.scheduled(b, step, nil)
	return nil
}

func (g goResponseEffects) enqueueFailedStep(ctx context.Context, step responseStepBinding, hasPendingSteps bool) error {
	e, runCtx := g.executor, step.runCtx
	groupID := uuid.New().String()
	ctx = state.WithGroupID(ctx, groupID)
	b := g.discoveryBinding(ctx, step, groupID, e.now(), nil, true)
	if shouldEnqueueDiscovery(hasPendingSteps, runCtx.ParallelMode()) {
		if err := e.maybeResetForceStepPlan(ctx, runCtx.Metadata()); err != nil {
			return fmt.Errorf("error resetting force step plan: %w", err)
		}
		g.prepareDiscovery(b, step, true)
		err := g.enqueue(b)
		if errors.Is(err, queue.ErrQueueItemExists) {
			g.finishSpan(b, false)
			return nil
		}
		if err != nil {
			g.finishSpan(b, false)
			logger.StdlibLogger(ctx).Error("error scheduling step failure queue item", "error", err)
			return retryableEnqueueError(err)
		}
		g.finishSpan(b, true)
	}
	g.scheduled(b, step, nil)
	return nil
}

func (e *executor) handleGeneratorStepPlanned(ctx context.Context, runCtx execution.RunContext, gen state.GeneratorOpcode, edge queue.PayloadEdge, group OpcodeGroup) error {
	g := goResponseEffects{executor: e}
	step := responseStepBinding{runCtx: runCtx, gen: gen, edge: edge}
	groupID := uuid.New().String()
	ctx = state.WithGroupID(ctx, groupID)
	b := g.plannedBinding(ctx, step, groupID, e.now().Add(-time.Millisecond), group.ParallelCoalesceKey)
	// For checkpoint callers the handled-at fallback still samples here, after
	// discovery span creation. The explicit-time span helper does not read it.
	g.preparePlannedDiscovery(b, step)
	g.preparePlannedSpan(b, step, e.opcodeHandledAt(group))
	err := g.enqueue(b)
	if errors.Is(err, queue.ErrQueueItemExists) {
		g.finishSpan(b, false)
		return nil
	}
	g.finishSpan(b, true)
	g.scheduled(b, step, &gen.Name)
	return err
}
