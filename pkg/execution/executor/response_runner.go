package executor

import (
	"context"
	"errors"
	"fmt"

	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
)

// responseRunner owns staging and accepted-save continuations. It has no
// lifecycle beyond the call; Go effects retain execution-local contexts and
// perform writes, callbacks and queue recipes at their original activation sites.
// Retryable errors, planned steps and the other opcode handlers still own their
// Go sequencing; this is not yet a whole-response continuation boundary.
type responseRunner struct {
	effects goResponseEffects
}

func (e *executor) responseRunner() responseRunner {
	return responseRunner{effects: goResponseEffects{executor: e}}
}

func (r responseRunner) run(ctx context.Context, i *runInstance, resp *state.DriverResponse) error {
	nonLazy := nonLazyOpCount(resp.Generator)
	if update := responseMetadataUpdate(i.md.Config, resp, nonLazy); update != nil {
		if err := r.effects.updateMetadata(ctx, i.md.ID, *update); err != nil {
			return fmt.Errorf("error updating function metadata: %w", err)
		}
	}

	// Persist metadata before the limit check, without refreshing this response's
	// loaded snapshot. Coalescing below must still consult that original version.
	if len(resp.Generator) > consts.DefaultMaxStepLimit {
		return state.WrapInStandardError(
			state.ErrFunctionOverflowed,
			state.InngestErrFunctionOverflowed,
			fmt.Sprintf("The function run exceeded the step limit of %d steps.", consts.DefaultMaxStepLimit),
			"",
		)
	}

	groups := opGroups(resp.Generator)
	// Sample only after metadata/limits, once for both concurrent groups.
	handledAt := r.effects.now()
	groups.PriorityGroup.HandledAt = handledAt
	groups.OtherGroup.HandledAt = handledAt

	nonLazyIDs := groups.NonLazyIDs()
	if hasPlanOp(resp.Generator) && len(nonLazyIDs) > 1 && i.md.ShouldCoalesceParallelism(resp) {
		ck := computeParallelCoalesceKey(i.md.ID.RunID.String(), nonLazyIDs)
		groups.PriorityGroup.ParallelCoalesceKey = ck
		groups.OtherGroup.ParallelCoalesceKey = ck

		// The replacement pending set and coalesce key must describe the same
		// batch. No sibling may save/enqueue until this write succeeds.
		if err := r.effects.savePending(ctx, i.md.ID, nonLazyIDs); err != nil {
			return fmt.Errorf("error saving pending steps: %w", err)
		}
	}

	for _, group := range groups.All() {
		// The Go join waits for every sibling and selects the racing first
		// error. Apply response preferences to that error without ranking errors
		// or cancelling siblings.
		if err := r.effects.runGroup(ctx, i, group); err != nil {
			if errors.Is(err, state.ErrStateOverflowed) {
				return err
			}
			if queue.IsAlwaysRetryable(err) {
				return err
			}
			if resp.NoRetry {
				return queue.NeverRetryError(err)
			}
			if resp.RetryAt != nil {
				return queue.RetryAtError(err, resp.RetryAt)
			}
			return err
		}
	}
	return nil
}

// responseMetadataUpdate proposes a write; accepting it must not replace the
// loaded metadata snapshot used by the remainder of the current response.
func responseMetadataUpdate(config sv2.Config, resp *state.DriverResponse, nonLazy int) *sv2.MutableConfig {
	var update *sv2.MutableConfig
	proposal := func() *sv2.MutableConfig {
		return &sv2.MutableConfig{
			ForceStepPlan:  config.ForceStepPlan,
			RequestVersion: resp.RequestVersion,
			StartedAt:      config.StartedAt,
		}
	}
	if config.RequestVersion == -1 {
		update = proposal()
	}
	if nonLazy > 1 && !config.ForceStepPlan {
		if update == nil {
			update = proposal()
		}
		update.ForceStepPlan = true
	}
	if resp.HasAI() {
		if update == nil {
			update = proposal()
		}
		update.HasAI = true
	}
	return update
}

func (r responseRunner) step(ctx context.Context, step responseStepBinding) error {
	ctx, request, err := r.effects.prepareStep(ctx, step)
	if err != nil {
		return err
	}
	saved := r.effects.saveStep(ctx, request)
	if errors.Is(saved.err, state.ErrDuplicateResponse) || errors.Is(saved.err, state.ErrIdempotentResponse) {
		// Keep the authoritative pending result even when a prior attempt
		// persisted output but did not finish scheduling discovery.
		r.effects.logExistingStepOutput(ctx, step, saved.err)
	} else if saved.err != nil {
		return saved.err
	}

	r.effects.stepFinished(ctx, step)
	r.effects.releaseCapacity(ctx, step)

	return r.effects.enqueueStep(ctx, step, saved.hasPending)
}

func (r responseRunner) failedStep(ctx context.Context, step responseStepBinding) error {
	ctx, request, err := r.effects.prepareFailedStep(ctx, step)
	if err != nil {
		return err
	}
	saved := r.effects.saveStep(ctx, request)
	// Permanent failures intentionally reject duplicate responses; only the
	// idempotent response error permits the continuation.
	if saved.err != nil && !errors.Is(saved.err, state.ErrIdempotentResponse) {
		return saved.err
	}

	r.effects.stepFailed(ctx, step)
	r.effects.releaseCapacity(ctx, step)
	return r.effects.enqueueFailedStep(ctx, step, saved.hasPending)
}
