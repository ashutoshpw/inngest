//go:build linux && response_continuation_spike && cgo

package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	native "github.com/inngest/inngest/internal/responsecontinuationspike"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
)

// Test-private experimental host, shared by behavioral witnesses and measurement.
// Fault hooks remain nil in measurements; there is no production selector.
type continuationBytes []byte

func (b continuationBytes) MarshalJSON() ([]byte, error) {
	v := make([]uint16, len(b))
	for i, c := range b {
		v[i] = uint16(c)
	}
	return json.Marshal(v)
}
func ownedJSON(v any) continuationBytes {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}
func modeNative(m enums.ParallelMode) string {
	switch m {
	case enums.ParallelModeRace:
		return "Race"
	case enums.ParallelModeWait:
		return "Wait"
	default:
		return "None"
	}
}
func timeNative(t time.Time) json.RawMessage {
	ns := new(big.Int).Mul(big.NewInt(t.Unix()), big.NewInt(1_000_000_000))
	ns.Add(ns, big.NewInt(int64(t.Nanosecond())))
	return json.RawMessage(ns.String())
}

type nativeBranch struct {
	// Original full Go descriptor and RunContext survive Rust's lossy projection.
	instance     runInstance
	step         responseStepBinding
	ctx          context.Context
	save         responseSaveRequest
	queue        *responseQueueBinding
	spanFinished bool
	lifecycleAt  time.Time
	recipeAt     time.Time
	callbackAt   time.Time
}
type nativeCompletion struct {
	request     native.Request
	observation any
	err         error
}
type continuationDriver struct {
	effects     goResponseEffects
	original    *runInstance
	branches    []nativeBranch
	session     *native.Session
	initial     native.Progress
	generation  uint64
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	doneOnce    sync.Once
	watcherDone chan struct{}
	errorsMu    sync.Mutex
	errors      map[uint64]error
	nextError   uint64
	closeOnce   sync.Once
	closeErr    error

	handledAt time.Time

	callbacks sync.WaitGroup

	// Admission and close share this lock. The effect is admitted at the
	// successful enterEffect check; stopAdmission closes the gate under the same
	// lock. A reserved goroutine that loses that race never calls its Go port. If
	// enterEffect wins, the effect is admitted before close and may start after
	// Close begins because goroutine scheduling is independent of the gate.
	admissionMu     sync.Mutex
	runStarted      bool
	admissionClosed bool
	admissionErr    error

	// All functions below and this driver live only in this test file. They let
	// fault witnesses deliver malformed transport results after a real effect.
	resume func(native.Key, any) (native.Progress, error)

	// beforeAdmission is a test-only barrier for forcing a close/dispatch race.
	// It runs after the runner's context check and before it admits the request.
	beforeAdmission func(native.Request)
}

var (
	nativeGeneration            atomic.Uint64
	errContinuationDriverClosed = errors.New("native driver already started or closed")
)

func newContinuationDriver(ctx context.Context, e *executor, i *runInstance, resp *state.DriverResponse) (*continuationDriver, error) {
	parentCtx := ctx
	ctx, cancel := context.WithCancel(parentCtx)
	d := &continuationDriver{effects: goResponseEffects{executor: e}, original: i, ctx: ctx, cancel: cancel, done: make(chan struct{}), watcherDone: make(chan struct{}), errors: map[uint64]error{}, generation: nativeGeneration.Add(1)}
	d.bindError(ErrHandledStepError)
	d.bindError(state.WrapInStandardError(state.ErrFunctionOverflowed, state.InngestErrFunctionOverflowed, fmt.Sprintf("The function run exceeded the step limit of %d steps.", consts.DefaultMaxStepLimit), ""))
	ops := make([]any, len(resp.Generator))
	d.branches = make([]nativeBranch, len(ops))
	for index, op := range resp.Generator {
		if op == nil {
			continue
		}
		var user any
		if op.Error != nil {
			user = map[string]any{"name": op.Error.Name, "message": op.Error.Message, "no_retry": op.Error.NoRetry}
		}
		ops[index] = map[string]any{"tag": int(op.Op), "id": op.ID, "name": op.Name, "display_name": op.DisplayName, "data": continuationBytes(op.Data), "options": ownedJSON(op.Opts), "error": user, "parallel_mode": modeNative(op.ParallelMode()), "has_ai": op.HasAI()}
		branch := &d.branches[index]
		branch.instance = *i
		branch.ctx = ctx
		branch.step = responseStepBinding{runCtx: &branch.instance, gen: *op}
	}
	var edge any
	if payload, ok := i.item.Payload.(queue.PayloadEdge); ok {
		edge = map[string]any{"incoming": payload.Edge.Incoming, "outgoing": payload.Edge.Outgoing, "incoming_generator_step": payload.Edge.IncomingGeneratorStep, "incoming_generator_step_name": payload.Edge.IncomingGeneratorStepName}
		for j := range d.branches {
			d.branches[j].step.edge = payload
		}
	}
	var retryAt any
	if resp.RetryAt != nil {
		retryAt = resp.RetryAt.UnixNano()
	}
	input, err := json.Marshal(map[string]any{
		"generation": d.generation, "ops": ops,
		"config":           map[string]any{"request_version": i.md.Config.RequestVersion, "force_step_plan": i.md.Config.ForceStepPlan, "started_at": timeNative(i.md.Config.StartedAt), "has_ai": i.md.Config.HasAI},
		"response_version": resp.RequestVersion, "response_no_retry": resp.NoRetry, "response_user_no_retry": resp.UserError != nil && resp.UserError.NoRetry, "retry_at": retryAt,
		"run_id": i.md.ID.RunID.String(), "idempotency_key": i.md.IdempotencyKey(), "edge": edge, "attempt": i.item.Attempt, "max_attempts": i.item.GetMaxAttempts(),
		"context_mode": modeNative(i.ParallelMode()), "context_group": i.item.GroupID, "context_coalesce_key": i.item.ParallelCoalesceKey,
		"queue":              map[string]any{"workspace_id": i.md.ID.Tenant.EnvID.String(), "identifier": ownedJSON(sv2.V1FromMetadata(i.md)), "priority_factor": i.PriorityFactor(), "custom_concurrency_keys": ownedJSON(i.ConcurrencyKeys()), "step_semaphores": ownedJSON(stepSemaphores(i.md))},
		"handled_step_error": 1, "function_overflow": 2,
	})
	if err == nil {
		d.session, d.initial, err = native.Start(input)
	}
	if err != nil {
		cancel()
		return nil, err
	}
	d.resume = d.session.Resume
	go d.watchExternalCancellation(parentCtx)
	return d, nil
}

// Parent cancellation races with dispatch through admissionMu. It takes effect
// for admission when this watcher (or enterEffect) closes the gate; an effect
// whose enterEffect check wins that lock may already be admitted.
func (d *continuationDriver) watchExternalCancellation(ctx context.Context) {
	defer close(d.watcherDone)
	select {
	case <-ctx.Done():
		d.stopAdmission(ctx.Err())
	case <-d.done:
	}
}

func (d *continuationDriver) beginRun() error {
	d.admissionMu.Lock()
	defer d.admissionMu.Unlock()
	if d.runStarted {
		return errContinuationDriverClosed
	}
	if d.admissionClosed {
		if d.admissionErr != nil {
			return d.admissionErr
		}
		return errContinuationDriverClosed
	}
	d.runStarted = true
	return nil
}

// stopAdmission is Close's linearization point. Any dispatch that already
// passed enterEffect is issued work and is drained; later effects cannot enter
// their Go port. The external-context watcher uses the same transition.
func (d *continuationDriver) stopAdmission(reason error) {
	d.admissionMu.Lock()
	if !d.admissionClosed {
		d.admissionClosed = true
		d.admissionErr = reason
	}
	started := d.runStarted
	d.admissionMu.Unlock()

	d.cancel()
	if !started {
		d.doneOnce.Do(func() { close(d.done) })
	}
}

func (d *continuationDriver) finishRun() {
	d.admissionMu.Lock()
	if !d.admissionClosed {
		d.admissionClosed = true
		d.admissionErr = errContinuationDriverClosed
	}
	d.admissionMu.Unlock()
	d.doneOnce.Do(func() { close(d.done) })
}

func (d *continuationDriver) enterEffect() error {
	d.admissionMu.Lock()
	if d.admissionClosed {
		err := d.ctx.Err()
		if err == nil {
			err = d.admissionErr
		}
		if err == nil {
			err = errContinuationDriverClosed
		}
		d.admissionMu.Unlock()
		return err
	}
	if err := d.ctx.Err(); err != nil {
		d.admissionClosed = true
		d.admissionErr = err
		d.admissionMu.Unlock()
		d.cancel()
		return err
	}
	d.admissionMu.Unlock()
	return nil
}

func (d *continuationDriver) dispatch(request native.Request, active *int, completed chan<- nativeCompletion) error {
	if d.beforeAdmission != nil {
		d.beforeAdmission(request)
	}
	d.admissionMu.Lock()
	if d.admissionClosed {
		err := d.ctx.Err()
		if err == nil {
			err = d.admissionErr
		}
		if err == nil {
			err = errContinuationDriverClosed
		}
		d.admissionMu.Unlock()
		return err
	}
	if err := d.ctx.Err(); err != nil {
		d.admissionClosed = true
		d.admissionErr = err
		d.admissionMu.Unlock()
		d.cancel()
		return err
	}
	(*active)++
	d.admissionMu.Unlock()

	go func(request native.Request) {
		if err := d.enterEffect(); err != nil {
			completed <- nativeCompletion{request: request, err: err}
			return
		}
		observation, err := d.effect(request)
		completed <- nativeCompletion{request: request, observation: observation, err: err}
	}(request)
	return nil
}
func (d *continuationDriver) bindError(err error) *native.Fault {
	if err == nil {
		return nil
	}
	fault := &native.Fault{Duplicate: errors.Is(err, state.ErrDuplicateResponse) || errors.Is(err, queue.ErrQueueItemExists), Idempotent: errors.Is(err, state.ErrIdempotentResponse), StateOverflow: errors.Is(err, state.ErrStateOverflowed), AlwaysRetry: queue.IsAlwaysRetryable(err)}
	d.errorsMu.Lock()
	defer d.errorsMu.Unlock()
	d.nextError++
	d.errors[d.nextError] = err
	fault.Handle = d.nextError
	return fault
}
func (d *continuationDriver) originalError(handle uint64) error {
	d.errorsMu.Lock()
	defer d.errorsMu.Unlock()
	return d.errors[handle]
}
func (d *continuationDriver) disposition(p *native.Disposition) error {
	if p == nil {
		return nil
	}
	err := d.originalError(p.Fault.Handle)
	if err == nil {
		return fmt.Errorf("unknown original error handle %d", p.Fault.Handle)
	}
	switch p.Context {
	case "Original":
	case "Metadata":
		err = fmt.Errorf("error updating function metadata: %w", err)
	case "Pending":
		err = fmt.Errorf("error saving pending steps: %w", err)
	case "Reset":
		err = fmt.Errorf("error resetting force step plan: %w", err)
	case "Discovery":
		err = retryableEnqueueError(err)
	default:
		return fmt.Errorf("unknown native error context %q", p.Context)
	}
	kind, payload, decodeErr := nativeVariant(p.Policy)
	if decodeErr != nil {
		return decodeErr
	}
	switch kind {
	case "Original":
		return err
	case "Always":
		if p.Context == "Discovery" {
			return err
		}
		return queue.AlwaysRetryError(err)
	case "Never":
		return queue.NeverRetryError(err)
	case "At":
		var at int64
		if err := json.Unmarshal(payload, &at); err != nil {
			return err
		}
		if d.original.resp.RetryAt == nil || d.original.resp.RetryAt.UnixNano() != at {
			return errors.New("unbound native retry time")
		}
		return queue.RetryAtError(err, d.original.resp.RetryAt)
	default:
		return fmt.Errorf("unknown native retry policy %q", kind)
	}
}
func nativeVariant(raw json.RawMessage) (string, json.RawMessage, error) {
	var name string
	if json.Unmarshal(raw, &name) == nil {
		return name, nil, nil
	}
	var v map[string]json.RawMessage
	if err := json.Unmarshal(raw, &v); err != nil {
		return "", nil, err
	}
	if len(v) != 1 {
		return "", nil, errors.New("invalid native variant")
	}
	for k, p := range v {
		return k, p, nil
	}
	panic("unreachable")
}
func nativeBranchIndex(k native.Key) (int, error) {
	name, data, err := nativeVariant(k.Owner)
	if err != nil {
		return 0, err
	}
	if name == "Response" {
		return -1, nil
	}
	if name != "Branch" {
		return 0, errors.New("unknown native owner")
	}
	var index int
	err = json.Unmarshal(data, &index)
	return index, err
}
func nativeGroup(raw json.RawMessage) (string, error) {
	var n big.Int
	if _, ok := n.SetString(string(raw), 10); !ok || n.Sign() <= 0 || n.BitLen() > 128 {
		return "", errors.New("invalid native UUID")
	}
	bytes := n.FillBytes(make([]byte, 16))
	u, err := uuid.FromBytes(bytes)
	return u.String(), err
}

// run is the only Resume owner. Issued effects run concurrently and report in
// observed completion order; an ordinary branch error never cancels siblings.
// Cancellation/poison stops dispatch and joins already-issued work, then drains
// async callbacks before releasing native state and Go bindings. No fallback.
func (d *continuationDriver) run() (result error) {
	if err := d.beginRun(); err != nil {
		return err
	}
	completed := make(chan nativeCompletion, len(d.branches)+1)
	active := 0
	completedNormally := false
	outstanding := map[string]bool{}
	defer func() {
		if !completedNormally {
			d.stopAdmission(errContinuationDriverClosed)
		}
		for active > 0 {
			<-completed
			active--
		}
		d.finishRun()
	}()

	p := d.initial
	for {
		if err := d.ctx.Err(); err != nil {
			return err
		}
		for _, request := range p.Ready {
			if err := d.ctx.Err(); err != nil {
				return err
			}
			index, err := nativeBranchIndex(request.Key)
			if err != nil || index < -1 || index >= len(d.branches) || request.Key.Generation != d.generation || request.Key.Sequence == 0 {
				return errors.New("invalid native ready key")
			}
			key := fmt.Sprintf("%d/%d/%d", request.Key.Generation, index, request.Key.Sequence)
			if outstanding[key] {
				return errors.New("duplicate native ready key")
			}
			if err := d.dispatch(request, &active, completed); err != nil {
				return err
			}
			outstanding[key] = true
		}
		if p.Done {
			if active != 0 {
				return errors.New("native completed before effects joined")
			}
			completedNormally = true
			return d.disposition(p.Completion)
		}
		if active == 0 {
			return errors.New("native stalled with no outstanding request")
		}
		select {
		case <-d.ctx.Done():
			return d.ctx.Err()
		case completion := <-completed:
			active--
			if completion.err != nil {
				return completion.err
			}
			var err error
			p, err = d.resume(completion.request.Key, completion.observation)
			if err != nil {
				return err
			}
		}
	}
}
func (d *continuationDriver) drain() error {
	<-d.done
	<-d.watcherDone
	d.closeOnce.Do(func() {
		d.callbacks.Wait()
		for j := range d.branches {
			if b := d.branches[j].queue; b != nil {
				if b.span != nil && !d.branches[j].spanFinished {
					d.effects.finishSpan(b, false)
				}
			}
		}
		d.closeErr = d.session.Close()
		d.cancel()
	})
	return d.closeErr
}
func (d *continuationDriver) Close() error {
	d.stopAdmission(errContinuationDriverClosed)
	return d.drain()
}
func runNativeResponse(ctx context.Context, e *executor, i *runInstance, resp *state.DriverResponse) error {
	d, err := newContinuationDriver(ctx, e, i, resp)
	if err != nil {
		return err
	}
	result := d.run()
	if closeErr := d.drain(); closeErr != nil {
		if result == nil {
			return closeErr
		}
		return errors.Join(result, closeErr)
	}
	return result
}

type nativeRecipe struct {
	JobID string          `json:"job_id"`
	Group json.RawMessage `json:"group"`
	Kind  string          `json:"kind"`
	Edge  struct {
		Incoming                  string `json:"incoming"`
		Outgoing                  string `json:"outgoing"`
		IncomingGeneratorStep     string `json:"incoming_generator_step"`
		IncomingGeneratorStepName string `json:"incoming_generator_step_name"`
	} `json:"edge"`
	At                  int64   `json:"at"`
	Attempt             int     `json:"attempt"`
	MaxAttempts         int     `json:"max_attempts"`
	ParallelMode        string  `json:"parallel_mode"`
	ParallelCoalesceKey *string `json:"parallel_coalesce_key"`
	Bindings            struct {
		WorkspaceID           uuid.UUID `json:"workspace_id"`
		Identifier            []byte    `json:"identifier"`
		PriorityFactor        *int64    `json:"priority_factor"`
		CustomConcurrencyKeys []byte    `json:"custom_concurrency_keys"`
		StepSemaphores        []byte    `json:"step_semaphores"`
	} `json:"bindings"`
}

func (d *continuationDriver) materialize(b *nativeBranch, r nativeRecipe) error {
	group, err := nativeGroup(r.Group)
	if err != nil {
		return err
	}
	item := queue.Item{JobID: &r.JobID, GroupID: group, WorkspaceID: r.Bindings.WorkspaceID, PriorityFactor: r.Bindings.PriorityFactor, Attempt: r.Attempt, MaxAttempts: &r.MaxAttempts, Metadata: map[string]any{}, ParallelCoalesceKey: r.ParallelCoalesceKey}
	switch r.Kind {
	case "Edge":
		item.Kind = queue.KindEdge
	case "EdgeError":
		item.Kind = queue.KindEdgeError
	default:
		return errors.New("invalid native recipe kind")
	}
	switch r.ParallelMode {
	case "None":
		item.ParallelMode = enums.ParallelModeNone
	case "Wait":
		item.ParallelMode = enums.ParallelModeWait
	case "Race":
		item.ParallelMode = enums.ParallelModeRace
	default:
		return errors.New("invalid native recipe mode")
	}
	if err := json.Unmarshal(r.Bindings.Identifier, &item.Identifier); err != nil {
		return err
	}
	if err := json.Unmarshal(r.Bindings.CustomConcurrencyKeys, &item.CustomConcurrencyKeys); err != nil {
		return err
	}
	if len(r.Bindings.StepSemaphores) > 0 {
		if err := json.Unmarshal(r.Bindings.StepSemaphores, &item.Semaphores); err != nil {
			return err
		}
	}
	item.Payload = queue.PayloadEdge{Edge: inngest.Edge{Incoming: r.Edge.Incoming, Outgoing: r.Edge.Outgoing, IncomingGeneratorStep: r.Edge.IncomingGeneratorStep, IncomingGeneratorStepName: r.Edge.IncomingGeneratorStepName}}
	b.queue = &responseQueueBinding{ctx: state.WithGroupID(b.ctx, group), item: item, at: b.recipeAt.Add(time.Duration(r.At - b.recipeAt.UnixNano()))}
	return nil
}
func (d *continuationDriver) effect(r native.Request) (observation any, err error) {
	kind, payload, err := nativeVariant(r.Effect)
	if err != nil {
		return nil, err
	}
	index, err := nativeBranchIndex(r.Key)
	if err != nil {
		return nil, err
	}
	var b *nativeBranch
	if index >= 0 {
		b = &d.branches[index]
	}
	// Only Go effect panics have runGroup semantics. Decode/protocol/native errors
	// are failures, not recovered branch panics. Async listener panics are outside
	// this goroutine and keep Go's existing process-level semantics.
	defer func() {
		if recovered := recover(); recovered != nil {
			if b == nil {
				err = fmt.Errorf("response effect panic: %v", recovered)
				return
			}
			d.effects.executor.log.Error("panic in handleGenerator", "error", recovered, "stack", string(debug.Stack()))
			observation = "BranchPanicked"
			err = nil
		}
	}()
	switch kind {
	case "Clock":
		var site string
		if err := json.Unmarshal(payload, &site); err != nil {
			return nil, err
		}
		now := d.effects.now()
		// Keep the actual bound Go time (location/monotonic data included). Rust
		// receives nanoseconds to decide recipes; effects do not read the clock again.
		if !now.Equal(time.Unix(0, now.UnixNano())) {
			return nil, errors.New("experimental clock outside signed nanosecond bounds")
		}
		switch site {
		case "Handling":
			d.handledAt = now
		case "Lifecycle":
			b.lifecycleAt = now
		case "Recipe":
			b.recipeAt = now
		case "Callback":
			b.callbackAt = now
		default:
			return nil, errors.New("invalid native clock site")
		}
		return map[string]any{"Clock": now.UnixNano()}, nil
	case "Uuid":
		id := uuid.New()
		var site string
		if err := json.Unmarshal(payload, &site); err != nil {
			return nil, err
		}
		if site == "Discovery" || site == "Planned" {
			b.ctx = state.WithGroupID(b.ctx, id.String())
		}
		n := new(big.Int).SetBytes(id[:])
		return map[string]any{"Uuid": json.RawMessage(n.String())}, nil
	case "UpdateMetadata", "ResetMetadata":
		var c struct {
			RequestVersion int             `json:"request_version"`
			ForceStepPlan  bool            `json:"force_step_plan"`
			StartedAt      json.RawMessage `json:"started_at"`
			HasAI          bool            `json:"has_ai"`
		}
		if err := json.Unmarshal(payload, &c); err != nil {
			return nil, err
		}
		effectCtx := d.ctx
		if b != nil {
			effectCtx = b.ctx
		}
		if string(c.StartedAt) != string(timeNative(d.original.md.Config.StartedAt)) {
			return nil, errors.New("native metadata changed StartedAt")
		}
		fault := d.bindError(d.effects.updateMetadata(effectCtx, d.original.md.ID, sv2.MutableConfig{RequestVersion: c.RequestVersion, ForceStepPlan: c.ForceStepPlan, StartedAt: d.original.md.Config.StartedAt, HasAI: c.HasAI}))
		return map[string]any{"Ack": fault}, nil
	case "SavePending":
		var ids []string
		if err := json.Unmarshal(payload, &ids); err != nil {
			return nil, err
		}
		return map[string]any{"Ack": d.bindError(d.effects.savePending(d.ctx, d.original.md.ID, ids))}, nil
	case "ActivateBranch":
		var v struct {
			History json.RawMessage `json:"history_group"`
		}
		if err := json.Unmarshal(payload, &v); err != nil {
			return nil, err
		}
		k, p, err := nativeVariant(v.History)
		if err != nil {
			return nil, err
		}
		switch k {
		case "Existing":
			if err := json.Unmarshal(p, &b.instance.item.GroupID); err != nil {
				return nil, err
			}
		case "Generated":
			group, err := nativeGroup(p)
			if err != nil {
				return nil, err
			}
			b.instance.item.GroupID = group
		default:
			return nil, errors.New("invalid native history")
		}
		d.effects.activateBranch(b.ctx, b.step.gen)
		return "Done", nil
	case "PrepareSave", "PrepareRetrySpan":
		var v struct {
			Opcode struct {
				Tag int `json:"tag"`
			} `json:"opcode"`
		}
		if err := json.Unmarshal(payload, &v); err != nil {
			return nil, err
		}
		// Only the tag may change (StepError -> StepFailed). Timing, Userland,
		// metadata updates, error identity and every original option remain Go-owned.
		b.step.gen.Op = enums.Opcode(v.Opcode.Tag)
		if kind == "PrepareRetrySpan" {
			d.effects.prepareRetrySpan(b.ctx, b.step)
			return "Done", nil
		}
		var prepErr error
		if b.step.gen.Op == enums.OpcodeStepFailed {
			b.ctx, b.save, prepErr = d.effects.prepareFailedStep(b.ctx, b.step)
		} else {
			b.ctx, b.save, prepErr = d.effects.prepareStep(b.ctx, b.step)
		}
		if prepErr != nil {
			return map[string]any{"Prepared": map[string]any{"Err": d.bindError(prepErr)}}, nil
		}
		return map[string]any{"Prepared": map[string]any{"Ok": continuationBytes(b.save.output)}}, nil
	case "SaveStep":
		var v struct {
			ID     string `json:"id"`
			Output []byte `json:"output"`
		}
		if err := json.Unmarshal(payload, &v); err != nil {
			return nil, err
		}
		request := responseSaveRequest{runID: b.save.runID, stepID: v.ID, output: v.Output}
		result := d.effects.saveStep(b.ctx, request)
		return map[string]any{"Saved": map[string]any{"has_pending": result.hasPending, "error": d.bindError(result.err)}}, nil
	case "LogExistingOutput":
		var fault native.Fault
		if err := json.Unmarshal(payload, &fault); err != nil {
			return nil, err
		}
		d.effects.logExistingStepOutput(b.ctx, b.step, d.originalError(fault.Handle))
		return "Done", nil
	case "Lifecycle":
		var v struct {
			Status  string `json:"status"`
			Attempt int    `json:"attempt"`
			At      int64  `json:"at"`
		}
		if err := json.Unmarshal(payload, &v); err != nil {
			return nil, err
		}
		status := enums.StepStatusCompleted
		switch v.Status {
		case "Finished":
		case "Failed":
			status = enums.StepStatusFailed
		case "Errored":
			status = enums.StepStatusErrored
		default:
			return nil, errors.New("invalid native lifecycle status")
		}
		if v.At != b.lifecycleAt.UnixNano() {
			return nil, errors.New("unbound native lifecycle time")
		}
		d.effects.lifecycleAt(b.ctx, b.step, status, v.Attempt, b.lifecycleAt)
		return "Done", nil
	case "ReleaseCapacity":
		d.effects.releaseCapacity(b.ctx, b.step)
		return "Done", nil
	case "PrepareDiscovery":
		var recipe nativeRecipe
		if err := json.Unmarshal(payload, &recipe); err != nil {
			return nil, err
		}
		if err := d.materialize(b, recipe); err != nil {
			return nil, err
		}
		d.effects.prepareDiscovery(b.queue, b.step, recipe.Kind == "EdgeError")
		return "Done", nil
	case "PreparePlanned":
		var v struct {
			Recipe    nativeRecipe `json:"recipe"`
			HandledAt int64        `json:"handled_at"`
		}
		if err := json.Unmarshal(payload, &v); err != nil {
			return nil, err
		}
		if err := d.materialize(b, v.Recipe); err != nil {
			return nil, err
		}
		d.effects.preparePlannedDiscovery(b.queue, b.step)
		if v.HandledAt != d.handledAt.UnixNano() {
			return nil, errors.New("unbound native handling time")
		}
		d.effects.preparePlannedSpan(b.queue, b.step, d.handledAt)
		return "Done", nil
	case "Enqueue":
		return map[string]any{"Ack": d.bindError(d.effects.enqueue(b.queue))}, nil
	case "FinishSpan":
		var v struct {
			Send bool `json:"send"`
		}
		if err := json.Unmarshal(payload, &v); err != nil {
			return nil, err
		}
		d.effects.finishSpan(b.queue, v.Send)
		b.spanFinished = true
		return "Done", nil
	case "AsyncScheduled", "SyncScheduled":
		var v struct {
			Item json.RawMessage `json:"item"`
			Name *string         `json:"name"`
			At   int64           `json:"at"`
		}
		if err := json.Unmarshal(payload, &v); err != nil {
			return nil, err
		}
		k, p, err := nativeVariant(v.Item)
		if err != nil {
			return nil, err
		}
		if b.queue == nil {
			switch k {
			case "Recipe":
				var recipe nativeRecipe
				if err := json.Unmarshal(p, &recipe); err != nil {
					return nil, err
				}
				if err := d.materialize(b, recipe); err != nil {
					return nil, err
				}
			case "Original":
				var original struct {
					Attempt int `json:"attempt"`
				}
				if err := json.Unmarshal(p, &original); err != nil {
					return nil, err
				}
				b.instance.item.Attempt = original.Attempt
				b.queue = &responseQueueBinding{ctx: b.ctx, item: b.instance.LifecycleItem()}
			default:
				return nil, errors.New("invalid native scheduled item")
			}
		}
		if kind == "AsyncScheduled" {
			for _, listener := range d.effects.executor.lifecycles {
				d.callbacks.Add(1)
				d.effects.launchScheduled(b.queue, b.step, v.Name, &nativeTrackedListener{LifecycleListener: listener, done: d.callbacks.Done})
			}
		} else {
			if v.At != b.callbackAt.UnixNano() {
				return nil, errors.New("unbound native callback time")
			}
			d.effects.syncScheduled(b.queue, b.step, v.Name, b.callbackAt)
		}
		return "Done", nil
	default:
		return nil, fmt.Errorf("unsupported native effect %q", kind)
	}
}

// Callback accounting is owned solely by this isolated test harness. Production
// listener launch needs no test drain API or service lifecycle change.
type nativeTrackedListener struct {
	execution.LifecycleListener
	done func()
}

func (l *nativeTrackedListener) OnStepScheduled(ctx context.Context, md sv2.Metadata, item queue.Item, name *string) {
	defer l.done()
	l.LifecycleListener.OnStepScheduled(ctx, md, item, name)
}
