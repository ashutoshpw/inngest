//go:build linux && response_continuation_spike && cgo

package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"os/exec"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
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
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/meta"
	"github.com/inngest/inngest/pkg/tracing/metadata"
	"github.com/inngest/inngest/pkg/util/interval"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

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

// This reuses every literal and assertion at the original Go owner boundary.
// Only the decision owner changes; no fixture's Prepared field is consumed.
func TestResponseContinuationIntegratedContract(t *testing.T) {
	runResponseContinuationContract(t, runNativeResponse)
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

// Whole-batch rejection is observed before metadata, history or any real port.
func TestResponseContinuationIntegratedRejectsWholeBatch(t *testing.T) {
	for _, excluded := range []*state.GeneratorOpcode{nil, {Op: enums.OpcodeSleep, ID: "sleep", Name: "1s"}, {Op: enums.OpcodeStepError, ID: "invalid"}, {Op: enums.Opcode(255)}} {
		e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "eligible"}, excluded)
		i.md.Config.RequestVersion = -1
		require.Error(t, runNativeResponse(context.Background(), e, i, i.resp))
		require.Empty(t, events.snapshot())
		require.Empty(t, s.writes)
		require.Empty(t, q.calls)
	}
}

func TestResponseContinuationIntegratedSiblingProgressAndJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "a"}, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "b"})
		entered := make(chan string, 2)
		release := make(chan struct{})
		defer close(release)
		s.beforeSave = func(id string) {
			entered <- id
			if id == "b" {
				<-release
			}
		}
		original := errors.New("b save rejected")
		s.outcomes["b"] = stagedSaveOutcome{err: original}
		result := make(chan error, 1)
		go func() { result <- runNativeResponse(context.Background(), e, i, i.resp) }()
		require.ElementsMatch(t, []string{"a", "b"}, []string{<-entered, <-entered})
		synctest.Wait()
		require.Len(t, q.calls, 1, "a reaches enqueue while b's SaveStep is outstanding")
		require.Contains(t, events.snapshot(), "scheduled:edge:<nil>:0")
		select {
		case err := <-result:
			t.Fatalf("returned before sibling join: %v", err)
		default:
		}
		release <- struct{}{}
		require.Same(t, original, <-result)
	})
}

type nativeSavePort struct {
	sv2.RunService
	save func(context.Context, sv2.ID, string, []byte) (bool, error)
}

func (s *nativeSavePort) SaveStep(ctx context.Context, id sv2.ID, step string, output []byte) (bool, error) {
	return s.save(ctx, id, step, output)
}

// Explicit close must cancel the actual effect context, join a port that is
// still returning, and then close native state. No callbacks/next effects run.
func TestResponseContinuationIntegratedCloseOutstandingSave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"})
		entered := make(chan struct{})
		cancelSeen := make(chan struct{})
		release := make(chan struct{})
		defer close(release)
		calls := atomic.Int32{}
		e.smv2 = &nativeSavePort{RunService: s, save: func(ctx context.Context, _ sv2.ID, _ string, _ []byte) (bool, error) {
			calls.Add(1)
			close(entered)
			<-ctx.Done()
			close(cancelSeen)
			<-release
			return false, ctx.Err()
		}}
		d, err := newContinuationDriver(context.Background(), e, i, i.resp)
		require.NoError(t, err)
		result := make(chan error, 1)
		go func() { result <- d.run() }()
		<-entered
		closed := make(chan struct{})
		go func() { d.Close(); close(closed) }()
		<-cancelSeen
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("close released an outstanding Go effect")
		default:
		}
		release <- struct{}{}
		<-closed
		require.ErrorIs(t, <-result, context.Canceled)
		require.Equal(t, int32(1), calls.Load())
		require.Empty(t, q.calls)
		require.Empty(t, events.snapshot())
		_, err = d.session.Resume(native.Key{}, "Done")
		require.ErrorContains(t, err, "closed")
	})
}

type nativeBlockedListener struct {
	execution.LifecycleListener
	entered   chan struct{}
	release   chan struct{}
	cancelled chan struct{}
	value     any
	item      queue.Item
}

func (l *nativeBlockedListener) OnStepScheduled(ctx context.Context, _ sv2.Metadata, item queue.Item, _ *string) {
	l.item = item
	l.value = ctx.Value(nativeContextKey{})
	close(l.entered)
	<-ctx.Done()
	close(l.cancelled)
	<-l.release
}

type nativeContextKey struct{}

type nativePanicSync struct {
	execution.NoopSyncLifecycleListener
}

func (*nativePanicSync) OnStepScheduled(context.Context, sv2.Metadata, queue.Item, *string, time.Time) {
	panic("sync listener isolated panic")
}

func TestResponseContinuationIntegratedCloseDrainsCallbacksSeparately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, i, _, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepPlanned, ID: "p", Name: "planned"})
		e.tracerProvider = tracing.NewOtelTracerProvider(&continuationExporter{q: q, events: events}, 0)
		listener := &nativeBlockedListener{entered: make(chan struct{}), release: make(chan struct{}), cancelled: make(chan struct{})}
		defer close(listener.release)
		e.lifecycles = []execution.LifecycleListener{listener}
		e.syncLifecycles = append([]execution.SyncLifecycleListener{&nativePanicSync{}}, e.syncLifecycles...)
		value := &struct{ Label string }{"original-context"}
		d, err := newContinuationDriver(context.WithValue(context.Background(), nativeContextKey{}, value), e, i, i.resp)
		require.NoError(t, err)
		result := make(chan error, 1)
		go func() { result <- d.run() }()
		<-listener.entered
		// Response completion acknowledges launch, not delivery. Explicit close
		// cancels the actual listener context and separately joins the callback.
		require.NoError(t, <-result)
		require.Contains(t, events.snapshot(), "scheduled:edge:planned:0", "sync panic does not suppress later listeners")
		closed := make(chan struct{})
		go func() { _ = d.Close(); close(closed) }()
		<-listener.cancelled
		synctest.Wait()
		select {
		case <-closed:
			t.Fatal("closed before async listener drain")
		default:
		}
		require.Same(t, value, listener.value)
		require.Equal(t, q.calls[0].item.GroupID, listener.item.GroupID)
		require.NotEmpty(t, q.calls[0].item.Metadata[meta.PropagationKey])
		require.Equal(t, q.calls[0].item.Metadata, listener.item.Metadata)
		listener.release <- struct{}{}
		<-closed
	})
}

func TestResponseContinuationIntegratedRecoveredPanicJoinsSibling(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, i, s, q, _ := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "panics"}, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "sibling"})
		entered := make(chan struct{})
		release := make(chan struct{})
		defer close(release)
		e.smv2 = &nativeSavePort{RunService: s, save: func(ctx context.Context, id sv2.ID, step string, output []byte) (bool, error) {
			if step == "panics" {
				panic("recovered Go effect panic")
			}
			close(entered)
			<-release
			return s.SaveStep(ctx, id, step, output)
		}}
		result := make(chan error, 1)
		go func() { result <- runNativeResponse(context.Background(), e, i, i.resp) }()
		<-entered
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("panic skipped sibling join: %v", err)
		default:
		}
		release <- struct{}{}
		require.NoError(t, <-result)
		require.Equal(t, map[string]string{"sibling": `{"data":null}`}, s.writes)
		require.Len(t, q.calls, 1)
	})
}

// A protocol failure after the real store committed poisons this session. The
// same session cannot retry/resume/fallback and cannot issue lifecycle/queue work.
func TestResponseContinuationIntegratedPoisonAfterCommittedSave(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"}, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "blocked"})
		original := errors.New("commit acknowledged with error")
		entered := make(chan string, 2)
		releaseSave := make(chan struct{})
		releaseSibling := make(chan struct{})
		cancelSeen := make(chan struct{})
		defer close(releaseSave)
		defer close(releaseSibling)
		calls := atomic.Int32{}
		e.smv2 = &nativeSavePort{RunService: s, save: func(ctx context.Context, id sv2.ID, step string, out []byte) (bool, error) {
			calls.Add(1)
			entered <- step
			if step == "blocked" {
				<-ctx.Done()
				close(cancelSeen)
				<-releaseSibling
				return false, ctx.Err()
			}
			<-releaseSave
			pending, err := s.SaveStep(ctx, id, step, out)
			if err != nil {
				return pending, err
			}
			return pending, original
		}}
		d, err := newContinuationDriver(context.Background(), e, i, i.resp)
		require.NoError(t, err)
		resume := d.resume
		d.resume = func(key native.Key, observation any) (native.Progress, error) {
			if value, ok := observation.(map[string]any); ok && value["Saved"] != nil {
				key.Generation++
			}
			return resume(key, observation)
		}
		result := make(chan error, 1)
		go func() { result <- d.run() }()
		require.ElementsMatch(t, []string{"s", "blocked"}, []string{<-entered, <-entered})
		releaseSave <- struct{}{}
		<-cancelSeen
		synctest.Wait()
		select {
		case err := <-result:
			t.Fatalf("poison did not join outstanding sibling: %v", err)
		default:
		}
		releaseSibling <- struct{}{}
		require.ErrorContains(t, <-result, "poisoned")
		require.NoError(t, d.drain())
		require.Equal(t, int32(2), calls.Load())
		require.Equal(t, map[string]string{"s": `{"data":null}`}, s.writes)
		require.Empty(t, q.calls)
		require.Equal(t, []string{"metadata_update", "handler:save:s"}, events.snapshot())
		require.Same(t, original, d.originalError(3))
		_, err = d.session.Resume(native.Key{}, "Done")
		require.Error(t, err)
		require.Equal(t, int32(2), calls.Load())
	})
}

func TestResponseContinuationIntegratedRetainsFullDescriptor(t *testing.T) {
	g := &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s", Name: "sdk-name", Metadata: []metadata.ScopedUpdate{stepMetadataUpdate("userland.custom", metadata.Values{"k": json.RawMessage(`"v"`)})}}
	g.Timing = interval.Interval{A: 1793016000000000000, B: 42_000_000}
	g.Userland = &struct {
		ID    string `json:"id"`
		Index int    `json:"index,omitempty"`
	}{ID: "user-id", Index: 9}
	e, i, s, q, _ := newStagedBatch(t, g)
	i.md.Config.StartedAt = time.Unix(0, 0).UTC()
	i.md.Config.RequestVersion = -1
	boundTime := time.Date(2026, 10, 26, 12, 0, 0, 0, time.FixedZone("bound-clock", 0))
	e.clock = clockwork.NewFakeClockAt(boundTime)
	times := &nativeTimeListener{}
	e.syncLifecycles = append(e.syncLifecycles, times)
	i.md.Metrics.MetadataSize = 11
	i.md.Metrics.MetadataSizeLoaded = 7
	tracer := newRecordingTracerProvider()
	e.tracerProvider = tracer
	require.NoError(t, runNativeResponse(context.Background(), e, i, i.resp))
	require.Equal(t, map[string]int{"s": 8}, s.saveDeltas)
	require.Equal(t, boundTime, q.calls[0].at)
	require.Equal(t, boundTime, times.lifecycle)
	require.Equal(t, boundTime, times.scheduled)
	_, updates, _, _ := s.snapshot()
	require.Equal(t, i.md.Config.StartedAt, updates[0].StartedAt)
	found := false
	for _, call := range tracer.createCalls {
		if call.name == meta.SpanNameStep {
			found = true
			started, ok := meta.GetAttr(call.opts.Attributes, meta.Attrs.StartedAt)
			require.True(t, ok)
			require.True(t, g.Timing.Start().Equal(*started))
			id, ok := meta.GetAttr(call.opts.Attributes, meta.Attrs.StepUserlandID)
			require.True(t, ok)
			require.Equal(t, "user-id", *id)
		}
	}
	require.True(t, found)
}

// Activation is a real metric/span effect. None still counts, and permanent
// StepError conversion must leave the incoming execution span in Error state.
func TestResponseContinuationIntegratedActivation(t *testing.T) {
	// The metrics registry caches instruments globally. Isolate its real provider
	// lifecycle so this contract also works after other tests or with -count.
	const marker = "RESPONSE_NATIVE_ACTIVATION_CHILD"
	if os.Getenv(marker) != "1" {
		cmd := exec.Command(os.Args[0], "-test.run=^TestResponseContinuationIntegratedActivation$", "-test.timeout=20s")
		cmd.Env = append(os.Environ(), marker+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, string(output))
		return
	}
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	previous := otel.GetMeterProvider()
	otel.SetMeterProvider(provider)
	defer otel.SetMeterProvider(previous)
	defer provider.Shutdown(context.Background())
	var before metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &before))
	e, i, _, _, _ := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeNone, ID: "none"}, &state.GeneratorOpcode{Op: enums.OpcodeStepError, ID: "failed", Error: &state.UserError{Name: "Permanent", NoRetry: true}})
	traceProvider := sdktrace.NewTracerProvider()
	defer traceProvider.Shutdown(context.Background())
	ctx, incoming := traceProvider.Tracer("activation-contract").Start(context.Background(), "incoming")
	require.NoError(t, runNativeResponse(ctx, e, i, i.resp))
	incoming.End()
	snapshot := incoming.(sdktrace.ReadOnlySpan)
	require.Equal(t, codes.Error, snapshot.Status().Code)
	require.Equal(t, "Permanent", snapshot.Status().Description)
	var after metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &after))
	counts := func(metrics metricdata.ResourceMetrics) map[string]int64 {
		result := map[string]int64{}
		for _, scope := range metrics.ScopeMetrics {
			for _, metric := range scope.Metrics {
				if metric.Name == "inngest_executor_handle_generator_total" {
					sum, ok := metric.Data.(metricdata.Sum[int64])
					require.True(t, ok)
					for _, point := range sum.DataPoints {
						op, ok := point.Attributes.Value(attribute.Key("op"))
						require.True(t, ok)
						result[op.AsString()] += point.Value
					}
				}
			}
		}
		return result
	}
	a, b := counts(after), counts(before)
	require.Equal(t, int64(1), a["None"]-b["None"])
	require.Equal(t, int64(1), a["StepError"]-b["StepError"])
	require.Equal(t, int64(0), a["StepFailed"]-b["StepFailed"], "activation precedes local conversion")
}

// A process boundary is required to observe Go's unrecovered async panic. A
// sync listener panic above remains recovered and later listeners still run.
func TestResponseContinuationIntegratedAsyncPanicSemantics(t *testing.T) {
	const marker = "RESPONSE_NATIVE_ASYNC_PANIC_CHILD"
	if os.Getenv(marker) == "1" {
		e, i, _, _, _ := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepPlanned, ID: "p"})
		e.lifecycles = []execution.LifecycleListener{&nativePanicAsync{}}
		_ = runNativeResponse(context.Background(), e, i, i.resp)
		t.Fatal("async listener panic was suppressed")
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestResponseContinuationIntegratedAsyncPanicSemantics$", "-test.timeout=20s")
	cmd.Env = append(os.Environ(), marker+"=1")
	output, err := cmd.CombinedOutput()
	require.Error(t, err)
	require.Contains(t, string(output), "panic: async-listener-must-remain-unrecovered")
}

type nativePanicAsync struct{ execution.LifecycleListener }

func (*nativePanicAsync) OnStepScheduled(context.Context, sv2.Metadata, queue.Item, *string) {
	panic("async-listener-must-remain-unrecovered")
}

type nativeTimeListener struct {
	execution.NoopSyncLifecycleListener
	lifecycle, scheduled time.Time
}

func (l *nativeTimeListener) OnStepRunFinished(_ context.Context, _ sv2.Metadata, _ queue.Item, _ inngest.Edge, _ state.GeneratorOpcode, at time.Time) {
	l.lifecycle = at
}
func (l *nativeTimeListener) OnStepScheduled(_ context.Context, _ sv2.Metadata, _ queue.Item, _ *string, at time.Time) {
	l.scheduled = at
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

// The shared literals cannot observe runInstance's private capacity callback.
// Reuse the existing RunContext observation wrapper to guard the native release
// request without exporting or injecting anything into the production queue.
func TestResponseContinuationIntegratedReleasePort(t *testing.T) {
	for _, op := range []enums.Opcode{enums.OpcodeStepRun, enums.OpcodeStepFailed} {
		for _, saveErr := range []error{nil, context.Canceled} {
			t.Run(fmt.Sprintf("%s/rejected=%t", op, saveErr != nil), func(t *testing.T) {
				e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{Op: op, ID: "s", Error: &state.UserError{Name: "E", Message: "x"}})
				s.outcomes["s"] = stagedSaveOutcome{err: saveErr}
				d, err := newContinuationDriver(context.Background(), e, i, i.resp)
				require.NoError(t, err)
				for index := range d.branches {
					b := &d.branches[index]
					b.step.runCtx = &stagedReleaseContext{runInstance: &b.instance, events: events}
				}
				result := d.run()
				require.NoError(t, d.drain())
				if saveErr != nil {
					require.Same(t, saveErr, result)
					require.Equal(t, []string{"handler:save:s"}, events.snapshot())
					require.Empty(t, q.calls)
				} else {
					require.NoError(t, result)
					observed := events.snapshot()
					require.Len(t, observed, 5)
					require.Equal(t, "handler:save:s", observed[0])
					require.Equal(t, "release", observed[2])
					require.Equal(t, "handler:enqueue:s", observed[3])
				}
			})
		}
	}
}

func TestResponseContinuationIntegratedCloseDispatchAdmission(t *testing.T) {
	for _, shutdown := range []string{"Close", "ExternalContextCancellation"} {
		t.Run(shutdown, func(t *testing.T) {
			e, i, s, q, events := newStagedBatch(t, &state.GeneratorOpcode{
				Op: enums.OpcodeStepError, ID: "s", Error: &state.UserError{Name: "Permanent", NoRetry: true},
			})
			traceProvider := sdktrace.NewTracerProvider()
			defer traceProvider.Shutdown(context.Background())
			parentCtx, cancelParent := context.WithCancel(context.Background())
			defer cancelParent()
			ctx, incoming := traceProvider.Tracer("close-dispatch-admission").Start(parentCtx, "incoming")
			d, err := newContinuationDriver(ctx, e, i, i.resp)
			require.NoError(t, err)

			blocked := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseDispatch := func() { releaseOnce.Do(func() { close(release) }) }
			defer func() {
				releaseDispatch()
				_ = d.Close()
			}()
			d.beforeAdmission = func(request native.Request) {
				kind, _, err := nativeVariant(request.Effect)
				if err == nil && kind == "ActivateBranch" {
					close(blocked)
					<-release
				}
			}

			runResult := make(chan error, 1)
			go func() { runResult <- d.run() }()
			select {
			case <-blocked:
			case <-time.After(10 * time.Second):
				t.Fatal("runner did not reach the activation admission window")
			}

			var closeResult chan error
			if shutdown == "Close" {
				closeResult = make(chan error, 1)
				go func() { closeResult <- d.Close() }()
				select {
				case <-d.ctx.Done():
				case <-time.After(10 * time.Second):
					t.Fatal("close did not cancel the driver while dispatch was paused")
				}
			} else {
				cancelParent()
				select {
				case <-d.watcherDone:
				case <-time.After(10 * time.Second):
					t.Fatal("external cancellation watcher did not close dispatch admission")
				}
			}
			releaseDispatch()

			require.ErrorIs(t, <-runResult, context.Canceled)
			if closeResult != nil {
				require.NoError(t, <-closeResult)
			} else {
				require.NoError(t, d.drain())
			}
			select {
			case <-d.watcherDone:
			default:
				t.Fatal("driver drain returned before joining the context watcher")
			}
			incoming.End()
			require.Equal(t, codes.Unset, incoming.(sdktrace.ReadOnlySpan).Status().Code,
				"a ready activation paused before admission must not run after shutdown")
			require.Empty(t, s.writes)
			require.Empty(t, q.calls)
			require.Empty(t, events.snapshot())
		})
	}
}

func TestResponseContinuationIntegratedCloseBeforeDispatch(t *testing.T) {
	e, i, _, _, events := newStagedBatch(t, &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"})
	d, err := newContinuationDriver(context.Background(), e, i, i.resp)
	require.NoError(t, err)
	require.NoError(t, d.Close())
	require.NoError(t, d.Close())
	require.ErrorContains(t, d.run(), "already started or closed")
	require.Empty(t, events.snapshot())
}
