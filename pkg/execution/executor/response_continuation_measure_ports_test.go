//go:build linux && response_continuation_spike && cgo

package executor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/constraintapi"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/meta"
	"github.com/inngest/inngest/pkg/tracing/metadata"
	"github.com/inngest/inngest/pkg/util/interval"
	"github.com/jonboulle/clockwork"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

var measureCommitError = errors.New("measurement enqueue committed then returned original error")
var measureWorkloads = []string{"run64", "mixed8", "run64k", "retryable", "enqueue-error"}

// No span retention, queues, external exporter or telemetry goroutines. The
// real tracing provider still constructs spans, metadata and propagation maps.
type measureTraceSink struct {
	spans atomic.Uint64
	work  atomic.Uint64
}

func (s *measureTraceSink) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	for _, span := range spans {
		s.spans.Add(1)
		s.work.Add(uint64(len(span.Name()) + len(span.Attributes())))
	}
	return nil
}
func (*measureTraceSink) Shutdown(context.Context) error { return nil }

// SyncScheduled is invoked once after every async launch in both real owners.
// Counting that event before sealing at owner return closes the zero-count race
// even if a callback has not started yet. No production launch hook is needed.
type measureCallbacks struct {
	mu                  sync.Mutex
	expected, completed int
	sealed              bool
	changed             chan struct{}
	work                atomic.Uint64
	invalid             atomic.Bool
}

func newMeasureCallbacks() *measureCallbacks {
	return &measureCallbacks{changed: make(chan struct{}, 1)}
}
func (c *measureCallbacks) notify() {
	select {
	case c.changed <- struct{}{}:
	default:
	}
}
func (c *measureCallbacks) launched() {
	c.mu.Lock()
	if c.sealed {
		c.invalid.Store(true)
	}
	c.expected++
	c.mu.Unlock()
	c.notify()
}
func (c *measureCallbacks) complete() { c.mu.Lock(); c.completed++; c.mu.Unlock(); c.notify() }
func (c *measureCallbacks) wait(ctx context.Context) error {
	c.mu.Lock()
	c.sealed = true
	c.mu.Unlock()
	for {
		c.mu.Lock()
		expected, completed := c.expected, c.completed
		c.mu.Unlock()
		if completed > expected {
			return errors.New("callback count exceeded launches")
		}
		if completed == expected {
			return nil
		}
		select {
		case <-c.changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
func (c *measureCallbacks) consume(ctx context.Context, md sv2.Metadata, item queue.Item) {
	// Bounded identical work for both listeners; consume actual carrier and input.
	n := uint64(len(item.GroupID) + len(md.ID.RunID.String()) + len(item.Metadata))
	if ctx.Value(measureContextKey{}) != "response-owner" {
		c.invalid.Store(true)
	}
	if carrier, ok := item.Metadata[meta.PropagationKey]; ok {
		bytes, err := json.Marshal(carrier)
		if err != nil {
			c.invalid.Store(true)
		}
		h := sha256.Sum256(bytes)
		n += uint64(h[0])
	}
	c.work.Add(n)
}

type measureAsyncListener struct {
	execution.NoopLifecyceListener
	c *measureCallbacks
}

func (l *measureAsyncListener) OnStepScheduled(ctx context.Context, md sv2.Metadata, item queue.Item, _ *string) {
	defer l.c.complete()
	l.c.consume(ctx, md, item)
}

type measureSyncListener struct {
	execution.NoopSyncLifecycleListener
	c *measureCallbacks
}

func (l *measureSyncListener) OnStepScheduled(ctx context.Context, md sv2.Metadata, item queue.Item, _ *string, _ time.Time) {
	l.c.launched()
	l.c.consume(ctx, md, item)
}
func (l *measureSyncListener) OnStepRunFinished(ctx context.Context, md sv2.Metadata, item queue.Item, _ inngest.Edge, _ state.GeneratorOpcode, _ time.Time) {
	l.c.consume(ctx, md, item)
}
func (l *measureSyncListener) OnStepRunFailed(ctx context.Context, md sv2.Metadata, item queue.Item, _ inngest.Edge, _ state.GeneratorOpcode, _ enums.StepStatus, _ int, _ time.Time) {
	l.c.consume(ctx, md, item)
}

// Controlled ports substitute ONLY state and queue I/O. Full input bytes are
// consumed without retaining growing payloads. Pending is an authoritative set,
// not a guessed handler branch or a fixture's Prepared answer.
type measureControlledState struct {
	sv2.RunService
	mu      sync.Mutex
	pending map[string]bool
	saved   map[string][32]byte
	delta   int
	updates int
	config  sv2.MutableConfig
}

func (s *measureControlledState) UpdateMetadata(_ context.Context, _ sv2.ID, config sv2.MutableConfig) error {
	bytes, err := json.Marshal(config)
	if err != nil {
		return err
	}
	_ = sha256.Sum256(bytes)
	s.mu.Lock()
	s.updates++
	s.config = config
	s.mu.Unlock()
	return nil
}
func (s *measureControlledState) SavePending(_ context.Context, _ sv2.ID, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clear(s.pending)
	for _, id := range ids {
		s.pending[id] = true
	}
	return nil
}
func (s *measureControlledState) SaveStep(ctx context.Context, _ sv2.ID, id string, bytes []byte) (bool, error) {
	hash := sha256.Sum256(bytes)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.saved[id]; ok {
		return len(s.pending) > 0, map[bool]error{true: state.ErrIdempotentResponse, false: state.ErrDuplicateResponse}[old == hash]
	}
	s.saved[id] = hash
	delete(s.pending, id)
	s.delta += state.MetadataSizeDeltaFromContext(ctx)
	return len(s.pending) > 0, nil
}

type measureControlledQueue struct {
	queue.Queue
	count atomic.Int64
	work  atomic.Uint64
	after error
}

func (q *measureControlledQueue) Enqueue(ctx context.Context, item queue.Item, _ time.Time, _ queue.EnqueueOpts) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	bytes, err := json.Marshal(item)
	if err != nil {
		return err
	}
	hash := sha256.Sum256(bytes)
	q.work.Add(uint64(hash[0]))
	q.count.Add(1)
	return q.after
}

type measureCommitQueue struct{ queue.Queue }

func (q *measureCommitQueue) Enqueue(ctx context.Context, item queue.Item, at time.Time, opts queue.EnqueueOpts) error {
	if err := q.Queue.Enqueue(ctx, item, at, opts); err != nil {
		return err
	}
	return measureCommitError
}

type measurePorts struct {
	profile            string
	keyQueues          bool
	service, reader    sv2.RunService
	producer           queue.Queue
	client, readClient rueidis.Client
	itemsKey           string
	sink               *measureTraceSink
	incoming           *sdktrace.TracerProvider
	precreated         bool // row setup creates every real backend run before execution
}

func newMeasurePorts(ctx context.Context, profile string, keyQueues bool, addr string) (*measurePorts, error) {
	p := &measurePorts{profile: profile, keyQueues: keyQueues, sink: &measureTraceSink{}, incoming: sdktrace.NewTracerProvider(sdktrace.WithSampler(sdktrace.AlwaysSample()))}
	if profile == "controlled" {
		return p, nil
	}
	if profile != "real-valkey" {
		return nil, fmt.Errorf("unknown profile %q", profile)
	}
	if addr == "" {
		return nil, errors.New("real-valkey requires RESPONSE_MEASURE_VALKEY_ADDR; no fallback")
	}
	var err error
	p.client, err = rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		return nil, err
	}
	p.readClient, err = rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{addr}, DisableCache: true})
	if err != nil {
		p.client.Close()
		return nil, err
	}
	fail := func(err error) (*measurePorts, error) { p.Close(); return nil, err }
	if err = p.client.Do(ctx, p.client.B().Ping().Build()).Error(); err != nil {
		return fail(err)
	}
	prefix := "response-measure-" + uuid.NewString()
	p.service, err = measureStatePort(ctx, p.client, prefix+":state", prefix+":queue")
	if err != nil {
		return fail(err)
	}
	p.reader, err = measureStatePort(ctx, p.readClient, prefix+":state", prefix+":queue")
	if err != nil {
		return fail(err)
	}
	opts := []queue.QueueOpt{queue.WithClock(clockwork.NewRealClock()), queue.WithRunMode(queue.QueueRunMode{Partition: true, ShadowPartition: keyQueues}), queue.WithAllowKeyQueues(func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) bool { return keyQueues })}
	qc := redis_state.NewQueueClient(p.client, prefix+":queue")
	shard := redis_state.NewQueueShard(consts.DefaultQueueShardName, qc, opts...)
	registry, err := queue.NewSingleShardRegistry(shard)
	if err != nil {
		return fail(err)
	}
	p.producer, err = queue.New(ctx, "response-owner-measure", registry, opts...)
	if err != nil {
		return fail(err)
	}
	p.itemsKey = qc.KeyGenerator().QueueItem()
	return p, nil
}
func measureStatePort(ctx context.Context, client rueidis.Client, stateKey, queueKey string) (sv2.RunService, error) {
	u := redis_state.NewUnshardedClient(client, stateKey, queueKey)
	sh := redis_state.NewShardedClient(redis_state.ShardedClientOpts{UnshardedClient: u, FunctionRunStateClient: client, BatchClient: client, StateDefaultKey: stateKey, QueueDefaultKey: queueKey, FnRunIsSharded: redis_state.AlwaysShardOnRun})
	manager, err := redis_state.New(ctx, redis_state.WithShardedClient(sh))
	if err != nil {
		return nil, err
	}
	return redis_state.MustRunServiceV2(manager), nil
}
func (p *measurePorts) Close() {
	if p.client != nil {
		p.client.Close()
	}
	if p.readClient != nil {
		p.readClient.Close()
	}
	_ = p.incoming.Shutdown(context.Background())
}

type measureContextKey struct{}
type measureInput struct {
	startOffset     *atomic.Int64
	cleanup         context.CancelFunc
	e               *executor
	i               *runInstance
	ctx             context.Context
	span            trace.Span
	callbacks       *measureCallbacks
	controlled      *measureControlledState
	controlledQueue *measureControlledQueue
}

func measureOps(workload string) ([]*state.GeneratorOpcode, error) {
	size := 64
	switch workload {
	case "run64", "mixed8", "retryable", "enqueue-error":
	case "run64k":
		size = 64 * 1024
	default:
		return nil, fmt.Errorf("unknown workload %q", workload)
	}
	// The actual StepRun output envelope is exactly size bytes of valid JSON.
	// Data is materialized by the real Go preparation helper in both lanes.
	data := json.RawMessage(`"` + strings.Repeat("x", size-11) + `"`)
	tags := []enums.Opcode{enums.OpcodeStepRun}
	if workload == "retryable" {
		tags = []enums.Opcode{enums.OpcodeStepError}
	}
	if workload == "mixed8" {
		tags = []enums.Opcode{enums.OpcodeNone, enums.OpcodeStep, enums.OpcodeStepRun, enums.OpcodeStepError, enums.OpcodeStepFailed, enums.OpcodeStepPlanned, enums.OpcodeStepRun, enums.OpcodeNone}
	}
	ops := make([]*state.GeneratorOpcode, len(tags))
	for j, tag := range tags {
		ops[j] = &state.GeneratorOpcode{Op: tag, ID: fmt.Sprintf("s%d", j), Name: "measured-step", Data: data, Opts: map[string]any{"parallelMode": "none"}, Timing: interval.Interval{A: time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC).UnixNano(), B: 42_000_000}, Metadata: []metadata.ScopedUpdate{{Scope: enums.MetadataScopeStep, Update: metadata.Update{RawUpdate: metadata.RawUpdate{Kind: "userland.custom", Op: enums.MetadataOpcodeMerge, Values: metadata.Values{"k": json.RawMessage(`"v"`)}}}}}}
		ops[j].Userland = &struct {
			ID    string `json:"id"`
			Index int    `json:"index,omitempty"`
		}{ID: "user-step", Index: j}
		if tag == enums.OpcodeStepError || tag == enums.OpcodeStepFailed {
			ops[j].Error = &state.UserError{Name: "MeasuredError", Message: "expected", NoRetry: workload != "retryable"}
		}
	}
	return ops, nil
}
func measureInstance(workload string, index int) *runInstance {
	max := 3
	priority := int64(7)
	edge := inngest.Edge{Incoming: "function", Outgoing: "previous"}
	// Same deterministic identities and input clock values in paired lanes; each
	// process uses independent storage namespaces and never reuses a run.
	runID := ulid.MustParse("01ARZ3NDEKTSV4RRFFQ69G5FAV")
	for j := 0; j < 4; j++ {
		runID[15-j] = byte(uint(index+1) >> (8 * j))
	}
	id := sv2.ID{RunID: runID, FunctionID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Tenant: sv2.Tenant{EnvID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), AccountID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), AppID: uuid.MustParse("00000000-0000-0000-0000-000000000004")}}
	config := *sv2.InitConfig(&sv2.Config{RequestVersion: -1, Idempotency: fmt.Sprintf("measure-%d", index), StartedAt: time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC), PriorityFactor: &priority, CustomConcurrencyKeys: []sv2.CustomConcurrency{{Key: "f:00000000-0000-0000-0000-000000000001:measure", Hash: "measure", Limit: 8}}, Semaphores: []constraintapi.Semaphore{{ID: "fn:00000000-0000-0000-0000-000000000001", Weight: 1, Release: constraintapi.SemaphoreReleaseAuto}}})
	if workload == "mixed8" {
		config.RequestVersion = 2
		config.ForceStepPlan = true
	}
	i := &runInstance{md: sv2.Metadata{ID: id, Config: config}, edge: edge, resp: &state.DriverResponse{RequestVersion: 2}, item: queue.Item{Payload: queue.PayloadEdge{Edge: edge}, Attempt: 1, MaxAttempts: &max, PriorityFactor: &priority, GroupID: "00000000-0000-0000-0000-000000000005"}}
	i.item.Identifier = sv2.V1FromMetadata(i.md)
	i.item.WorkspaceID = id.Tenant.EnvID
	i.item.CustomConcurrencyKeys = config.CustomConcurrencyKeys
	i.item.Semaphores = config.Semaphores
	i.md.Metrics.MetadataSize = 11
	i.md.Metrics.MetadataSizeLoaded = 7
	return i
}

// Real backend runs remain independent and precreated outside the closed loop.
// This constructs metadata only; it does not allocate response payloads or spans.
func (p *measurePorts) prepareRun(ctx context.Context, i *runInstance) error {
	config := i.md.Config
	if _, err := p.service.Create(ctx, sv2.CreateState{Metadata: i.md, Events: []json.RawMessage{json.RawMessage(`{"name":"measure","data":{}}`)}}); err != nil {
		return err
	}
	// Create initializes new-run storage rather than loading these existing-run
	// metrics/config. Establish the independently loaded snapshot explicitly.
	if err := p.service.UpdateMetadata(ctx, i.md.ID, sv2.MutableConfig{RequestVersion: config.RequestVersion, ForceStepPlan: config.ForceStepPlan, StartedAt: config.StartedAt, HasAI: config.HasAI}); err != nil {
		return err
	}
	if err := sv2.TryIncrementMetadataSize(ctx, p.service, i.md.ID, 7); err != nil {
		return err
	}
	stored, err := p.reader.LoadMetadata(ctx, i.md.ID)
	if err != nil {
		return err
	}
	if stored.Config.RequestVersion != config.RequestVersion || stored.Config.ForceStepPlan != config.ForceStepPlan || !stored.Config.StartedAt.Equal(config.StartedAt) || stored.Metrics.MetadataSize != 7 {
		return errors.New("precreated input readback mismatch")
	}
	return nil
}

func (p *measurePorts) input(ctx context.Context, workload string, index int) (*measureInput, error) {
	ops, err := measureOps(workload)
	if err != nil {
		return nil, err
	}
	c := newMeasureCallbacks()
	e := &executor{clock: clockwork.NewRealClock(), log: logger.VoidLogger(), tracerProvider: tracing.NewOtelTracerProvider(p.sink, 0), lifecycles: []execution.LifecycleListener{&measureAsyncListener{c: c}}, syncLifecycles: []execution.SyncLifecycleListener{&measureSyncListener{c: c}}}
	i := measureInstance(workload, index)
	i.resp.Generator = ops
	config := i.md.Config
	input := &measureInput{e: e, i: i, callbacks: c}
	if p.profile == "controlled" {
		s := &measureControlledState{pending: map[string]bool{}, saved: map[string][32]byte{}, config: sv2.MutableConfig{RequestVersion: config.RequestVersion, ForceStepPlan: config.ForceStepPlan, StartedAt: config.StartedAt}}
		q := &measureControlledQueue{}
		if workload == "enqueue-error" {
			q.after = measureCommitError
		}
		e.smv2 = s
		e.queue = q
		input.controlled = s
		input.controlledQueue = q
	} else {
		e.smv2 = p.service
		e.queue = p.producer
		if workload == "enqueue-error" {
			e.queue = &measureCommitQueue{Queue: p.producer}
		}
		if !p.precreated {
			if err := p.prepareRun(ctx, i); err != nil {
				return nil, err
			}
		}

	}
	// Input context/span setup is outside the response-owner boundary for both.
	input.ctx, input.span = p.incoming.Tracer("response-owner-measure").Start(context.WithValue(ctx, measureContextKey{}, "response-owner"), "incoming")
	return input, nil
}

// Independent literal expectations and persisted readback, outside owner timing.
// No Prepared fields, recorders or expected answers are used by a timed port.
func (p *measurePorts) validate(ctx context.Context, in *measureInput, workload string, result error) error {
	switch workload {
	case "retryable":
		if !errors.Is(result, ErrHandledStepError) {
			return fmt.Errorf("retry result: %w", result)
		}
	case "enqueue-error":
		if result == nil || !queue.IsAlwaysRetryable(result) || !strings.Contains(result.Error(), measureCommitError.Error()) {
			return fmt.Errorf("commit-error disposition: %v", result)
		}
	default:
		if result != nil {
			return result
		}
	}
	expected := map[string]string{}
	data := `"` + strings.Repeat("x", 53) + `"`
	switch workload {
	case "run64", "enqueue-error":
		expected["s0"] = `{"data":` + data + `}`
	case "run64k":
		expected["s0"] = `{"data":"` + strings.Repeat("x", 65525) + `"}`
	case "mixed8":
		expected["s1"] = data
		expected["s2"] = `{"data":` + data + `}`
		expected["s3"] = `{"error":{"name":"MeasuredError","message":"expected","noRetry":true}}`
		expected["s4"] = `{"error":{"name":"MeasuredError","message":"expected","noRetry":true}}`
		expected["s6"] = `{"data":` + data + `}`
	}
	wantCallbacks := 1
	wantQueue := 1
	if workload == "mixed8" {
		wantCallbacks = 6
		wantQueue = 1
	}
	if workload == "enqueue-error" {
		wantCallbacks = 0
	}
	if workload == "retryable" {
		wantQueue = 0
	}
	in.callbacks.mu.Lock()
	launched, completed := in.callbacks.expected, in.callbacks.completed
	in.callbacks.mu.Unlock()
	if launched != wantCallbacks || completed != wantCallbacks || in.callbacks.invalid.Load() {
		return fmt.Errorf("callback outcome launched=%d delivered=%d expected=%d invalid=%v", launched, completed, wantCallbacks, in.callbacks.invalid.Load())
	}
	if in.i.md.Config.RequestVersion != map[bool]int{true: 2, false: -1}[workload == "mixed8"] {
		return errors.New("loaded metadata snapshot changed")
	}
	if p.profile == "controlled" {
		s := in.controlled
		s.mu.Lock()
		defer s.mu.Unlock()
		if len(s.saved) != len(expected) {
			return fmt.Errorf("controlled writes %d expected %d", len(s.saved), len(expected))
		}
		if s.config.RequestVersion != 2 || s.config.ForceStepPlan != (workload == "mixed8") || !s.config.StartedAt.Equal(in.i.md.Config.StartedAt) {
			return errors.New("controlled metadata mismatch")
		}
		pending := []string{}
		for id := range s.pending {
			pending = append(pending, id)
		}
		slices.Sort(pending)
		if workload == "mixed8" {
			if !slices.Equal(pending, []string{"s0", "s5", "s7"}) {
				return fmt.Errorf("controlled pending %v", pending)
			}
		} else if len(pending) != 0 {
			return errors.New("unexpected controlled pending")
		}
		for id, bytes := range expected {
			if s.saved[id] != sha256.Sum256([]byte(bytes)) {
				return fmt.Errorf("controlled output %s", id)
			}
		}
		if int(in.controlledQueue.count.Load()) != wantQueue {
			return fmt.Errorf("queue writes %d expected %d", in.controlledQueue.count.Load(), wantQueue)
		}
		// Each real owner copies the loaded RunMetrics value into each branch:
		// four prior bytes plus four emitted metadata bytes per saved branch.
		if len(expected) > 0 && s.delta != 8*len(expected) {
			return fmt.Errorf("metadata delta %d expected %d", s.delta, 8*len(expected))
		}
		return nil
	}
	outputs, err := p.reader.LoadSteps(ctx, in.i.md.ID)
	if err != nil {
		return err
	}
	if len(outputs) != len(expected) {
		return fmt.Errorf("persisted outputs %d expected %d", len(outputs), len(expected))
	}
	for id, bytes := range expected {
		if string(outputs[id]) != bytes {
			return fmt.Errorf("persisted output %s", id)
		}
	}
	md, err := p.reader.LoadMetadata(ctx, in.i.md.ID)
	if err != nil {
		return err
	}
	if md.Config.RequestVersion != 2 || md.Config.ForceStepPlan != (workload == "mixed8") || !md.Config.StartedAt.Equal(in.i.md.Config.StartedAt) {
		return errors.New("persisted metadata update missing")
	}
	if len(expected) > 0 && md.Metrics.MetadataSize != 7+8*len(expected) {
		return fmt.Errorf("persisted metadata size %d expected %d", md.Metrics.MetadataSize, 7+8*len(expected))
	}
	pending, err := p.reader.LoadPending(ctx, in.i.md.ID)
	if err != nil {
		return err
	}
	if workload == "mixed8" {
		slices.Sort(pending)
		if !slices.Equal(pending, []string{"s0", "s5", "s7"}) {
			return fmt.Errorf("pending %v", pending)
		}
	} else if len(pending) != 0 {
		return fmt.Errorf("unexpected pending %v", pending)
	}
	if workload == "retryable" {
		return nil
	}
	suffix := "-s0"
	if workload == "mixed8" {
		suffix = "-s5-plan"
	}
	key := queue.HashID(ctx, in.i.md.IdempotencyKey()+suffix)
	raw, err := p.readClient.Do(ctx, p.readClient.B().Hget().Key(p.itemsKey).Field(key).Build()).ToString()
	if err != nil {
		return err
	}
	var stored queue.QueueItem
	if err = json.Unmarshal([]byte(raw), &stored); err != nil {
		return err
	}
	wantSemaphores := 1
	if workload == "mixed8" {
		wantSemaphores = 0
	}
	edge, ok := stored.Data.Payload.(queue.PayloadEdge)
	if !ok || edge.Edge.Incoming != "function" || stored.Data.Kind != queue.KindEdge || stored.Data.Attempt != 0 || stored.Data.PriorityFactor == nil || *stored.Data.PriorityFactor != 7 || stored.Data.GetMaxAttempts() != 3 || len(stored.Data.CustomConcurrencyKeys) != 1 || len(stored.Data.Semaphores) != wantSemaphores {
		return fmt.Errorf("persisted queue payload/options mismatch: edge=%v kind=%s attempt=%d priority=%v max=%d keys=%d semaphores=%d", ok, stored.Data.Kind, stored.Data.Attempt, stored.Data.PriorityFactor, stored.Data.GetMaxAttempts(), len(stored.Data.CustomConcurrencyKeys), len(stored.Data.Semaphores))
	}
	if workload == "mixed8" {
		if edge.Edge.IncomingGeneratorStep != "s5" || edge.Edge.Outgoing != "previous" || stored.Data.ParallelCoalesceKey == nil {
			return errors.New("persisted planned queue mismatch")
		}
	} else if edge.Edge.Outgoing != "s0" {
		return errors.New("persisted discovery edge mismatch")
	}
	if stored.Data.Identifier.RunID != in.i.md.ID.RunID || stored.Data.Metadata[meta.PropagationKey] == nil {
		return errors.New("persisted queue identity/carrier missing")
	}
	return nil
}

func (p *measurePorts) validateRow(ctx context.Context, workload string, count int) error {
	if p.readClient == nil {
		return nil
	}
	actual, err := p.readClient.Do(ctx, p.readClient.B().Hlen().Key(p.itemsKey).Build()).AsInt64()
	if err != nil {
		return err
	}
	want := int64(count)
	if workload == "retryable" {
		want = 0
	}
	if actual != want {
		return fmt.Errorf("persisted queue count %d expected %d", actual, want)
	}
	return nil
}
