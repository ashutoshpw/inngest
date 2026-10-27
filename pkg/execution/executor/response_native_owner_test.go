//go:build native_step_integration

package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/metadata/extractors"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

// This private, unstarted setup owns its run and both locks. It has no worker or
// published RunContext. It never removes observers from an existing runtime.
// Locks remain held across the one native invocation, not across Go effects.
// It is a constructed-domain proof, not a general eligibility function.
type constructedResponseSource struct {
	e                            *executor
	run                          runInstance
	raw                          []byte
	response                     state.DriverResponse
	metadataOwner, responseOwner sync.Mutex
}

func newConstructedResponseSource() *constructedResponseSource {
	max := 5
	priority := int64(19)
	s := &constructedResponseSource{e: &executor{log: logger.VoidLogger(), tracerProvider: tracing.NewNoopTracerProvider()}}
	s.run.md = sv2.Metadata{ID: sv2.ID{RunID: ulid.MustParse("01ARZ3NDEKTSV4RRFFQ69G5FAV"), FunctionID: uuid.MustParse("00000000-0000-0000-0000-000000000001"), Tenant: sv2.Tenant{AccountID: uuid.MustParse("00000000-0000-0000-0000-000000000003"), EnvID: uuid.MustParse("00000000-0000-0000-0000-000000000002"), AppID: uuid.MustParse("00000000-0000-0000-0000-000000000004")}}, Config: *sv2.InitConfig(&sv2.Config{RequestVersion: 2, Idempotency: "source-key", EventIDs: []ulid.ULID{ulid.MustParse("01ARZ3NDEKTSV4RRFFQ69G5FAW")}})}
	s.run.events = []json.RawMessage{json.RawMessage(`{"name":"native.test","data":{}}`)}
	s.run.item = queue.Item{Kind: queue.KindEdge, MaxAttempts: &max, PriorityFactor: &priority, Payload: queue.PayloadEdge{Edge: inngest.Edge{Incoming: "source-incoming", Outgoing: "previous"}}}
	s.run.resp = &state.DriverResponse{RequestVersion: 99, NoRetry: true}
	s.raw = []byte(`[{"op":"StepRun","id":"s","data":9007199254740993}]`)
	s.response.RequestVersion = 3
	return s
}
func (s *constructedResponseSource) withInput(ctx context.Context, keys bool, use func(map[string]any)) error {
	if !s.responseOwner.TryLock() {
		return errors.New("response ownership")
	}
	defer s.responseOwner.Unlock()
	if !s.metadataOwner.TryLock() {
		return errors.New("metadata ownership")
	}
	defer s.metadataOwner.Unlock()
	e, i := s.e, &s.run
	if len(e.lifecycles) > 0 || len(e.syncLifecycles) > 0 {
		return errors.New("observers")
	}
	if reflect.TypeOf(e.tracerProvider) != reflect.TypeOf(tracing.NewNoopTracerProvider()) || e.traceReader != nil || e.conditionalTracer != nil || trace.SpanContextFromContext(ctx).IsValid() || i.execSpan != nil || i.parentSpan != nil || i.md.Config.SpanID != "" || len(i.md.Config.Context) > 0 || len(i.item.Metadata) > 0 {
		return errors.New("tracing")
	}
	if e.capacityManager != nil || (i.item.RunInfo != nil && i.item.RunInfo.CapacityLease != nil) {
		return errors.New("capacity")
	}
	if e.stateSizeLimit != nil {
		return errors.New("nonstatic resolver")
	}
	if i.md.Config.RequestVersion != 2 || i.md.Config.ForceStepPlan {
		return errors.New("loaded config")
	}
	if i.f.Concurrency != nil || i.f.EventBatch != nil || i.f.Throttle != nil || i.f.Singleton != nil || i.f.Debounce != nil || i.f.RateLimit != nil {
		return errors.New("function extensions")
	}
	if i.md.Config.HasAI || len(i.events) != 1 || len(i.md.Config.EventIDs) != 1 || i.md.Config.BatchID != nil || i.md.Config.OriginalRunID != nil || i.md.Config.ReplayID != nil || len(i.md.Config.CustomConcurrencyKeys) > 0 || len(i.md.Config.Semaphores) > 0 {
		return errors.New("run extensions")
	}
	if i.item.QueueName != nil || i.item.Throttle != nil || i.item.Singleton != nil || i.item.ParallelCoalesceKey != nil || i.item.ParallelMode != enums.ParallelModeNone || len(i.item.CustomConcurrencyKeys) > 0 || len(i.item.Semaphores) > 0 {
		return errors.New("item extensions")
	}
	var original []*state.GeneratorOpcode
	if err := json.Unmarshal(s.raw, &original); err != nil {
		return err
	}
	if len(original) != 1 || original[0] == nil || original[0].Op != enums.OpcodeStepRun || original[0].HasAI() || bytes.Contains(original[0].Data, []byte("inputTokens")) || bytes.Contains(original[0].Data, []byte("totalUsage")) || len(original[0].Metadata) > 0 || original[0].ParallelMode() != enums.ParallelModeNone {
		return errors.New("generator extensions")
	}
	if original[0].Opts != nil {
		opts, err := json.Marshal(original[0].Opts)
		if err != nil || (string(opts) != "{}" && string(opts) != "null") {
			return errors.New("generator extensions")
		}
	}
	edge, ok := i.item.Payload.(queue.PayloadEdge)
	if !ok {
		return errors.New("edge")
	}
	// No Output(), resolver, metric swap, save answer, recipe, UUID, carrier or
	// clock enters this source projection. Native reparses original bytes strictly.
	mode := "Partition"
	if keys {
		mode = "Key"
	}
	data := make([]uint16, len(s.raw))
	for j, b := range s.raw {
		data[j] = uint16(b)
	}
	loaded := map[string]any{"requestVersion": i.md.Config.RequestVersion, "forceStepPlan": i.md.Config.ForceStepPlan, "stateBytes": i.md.Metrics.StateSize, "metadataSize": i.md.Metrics.MetadataSize, "metadataSizeLoaded": i.md.Metrics.MetadataSizeLoaded}
	var retry any
	if s.response.RetryAt != nil {
		retry = s.response.RetryAt.Format("2006-01-02T15:04:05.999999999Z07:00")
	}
	source := map[string]any{"domain": map[string]any{"observerRegistrations": len(e.lifecycles) + len(e.syncLifecycles), "tracing": "Absent", "capacityLease": "Absent", "metadataWriters": 1, "responseOwners": 1, "metadataGeneration": "Absent"}, "response": data, "responseVersion": s.response.RequestVersion, "noRetry": s.response.NoRetry, "retryAt": retry, "loaded": loaded, "stateLimit": nil, "queue": map[string]any{"prefix": redis_state.QueueDefaultKey, "mode": mode}, "idempotencyKey": i.md.Config.Idempotency, "incoming": edge.Edge.Incoming, "priorityFactor": i.PriorityFactor(), "identifierPriorityFactor": i.md.Config.PriorityFactor, "maxAttempts": i.MaxAttempts(), "functionVersion": i.md.Config.FunctionVersion, "eventId": i.md.Config.EventIDs[0].String(), "eventIds": []string{i.md.Config.EventIDs[0].String()}}
	use(source)
	return nil
}
func TestNativeResponseConstructedSources(t *testing.T) {
	ctx := t.Context()
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	client, err := queuehelper.NewValkeyClient(v.Addr, "", v.Password, false)
	require.NoError(t, err)
	defer client.Close()
	s := newConstructedResponseSource()
	svc := nativeStatePortMode(t, ctx, client, redis_state.StateDefaultKey, redis_state.QueueDefaultKey, false)
	created, err := svc.Create(ctx, sv2.CreateState{Metadata: s.run.md, Events: s.run.events})
	require.NoError(t, err)
	s.run.md = created.Metadata
	s.run.md.Metrics.MetadataSize = 17
	s.run.md.Metrics.MetadataSizeLoaded = 0
	keys := os.Getenv("EXPERIMENTAL_KEY_QUEUES_ENABLE") == "true"
	require.NoError(t, s.withInput(ctx, keys, func(source map[string]any) {
		id := s.run.md.ID
		require.Equal(t, 3, source["responseVersion"])
		require.Equal(t, false, source["noRetry"], "explicit response differs from RunContext response")
		require.Equal(t, 99, s.run.resp.RequestVersion)
		require.True(t, s.run.resp.NoRetry)
		input := map[string]any{"store": map[string]any{"address": strings.Replace(v.Addr, "localhost:", "127.0.0.1:", 1), "password": v.Password, "connectTimeoutMs": 500, "readTimeoutMs": 500, "writeTimeoutMs": 500}, "state": map[string]any{"prefix": redis_state.StateDefaultKey, "layout": "NeverShard", "identity": map[string]any{"runId": id.RunID.String(), "functionId": id.FunctionID.String(), "accountId": id.Tenant.AccountID.String(), "envId": id.Tenant.EnvID.String(), "appId": id.Tenant.AppID.String()}}, "budgetMs": 5000, "disabledRetries": true, "operation": map[string]any{"kind": "response", "input": source}}
		raw, e := json.Marshal(input)
		require.NoError(t, e)
		binary := os.Getenv("NATIVE_STEP_TEST_BINARY")
		require.NotEmpty(t, binary)
		require.FileExists(t, binary)
		cmd := exec.CommandContext(ctx, binary)
		cmd.Stdin = bytes.NewReader(raw)
		out, e := cmd.Output()
		require.NoError(t, e)
		var result struct {
			Disposition string
			GroupID     string
		}
		require.NoError(t, json.Unmarshal(out, &result))
		require.Equal(t, "Success", result.Disposition)
		fresh := nativeStatePortMode(t, ctx, client, redis_state.StateDefaultKey, redis_state.QueueDefaultKey, false)
		steps, e := fresh.LoadSteps(ctx, id)
		require.NoError(t, e)
		require.Equal(t, `{"data":9007199254740993}`, string(steps["s"]))
		loaded, e := fresh.LoadMetadata(ctx, id)
		require.NoError(t, e)
		require.Equal(t, 17, loaded.Metrics.MetadataSize)
		items, e := client.Do(ctx, client.B().Hgetall().Key("{"+redis_state.QueueDefaultKey+"}:queue:item").Build()).AsStrMap()
		require.NoError(t, e)
		require.Len(t, items, 1)
		for _, raw := range items {
			var item queue.QueueItem
			require.NoError(t, json.Unmarshal([]byte(raw), &item))
			require.Equal(t, "source-incoming", item.Data.Payload.(queue.PayloadEdge).Edge.Incoming)
			require.EqualValues(t, 19, *item.Data.PriorityFactor)
			require.Equal(t, 5, *item.Data.MaxAttempts)
			require.Empty(t, item.Data.Metadata)
			require.Equal(t, result.GroupID, item.Data.GroupID)
		}
	}))
	for _, name := range []string{"observer", "sync-observer", "tracing", "carrier", "capacity", "metadata-writer", "response-owner", "loaded-minus-one", "force-plan", "resolver", "function-concurrency", "sdk-metadata", "experiment", "ai", "ai-output"} {
		t.Run(name, func(t *testing.T) {
			s := newConstructedResponseSource()
			var want string
			switch name {
			case "observer":
				s.e.lifecycles = []execution.LifecycleListener{&continuationAsyncLifecycle{}}
				want = "observers"
			case "sync-observer":
				s.e.syncLifecycles = []execution.SyncLifecycleListener{&stagedBatchLifecycle{}}
				want = "observers"
			case "tracing":
				s.e.tracerProvider = tracing.NewOtelTracerProvider(nil, 0)
				want = "tracing"
			case "carrier":
				s.run.item.Metadata = map[string]any{"user-otel-ctx": "actual carrier"}
				want = "tracing"
			case "capacity":
				s.run.item.RunInfo = &queue.RunInfo{CapacityLease: &queue.CapacityLease{}}
				want = "capacity"
			case "metadata-writer":
				s.metadataOwner.Lock()
				defer s.metadataOwner.Unlock()
				want = "metadata ownership"
			case "response-owner":
				s.responseOwner.Lock()
				defer s.responseOwner.Unlock()
				want = "response ownership"
			case "loaded-minus-one":
				s.run.md.Config.RequestVersion = -1
				want = "loaded config"
			case "force-plan":
				s.run.md.Config.ForceStepPlan = true
				want = "loaded config"
			case "resolver":
				s.e.stateSizeLimit = func(sv2.ID) int { t.Fatal("resolver activated"); return 0 }
				want = "nonstatic resolver"
			case "function-concurrency":
				s.run.f.Concurrency = &inngest.ConcurrencyLimits{}
				want = "function extensions"
			case "sdk-metadata":
				s.raw = []byte(`[{"op":"StepRun","id":"s","metadata":[{"scope":"step","kind":"userland.custom","op":"merge","values":{"k":"v"}}]}]`)
			case "experiment":
				s.raw = []byte(`[{"op":"StepRun","id":"s","opts":{"experimentName":"x","variant":"a","selectionStrategy":"weighted"}}]`)
			case "ai":
				s.raw = []byte(`[{"op":"StepRun","id":"s","opts":{"type":"step.ai.wrap"}}]`)
			case "ai-output":
				s.raw = []byte(`[{"op":"StepRun","id":"s","data":{"totalUsage":{"inputTokens":1}}}]`)
			}
			if name == "sdk-metadata" || name == "experiment" || name == "ai" || name == "ai-output" {
				var generators []*state.GeneratorOpcode
				require.NoError(t, json.Unmarshal(s.raw, &generators))
				g := generators[0]
				switch name {
				case "sdk-metadata":
					require.NoError(t, g.Metadata[0].ValidateAllowed())
				case "experiment":
					proposal, e := extractors.ExtractExperimentOptsMetadata(g.Opts)
					require.NoError(t, e)
					require.NotNil(t, proposal)
				case "ai":
					require.True(t, g.HasAI())
				case "ai-output":
					proposal, e := extractors.ExtractAIOutputMetadata([]byte(`{"data":{"totalUsage":{"inputTokens":1}}}`), 0)
					require.NoError(t, e)
					require.NotEmpty(t, proposal)
				}
			}
			projected := false
			e := s.withInput(ctx, false, func(map[string]any) { projected = true })
			if want != "" {
				require.EqualError(t, e, want)
			} else {
				require.Error(t, e)
			}
			require.False(t, projected, "source must reject before projection or native invocation")
		})
	}
}
