package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/tracing"
	"github.com/inngest/inngest/pkg/tracing/meta"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

type nativePreparationCase struct {
	Name     string
	Data     string
	Prepared string
	Bytes    int
}

func nativePreparationCases(t *testing.T) []nativePreparationCase {
	t.Helper()
	b, err := os.ReadFile("testdata/compatibility/native-step-preparation-v1.json")
	require.NoError(t, err)
	var fixture struct {
		FormatVersion int
		Cases         []nativePreparationCase
	}
	require.NoError(t, json.Unmarshal(b, &fixture))
	require.Equal(t, 1, fixture.FormatVersion)
	return fixture.Cases
}

type nativeBaselineSave struct {
	sv2.RunService
	before func(context.Context, []byte)
	after  func()
	calls  int
}

func (s *nativeBaselineSave) SaveStep(ctx context.Context, id sv2.ID, step string, data []byte) (bool, error) {
	s.calls++
	s.before(ctx, data)
	pending, err := s.RunService.SaveStep(ctx, id, step, data)
	if s.after != nil {
		s.after()
	}
	return pending, err
}

// This is the ordinary Go entry, with actual trace preparation and both durable
// adapters. The portable fixture is a byte oracle, not a decoded JSON equality.
func TestNativeStepGoPersistence(t *testing.T) {
	fixture := loadResponsePersistenceFixture(t)
	valkey, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	connect := func() rueidis.Client {
		c, e := queuehelper.NewValkeyClient(valkey.Addr, valkey.Username, valkey.Password, false)
		require.NoError(t, e)
		t.Cleanup(c.Close)
		return c
	}
	writer, reader := connect(), connect()
	ctx := t.Context()
	for _, keyQueues := range []bool{false, true} {
		for _, sharded := range []bool{false, true} {
			for _, prefix := range []string{"default", "custom"} {
				for _, tc := range nativePreparationCases(t) {
					t.Run(fmt.Sprintf("keys=%t/sharded=%t/%s/%s", keyQueues, sharded, prefix, tc.Name), func(t *testing.T) {
						require.NoError(t, writer.Do(ctx, writer.B().Flushdb().Build()).Error())
						stateKey, queueKey := redis_state.StateDefaultKey, redis_state.QueueDefaultKey
						if prefix == "custom" {
							stateKey = "native:state"
							queueKey = "native:queue"
						}
						g := &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s", Data: json.RawMessage(tc.Data)}
						if tc.Data == "" {
							g.Data = nil
						}
						e, i, _, _, events := newStagedBatch(t, g)
						exporter := tracetest.NewInMemoryExporter()
						e.tracerProvider = tracing.NewOtelTracerProvider(exporter, 0)
						svc := nativeStatePortMode(t, ctx, writer, stateKey, queueKey, sharded)
						fresh := nativeStatePortMode(t, ctx, reader, stateKey, queueKey, sharded)
						// Empty SDK metadata, experiments and ForceStepPlan remain the eligibility
						// preconditions. Create a run and an actual ordinary initial edge first.
						i.md.Config.SpanID = "0123456789abcdef"
						i.md.Config.EventIDs = []ulid.ULID{ulid.MustParse("01ARZ3NDEKTSV4RRFFQ69G5FAW")}
						i.md.Config.ScheduledAt = e.clock.Now()
						created, err := svc.Create(ctx, sv2.CreateState{Metadata: i.md, Events: []json.RawMessage{json.RawMessage(`{"name":"native.test","data":{}}`)}})
						require.NoError(t, err)
						i.md = created.Metadata
						realQueue, itemsKey := nativeQueuePort(t, ctx, writer, queueKey, e.clock, keyQueues)
						initial := i.item
						initial.Identifier = sv2.V1FromMetadata(i.md)
						initial.WorkspaceID = i.md.ID.Tenant.EnvID
						initial.Kind = queue.KindEdge
						initial.GroupID = "00000000-0000-4000-8000-000000000005"
						job := "initial-edge"
						initial.JobID = &job
						require.NoError(t, realQueue.Enqueue(ctx, initial, e.clock.Now(), queue.EnqueueOpts{}))
						preItems, err := reader.Do(ctx, reader.B().Hgetall().Key(itemsKey).Build()).AsStrMap()
						require.NoError(t, err)
						require.Len(t, preItems, 1)
						initialID := fixture.Queue.HashAnswers["initial-edge"]
						var initialExpected map[string]any
						require.NoError(t, json.Unmarshal(fixture.Queue.Item, &initialExpected))
						initialExpected["id"] = initialID
						initialData := initialExpected["data"].(map[string]any)
						initialData["atts"] = float64(1)
						delete(initialData, "metadata")
						initialData["payload"].(map[string]any)["edge"].(map[string]any)["outgoing"] = "previous"
						initialJSON, err := json.Marshal(initialExpected)
						require.NoError(t, err)
						require.JSONEq(t, string(initialJSON), preItems[initialID])
						stateTag := stateKey
						if sharded {
							stateTag += ":01ARZ3NDEKTSV4RRFFQ69G5FAV"
						}
						saveKeys := make([]string, len(fixture.SaveKeys))
						for j, key := range fixture.SaveKeys {
							saveKeys[j] = strings.ReplaceAll(key, "STATE", stateTag)
						}
						idempotencyTag := stateKey
						if sharded {
							idempotencyTag += ":00000000-0000-0000-0000-000000000003"
						}
						preExpected := expectedResponseQueueSnapshot(t, fixture, queueKey, keyQueues, preItems[initialID], initialID)
						preExpected["{"+idempotencyTag+"}:key:run-key"] = responseStoredValue{"string", "01ARZ3NDEKTSV4RRFFQ69G5FAV", 86400000}
						preExpected[saveKeys[1]] = responseStoredValue{"hash", maps.Clone(fixture.InitialMetadata), -1}
						preExpected[strings.Replace(saveKeys[0], ":actions:", ":bulk-events:", 1)] = responseStoredValue{"string", fixture.InitialEvents, -1}
						observed := []string{"precreated"}
						precreated := snapshotResponseStore(t, ctx, reader)
						requireResponseSnapshot(t, preExpected, precreated)
						postSaveExpected := maps.Clone(preExpected)
						postSaveMetadata := maps.Clone(fixture.InitialMetadata)
						postSaveMetadata["state_size"] = strconv.Itoa(34 + tc.Bytes)
						postSaveMetadata["step_count"] = "1"
						postSaveExpected[saveKeys[1]] = responseStoredValue{"hash", postSaveMetadata, -1}
						postSaveExpected[saveKeys[0]] = responseStoredValue{"hash", map[string]string{"s": tc.Prepared}, -1}
						postSaveExpected[saveKeys[2]] = responseStoredValue{"list", []string{"s"}, -1}

						saved := &nativeBaselineSave{RunService: svc}
						saved.before = func(saveCtx context.Context, b []byte) {
							observed = append(observed, "pre-save")
							requireResponseSnapshot(t, preExpected, snapshotResponseStore(t, ctx, reader))
							require.Equal(t, tc.Prepared, string(b))
							require.Len(t, b, tc.Bytes)
							require.Zero(t, state.MetadataSizeDeltaFromContext(saveCtx))
							out, err := fresh.LoadSteps(ctx, i.md.ID)
							require.NoError(t, err)
							require.Empty(t, out)
							spans := exporter.GetSpans()
							require.NotEmpty(t, spans, "real step span emitted before save")
							require.Equal(t, meta.SpanNameStep, spans[len(spans)-1].Name)
							require.Empty(t, events.snapshot(), "save precedes lifecycle and discovery")
						}
						saved.after = func() {
							observed = append(observed, "post-save")
							requireResponseSnapshot(t, postSaveExpected, snapshotResponseStore(t, ctx, reader))
							out, err := fresh.LoadSteps(ctx, i.md.ID)
							require.NoError(t, err)
							require.Equal(t, tc.Prepared, string(out["s"]))
							require.Len(t, out, 1)
							metadata, err := fresh.LoadMetadata(ctx, i.md.ID)
							require.NoError(t, err)
							require.Equal(t, created.Metadata.Metrics.StateSize+tc.Bytes, metadata.Metrics.StateSize)
							require.Equal(t, 1, metadata.Metrics.StepCount)
							require.Zero(t, metadata.Metrics.MetadataSize)
							require.Equal(t, []string{"s"}, metadata.Stack)
							items, err := reader.Do(ctx, reader.B().Hgetall().Key(itemsKey).Build()).AsStrMap()
							require.NoError(t, err)
							require.Equal(t, preItems, items, "checkpoint is committed before discovery")
						}
						e.smv2 = saved
						discoveryGroup := ""
						e.queue = &nativeBaselineQueue{Queue: realQueue, observe: func(enqueueCtx context.Context, queued queue.Item) {
							require.Equal(t, state.GroupIDFromContext(enqueueCtx), queued.GroupID)
							require.NotEmpty(t, queued.GroupID)
							discoveryGroup = queued.GroupID
						}, before: func() {
							observed = append(observed, "pre-discovery")
							requireResponseSnapshot(t, postSaveExpected, snapshotResponseStore(t, ctx, reader))
						}}
						require.NoError(t, e.HandleGeneratorResponse(ctx, i, i.resp))
						require.Equal(t, 1, saved.calls)
						require.Equal(t, []string{"finished:s", "scheduled:edge:<nil>:0"}, events.snapshot())
						stored, err := reader.Do(ctx, reader.B().Hgetall().Key(itemsKey).Build()).AsStrMap()
						require.NoError(t, err)
						require.Len(t, stored, 2)
						id := fixture.Queue.HashID
						require.Equal(t, id, queue.HashID(ctx, "00000000-0000-0000-0000-000000000001:run-key-s"))
						var item queue.QueueItem
						require.NoError(t, json.Unmarshal([]byte(stored[id]), &item))
						require.Equal(t, id, item.ID)
						require.Equal(t, 1, item.GenerationID)
						require.Equal(t, i.md.ID.RunID, item.Data.Identifier.RunID)
						require.Equal(t, i.md.ID.Tenant.AccountID, item.Data.Identifier.AccountID)
						require.Equal(t, i.md.ID.Tenant.EnvID, item.WorkspaceID)
						require.Equal(t, i.md.ID.Tenant.AppID, item.Data.Identifier.AppID)
						require.Equal(t, int64(1793016000000), item.WallTimeMS)
						require.Equal(t, int64(1793015993000), item.AtMS)
						require.Equal(t, int64(1793016000000), item.EnqueuedAt)
						require.Zero(t, item.Data.Attempt)
						require.Equal(t, 3, *item.Data.MaxAttempts)
						require.Equal(t, queue.PayloadEdge{Edge: i.edge}, initial.Payload)
						edge, ok := item.Data.Payload.(queue.PayloadEdge)
						require.True(t, ok)
						require.Equal(t, "s", edge.Edge.Outgoing)
						require.Equal(t, "function", edge.Edge.Incoming)
						require.Empty(t, edge.Edge.IncomingGeneratorStep)
						require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, item.Data.GroupID)
						require.Equal(t, discoveryGroup, item.Data.GroupID)
						_, err = uuid.Parse(item.Data.GroupID)
						require.NoError(t, err)
						require.Len(t, item.Data.Metadata, 1)
						carrier, ok := item.Data.Metadata[meta.PropagationKey].(string)
						require.True(t, ok)
						var ref meta.SpanReference
						require.NoError(t, json.Unmarshal([]byte(carrier), &ref))
						var rawCarrier map[string]string
						require.NoError(t, json.Unmarshal([]byte(carrier), &rawCarrier))
						require.Equal(t, fixture.Tracing.RootParent, ref.DynamicSpanTraceParent)
						require.Equal(t, fixture.Tracing.TraceID, strings.Split(ref.TraceParent, "-")[1])
						require.Equal(t, map[string]string{"tp": ref.TraceParent, "ts": "", "dstp": fixture.Tracing.RootParent, "dsid": ref.DynamicSpanID}, rawCarrier)

						require.Regexp(t, `^00-[0-9a-f]{32}-[0-9a-f]{16}-01$`, ref.TraceParent)
						require.Regexp(t, `^[0-9a-f]{16}$`, ref.DynamicSpanID)
						discovery := exporter.GetSpans()[len(exporter.GetSpans())-1]
						require.Equal(t, meta.SpanNameStepDiscovery, discovery.Name)
						require.Equal(t, discovery.SpanContext.TraceID().String(), strings.Split(ref.TraceParent, "-")[1])
						require.Equal(t, discovery.SpanContext.SpanID().String(), strings.Split(ref.TraceParent, "-")[2])
						require.Equal(t, strings.Split(ref.TraceParent, "-")[2], ref.DynamicSpanID)
						require.Equal(t, strings.Split(ref.TraceParent, "-")[1], strings.Split(ref.DynamicSpanTraceParent, "-")[1])
						require.Equal(t, "9eeb205ddee9ada3", strings.Split(ref.DynamicSpanTraceParent, "-")[2])
						require.Equal(t, "00", strings.Split(ref.DynamicSpanTraceParent, "-")[3])
						t.Logf("raw prepared=%q saved=%q queued=%s", tc.Prepared, tc.Prepared, stored[id])
						var discoveryExpected map[string]any
						require.NoError(t, json.Unmarshal(fixture.Queue.Item, &discoveryExpected))
						discoveryData := discoveryExpected["data"].(map[string]any)
						// Validate generated UUID and carrier format/relationships above before
						// substituting ONLY those two fields into the literal queue contract.
						discoveryData["groupID"] = item.Data.GroupID
						discoveryData["metadata"] = item.Data.Metadata
						discoveryJSON, err := json.Marshal(discoveryExpected)
						require.NoError(t, err)
						require.JSONEq(t, string(discoveryJSON), stored[id])
						observed = append(observed, "post-enqueue")
						postExpected := mergeResponseSnapshots(t, postSaveExpected, expectedResponseQueueSnapshot(t, fixture, queueKey, keyQueues, stored[id], id))
						requireResponseSnapshot(t, postExpected, snapshotResponseStore(t, ctx, reader))

						// Existing sequencing fixtures own duplicate acceptance. Here a fresh
						// adapter independently verifies durable identity with no second increment.
						pending, err := fresh.SaveStep(ctx, i.md.ID, "s", []byte(tc.Prepared))
						require.Same(t, state.ErrIdempotentResponse, err)
						require.False(t, pending)
						metadata, err := fresh.LoadMetadata(ctx, i.md.ID)
						require.NoError(t, err)
						require.Equal(t, 1, metadata.Metrics.StepCount)
						requireResponseSnapshot(t, postExpected, snapshotResponseStore(t, ctx, reader))
						observed = append(observed, "fresh-adapter-duplicate")
						require.Equal(t, fixture.ObservationSequence, observed)
						if tc.Name == "large-integer" {
							// Both actual SaveStep sentinels are accepted by the success owner. Queue
							// dedupe drops the new discovery span and does not invoke scheduled again.
							for _, data := range []json.RawMessage{json.RawMessage(tc.Data), json.RawMessage(`1e+06`)} {
								g.Data = data
								e.smv2 = fresh
								duplicateQueue := &nativeBaselineQueue{Queue: realQueue, before: func() { requireResponseSnapshot(t, postExpected, snapshotResponseStore(t, ctx, reader)) }}
								e.queue = duplicateQueue
								beforeSpans := len(exporter.GetSpans())
								require.NoError(t, e.HandleGeneratorResponse(ctx, i, i.resp))
								require.Equal(t, 1, duplicateQueue.calls)
								requireResponseSnapshot(t, postExpected, snapshotResponseStore(t, ctx, reader))
								spans := exporter.GetSpans()
								require.Len(t, spans, beforeSpans+1, "deduplicated discovery span is dropped by the real execution processor")
								require.Equal(t, meta.SpanNameStep, spans[len(spans)-1].Name)
							}

							require.Equal(t, []string{"finished:s", "scheduled:edge:<nil>:0", "finished:s", "finished:s"}, events.snapshot())
							original := errors.New("generic discovery port failure")
							failedQueue := &nativeBaselineQueue{Queue: realQueue, fail: original, before: func() { requireResponseSnapshot(t, postExpected, snapshotResponseStore(t, ctx, reader)) }}
							e.queue = failedQueue
							err = e.HandleGeneratorResponse(ctx, i, i.resp)
							require.True(t, queue.IsAlwaysRetryable(err))
							require.IsType(t, queue.AlwaysRetryError(original), err)
							require.EqualError(t, err, "error enqueueing discovery step: generic discovery port failure")
							require.Equal(t, 1, failedQueue.calls)
							requireResponseSnapshot(t, postExpected, snapshotResponseStore(t, ctx, reader))
							require.Equal(t, []string{"finished:s", "scheduled:edge:<nil>:0", "finished:s", "finished:s", "finished:s"}, events.snapshot())

						}
					})
				}
			}
		}
	}
}

func TestNativeStepPreparationAccountingThreshold(t *testing.T) {
	// Reuse the existing release observer and overflow identity assertion, but pin
	// the actual output+loaded-state threshold: equality accepts, +1 rejects.
	for _, limit := range []int{47, 46} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			g := state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"}
			e, i, s, q, events := newStagedBatch(t, &g)
			i.md.Metrics.StateSize = 34
			e.stateSizeLimit = func(sv2.ID) int { return limit }
			err := e.HandleGenerator(context.Background(), &stagedReleaseContext{runInstance: i, events: events}, g)
			if limit == 47 {
				require.NoError(t, err)
				require.Equal(t, `{"data":null}`, s.writes["s"])
				require.Len(t, q.calls, 1)
			} else {
				requireContinuationPreparationOverflow(t, err, 46)
				require.Empty(t, s.writes)
				require.Empty(t, q.calls)
				require.Empty(t, events.snapshot())
			}
		})
	}
}

type nativeBaselineQueue struct {
	queue.Queue
	before  func()
	observe func(context.Context, queue.Item)
	fail    error
	calls   int
}

func (q *nativeBaselineQueue) Enqueue(ctx context.Context, item queue.Item, at time.Time, opts queue.EnqueueOpts) error {
	q.calls++
	q.before()
	if q.observe != nil {
		q.observe(ctx, item)
	}
	if q.fail != nil {
		return q.fail
	}
	return q.Queue.Enqueue(ctx, item, at, opts)
}

// The step owner chooses the unhashed identity; queue persistence independently
// owns its hash and fresh-load semantics in TestNativeStepQueuePersistenceCompatibility.
func TestNativeStepJobIdentity(t *testing.T) {
	f := loadResponsePersistenceFixture(t)
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprint(fallback), func(t *testing.T) {
			g := &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s"}
			e, i, _, q, _ := newStagedBatch(t, g)
			expected := "00000000-0000-0000-0000-000000000001:run-key-s"
			if fallback {
				i.md.Config.Idempotency = ""
				expected = f.Queue.FallbackJobID
			}
			require.NoError(t, e.HandleGeneratorResponse(t.Context(), i, i.resp))
			require.Len(t, q.calls, 1)
			require.NotNil(t, q.calls[0].item.JobID)
			require.Equal(t, expected, *q.calls[0].item.JobID)
			require.Equal(t, f.Queue.HashAnswers[expected], queue.HashID(t.Context(), *q.calls[0].item.JobID))
		})
	}
}
