//go:build native_step_integration

package executor

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/cqrs"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	"github.com/inngest/inngest/pkg/logger"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/jonboulle/clockwork"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

// A is a native producer, never an executing Go runtime or per-effect bridge.
// Only stores and HTTP survive. B is the existing ordinary Go scanner/executor.
func TestRecoveryReplaysNativeResponseOwner(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(recoveryFixtureDir, "native-boundary.json"))
	require.NoError(t, err)
	var f nativeReplayFixture
	require.NoError(t, json.Unmarshal(raw, &f))
	for _, fault := range []string{"", "after"} {
		t.Run("queue-"+fault, func(t *testing.T) {
			ctx := logger.WithStdlib(t.Context(), logger.VoidLogger())
			v, err := queuehelper.StartValkey(t)
			require.NoError(t, err)
			keys := os.Getenv("EXPERIMENTAL_KEY_QUEUES_ENABLE") == "true"
			t.Logf("key queues: %t", keys)
			directory := t.TempDir()
			now := time.Now().UTC()
			retryAt := now.Add(10 * time.Minute)
			id := sv2.ID{RunID: ulid.MustParse("01K8F08000AAAAAAAAAAAAAAAA"), FunctionID: uuid.MustParse("11111111-2222-3333-4444-555555555555"), Tenant: sv2.Tenant{AccountID: uuid.MustParse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"), EnvID: uuid.MustParse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"), AppID: uuid.MustParse("cccccccc-cccc-cccc-cccc-cccccccccccc")}}
			requests := make(chan nativeReplayHTTPRequest, 4)
			atReplay := make(chan nativeReplayReadout, 4)
			effects := recoveryValkeyClient(t, v)
			checkpoint, terminal := recoveryFixture(t, "native-checkpoint.http"), recoveryFixture(t, "native-terminal.http")
			sdk := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, e := io.ReadAll(r.Body)
				var request struct {
					Steps map[string]json.RawMessage `json:"steps"`
				}
				if e != nil || json.Unmarshal(body, &request) != nil {
					http.Error(w, "bad request", 400)
					return
				}
				response := checkpoint
				if saved, ok := request.Steps["checkpoint-output"]; ok && string(saved) == f.Output {
					atReplay <- readNativeReplayStore(r.Context(), v, f)
					response = terminal
					requests <- nativeReplayHTTPRequest{recoveryHTTPRequest: recoveryHTTPRequest{Body: body, Version: r.Header.Get("x-inngest-req-version"), StepID: r.URL.Query().Get("stepId")}, Header: r.Header.Clone()}
				} else {
					if e := effects.Do(r.Context(), effects.B().Incr().Key("fixture:external-effect").Build()).Error(); e != nil {
						http.Error(w, e.Error(), 500)
						return
					}
				}
				for k, values := range response.Header {
					w.Header()[k] = values
				}
				w.WriteHeader(response.StatusCode)
				_, _ = w.Write(response.Body)
			}))
			defer sdk.Close()
			// Reuse persistent setup without constructing A's executor or starting its
			// scanner. No listeners, tracing provider, spans/carrier, capacity lease,
			// metadata generator, or concurrent writer is registered in this producer.
			a := newRecoveryRuntime(t, ctx, v, directory, clockwork.NewRealClock(), retryAt, keys)
			require.Nil(t, a.exec)
			require.Nil(t, a.cancel)
			require.Nil(t, a.done)
			zero := 0
			fn := inngest.Function{ID: id.FunctionID, FunctionVersion: 1, Name: "recovery-fixture", Slug: "recovery-fixture", Steps: []inngest.Step{{ID: "step", Name: "step", URI: sdk.URL, Retries: &zero}}}
			config, err := json.Marshal(fn)
			require.NoError(t, err)
			_, err = a.cqrs.UpsertApp(ctx, cqrs.UpsertAppParams{ID: id.Tenant.AppID, Name: fn.Name})
			require.NoError(t, err)
			_, err = a.cqrs.UpsertFunction(ctx, cqrs.UpsertFunctionParams{ID: fn.ID, AppID: id.Tenant.AppID, Name: fn.Name, Slug: fn.Slug, Config: string(config)})
			require.NoError(t, err)
			sourceEvent := json.RawMessage(`{"id":"source-event-v1","name":"fixture/recovery","ts":1793016000000,"data":{"order":"order-1"}}`)
			priority := int64(0)
			md := sv2.Metadata{ID: id, Config: *sv2.InitConfig(&sv2.Config{FunctionVersion: 1, RequestVersion: 2, Idempotency: f.Idempotency, EventIDs: []ulid.ULID{ulid.MustParse("01K8F08000BBBBBBBBBBBBBBBB")}, ScheduledAt: time.UnixMilli(1793016000000), PriorityFactor: &priority})}
			created, err := a.state.Create(ctx, sv2.CreateState{Metadata: md, Events: []json.RawMessage{sourceEvent}})
			require.NoError(t, err)
			// Precreate the accepted persisted v2/force-plan-off representation.
			// This is setup before native ownership, not a response proposal.
			require.NoError(t, a.clients[0].Do(ctx, a.clients[0].B().Hset().Key(f.StateKeys["metadata"]).FieldValue().FieldValue("die", "0").Build()).Error())
			loaded, err := a.state.LoadMetadata(ctx, id)
			require.NoError(t, err)
			require.Equal(t, created.Metadata.Metrics.StateSize, loaded.Metrics.StateSize)
			require.Equal(t, 2, loaded.Config.RequestVersion)
			require.False(t, loaded.Config.ForceStepPlan)
			require.False(t, loaded.Config.HasAI)
			require.Empty(t, loaded.Config.SpanID)
			require.Empty(t, loaded.Config.Context)
			require.Nil(t, loaded.Config.BatchID)
			require.Nil(t, loaded.Config.ReplayID)
			require.Nil(t, loaded.Config.OriginalRunID)
			require.Empty(t, loaded.Config.CustomConcurrencyKeys)
			require.Empty(t, loaded.Config.Semaphores)
			require.Len(t, loaded.Config.EventIDs, 1)
			// Obtain original generator bytes from a real HTTP response. There is no Go
			// response handler, preparation, tracing, save/enqueue hook or callback in A.
			request, err := http.Post(sdk.URL, "application/json", bytes.NewBufferString(`{"steps":{}}`))
			require.NoError(t, err)
			response, err := io.ReadAll(request.Body)
			require.NoError(t, err)
			require.NoError(t, request.Body.Close())
			require.Equal(t, http.StatusPartialContent, request.StatusCode)
			require.Equal(t, checkpoint.Body, response)
			data := make([]uint16, len(response))
			for j, b := range response {
				data[j] = uint16(b)
			}
			mode := "Partition"
			if keys {
				mode = "Key"
			}
			responseVersion, err := strconv.Atoi(request.Header.Get("X-Inngest-Req-Version"))
			require.NoError(t, err)
			source := map[string]any{"domain": map[string]any{"observerRegistrations": 0, "tracing": "Absent", "capacityLease": "Absent", "metadataWriters": 1, "responseOwners": 1, "metadataGeneration": "Absent"}, "response": data, "responseVersion": responseVersion, "noRetry": true, "retryAt": "2026-10-28T12:00:00Z", "loaded": map[string]any{"requestVersion": loaded.Config.RequestVersion, "forceStepPlan": loaded.Config.ForceStepPlan, "stateBytes": loaded.Metrics.StateSize, "metadataSize": loaded.Metrics.MetadataSize, "metadataSizeLoaded": loaded.Metrics.MetadataSizeLoaded}, "stateLimit": nil, "queue": map[string]any{"prefix": redis_state.QueueDefaultKey, "mode": mode}, "idempotencyKey": loaded.Config.Idempotency, "incoming": "step", "priorityFactor": &priority, "identifierPriorityFactor": loaded.Config.PriorityFactor, "maxAttempts": fn.MaxAttempts(), "functionVersion": loaded.Config.FunctionVersion, "eventId": loaded.Config.EventIDs[0].String(), "eventIds": []string{loaded.Config.EventIDs[0].String()}}
			relay := startRecoveryNativeRelay(t, v.Addr, fault)
			input := map[string]any{"store": map[string]any{"address": relay.address(), "password": v.Password, "connectTimeoutMs": 500, "readTimeoutMs": 1000, "writeTimeoutMs": 500}, "state": map[string]any{"prefix": redis_state.StateDefaultKey, "layout": "AlwaysShard", "identity": map[string]any{"runId": id.RunID.String(), "functionId": id.FunctionID.String(), "accountId": id.Tenant.AccountID.String(), "envId": id.Tenant.EnvID.String(), "appId": id.Tenant.AppID.String()}}, "budgetMs": 5000, "disabledRetries": true, "operation": map[string]any{"kind": "response", "input": source}}
			var observe func()
			if fault == "after" {
				observe = func() {
					read := readNativeReplayStore(ctx, v, f)
					requireNativeReplayCheckpoint(t, f, read)
					require.Contains(t, read.Items, f.DiscoveryJob)
					require.NoError(t, ctx.Err())
				}
			}
			out := callRecoveryNative(t, ctx, input, relay, observe)
			require.NotNil(t, out.Save)
			require.Equal(t, "None", out.Save.Error)
			require.Equal(t, "Confirmed", out.Save.WriteStatus)
			require.False(t, out.Save.HasPending)
			require.NotNil(t, out.Enqueue)
			if fault == "" {
				require.Equal(t, "Success", out.Disposition)
				require.Equal(t, "Enqueued", out.Enqueue.Class)
			} else {
				require.Equal(t, "AlwaysRetry", out.Disposition)
				require.Equal(t, "Failed", out.Enqueue.Class)
				require.Equal(t, "MayHaveCommitted", out.Enqueue.WriteStatus)
				require.Nil(t, out.Enqueue.Result)
			}
			relay.mu.Lock()
			require.GreaterOrEqual(t, len(relay.scripts), 2)
			require.LessOrEqual(t, len(relay.scripts), 4)
			relay.mu.Unlock()
			drained := readNativeReplayStore(ctx, v, f)
			requireNativeReplayCheckpoint(t, f, drained)
			require.Len(t, drained.Items, 1)
			require.Contains(t, drained.Items, f.DiscoveryJob)
			var item queue.QueueItem
			require.NoError(t, json.Unmarshal([]byte(drained.Items[f.DiscoveryJob]), &item))
			require.Empty(t, item.Data.Metadata)
			require.Equal(t, out.GroupID, item.Data.GroupID)
			group, e := uuid.Parse(item.Data.GroupID)
			require.NoError(t, e)
			require.Equal(t, uuid.Version(4), group.Version())
			require.Equal(t, out.NowMS, item.WallTimeMS)
			a.close(t, ctx)
			a = nil
			// A has ceased before B opens fresh DB/Redis connections. No captured item is
			// injected, leased manually or sent to Execute; the normal scanner finds it.
			clockB := clockwork.NewFakeClockAt(now.Add(time.Minute))
			b := newRecoveryRuntime(t, ctx, v, directory, clockB, retryAt, keys)
			b.makeExecutor(t, ctx, b.queue)
			function, err := b.loader.LoadFunction(ctx, id.Tenant.EnvID, id.FunctionID)
			require.NoError(t, err)
			require.Equal(t, fn, *function.Function)
			persisted := readRecoveryCheckpoint(ctx, b.state, id)
			require.NoError(t, persisted.Err)
			recoveryAssertJSON(t, "checkpoint.state.json", persisted.JSON)
			results := b.start(t, ctx)
			recoveryEventually(t, clockB, func() bool { return len(results) > 0 })
			result := <-results
			require.NoError(t, result.Err)
			require.Equal(t, f.DiscoveryJob, result.JobID)
			require.Zero(t, result.Attempt)
			replay := takeNativeReplayObservation(t, requests)
			recoveryAssertRequest(t, "replay.request.json", replay.recoveryHTTPRequest, f.DiscoveryJob, 1)
			requireNativeReplayHTTPRequest(t, f, replay, drained.Items[f.DiscoveryJob])
			requireNativeReplayCheckpoint(t, f, takeNativeReplayObservation(t, atReplay))
			scope := queue.Scope{AccountID: id.Tenant.AccountID, EnvID: id.Tenant.EnvID, FunctionID: id.FunctionID}
			recoveryEventually(t, clockB, func() bool {
				jobs, e := b.queue.RunJobs(ctx, consts.DefaultQueueShardName, scope, id.RunID, 100, 0)
				exists, ee := b.state.Exists(ctx, id)
				return e == nil && ee == nil && len(jobs) == 0 && !exists
			})
			b.close(t, ctx)
			require.Empty(t, requests)
			require.Empty(t, atReplay)
			require.Empty(t, results)
			final := readNativeReplayStore(ctx, v, f)
			require.NoError(t, final.Err)
			require.Empty(t, final.Actions)
			require.Empty(t, final.Items)
			require.Empty(t, final.Metadata)
			require.Empty(t, final.Stack)
			require.Empty(t, final.Pending)
			for _, name := range []string{"run", "start", "edge", "ready", "backlog", "backlog_start"} {
				require.Empty(t, final.Indexes[name], name)
			}
			count, err := effects.Do(ctx, effects.B().Get().Key("fixture:external-effect").Build()).AsInt64()
			require.NoError(t, err)
			require.EqualValues(t, 1, count, "real replay must not repeat the non-deduplicated effect")
			require.Greater(t, final.Seen["seen_discovery"], int64(0))
			require.Equal(t, int64(-2), final.Seen["seen_original"])
			require.NoError(t, ctx.Err())
		})
	}
}
