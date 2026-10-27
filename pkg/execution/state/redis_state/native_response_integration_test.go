//go:build native_step_integration

package redis_state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	osqueue "github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/stretchr/testify/require"
)

// Construct a producer with only precreation/read ports: no executor, listener,
// tracing provider, worker, lease, SDK call, metadata generator or shared writer
// exists here. Counters/response are exclusively owned by this synchronous test.
// Executor-source qualification has its separate private owner-boundary witness.
func nativeResponseOperation(keys bool) map[string]any {
	old := nativeEnqueueOperation(QueueDefaultKey, keys)
	d := old["discovery"].(map[string]any)
	source := map[string]any{
		"domain":   map[string]any{"observerRegistrations": 0, "tracing": "Absent", "capacityLease": "Absent", "metadataWriters": 1, "responseOwners": 1, "metadataGeneration": "Absent"},
		"response": old["response"], "responseVersion": 3, "noRetry": false, "retryAt": nil,
		"loaded": map[string]any{"requestVersion": 2, "forceStepPlan": false, "stateBytes": 34, "metadataSize": 17, "metadataSizeLoaded": 6}, "stateLimit": nil,
	}
	for _, key := range []string{"queue", "idempotencyKey", "incoming", "priorityFactor", "identifierPriorityFactor", "maxAttempts", "functionVersion", "eventId", "eventIds"} {
		source[key] = d[key]
	}
	return map[string]any{"kind": "response", "input": source}
}
func nativeNested(t *testing.T, out map[string]json.RawMessage, key string) map[string]json.RawMessage {
	t.Helper()
	var v map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(out[key], &v))
	return v
}
func nativeResponseField(t *testing.T, out map[string]json.RawMessage, key, want string) {
	t.Helper()
	require.JSONEq(t, strconv.Quote(want), string(out[key]), key)
}
func TestNativeRustResponseOwner(t *testing.T) {
	f := loadNativeEffects(t)
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	_, writer := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
	for _, keysMode := range []bool{false, true} {
		for _, scenario := range []string{"fresh", "idempotent", "pending", "idempotent-pending", "conflict", "save-before", "save-after", "save-after-retry", "queue-before", "queue-after", "queue-error", "negative-delta", "zero-delta", "no-retry", "retry-at", "save-error", "absent-limit", "zero-limit", "overflow", "bad-output"} {
			t.Run(fmt.Sprintf("keys=%t/%s", keysMode, scenario), func(t *testing.T) {
				ctx := t.Context()
				require.NoError(t, writer.Do(ctx, writer.B().Flushdb().Build()).Error())
				svc := nativeAdapter(t, writer, StateDefaultKey, false)
				md := nativeMetadata()
				md.Config.SpanID = ""
				md.Metrics.MetadataSize = 6
				md.Metrics.MetadataSizeLoaded = 6
				created, err := svc.Create(ctx, sv2.CreateState{Metadata: md, Events: []json.RawMessage{json.RawMessage(f.InitialEvents[1 : len(f.InitialEvents)-1])}})
				require.NoError(t, err)
				stateKeys := nativeSaveKeys(f, StateDefaultKey, false)
				require.NoError(t, writer.Do(ctx, writer.B().Hset().Key(stateKeys[1]).FieldValue().FieldValue("metadata_size", "6").Build()).Error())
				// All setup ends before the one helper operation, including fault setup.
				if scenario == "idempotent" || scenario == "idempotent-pending" || scenario == "conflict" {
					output := f.Output
					if scenario == "conflict" {
						output = f.Conflict
					}
					_, err = svc.SaveStep(ctx, md.ID, "s", []byte(output))
					require.NoError(t, err)
				}
				if scenario == "pending" || scenario == "idempotent-pending" || scenario == "conflict" {
					require.NoError(t, svc.SavePending(ctx, md.ID, []string{"s", "held"}))
				}
				op := nativeResponseOperation(keysMode)
				source := op["input"].(map[string]any)
				loaded := source["loaded"].(map[string]any)
				loaded["stateBytes"] = created.Metadata.Metrics.StateSize
				if scenario == "negative-delta" {
					loaded["metadataSize"] = 2
				}
				if scenario == "zero-delta" {
					loaded["metadataSize"] = 6
				}
				source["noRetry"] = true
				source["retryAt"] = "2026-10-28T12:00:00Z"
				if scenario == "retry-at" || scenario == "save-error" {
					source["noRetry"] = false
				}
				if scenario == "save-error" {
					source["retryAt"] = nil
				}
				if scenario == "absent-limit" || scenario == "zero-limit" {
					loaded["stateBytes"] = 32 * 1024 * 1024
					require.NoError(t, writer.Do(ctx, writer.B().Hset().Key(stateKeys[1]).FieldValue().FieldValue("state_size", "33554432").Build()).Error())
					if scenario == "zero-limit" {
						source["stateLimit"] = 0
					}
				}
				if scenario == "overflow" {
					source["stateLimit"] = 1
				}
				if scenario == "bad-output" {
					source["response"] = nativeByteArray([]byte(`[{"op":"StepRun","id":"s","data":}]`))
				}
				// Real WRONGTYPE causes Lua failure without manufacturing an adapter answer.
				if scenario == "queue-error" {
					require.NoError(t, writer.Do(ctx, writer.B().Set().Key("{"+QueueDefaultKey+"}:queue:item").Value("wrong type").Build()).Error())
				}
				if scenario == "no-retry" || scenario == "retry-at" || scenario == "save-error" {
					require.NoError(t, writer.Do(ctx, writer.B().Set().Key(stateKeys[0]).Value("wrong type").Build()).Error())
				}
				require.NoError(t, writer.Do(ctx, writer.B().ScriptLoad().Script(nativeScript(t, f.Scripts["saveResponse"])).Build()).Error())
				require.NoError(t, writer.Do(ctx, writer.B().ScriptLoad().Script(nativeScript(t, f.Scripts["queue/enqueue"])).Build()).Error())
				mode := ""
				switch scenario {
				case "save-before":
					mode = "before"
				case "save-after", "save-after-retry":
					mode = "after"
				case "queue-before":
					mode = "queue-before"
				case "queue-after":
					mode = "queue-after"
				}
				relay := startNativeRelay(t, v.Addr, mode)
				input := nativeInput(relay.listener.Addr().String(), v.Password, StateDefaultKey, false, op)
				input["disabledRetries"] = scenario != "save-after-retry"
				before := snapshotNativeStore(t, ctx, writer)
				var observe func()
				if scenario == "save-after" || scenario == "save-after-retry" || scenario == "queue-after" {
					observe = func() {
						fresh, r := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
						steps, e := fresh.LoadSteps(ctx, md.ID)
						require.NoError(t, e)
						require.Equal(t, f.Output, string(steps["s"]))
						loaded, e := fresh.LoadMetadata(ctx, md.ID)
						require.NoError(t, e)
						require.Equal(t, 17, loaded.Metrics.MetadataSize)
						snap := snapshotNativeStore(t, ctx, r)
						if scenario == "queue-after" {
							require.Contains(t, snap, "{"+QueueDefaultKey+"}:queue:item")
						} else {
							require.NotContains(t, snap, "{"+QueueDefaultKey+"}:queue:item")
						}
						require.NoError(t, ctx.Err(), "real ACK loss while caller is live")
					}
				}
				lo := time.Now().UnixMilli()
				out := invokeNative(t, input, relay, observe)
				hi := time.Now().UnixMilli()
				t.Logf("outcome=%s", mustNativeJSON(t, out))
				if scenario == "overflow" || scenario == "zero-limit" || scenario == "bad-output" {
					disposition := "StateOverflow"
					if scenario == "bad-output" {
						disposition = "NeverRetry"
					}
					nativeResponseField(t, out, "disposition", disposition)
					require.Equal(t, "null", string(out["save"]))
					require.Equal(t, "null", string(out["groupId"]))
					require.Equal(t, "null", string(out["nowMs"]))
					require.Equal(t, "6", string(out["metadataSizeLoaded"]))
					requireNativeSnapshot(t, before, snapshotNativeStore(t, ctx, writer))
					commands, _ := relay.inventory()
					require.Empty(t, commands)
					return
				}
				saved := nativeNested(t, out, "save")
				saves, enqueues := 0, 0
				commands, _ := relay.inventory()
				for _, c := range commands {
					if c.name == "AUTH" {
						continue
					}
					if c.name == "HGET" {
						continue
					}
					require.Equal(t, "EVALSHA", c.name)
					switch string(c.args[1]) {
					case "5":
						saves++
						require.Equal(t, []byte("s"), c.args[7])
						delta := "11"
						if scenario == "negative-delta" || scenario == "zero-delta" {
							delta = "0"
						}
						require.Equal(t, []byte(delta), c.args[9])
					case "22":
						enqueues++
					default:
						t.Fatal("unexpected script")
					}
				}
				wantSaves := 1
				if scenario == "save-after-retry" {
					wantSaves = 2
				}
				require.Equal(t, wantSaves, saves, "native adapter retry mode")
				require.Equal(t, strconv.Itoa(loaded["metadataSize"].(int)), string(out["metadataSizeLoaded"]), "swap consumes at save boundary even on failed save")
				saveFailure := scenario == "save-before" || scenario == "save-after" || scenario == "no-retry" || scenario == "retry-at" || scenario == "save-error"
				if saveFailure {
					policy := "NeverRetry"
					if scenario == "save-error" {
						policy = "Error"
					}
					if scenario == "retry-at" {
						policy = "RetryAt"
					}
					nativeResponseField(t, out, "disposition", policy)
					require.Equal(t, "null", string(out["groupId"]))
					require.Equal(t, "null", string(out["nowMs"]))
					require.Zero(t, enqueues)
					nativeResponseField(t, saved, "error", "ErrMaxAttemptReached")
					if scenario == "save-after" {
						nativeResponseField(t, saved, "writeStatus", "MayHaveCommitted")
					}
				} else {
					var group string
					require.NoError(t, json.Unmarshal(out["groupId"], &group))
					g, e := uuid.Parse(group)
					require.NoError(t, e)
					require.Equal(t, uuid.Version(4), g.Version())
					var now int64
					require.NoError(t, json.Unmarshal(out["nowMs"], &now))
					require.GreaterOrEqual(t, now, lo)
					require.LessOrEqual(t, now, hi)
					pending := scenario == "pending" || scenario == "idempotent-pending"
					require.Equal(t, strconv.FormatBool(pending), string(saved["hasPending"]))
					if pending {
						require.Zero(t, enqueues)
						require.Equal(t, "null", string(out["enqueue"]))
						nativeResponseField(t, out, "disposition", "Success")
					} else {
						require.Equal(t, 1, enqueues, "no queue retry inside invocation")
						queued := nativeNested(t, out, "enqueue")
						if strings.HasPrefix(scenario, "queue-") {
							nativeResponseField(t, out, "disposition", "AlwaysRetry")
							nativeResponseField(t, queued, "class", "Failed")
							require.Equal(t, "null", string(out["retryAt"]))
						} else {
							nativeResponseField(t, out, "disposition", "Success")
							nativeResponseField(t, queued, "class", "Enqueued")
						}
					}
					if scenario == "conflict" {
						nativeResponseField(t, saved, "error", "ErrDuplicateResponse")
						nativeResponseField(t, saved, "writeStatus", "NotApplied")
					}
					if scenario == "save-after-retry" {
						nativeResponseField(t, saved, "error", "None")
						require.JSONEq(t, `[[-2,0]]`, string(saved["results"]))
					}
					if scenario == "idempotent" || scenario == "idempotent-pending" {
						nativeResponseField(t, saved, "error", "ErrIdempotentResponse")
					}
				}
				if scenario != "no-retry" && scenario != "retry-at" && scenario != "save-error" {
					fresh, r := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
					steps, e := fresh.LoadSteps(ctx, md.ID)
					require.NoError(t, e)
					if scenario == "save-before" {
						require.Empty(t, steps)
					} else {
						want := f.Output
						if scenario == "conflict" {
							want = f.Conflict
						}
						require.Equal(t, want, string(steps["s"]))
					}
					if !saveFailure {
						m, e := fresh.LoadMetadata(ctx, md.ID)
						require.NoError(t, e)
						want := 17
						if scenario == "idempotent" || scenario == "idempotent-pending" || scenario == "conflict" || scenario == "negative-delta" || scenario == "zero-delta" {
							want = 6
						}
						require.Equal(t, want, m.Metrics.MetadataSize)
					}
					if scenario == "fresh" {
						// Full durable comparison against the real Go store/queue owners, using
						// only post-effect UUID/time observations to bind generated fields.
						got := snapshotNativeStore(t, ctx, r)
						require.NoError(t, writer.Do(ctx, writer.B().Flushdb().Build()).Error())
						_, e = svc.Create(ctx, sv2.CreateState{Metadata: md, Events: []json.RawMessage{json.RawMessage(f.InitialEvents[1 : len(f.InitialEvents)-1])}})
						require.NoError(t, e)
						require.NoError(t, writer.Do(ctx, writer.B().Hset().Key(stateKeys[1]).FieldValue().FieldValue("metadata_size", "6").Build()).Error())
						_, e = svc.SaveStep(state.WithMetadataSizeDelta(ctx, 11), md.ID, "s", []byte(f.Output))
						require.NoError(t, e)
						var literal osqueue.QueueItem
						require.NoError(t, json.Unmarshal(f.Queue.Item, &literal))
						var group string
						require.NoError(t, json.Unmarshal(out["groupId"], &group))
						var now int64
						require.NoError(t, json.Unmarshal(out["nowMs"], &now))
						literal.Data.GroupID = group
						literal.Data.Metadata = nil
						job := f.Queue.JobID
						literal.Data.JobID = &job
						producer, _ := nativeQueueProducer(t, writer, QueueDefaultKey, keysMode, now)
						require.NoError(t, producer.Enqueue(ctx, literal.Data, time.UnixMilli(now), osqueue.EnqueueOpts{}))
						requireNativeSnapshot(t, snapshotNativeStore(t, ctx, r), got)
					}
				}
				if scenario == "save-after" || scenario == "queue-after" || scenario == "queue-before" {
					// A separate, explicit test invocation is the only later retry. It consumes
					// a fresh loaded snapshot. It is not worker/crash recovery or fencing.
					retry := nativeResponseOperation(keysMode)
					retrySource := retry["input"].(map[string]any)
					later, e := svc.LoadMetadata(ctx, md.ID)
					require.NoError(t, e)
					retrySource["loaded"].(map[string]any)["stateBytes"] = later.Metrics.StateSize
					retrySource["loaded"].(map[string]any)["metadataSize"] = later.Metrics.MetadataSize
					retrySource["loaded"].(map[string]any)["metadataSizeLoaded"] = later.Metrics.MetadataSizeLoaded
					retried := invokeNative(t, nativeInput(v.Addr, v.Password, StateDefaultKey, false, retry), nil, nil)
					nativeResponseField(t, retried, "disposition", "Success")
					nativeResponseField(t, nativeNested(t, retried, "save"), "error", "ErrIdempotentResponse")
					want := "Enqueued"
					if scenario == "queue-after" {
						want = "Duplicate"
					}
					nativeResponseField(t, nativeNested(t, retried, "enqueue"), "class", want)
					m, e := svc.LoadMetadata(ctx, md.ID)
					require.NoError(t, e)
					require.Equal(t, 17, m.Metrics.MetadataSize)
					require.Equal(t, 1, m.Metrics.StepCount)
				}
			})
		}
	}
}
func mustNativeJSON(t *testing.T, v any) string {
	t.Helper()
	b, e := json.Marshal(v)
	require.NoError(t, e)
	return string(b)
}

// Positive owner cases above and the real artifact preflight keep these controls
// from passing merely because the helper cannot run. Every case changes one
// capability on an otherwise valid descriptor and observes zero actual sockets.
func TestNativeRustResponseAdmission(t *testing.T) {
	probe := nativePreparation("", 0)
	probe["kind"] = "prepare"
	out := invokeNative(t, nativeInput("127.0.0.1:1", "unused", StateDefaultKey, false, probe), nil, nil)
	nativeResponseField(t, out, "step", "s")
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	for _, name := range []string{"observers", "tracing", "capacity", "metadata-writer", "response-owner", "generation", "loaded-minus-one", "force-plan", "batch", "race", "sdk-metadata", "experiment", "ai", "prefix", "queue-prefix", "negative-counter", "priority-overflow", "prepared-output", "save-answer", "pending-answer", "enqueue-permission", "group", "carrier", "clock", "recipe", "concurrency", "semaphore", "singleton", "throttle", "queue-override", "routing", "replay"} {
		t.Run(name, func(t *testing.T) {
			relay := startNativeRelay(t, v.Addr, "")
			op := nativeResponseOperation(false)
			source := op["input"].(map[string]any)
			domain := source["domain"].(map[string]any)
			loaded := source["loaded"].(map[string]any)
			strict := false
			switch name {
			case "observers":
				domain["observerRegistrations"] = 1
			case "tracing":
				domain["tracing"] = "Present"
			case "capacity":
				domain["capacityLease"] = "Present"
			case "metadata-writer":
				domain["metadataWriters"] = 2
			case "response-owner":
				domain["responseOwners"] = 2
			case "generation":
				domain["metadataGeneration"] = "Present"
			case "loaded-minus-one":
				loaded["requestVersion"] = -1
			case "force-plan":
				loaded["forceStepPlan"] = true
			case "batch":
				source["eventIds"] = []string{"01ARZ3NDEKTSV4RRFFQ69G5FAW", "01ARZ3NDEKTSV4RRFFQ69G5FAX"}
			case "race":
				source["response"] = nativeByteArray([]byte(`[{"op":"StepRun","id":"s","opts":{"parallelMode":"race"}}]`))
			case "sdk-metadata":
				source["response"] = nativeByteArray([]byte(`[{"op":"StepRun","id":"s","metadata":[{"scope":"step","kind":"userland.custom","op":"merge","values":{"k":"v"}}]}]`))
			case "experiment":
				source["response"] = nativeByteArray([]byte(`[{"op":"StepRun","id":"s","opts":{"experimentName":"x","variant":"a","selectionStrategy":"weighted"}}]`))
			case "ai":
				source["response"] = nativeByteArray([]byte(`[{"op":"StepRun","id":"s","data":{"totalUsage":{"inputTokens":1}}}]`))
			case "queue-prefix":
				source["queue"].(map[string]any)["prefix"] = "bad{prefix}"
			case "negative-counter":
				loaded["metadataSize"] = -1
			case "priority-overflow":
				source["priorityFactor"] = int64(9223372036854775807)
			case "prefix": // set on the actual state input below
			default:
				strict = true
				fields := map[string]string{"prepared-output": "preparedOutput", "save-answer": "saveCompleted", "pending-answer": "hasPending", "enqueue-permission": "shouldEnqueue", "group": "groupId", "carrier": "carrier", "clock": "nowMs", "recipe": "keys", "concurrency": "customConcurrencyKeys", "semaphore": "semaphores", "singleton": "singleton", "throttle": "throttle", "queue-override": "queueName", "routing": "forceQueueShardName", "replay": "replayId"}
				source[fields[name]] = true
			}
			input := nativeInput(relay.listener.Addr().String(), v.Password, StateDefaultKey, false, op)
			if name == "prefix" {
				input["state"].(map[string]any)["prefix"] = "bad{prefix}"
			}
			if strict {
				raw, e := json.Marshal(input)
				require.NoError(t, e)
				cmd := exec.Command(os.Getenv("NATIVE_STEP_TEST_BINARY"))
				cmd.Stdin = bytes.NewReader(raw)
				out, e := cmd.Output()
				var exit *exec.ExitError
				require.ErrorAs(t, e, &exit)
				require.Equal(t, 2, exit.ExitCode())
				require.Equal(t, "invalid helper input\n", string(exit.Stderr))
				require.Empty(t, out)
			} else {
				out := invokeNative(t, input, nil, nil)
				require.NotEqual(t, "null", string(out["preparationError"]))
				require.Equal(t, "null", string(out["save"]))
				require.Equal(t, "null", string(out["groupId"]))
				require.Equal(t, "null", string(out["nowMs"]))
			}
			commands, _ := relay.inventory()
			require.Empty(t, commands)
			relay.mu.Lock()
			require.Empty(t, relay.connections, "including AUTH/handshake")
			relay.mu.Unlock()
		})
	}
}
