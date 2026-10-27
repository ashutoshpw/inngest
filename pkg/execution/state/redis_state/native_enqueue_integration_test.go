//go:build native_step_integration

package redis_state

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	osqueue "github.com/inngest/inngest/pkg/execution/queue"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/stretchr/testify/require"
)

func nativeEnqueueOperation(qprefix string, keys bool) map[string]any {
	op := nativePreparation("9007199254740993", 0)
	op["kind"] = "enqueue"
	mode := "Partition"
	if keys {
		mode = "Key"
	}
	op["discovery"] = map[string]any{
		"queue": map[string]any{"prefix": qprefix, "mode": mode}, "saveCompleted": true, "hasPending": false,
		"idempotencyKey": "run-key", "incoming": "function", "groupId": "00000000-0000-4000-8000-000000000005",
		"carrier": `{"tp":"00-0123456789abcdef0123456789abcdef-0123456789abcdef-01","ts":"","dstp":"00-0123456789abcdef0123456789abcdef-0000000000000000-00","dsid":"0123456789abcdef"}`,
		"nowMs":   int64(1793016000000), "priorityFactor": 7, "identifierPriorityFactor": nil, "maxAttempts": 3, "functionVersion": 0,
		"eventId": "01ARZ3NDEKTSV4RRFFQ69G5FAW", "eventIds": []string{"01ARZ3NDEKTSV4RRFFQ69G5FAW"},
	}
	return op
}

func assertNativeEnqueueWire(t *testing.T, relay *nativeRelay, f nativeEffectsFixture, prefix string, keys bool, names []string) {
	t.Helper()
	commands, _ := relay.inventory()
	got := []string{}
	for _, c := range commands {
		if c.name == "AUTH" {
			continue
		}
		if c.name == "HGET" {
			require.Equal(t, [][]byte{[]byte(nativeQueueKeys(f.Queue, prefix, keys)[0]), []byte(f.Queue.HashID)}, c.args)
			continue
		}
		require.Contains(t, []string{"EVALSHA", "EVAL"}, c.name, "no SCRIPT LOAD or other native write")
		got = append(got, c.name)
		script := f.Scripts["queue/enqueue"].SHA1
		if c.name == "EVAL" {
			script = nativeScript(t, f.Scripts["queue/enqueue"])
		}
		expected := [][]byte{[]byte(script), []byte("22")}
		for _, k := range nativeQueueKeys(f.Queue, prefix, keys) {
			expected = append(expected, []byte(k))
		}
		for _, a := range nativeQueueArgs(t, f.Queue, keys, compactNativeLiteral(t, f.Queue.Item)) {
			expected = append(expected, []byte(a))
		}
		require.Equal(t, expected, c.args, "independent literal item, all KEYS and ARGV")
	}
	require.Equal(t, names, got, "single attempt, exact NOSCRIPT fallback only")
}

func TestNativeRustEnqueueInterop(t *testing.T) {
	f := loadNativeEffects(t)
	body := nativeScript(t, f.Scripts["queue/enqueue"])
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	_, writer := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
	for _, keys := range []bool{false, true} {
		for _, sharded := range []bool{false, true} {
			for _, sp := range []string{StateDefaultKey, "native:state"} {
				for _, qp := range []string{QueueDefaultKey, "native:queue"} {
					for _, scenario := range []string{"fresh-cold", "before", "after", "fallback-id"} {
						t.Run(fmt.Sprintf("keys=%t/sharded=%t/%s/%s/%s", keys, sharded, sp, qp, scenario), func(t *testing.T) {
							ctx := t.Context()
							require.NoError(t, writer.Do(ctx, writer.B().Flushdb().Build()).Error())
							setup := nativeAdapter(t, writer, sp, sharded)
							_, err := setup.Create(ctx, sv2.CreateState{Metadata: nativeMetadata(), Events: []json.RawMessage{json.RawMessage(f.InitialEvents[1 : len(f.InitialEvents)-1])}})
							require.NoError(t, err)
							pre := snapshotNativeStore(t, ctx, writer)
							saved := invokeNative(t, nativeInput(v.Addr, v.Password, sp, sharded, nativePreparation("9007199254740993", 0)), nil, nil)
							require.JSONEq(t, `"None"`, string(saved["error"]))
							require.Equal(t, "false", string(saved["hasPending"]))
							expected := expectedNativeSaveSnapshot(t, f, nativeSaveCase{StateSizeDelta: 25, StepCount: 1, Stack: []string{"s"}}, nativeSaveKeys(f, sp, sharded), pre)
							requireNativeSnapshot(t, expected, snapshotNativeStore(t, ctx, writer))
							request := nativeEnqueueOperation(qp, keys)
							caseFixture := f
							if scenario == "fallback-id" {
								request["discovery"].(map[string]any)["idempotencyKey"] = ""
								caseFixture.Queue = f.Queue
								caseFixture.Queue.Keys = slices.Clone(f.Queue.Keys)
								hash := f.Queue.HashAnswers[f.Queue.FallbackJobID]
								caseFixture.Queue.HashID = hash
								for i, k := range caseFixture.Queue.Keys {
									caseFixture.Queue.Keys[i] = strings.ReplaceAll(k, f.Queue.HashID, hash)
								}
								caseFixture.Queue.Item = bytes.ReplaceAll(f.Queue.Item, []byte(f.Queue.HashID), []byte(hash))
							}
							mode := ""
							if scenario == "before" || scenario == "after" {
								mode = scenario
							}
							names := []string{"EVALSHA"}
							if scenario == "fresh-cold" {
								require.NoError(t, writer.Do(ctx, writer.B().ScriptFlush().Build()).Error())
								names = append(names, "EVAL")
							} else {
								require.NoError(t, writer.Do(ctx, writer.B().ScriptLoad().Script(body).Build()).Error())
							}
							committed := nativeSnapshot{}
							for k, v := range expected {
								committed[k] = v
							}
							for k, v := range expectedNativeQueueSnapshot(t, caseFixture, qp, keys, compactNativeLiteral(t, caseFixture.Queue.Item)) {
								committed[k] = v
							}
							relay := startNativeRelay(t, v.Addr, mode)
							var observe func()
							if mode == "after" {
								observe = func() {
									_, reader := freshNativeRead(t, v.Addr, v.Password, sp, sharded)
									requireNativeSnapshot(t, committed, snapshotNativeStore(t, ctx, reader))
								}
							}
							out := invokeNative(t, nativeInput(relay.listener.Addr().String(), v.Password, sp, sharded, request), relay, observe)
							assertNativeEnqueueWire(t, relay, caseFixture, qp, keys, names)
							if mode != "" {
								require.JSONEq(t, `"Failed"`, string(out["class"]))
								require.JSONEq(t, `"MayHaveCommitted"`, string(out["writeStatus"]))
								require.Equal(t, "null", string(out["result"]))
							} else {
								require.JSONEq(t, `"Enqueued"`, string(out["class"]))
								require.Equal(t, "0", string(out["result"]))
							}
							if mode == "before" {
								committed = expected
							}
							fresh, reader := freshNativeRead(t, v.Addr, v.Password, sp, sharded)
							requireNativeSnapshot(t, committed, snapshotNativeStore(t, ctx, reader))
							steps, err := fresh.LoadSteps(ctx, nativeMetadata().ID)
							require.NoError(t, err)
							require.Equal(t, f.Output, string(steps["s"]))
							if mode != "" {
								return
							}
							_, shard := nativeQueueProducer(t, reader, qp, keys, f.Queue.NowMS)
							loaded, err := shard.LoadQueueItem(ctx, caseFixture.Queue.HashID)
							require.NoError(t, err)
							require.Equal(t, caseFixture.Queue.HashID, *loaded.Data.JobID)
							// A different candidate run with the same logical id must report the live
							// item's current RunID, not merely echo the native request's RunID.
							dupInput := nativeInput(v.Addr, v.Password, sp, sharded, request)
							dupInput["state"].(map[string]any)["identity"].(map[string]any)["runId"] = "01ARZ3NDEKTSV4RRFFQ69G5FAW"
							if scenario == "fallback-id" {
								dupInput = nativeInput(v.Addr, v.Password, sp, sharded, request)
							}
							dup := invokeNative(t, dupInput, nil, nil)
							require.JSONEq(t, `"Duplicate"`, string(dup["class"]))
							require.JSONEq(t, strconv.Quote("01ARZ3NDEKTSV4RRFFQ69G5FAV"), string(dup["existingRunId"]))
							requireNativeSnapshot(t, committed, snapshotNativeStore(t, ctx, reader))
							period := time.Minute
							loaded.IdempotencyPeriod = &period
							require.NoError(t, shard.Dequeue(ctx, *loaded))
							tombBefore := snapshotNativeStore(t, ctx, reader)
							tomb := invokeNative(t, nativeInput(v.Addr, v.Password, sp, sharded, request), nil, nil)
							require.JSONEq(t, `"Duplicate"`, string(tomb["class"]))
							require.Equal(t, "null", string(tomb["existingRunId"]))
							tombAfter := snapshotNativeStore(t, ctx, reader)
							seenKey := nativeQueueKeys(caseFixture.Queue, qp, keys)[5]
							seen := tombBefore[seenKey]
							require.Greater(t, tombAfter[seenKey].PTTL, int64(0))
							require.LessOrEqual(t, tombAfter[seenKey].PTTL, seen.PTTL)
							require.InDelta(t, seen.PTTL, tombAfter[seenKey].PTTL, 5000)
							seen.PTTL = tombAfter[seenKey].PTTL
							tombBefore[seenKey] = seen
							requireNativeSnapshot(t, tombBefore, tombAfter)
						})
					}
				}
			}
		}
	}
}

func TestNativeRustEnqueueRejectsBeforeSocket(t *testing.T) {
	// Validate the artifact and a successful request before testing rejection:
	// a missing executable or a helper that only fails must not pass this table.
	probe := nativePreparation("", 0)
	probe["kind"] = "prepare"
	prepared := invokeNative(t, nativeInput("127.0.0.1:1", "unused", StateDefaultKey, false, probe), nil, nil)
	require.JSONEq(t, `"s"`, string(prepared["step"]))
	var output []byte
	require.NoError(t, json.Unmarshal(prepared["output"], &output))
	require.Equal(t, `{"data":null}`, string(output))

	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	for _, name := range []string{"pending", "unsaved", "race", "force-plan", "failure", "planned", "parallel", "coalescing", "concurrency", "semaphore", "throttle", "singleton", "queue-override", "normalization", "promotion", "capacity", "routing", "recipe", "multiple-events", "batch", "batch-null"} {
		t.Run(name, func(t *testing.T) {
			relay := startNativeRelay(t, v.Addr, "")
			op := nativeEnqueueOperation(QueueDefaultKey, false)
			d := op["discovery"].(map[string]any)
			switch name {
			case "multiple-events":
				d["eventIds"] = []string{"01ARZ3NDEKTSV4RRFFQ69G5FAW", "01ARZ3NDEKTSV4RRFFQ69G5FAX"}
			case "batch", "batch-null":
				d["batchId"] = nil
				if name == "batch" {
					d["batchId"] = "01ARZ3NDEKTSV4RRFFQ69G5FAX"
				}
			case "pending":
				d["hasPending"] = true
			case "unsaved":
				d["saveCompleted"] = false
			case "force-plan":
				op["config"].(map[string]any)["forceStepPlan"] = true
			case "race":
				op["response"] = nativeByteArray([]byte(`[{"op":"StepRun","id":"s","opts":{"parallelMode":"race"}}]`))
			case "parallel":
				op["response"] = nativeByteArray([]byte(`[{"op":"StepRun","id":"s"},{"op":"StepRun","id":"sibling"}]`))
			case "failure", "planned":
				op["response"] = nativeByteArray([]byte(`[{"op":"StepPlanned","id":"s"}]`))
				if name == "failure" {
					op["response"] = nativeByteArray([]byte(`[{"op":"StepError","id":"s"}]`))
				}
			default:
				fields := map[string]string{"coalescing": "parallelCoalesceKey", "concurrency": "customConcurrencyKeys", "semaphore": "semaphores", "throttle": "throttle", "singleton": "singleton", "queue-override": "queueName", "normalization": "normalizeFromBacklogId", "promotion": "promotion", "capacity": "capacityLease", "routing": "forceQueueShardName", "recipe": "keys"}
				d[fields[name]] = true // The strict descriptor rejects unsupported extensions.
			}
			raw, err := json.Marshal(nativeInput(relay.listener.Addr().String(), v.Password, StateDefaultKey, false, op))
			require.NoError(t, err)
			cmd := exec.Command(os.Getenv("NATIVE_STEP_TEST_BINARY"))
			cmd.Stdin = bytes.NewReader(raw)
			out, err := cmd.Output()
			if slices.Contains([]string{"pending", "unsaved", "force-plan", "failure", "planned", "race", "parallel", "multiple-events"}, name) {
				require.NoError(t, err)
				require.Contains(t, string(out), `"preparationError":"Ineligible"`)
			} else {
				var exitErr *exec.ExitError
				require.ErrorAs(t, err, &exitErr, "descriptor rejection must come from the helper")
				require.Equal(t, 2, exitErr.ExitCode(), "helper input rejection exit status")
				require.Equal(t, "invalid helper input\n", string(exitErr.Stderr))
				require.Empty(t, out)
			}
			commands, _ := relay.inventory()
			require.Empty(t, commands)
			relay.mu.Lock()
			require.Empty(t, relay.connections, "no native socket, including handshake")
			relay.mu.Unlock()
		})
	}
}

// Compare actual Go owner writes, including the Lua update path, without changing
// the accepted zero-version fixture or deriving expectations from native output.
func TestNativeRustEnqueueOwnerParity(t *testing.T) {
	f := loadNativeEffects(t)
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	_, client := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
	ctx := t.Context()
	for _, keys := range []bool{false, true} {
		for _, carrier := range []bool{true, false} {
			t.Run(fmt.Sprintf("keys=%t/carrier=%t", keys, carrier), func(t *testing.T) {
				var want []nativeSnapshot
				for _, native := range []bool{false, true} {
					require.NoError(t, client.Do(ctx, client.B().Flushdb().Build()).Error())
					producer, _ := nativeQueueProducer(t, client, QueueDefaultKey, keys, f.Queue.NowMS)
					// The third item is older: shadow keeps newest version, backlog keeps earliest.
					for i, version := range []int{7, 11, 9} {
						key := fmt.Sprintf("version-%d", version)
						if native {
							op := nativeEnqueueOperation(QueueDefaultKey, keys)
							d := op["discovery"].(map[string]any)
							if !carrier {
								delete(d, "carrier")
							}
							d["functionVersion"], d["idempotencyKey"] = version, key
							out := invokeNative(t, nativeInput(v.Addr, v.Password, StateDefaultKey, false, op), nil, nil)
							require.JSONEq(t, `"Enqueued"`, string(out["class"]))
						} else {
							var item osqueue.QueueItem
							require.NoError(t, json.Unmarshal(f.Queue.Item, &item))
							item.Data.Identifier.WorkflowVersion = version
							if !carrier {
								item.Data.Metadata = nil
							}
							job := "00000000-0000-0000-0000-000000000001:" + key + "-s"
							item.Data.JobID = &job
							require.NoError(t, producer.Enqueue(ctx, item.Data, time.UnixMilli(f.Queue.NowMS), osqueue.EnqueueOpts{}))
						}
						got := snapshotNativeStore(t, ctx, client)
						if native {
							requireNativeSnapshot(t, want[i], got)
						} else {
							want = append(want, got)
							itemKey := "{" + QueueDefaultKey + "}:queue:item"
							for _, raw := range got[itemKey].Value.(map[string]string) {
								if !carrier {
									require.NotContains(t, raw, `"metadata"`, "Go omits an absent carrier")
								}
							}
							if keys {
								var shadow, backlog map[string]any
								require.NoError(t, json.Unmarshal([]byte(got["{"+QueueDefaultKey+"}:shadows"].Value.(map[string]string)["00000000-0000-0000-0000-000000000001"]), &shadow))
								require.NoError(t, json.Unmarshal([]byte(got["{"+QueueDefaultKey+"}:backlogs"].Value.(map[string]string)["fn:00000000-0000-0000-0000-000000000001"]), &backlog))
								latest := 11
								if i == 0 {
									latest = 7
								}
								require.EqualValues(t, latest, shadow["fv"])
								require.EqualValues(t, 7, backlog["fv"])
							}
						}
					}
				}
			})
		}
	}
}

func TestNativeRustEnqueueDuplicateDecode(t *testing.T) {
	f := loadNativeEffects(t)
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	_, client := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
	_, shard := nativeQueueProducer(t, client, QueueDefaultKey, false, f.Queue.NowMS)
	// Literal changes target the actual Go LoadQueueItem / Item.UnmarshalJSON owner.
	cases := []struct {
		name, path, value string
		bad               bool
	}{
		{"edge-array", "payload", `[]`, true},
		{"edge-string", "payload", `"bad"`, true},
		{"nested-edge-array", "payload", `{"edge":[]}`, true},
		{"attempt-string", "atts", `"bad"`, true},
		{"metadata-array", "metadata", `[]`, true},
		{"null-payload", "payload", `null`, false},
		{"empty-payload", "payload", `{}`, false},
		{"unknown-payload-field", "payload", `{"extra":[1],"edge":{"incoming":"function","outgoing":"s","future":true}}`, false},
		{"arbitrary-metadata", "metadata", `{"future":[true,3,null,{"x":"y"}]}`, false},
		{"null-attempt", "atts", `null`, false},
		{"null-edge", "payload", `{"edge":null}`, false},
		{"missing-payload", "payload", ``, false},
		{"bad-edge-field", "payload", `{"edge":{"incoming":17}}`, true},
		{"bad-identifier", "identifier", `[]`, true},
		{"missing-run", "identifier", `{}`, false},
		{"lowercase-run", "identifier", `{"runID":"01arz3ndektsv4rrffq69g5fav"}`, false},
		{"lenient-run", "identifier", `{"runID":"01ARZ3NDEKTSV4RRFFQ69G5FA!"}`, false},
		{"bad-workspace", "wsID", `"bad"`, true},
		{"raw-uuid", "wsID", `"00000000000000000000000000000002"`, false},
		{"upper-parallel", "pm", `"WAIT"`, false},
		{"null-parallel", "pm", `null`, true},
		{"singleton", "singleton", `{"k":"key","m":"skip"}`, false},
		{"bad-singleton", "singleton", `{"m":12}`, true},
		{"bad-outer-time", "@at", `"bad"`, true},
		{"null-outer-time", "@at", `null`, false},
		{"bad-lease", "@leaseID", `"bad"`, true},
		{"null-data", "@data", `null`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			require.NoError(t, client.Do(ctx, client.B().Flushdb().Build()).Error())
			var item map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(f.Queue.Item, &item))
			var data map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(item["data"], &data))
			if strings.HasPrefix(tc.path, "@") {
				item[strings.TrimPrefix(tc.path, "@")] = json.RawMessage(tc.value)
			} else {
				if tc.value == "" {
					delete(data, tc.path)
				} else {
					data[tc.path] = json.RawMessage(tc.value)
				}
				item["data"], err = json.Marshal(data)
				require.NoError(t, err)
			}
			raw, err := json.Marshal(item)
			require.NoError(t, err)
			require.NoError(t, client.Do(ctx, client.B().Hset().Key("{"+QueueDefaultKey+"}:queue:item").FieldValue().FieldValue(f.Queue.HashID, string(raw)).Build()).Error())
			loaded, goErr := shard.LoadQueueItem(ctx, f.Queue.HashID)
			if tc.bad {
				require.Error(t, goErr)
			} else {
				require.NoError(t, goErr)
			}
			before := snapshotNativeStore(t, ctx, client)
			out := invokeNative(t, nativeInput(v.Addr, v.Password, StateDefaultKey, false, nativeEnqueueOperation(QueueDefaultKey, false)), nil, nil)
			if tc.bad {
				require.JSONEq(t, `"Failed"`, string(out["class"]))
				require.JSONEq(t, `"duplicate-item-decode"`, string(out["lastFailure"]))
			} else {
				require.JSONEq(t, `"Duplicate"`, string(out["class"]))
				require.JSONEq(t, strconv.Quote(loaded.Data.Identifier.RunID.String()), string(out["existingRunId"]))
			}
			require.JSONEq(t, `"NotApplied"`, string(out["writeStatus"]))
			require.Equal(t, "1", string(out["result"]))
			requireNativeSnapshot(t, before, snapshotNativeStore(t, ctx, client))
		})
	}
}
