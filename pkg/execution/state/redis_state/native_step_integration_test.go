//go:build native_step_integration

package redis_state

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

func nativeInput(address, password, prefix string, sharded bool, operation map[string]any) map[string]any {
	address = strings.Replace(address, "localhost:", "127.0.0.1:", 1)
	layout := "NeverShard"
	if sharded {
		layout = "AlwaysShard"
	}
	return map[string]any{
		"store":    map[string]any{"address": address, "username": nil, "password": password, "connectTimeoutMs": 500, "readTimeoutMs": 500, "writeTimeoutMs": 500},
		"state":    map[string]any{"prefix": prefix, "layout": layout, "identity": map[string]any{"runId": "01ARZ3NDEKTSV4RRFFQ69G5FAV", "functionId": "00000000-0000-0000-0000-000000000001", "accountId": "00000000-0000-0000-0000-000000000003", "envId": "00000000-0000-0000-0000-000000000002", "appId": "00000000-0000-0000-0000-000000000004"}},
		"budgetMs": 5000, "disabledRetries": false, "operation": operation,
	}
}
func nativeSaveOperation(output string, delta int) map[string]any {
	return map[string]any{"kind": "save", "step": nativeByteArray([]byte("s")), "output": nativeByteArray([]byte(output)), "delta": delta}
}
func nativePreparation(data string, limit int) map[string]any {
	raw := `[{"op":"StepRun","id":"s"`
	if data != "" {
		raw += `,"data":` + data
	}
	raw += `}]`
	return map[string]any{"kind": "prepareSave", "response": nativeByteArray([]byte(raw)), "config": map[string]any{"requestVersion": 2, "forceStepPlan": false, "traceDisabled": true, "hasAi": false, "sdkMetadata": []any{}, "experiments": []any{}, "loadedStateBytes": 34, "stateLimit": limit}}
}
func freshNativeRead(t *testing.T, address, password, prefix string, sharded bool) (sv2.RunService, rueidis.Client) {
	t.Helper()
	c, err := queuehelper.NewValkeyClient(address, "", password, false)
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return nativeAdapter(t, c, prefix, sharded), c
}
func assertNativeWire(t *testing.T, relay *nativeRelay, body string, keys []string, output string, delta int) ([][]int64, []string) {
	t.Helper()
	commands, _ := relay.inventory()
	results := [][]int64{}
	names := []string{}
	for _, command := range commands {
		if command.name != "EVALSHA" && command.name != "EVAL" {
			require.Equal(t, "AUTH", command.name)
			continue
		}
		names = append(names, command.name)
		script := body
		if command.name == "EVALSHA" {
			script = "2c371556f31bb03e150253bbd588a9a34255061a"
		}
		expected := [][]byte{[]byte(script), []byte("5")}
		for _, key := range keys {
			expected = append(expected, []byte(key))
		}
		expected = append(expected, []byte("s"), []byte(output), []byte(strconv.Itoa(delta)))
		require.Equal(t, expected, command.args)
		if len(command.result) > 0 {
			results = append(results, command.result)
		}
	}
	return results, names
}

func TestNativeRustSaveStepInterop(t *testing.T) {
	f := loadNativeEffects(t)
	body := nativeScript(t, f.Scripts["saveResponse"])
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	_, writer := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
	require.NoError(t, writer.Do(t.Context(), writer.B().ScriptLoad().Script(body).Build()).Error())
	for _, sharded := range []bool{false, true} {
		for _, prefix := range []string{StateDefaultKey, "native:state"} {
			for _, tc := range f.AdapterCases {
				t.Run(fmt.Sprintf("sharded=%t/%s/pending=%d/%s", sharded, prefix, len(tc.Pending), tc.Name), func(t *testing.T) {
					ctx := t.Context()
					require.NoError(t, writer.Do(ctx, writer.B().Flushdb().Build()).Error())
					setup := nativeAdapter(t, writer, prefix, sharded)
					input := sv2.CreateState{Metadata: nativeMetadata(), Events: []json.RawMessage{json.RawMessage(`{"name":"native.test","data":{}}`)}}
					if tc.Name == "input-metadata" {
						input.StepInputs = []state.MemoizedStep{{ID: "s", Data: json.RawMessage(f.Input)}}
					}
					_, err := setup.Create(ctx, input)
					require.NoError(t, err)
					if len(tc.Pending) > 0 {
						require.NoError(t, setup.SavePending(ctx, input.Metadata.ID, tc.Pending))
					}
					keys := nativeSaveKeys(f, prefix, sharded)
					pre := snapshotNativeStore(t, ctx, writer)
					requireNativeSnapshot(t, expectedNativeInitialSnapshot(t, f, keys, prefix, sharded, tc.Name == "input-metadata", tc.Pending), pre)
					expected := expectedNativeSaveSnapshot(t, f, tc, keys, pre)
					output, mode := f.Output, ""
					switch tc.Name {
					case "identical", "conflict", "preexecution-conflict":
						seed := invokeNative(t, nativeInput(v.Addr, v.Password, prefix, sharded, nativeSaveOperation(f.Output, 0)), nil, nil)
						require.JSONEq(t, `"None"`, string(seed["error"]))
						requireNativeSnapshot(t, expected, snapshotNativeStore(t, ctx, writer))
						require.NoError(t, setup.SavePending(ctx, input.Metadata.ID, []string{"s", "held"}))
						expected[keys[4]] = nativeStoredValue{"set", []string{"held", "s"}, -1}
						if tc.Name != "identical" {
							output = f.Conflict
						}
						if tc.Name == "preexecution-conflict" {
							mode = "before"
						}
					case "commit-retry", "commit-no-retry":
						mode = "after"
					}
					fallback := tc.Name == "fresh" && !sharded && prefix == StateDefaultKey && len(tc.Pending) == 0
					if fallback {
						require.NoError(t, writer.Do(ctx, writer.B().ScriptFlush().Build()).Error())
					}
					relay := startNativeRelay(t, v.Addr, mode)
					request := nativeInput(relay.listener.Addr().String(), v.Password, prefix, sharded, nativeSaveOperation(output, tc.MetadataSizeDelta))
					request["disabledRetries"] = tc.Name == "commit-no-retry"
					var observe func()
					if mode == "after" {
						observe = func() {
							_, reader := freshNativeRead(t, v.Addr, v.Password, prefix, sharded)
							requireNativeSnapshot(t, expected, snapshotNativeStore(t, ctx, reader))
						}
					}
					out := invokeNative(t, request, relay, observe)
					wantError := tc.Error
					if wantError == "none" {
						wantError = "None"
					}
					require.JSONEq(t, strconv.Quote(wantError), string(out["error"]))
					require.Equal(t, strconv.FormatBool(tc.HasPending), string(out["hasPending"]))
					require.Equal(t, strconv.Itoa(tc.Attempts), string(out["attempts"]))
					status := "Confirmed"
					switch tc.Name {
					case "conflict":
						status = "NotApplied"
					case "commit-no-retry", "preexecution-conflict":
						status = "MayHaveCommitted"
					}
					require.JSONEq(t, strconv.Quote(status), string(out["writeStatus"]))

					results, names := assertNativeWire(t, relay, body, keys, output, tc.MetadataSizeDelta)
					require.Equal(t, tc.Results, results)
					if fallback {
						require.Equal(t, []string{"EVALSHA", "EVAL"}, names)
						commands, _ := relay.inventory()
						for _, c := range commands {
							if c.name == "EVALSHA" {
								require.Equal(t, "NOSCRIPT", c.serverErrorCode)
							}
						}
					} else {
						require.Len(t, names, tc.Attempts)
					}
					// Native results contain only acknowledged replies. Relay sees the lost one.
					var nativeResults [][]int64
					require.NoError(t, json.Unmarshal(out["results"], &nativeResults))
					acknowledged := results
					if mode == "after" {
						acknowledged = results[1:]
						require.JSONEq(t, `"io"`, string(out["lastFailure"]))
					}
					require.Equal(t, acknowledged, nativeResults)
					fresh, reader := freshNativeRead(t, v.Addr, v.Password, prefix, sharded)
					requireNativeSnapshot(t, expected, snapshotNativeStore(t, ctx, reader))
					steps, err := fresh.LoadSteps(ctx, input.Metadata.ID)
					require.NoError(t, err)
					require.Equal(t, f.Output, string(steps["s"]))
					stack, err := fresh.LoadStack(ctx, input.Metadata.ID)
					require.NoError(t, err)
					require.Equal(t, tc.Stack, stack)
					md, err := fresh.LoadMetadata(ctx, input.Metadata.ID)
					require.NoError(t, err)
					require.Equal(t, tc.StepCount, md.Metrics.StepCount)
					pending, err := fresh.LoadPending(ctx, input.Metadata.ID)
					require.NoError(t, err)
					slices.Sort(pending)
					require.Equal(t, len(tc.Remaining), len(pending))
					for i := range pending {
						require.Equal(t, tc.Remaining[i], pending[i])
					}
					if tc.Name == "input-metadata" {
						inputs, err := fresh.LoadStepInputs(ctx, input.Metadata.ID)
						require.NoError(t, err)
						require.Equal(t, f.Input, string(inputs["s"]))
					}
					// Existing Go sentinel owner remains an independent compatibility reader.
					has, err := fresh.SaveStep(ctx, input.Metadata.ID, "s", []byte(f.Output))
					require.Same(t, state.ErrIdempotentResponse, err)
					require.Equal(t, len(tc.Remaining) > 0, has)
					requireNativeSnapshot(t, expected, snapshotNativeStore(t, ctx, reader))
				})
			}
		}
	}
}

func TestNativeRustPreparationInterop(t *testing.T) {
	f := loadNativeEffects(t)
	var preparation struct {
		Cases []struct {
			Name, Data, Prepared string
			Bytes                int
		}
	}
	raw, err := os.ReadFile("../../executor/testdata/compatibility/native-step-preparation-v1.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &preparation))
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	_, writer := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
	for _, sharded := range []bool{false, true} {
		for _, prefix := range []string{StateDefaultKey, "native:state"} {
			for _, tc := range preparation.Cases {
				t.Run(fmt.Sprintf("sharded=%t/%s/%s", sharded, prefix, tc.Name), func(t *testing.T) {
					ctx := t.Context()
					require.NoError(t, writer.Do(ctx, writer.B().Flushdb().Build()).Error())
					setup := nativeAdapter(t, writer, prefix, sharded)
					md := nativeMetadata()
					_, err := setup.Create(ctx, sv2.CreateState{Metadata: md, Events: []json.RawMessage{json.RawMessage(`{"name":"native.test","data":{}}`)}})
					require.NoError(t, err)
					keys := nativeSaveKeys(f, prefix, sharded)
					pre := snapshotNativeStore(t, ctx, writer)
					requireNativeSnapshot(t, expectedNativeInitialSnapshot(t, f, keys, prefix, sharded, false, nil), pre)
					// Equality accepts; the next byte rejects before ANY native socket opens.
					denied := nativeInput("127.0.0.1:1", v.Password, prefix, sharded, nativePreparation(tc.Data, 34+tc.Bytes-1))
					out := invokeNative(t, denied, nil, nil)
					require.JSONEq(t, `"ErrStateOverflowed"`, string(out["preparationError"]))
					var standard state.StandardError
					require.NoError(t, json.Unmarshal(out["standardError"], &standard))
					require.Equal(t, state.InngestErrStateOverflowed, standard.Name)
					require.Equal(t, fmt.Sprintf("The function run exceeded the state size limit of %d bytes.", 34+tc.Bytes-1), standard.Message)
					require.Equal(t, standard.String(), standard.Error)
					require.Empty(t, standard.Stack)

					requireNativeSnapshot(t, pre, snapshotNativeStore(t, ctx, writer))
					out = invokeNative(t, nativeInput(v.Addr, v.Password, prefix, sharded, nativePreparation(tc.Data, 34+tc.Bytes)), nil, nil)
					require.JSONEq(t, `"None"`, string(out["error"]))
					projection := f
					projection.Output = tc.Prepared
					projection.OutputBytes = tc.Bytes
					expected := expectedNativeSaveSnapshot(t, projection, nativeSaveCase{Name: "fresh", StateSizeDelta: tc.Bytes, StepCount: 1, Stack: []string{"s"}}, keys, pre)
					fresh, reader := freshNativeRead(t, v.Addr, v.Password, prefix, sharded)
					requireNativeSnapshot(t, expected, snapshotNativeStore(t, ctx, reader))
					steps, err := fresh.LoadSteps(ctx, md.ID)
					require.NoError(t, err)
					require.Equal(t, tc.Prepared, string(steps["s"]))
					mdLoaded, err := fresh.LoadMetadata(ctx, md.ID)
					require.NoError(t, err)
					require.Equal(t, 34+tc.Bytes, mdLoaded.Metrics.StateSize)
				})
			}
		}
	}
}
