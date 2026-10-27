package redis_state

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestNativeStepSaveAdapter(t *testing.T) {
	f := loadNativeEffects(t)
	require.Len(t, f.Output, f.OutputBytes)
	require.Len(t, f.Input, f.InputBytes)
	body := nativeScript(t, f.Scripts["saveResponse"])
	writer, reader := nativeStoreClients(t)
	ctx := t.Context()
	require.NoError(t, reader.Do(ctx, reader.B().ScriptLoad().Script(body).Build()).Error())
	for _, sharded := range []bool{false, true} {
		for _, prefix := range []string{StateDefaultKey, "native:state"} {
			for _, tc := range f.AdapterCases {
				pending, scenario := tc.Pending, tc.Name
				t.Run(fmt.Sprintf("sharded=%t/%s/pending=%d/%s", sharded, prefix, len(pending), scenario), func(t *testing.T) {
					require.NoError(t, writer.Do(ctx, writer.B().Flushdb().Build()).Error())
					keys := nativeSaveKeys(f, prefix, sharded)
					fallback := scenario == "fresh" && !sharded && prefix == StateDefaultKey && len(pending) == 0
					if fallback {
						require.NoError(t, writer.Do(ctx, writer.B().ScriptFlush().Build()).Error())
					}
					delta := tc.MetadataSizeDelta
					fault := &nativeScriptClient{Client: writer, t: t, manifest: f.Scripts["saveResponse"], body: body, keys: keys, args: []string{"s", f.Output, strconv.Itoa(delta)}}
					opts := []MgrV2Opt{}
					if scenario == "commit-no-retry" {
						opts = append(opts, WithDisabledRetries())
					}
					svc := nativeAdapter(t, fault, prefix, sharded, opts...)
					fresh := nativeAdapter(t, reader, prefix, sharded)
					input := sv2.CreateState{Metadata: nativeMetadata(), Events: []json.RawMessage{json.RawMessage(`{"name":"native.test","data":{}}`)}}
					if scenario == "input-metadata" {
						input.StepInputs = []state.MemoizedStep{{ID: "s", Data: json.RawMessage(f.Input)}}
					}
					_, err := svc.Create(ctx, input)
					require.NoError(t, err)
					if len(pending) > 0 {
						require.NoError(t, svc.SavePending(ctx, input.Metadata.ID, pending))
					}
					pre := snapshotNativeStore(t, ctx, reader)
					require.NotContains(t, pre, keys[0])
					require.NotContains(t, pre, keys[2])
					require.Equal(t, int64(-1), pre[keys[1]].PTTL)
					initial := maps.Clone(f.InitialMetadata)
					if scenario == "input-metadata" {
						initial["state_size"] = strconv.Itoa(f.InitialStateSizeWithInput)
					}
					tag := prefix
					if sharded {
						tag += ":00000000-0000-0000-0000-000000000003"
					}
					initialExpected := nativeSnapshot{
						"{" + tag + "}:key:run-key": {"string", "01ARZ3NDEKTSV4RRFFQ69G5FAV", 86400000},
						keys[1]:                     {"hash", initial, -1},
						strings.Replace(keys[0], ":actions:", ":bulk-events:", 1): {"string", f.InitialEvents, -1},
					}
					if scenario == "input-metadata" {
						initialExpected[keys[3]] = nativeStoredValue{"hash", map[string]string{"s": f.Input}, -1}
					}
					if len(pending) > 0 {
						sorted := slices.Clone(pending)
						slices.Sort(sorted)
						initialExpected[keys[4]] = nativeStoredValue{"set", sorted, -1}
					}
					requireNativeSnapshot(t, initialExpected, pre)
					expected := nativeSnapshot{}
					for k, v := range pre {
						expected[k] = v
					}
					metadata := maps.Clone(initial)
					stateDelta := f.OutputBytes
					if scenario == "input-metadata" {
						stateDelta -= f.InputBytes
						require.Equal(t, map[string]string{"s": f.Input}, pre[keys[3]].Value)
					}
					add := func(field string, n int) {
						v, e := strconv.Atoi(metadata[field])
						if metadata[field] == "" {
							v = 0
							e = nil
						}
						require.NoError(t, e)
						metadata[field] = strconv.Itoa(v + n)
					}
					require.Equal(t, tc.StateSizeDelta, stateDelta)
					add("state_size", tc.StateSizeDelta)
					add("step_count", tc.StepCount)
					if delta > 0 {
						add("metadata_size", delta)
					}
					expected[keys[1]] = nativeStoredValue{"hash", metadata, -1}
					expected[keys[0]] = nativeStoredValue{"hash", map[string]string{"s": f.Output}, -1}
					expected[keys[2]] = nativeStoredValue{"list", tc.Stack, -1}
					delete(expected, keys[4])
					if len(pending) == 2 {
						expected[keys[4]] = nativeStoredValue{"set", []string{"held"}, -1}
					}
					fault.afterCommit = func() {
						got := snapshotNativeStore(t, ctx, reader)
						requireNativeSnapshot(t, expected, got)
						t.Logf("postcommit=%#v", got)
					}
					saveCtx := ctx
					if delta > 0 {
						saveCtx = state.WithMetadataSizeDelta(ctx, delta)
					}
					switch scenario {
					case "identical", "conflict", "preexecution-conflict":
						has, err := svc.SaveStep(saveCtx, input.Metadata.ID, "s", []byte(f.Output))
						require.NoError(t, err)
						require.Equal(t, len(pending) == 2, has)
						requireNativeSnapshot(t, expected, snapshotNativeStore(t, ctx, reader))
						// Reset pending through its real owner: idempotent re-save must report the
						// CURRENT pending set, not an old result, and must not remove 's'.
						require.NoError(t, svc.SavePending(ctx, input.Metadata.ID, []string{"s", "held"}))
						expected[keys[4]] = nativeStoredValue{"set", []string{"held", "s"}, -1}
						fault.calls = 0
						fault.results = nil
						if scenario != "identical" {
							fault.args[1] = f.Conflict
						}
						if scenario == "preexecution-conflict" {
							fault.mode = "before"
						}
					case "commit-retry", "commit-no-retry":
						fault.mode = "after"
					}
					has, err := svc.SaveStep(saveCtx, input.Metadata.ID, "s", []byte(fault.args[1]))
					switch tc.Error {
					case "none":
						require.NoError(t, err)
					case "ErrIdempotentResponse":
						require.Same(t, state.ErrIdempotentResponse, err)
					case "ErrDuplicateResponse":
						require.Same(t, state.ErrDuplicateResponse, err)
					case "ErrMaxAttemptReached":
						require.ErrorIs(t, err, util.ErrMaxAttemptReached)
						require.NotErrorIs(t, err, context.Canceled)
					default:
						t.Fatalf("unknown fixture error %q", tc.Error)
					}

					if fallback {
						require.Len(t, fault.commands, 2)
						require.Equal(t, "EVALSHA", fault.commands[0][0])
						require.Equal(t, "EVAL", fault.commands[1][0])
						require.Equal(t, body, fault.commands[1][1])
					}
					require.Equal(t, tc.HasPending, has)
					require.Equal(t, tc.Attempts, fault.calls)
					require.Equal(t, tc.Results, fault.results)

					actualPending, loadErr := fresh.LoadPending(ctx, input.Metadata.ID)
					require.NoError(t, loadErr)
					slices.Sort(actualPending)
					require.Equal(t, len(tc.Remaining), len(actualPending))
					for j := range actualPending {
						require.Equal(t, tc.Remaining[j], actualPending[j])
					}
					post := snapshotNativeStore(t, ctx, reader)
					requireNativeSnapshot(t, expected, post)
					wantPending := len(tc.Remaining) > 0
					has, err = fresh.SaveStep(saveCtx, input.Metadata.ID, "s", []byte(f.Output))
					require.Same(t, state.ErrIdempotentResponse, err)
					require.Equal(t, wantPending, has)
					requireNativeSnapshot(t, post, snapshotNativeStore(t, ctx, reader))
					t.Logf("pre-save=%#v post-save=%#v calls=%d results=%v", pre, post, fault.calls, fault.results)
				})
			}
		}
	}
}
