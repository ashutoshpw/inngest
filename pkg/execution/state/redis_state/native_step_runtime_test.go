//go:build native_step_integration

package redis_state

import (
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/inngest/inngest/pkg/execution/state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/stretchr/testify/require"
)

func TestNativeRustRuntimeQualification(t *testing.T) {
	f := loadNativeEffects(t)
	body := nativeScript(t, f.Scripts["saveResponse"])
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	_, writer := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
	require.NoError(t, writer.Do(t.Context(), writer.B().ScriptLoad().Script(body).Build()).Error())
	for _, mode := range []string{"before", "read-stall", "write-stall", "auth", "connect", "handshake-stall", "budget", "backoff-budget", "reconnect", "before-always"} {
		t.Run(mode, func(t *testing.T) {
			ctx := t.Context()
			require.NoError(t, writer.Do(ctx, writer.B().Flushdb().Build()).Error())
			setup := nativeAdapter(t, writer, StateDefaultKey, false)
			_, err := setup.Create(ctx, sv2.CreateState{Metadata: nativeMetadata(), Events: []json.RawMessage{json.RawMessage(`{"name":"native.test","data":{}}`)}})
			require.NoError(t, err)
			keys := nativeSaveKeys(f, StateDefaultKey, false)
			pre := snapshotNativeStore(t, ctx, writer)
			relayMode := mode
			if mode == "reconnect" {
				relayMode = "before"
			}
			if mode == "auth" || mode == "budget" || mode == "connect" {
				relayMode = ""
			}
			if mode == "backoff-budget" {
				relayMode = "before"
			}
			if mode == "handshake-stall" {
				relayMode = "handshake-stall"
			}
			relay := startNativeRelay(t, v.Addr, relayMode)
			input := nativeInput(relay.listener.Addr().String(), v.Password, StateDefaultKey, false, nativeSaveOperation(f.Output, 0))
			input["disabledRetries"] = true
			store := input["store"].(map[string]any)
			store["readTimeoutMs"] = 60
			store["writeTimeoutMs"] = 60
			store["connectTimeoutMs"] = 60
			switch mode {
			case "write-stall":
				// Far beyond both TCP buffers: the relay authenticates then stops reading.
				input["operation"] = map[string]any{"kind": "save", "step": nativeByteArray([]byte("s")), "output": nativeByteArray(make([]byte, 8<<20)), "delta": 0}
			case "auth":
				store["password"] = "incorrect"
			case "connect":
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				require.NoError(t, err)
				store["address"] = listener.Addr().String()
				require.NoError(t, listener.Close())
			case "budget":
				input["budgetMs"] = 0
			case "reconnect":
				input["disabledRetries"] = false
			case "backoff-budget":
				input["disabledRetries"] = false
				input["budgetMs"] = 30
			case "before-always":
				input["disabledRetries"] = false
				input["budgetMs"] = 40000
			}
			start := time.Now()
			out := invokeNative(t, input, relay, nil)
			elapsed := time.Since(start)
			commands, _ := relay.inventory()
			scriptCommands, forwarded, auth := 0, 0, 0
			for _, c := range commands {
				if c.name == "AUTH" {
					auth++
				}
				if c.name == "EVALSHA" || c.name == "EVAL" {
					scriptCommands++
					if c.forwarded {
						forwarded++
					}
				}
			}
			if mode == "before-always" {
				require.JSONEq(t, `"ErrMaxAttemptReached"`, string(out["error"]))
				require.Equal(t, "10", string(out["attempts"]))
				require.Equal(t, 10, scriptCommands)
				require.Equal(t, 10, auth)
				require.Zero(t, forwarded)
				expectedDelays := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond, 1600 * time.Millisecond, 3200 * time.Millisecond, 6400 * time.Millisecond, 10 * time.Second, 10 * time.Second}
				var attempts []time.Time
				for _, c := range commands {
					if c.name == "EVALSHA" {
						attempts = append(attempts, c.at)
					}
				}
				require.Len(t, attempts, 10)
				for i, delay := range expectedDelays {
					observed := attempts[i+1].Sub(attempts[i])
					require.GreaterOrEqual(t, observed, delay)
					require.Less(t, observed, delay+2*time.Second)
				}
				_, reader := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
				post := snapshotNativeStore(t, ctx, reader)
				// This deliberate retry exhaustion lasts 32.7s. Independently
				// account for real expiry time before the usual whole-map check.
				for key, value := range pre {
					if strings.Contains(key, ":key:run-key") {
						require.InDelta(t, value.PTTL-elapsed.Milliseconds(), post[key].PTTL, 1000)
						value.PTTL = post[key].PTTL
						pre[key] = value
					}
				}
				requireNativeSnapshot(t, pre, post)
				t.Logf("ten adapter attempts, real reconnects; backoff elapsed %s", elapsed)
				return
			}
			if mode == "reconnect" {
				require.JSONEq(t, `"None"`, string(out["error"]))
				require.Equal(t, "2", string(out["attempts"]))
				require.Equal(t, 2, scriptCommands)
				require.Equal(t, 1, forwarded)
				require.Equal(t, 2, auth)
				expected := expectedNativeSaveSnapshot(t, f, nativeSaveCase{StateSizeDelta: f.OutputBytes, StepCount: 1, Stack: []string{"s"}}, keys, pre)
				_, reader := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
				requireNativeSnapshot(t, expected, snapshotNativeStore(t, ctx, reader))
				return
			}
			wantError := "ErrMaxAttemptReached"
			if mode == "budget" || mode == "backoff-budget" {
				wantError = "Stopped"
			}
			require.JSONEq(t, strconv.Quote(wantError), string(out["error"]))
			require.Equal(t, 0, forwarded, "no command reached Valkey")
			if mode == "auth" || mode == "connect" || mode == "handshake-stall" || mode == "budget" || mode == "write-stall" {
				require.Zero(t, scriptCommands)
			} else {
				require.Equal(t, 1, scriptCommands)
			}
			if mode == "budget" {
				require.Equal(t, "0", string(out["attempts"]))
				require.Zero(t, auth)
			} else {
				require.Equal(t, "1", string(out["attempts"]))
			}
			if mode == "auth" || mode == "connect" || mode == "handshake-stall" || mode == "budget" {
				require.JSONEq(t, `"NotSent"`, string(out["writeStatus"]))
			} else {
				require.JSONEq(t, `"MayHaveCommitted"`, string(out["writeStatus"]))
			}
			if mode == "read-stall" || mode == "write-stall" {
				require.JSONEq(t, `"io-timeout"`, string(out["lastFailure"]))
			}
			// JSON parsing/IPC of the deliberately huge write is outside the I/O timer.
			require.Less(t, elapsed, 5*time.Second)
			_, reader := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
			requireNativeSnapshot(t, pre, snapshotNativeStore(t, ctx, reader))
		})
	}
	t.Run("authenticated-binary-safe", func(t *testing.T) {
		ctx := t.Context()
		require.NoError(t, writer.Do(ctx, writer.B().Flushdb().Build()).Error())
		// AUTH itself is RESP bulk data: embedded NUL/CRLF and URL metacharacters.
		password := "native\x00\r\n:@/%<&>"
		username := "native-user"
		require.NoError(t, writer.Do(ctx, writer.B().Arbitrary("ACL", "SETUSER", username, "on", ">"+password, "~*", "+@all").Build()).Error())
		setup := nativeAdapter(t, writer, StateDefaultKey, false)
		_, err := setup.Create(ctx, sv2.CreateState{Metadata: nativeMetadata(), Events: []json.RawMessage{json.RawMessage(`{"name":"native.test","data":{}}`)}})
		require.NoError(t, err)
		relay := startNativeRelay(t, v.Addr, "")
		step := []byte{'s', 0, '\r', '\n', 255}
		output := []byte{0, 255, '\r', '\n', '<', '>', 128}
		input := nativeInput(relay.listener.Addr().String(), password, StateDefaultKey, false, map[string]any{"kind": "save", "step": nativeByteArray(step), "output": nativeByteArray(output), "delta": 0})
		input["store"].(map[string]any)["username"] = username
		out := invokeNative(t, input, relay, nil)
		require.JSONEq(t, `"None"`, string(out["error"]))
		_, reader := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
		keys := nativeSaveKeys(f, StateDefaultKey, false)
		value, err := reader.Do(ctx, reader.B().Hget().Key(keys[0]).Field(string(step)).Build()).ToString()
		require.NoError(t, err)
		require.Equal(t, output, []byte(value))
		stack, err := reader.Do(ctx, reader.B().Lrange().Key(keys[2]).Start(0).Stop(-1).Build()).AsStrSlice()
		require.NoError(t, err)
		require.Equal(t, []string{string(step)}, stack)
		md, err := reader.Do(ctx, reader.B().Hgetall().Key(keys[1]).Build()).AsStrMap()
		require.NoError(t, err)
		require.Equal(t, "41", md["state_size"])
		require.Equal(t, "1", md["step_count"])
		commands, _ := relay.inventory()
		require.Len(t, commands, 2)
		require.Equal(t, "AUTH", commands[0].name)
		require.Equal(t, "EVALSHA", commands[1].name)
	})
	t.Run("eligibility-before-effects", func(t *testing.T) {
		changes := map[string]func(map[string]any){
			"tracing":         func(op map[string]any) { op["config"].(map[string]any)["traceDisabled"] = false },
			"AI":              func(op map[string]any) { op["config"].(map[string]any)["hasAi"] = true },
			"SDK-metadata":    func(op map[string]any) { op["config"].(map[string]any)["sdkMetadata"] = []any{map[string]any{}} },
			"experiments":     func(op map[string]any) { op["config"].(map[string]any)["experiments"] = []any{"x"} },
			"force-plan":      func(op map[string]any) { op["config"].(map[string]any)["forceStepPlan"] = true },
			"request-version": func(op map[string]any) { op["config"].(map[string]any)["requestVersion"] = 1 },
			"opcode-metadata": func(op map[string]any) {
				op["response"] = nativeByteArray([]byte(`[{"op":"StepRun","id":"s","metadata":[{}]}]`))
			},
		}
		pre := snapshotNativeStore(t, t.Context(), writer)
		for name, change := range changes {
			t.Run(name, func(t *testing.T) {
				relay := startNativeRelay(t, v.Addr, "")
				op := nativePreparation(`{}`, 0)
				change(op)
				out := invokeNative(t, nativeInput(relay.listener.Addr().String(), v.Password, StateDefaultKey, false, op), relay, nil)
				require.JSONEq(t, `"Ineligible"`, string(out["preparationError"]))
				commands, _ := relay.inventory()
				require.Empty(t, commands, "ineligible preparation must not open an authenticated Redis connection")
				_, reader := freshNativeRead(t, v.Addr, v.Password, StateDefaultKey, false)
				requireNativeSnapshot(t, pre, snapshotNativeStore(t, t.Context(), reader))
			})
		}
	})

}

// The unchanged seven literal cases remain the primary output oracle. These
// additional byte-scanner risks use the actual Go owner, never a Rust-produced
// expected value or floating-point Value reserialization.
func TestNativeRustJSONOwnerParity(t *testing.T) {
	cases := [][]byte{[]byte(`1e9999`), []byte(`{"a":1,"a":2}`), {'"', 255, '"'}, []byte(`"\ud800"`), []byte("\"<>&\u2028\u2029\""), []byte(`01`), []byte(`1.`), []byte(`+1`), []byte(`[1,]`), []byte(`{"a":1,}`), []byte("\"\n\""), []byte(`null true`), []byte(`"\u000x"`)}
	cases = append(cases, []byte(strings.Repeat("[", 9998)+"0"+strings.Repeat("]", 9998)), []byte(strings.Repeat("[", 9999)+"0"+strings.Repeat("]", 9999)))
	for n, data := range cases {
		t.Run(strconv.Itoa(n), func(t *testing.T) {
			op := nativePreparation(string(data), 0)
			op["kind"] = "prepare"
			out := invokeNative(t, nativeInput("127.0.0.1:1", "unused", StateDefaultKey, false, op), nil, nil)
			var accepted []state.GeneratorOpcode
			body := append([]byte(`[{"op":"StepRun","id":"s","data":`), data...)
			body = append(body, '}', ']') // close object/array
			goError := json.Unmarshal(body, &accepted)
			goOutput := ""
			if goError == nil {
				goOutput, goError = accepted[0].Output()
			}
			if goError != nil {
				require.JSONEq(t, `"InvalidJson"`, string(out["preparationError"]))
				return
			}
			require.NotContains(t, out, "preparationError")
			var output []byte
			require.NoError(t, json.Unmarshal(out["output"], &output))
			require.Equal(t, []byte(goOutput), output)
		})
	}
}
