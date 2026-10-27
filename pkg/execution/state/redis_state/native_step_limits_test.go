//go:build native_step_integration

package redis_state

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/stretchr/testify/require"
)

// Independent Go constants/owner protect against a native limit drifting while
// a Rust-only threshold test continues to use its own changed constant.
func TestNativeRustOutputAndDefaultStateLimits(t *testing.T) {
	for _, extra := range []int{0, 1} {
		t.Run(string(rune('0'+extra)), func(t *testing.T) {
			data := `"` + strings.Repeat("x", consts.MaxStepOutputSize-11+extra) + `"`
			goError := (state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "s", Data: json.RawMessage(data)}).Validate()
			op := nativePreparation(data, 1)
			op["kind"] = "prepare"
			out := invokeNative(t, nativeInput("127.0.0.1:1", "unused", StateDefaultKey, false, op), nil, nil)
			if extra == 0 {
				require.NoError(t, goError)
				require.JSONEq(t, `"ErrStateOverflowed"`, string(out["preparationError"]), "Go accepts the exact output limit; native must reach state validation")
			} else {
				require.Same(t, state.ErrStepOutputTooLarge, goError)
				require.JSONEq(t, `"ErrStepOutputTooLarge"`, string(out["preparationError"]))
				var message string
				require.NoError(t, json.Unmarshal(out["message"], &message))
				require.Equal(t, goError.Error(), message)
			}
		})
	}
	for _, extra := range []int{0, 1} {
		t.Run("default-state-"+string(rune('0'+extra)), func(t *testing.T) {
			op := nativePreparation("", 0)
			op["kind"] = "prepare"
			op["config"].(map[string]any)["loadedStateBytes"] = consts.DefaultMaxStateSizeLimit - 13 + extra
			out := invokeNative(t, nativeInput("127.0.0.1:1", "unused", StateDefaultKey, false, op), nil, nil)
			if extra == 0 {
				var output []byte
				require.NoError(t, json.Unmarshal(out["output"], &output))
				require.Equal(t, `{"data":null}`, string(output))
			} else {
				require.JSONEq(t, `"ErrStateOverflowed"`, string(out["preparationError"]))
			}
		})
	}
}

// Output() is the independent byte oracle; the literal errors below preserve
// native admission precedence rather than pretending Go owns that admission gate.
func TestNativeRustBoundedPreparation(t *testing.T) {
	for _, tc := range []struct {
		name, unit, tail, suffix, wantError string
		repeat, wantBytes                   int
	}{
		{name: "exact-limit", unit: "x", repeat: consts.MaxStepOutputSize - 11, wantBytes: consts.MaxStepOutputSize},
		{name: "plus-one-before-state-overflow", unit: "x", repeat: consts.MaxStepOutputSize - 10, wantBytes: consts.MaxStepOutputSize + 1, wantError: "ErrStepOutputTooLarge"},
		{name: "html-expands-past-limit", unit: "<>&", repeat: consts.MaxStepOutputSize/18 + 1, wantBytes: (consts.MaxStepOutputSize/18+1)*18 + 11, wantError: "ErrStepOutputTooLarge"},
		{name: "unicode-expands-past-limit", unit: "\u2028\u2029", repeat: consts.MaxStepOutputSize / 12, wantBytes: (consts.MaxStepOutputSize/12)*12 + 11, wantError: "ErrStepOutputTooLarge"},
		{name: "ai-totalUsage-after-cap", unit: "x", repeat: consts.MaxStepOutputSize, tail: "totalUsage", wantError: "Ineligible"},
		{name: "ai-inputTokens-after-cap", unit: "x", repeat: consts.MaxStepOutputSize, tail: "inputTokens", wantError: "Ineligible"},
		{name: "invalid-after-cap-and-ai", unit: "x", repeat: consts.MaxStepOutputSize, tail: "totalUsage", suffix: ",", wantError: "InvalidJson"},
		{name: "invalid-escape-after-cap", unit: "x", repeat: consts.MaxStepOutputSize, tail: `\u000x`, wantError: "InvalidJson"},
		{name: "unsupported-opts-after-cap", unit: "x", repeat: consts.MaxStepOutputSize, suffix: `,"opts":{"type":"step.ai.wrap"}`, wantError: "Ineligible"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := `"` + strings.Repeat(tc.unit, tc.repeat) + tc.tail + `"`
			body := `[{"op":"StepRun","id":"s","data":` + data + tc.suffix + `}]`
			var accepted []state.GeneratorOpcode
			err := json.Unmarshal([]byte(body), &accepted)
			var goOutput string
			if tc.wantError == "InvalidJson" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				goOutput, err = accepted[0].Output()
				require.NoError(t, err)
				if tc.wantBytes != 0 {
					require.Len(t, goOutput, tc.wantBytes)
				}
				if tc.wantError == "ErrStepOutputTooLarge" {
					require.Same(t, state.ErrStepOutputTooLarge, accepted[0].Validate())
				}
			}
			op := nativePreparation("", 1) // oversized output must win over state overflow
			op["kind"] = "prepare"
			op["response"] = nativeByteArray([]byte(body))
			if tc.wantError == "" {
				op["config"].(map[string]any)["stateLimit"] = nil
			}
			out := invokeNative(t, nativeInput("127.0.0.1:1", "unused", StateDefaultKey, false, op), nil, nil)
			if tc.wantError != "" {
				require.JSONEq(t, `"`+tc.wantError+`"`, string(out["preparationError"]))
				return
			}
			require.NotContains(t, out, "preparationError")
			var output []byte
			require.NoError(t, json.Unmarshal(out["output"], &output))
			require.Equal(t, []byte(goOutput), output)
		})
	}
	t.Run("raw-whitespace-over-limit", func(t *testing.T) {
		data := strings.Repeat(" ", consts.MaxStepOutputSize+1) + ` { "b": 2, "a": 1 } `
		goOutput, err := (state.GeneratorOpcode{Op: enums.OpcodeStepRun, Data: json.RawMessage(data)}).Output()
		require.NoError(t, err)
		require.Equal(t, `{"data":{"b":2,"a":1}}`, goOutput)
		op := nativePreparation(data, 0)
		op["kind"] = "prepare"
		out := invokeNative(t, nativeInput("127.0.0.1:1", "unused", StateDefaultKey, false, op), nil, nil)
		require.NotContains(t, out, "preparationError")
		var output []byte
		require.NoError(t, json.Unmarshal(out["output"], &output))
		require.Equal(t, []byte(goOutput), output)
	})
}
