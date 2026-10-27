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
