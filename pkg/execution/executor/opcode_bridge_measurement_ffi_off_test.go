//go:build linux && opcode_bridge_spike && (!opcode_bridge_ffi || !cgo)

package executor

import (
	"context"
	"errors"

	"github.com/inngest/inngest/internal/opcodespike"
)

func opcodeSpikeFFIEnabled() bool { return false }

func opcodeSpikeEvaluateFFI(context.Context, []opcodespike.Tag, uint64) (opcodespike.Projection, error) {
	return opcodespike.Projection{}, errors.New("FFI lane requires opcode_bridge_ffi and cgo")
}
