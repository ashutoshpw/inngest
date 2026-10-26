//go:build linux && opcode_bridge_spike && opcode_bridge_ffi && cgo

package executor

import (
	"context"

	"github.com/inngest/inngest/internal/opcodespike"
)

func opcodeSpikeFFIEnabled() bool { return true }

func opcodeSpikeEvaluateFFI(ctx context.Context, tags []opcodespike.Tag, requestID uint64) (opcodespike.Projection, error) {
	return opcodespike.EvaluateFFI(ctx, tags, requestID)
}
