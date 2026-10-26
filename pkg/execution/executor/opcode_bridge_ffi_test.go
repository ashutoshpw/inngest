//go:build linux && opcode_bridge_spike && opcode_bridge_ffi && cgo

package executor

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/inngest/inngest/internal/opcodespike"
)

var opcodeBridgeFFIAdapters = []struct {
	name     string
	evaluate func(context.Context, []opcodespike.Tag, uint64) (opcodespike.Projection, error)
}{
	{"framed", opcodespike.EvaluateFFI},
	{"direct", opcodespike.EvaluateDirectFFI},
}

func TestOpcodeBridgeFFIFixtureDifferential(t *testing.T) {
	fixture := loadOpcodePlanFixture(t)
	if len(fixture.Cases) != 27 {
		t.Fatalf("fixture cases = %d, want exact accepted corpus of 27", len(fixture.Cases))
	}
	for _, adapter := range opcodeBridgeFFIAdapters {
		t.Run(adapter.name, func(t *testing.T) {
			for index, tc := range fixture.Cases {
				t.Run(tc.Name, func(t *testing.T) {
					tags, err := opcodespike.TagsFromOpcodes(tc.Input)
					if err != nil {
						t.Fatalf("pre-FFI input validation: %v", err)
					}
					got, err := adapter.evaluate(context.Background(), tags, uint64(index+1))
					if err != nil {
						t.Fatalf("Rust FFI result: %v", err)
					}
					verified, err := verifyOpcodeBridgeResult(got, goOpcodeHelperOracle(tc.Input))
					if err != nil {
						t.Fatal(err)
					}
					want := opcodespike.Projection{
						PriorityIndices: uint32Indices(tc.Expected.PriorityIndices),
						OtherIndices:    uint32Indices(tc.Expected.OtherIndices),
						NonLazyIndices:  uint32Indices(tc.Expected.NonLazyIndices),
						HistoryGroup:    tc.Expected.HistoryGroup,
						HasPlanOp:       tc.Expected.HasPlanOp,
					}
					if !reflect.DeepEqual(verified, want) {
						t.Fatalf("FFI result = %+v, literal fixture = %+v", verified, want)
					}
					if got, want := opcodeSpikeMaterializeOutput(tc.Input, verified), opcodeSpikeGoOutput(tc.Input); !reflect.DeepEqual(got, want) {
						t.Fatalf("materialized groups/IDs/flags = %+v, Go = %+v", got, want)
					}
					if tc.Name == "nonlazy-ids-retain-duplicates-and-empty-values" {
						requireOpcodePayloadIdentity(t, tc.Input, materializeOpcodeProjection(tc.Input, verified))
					}
				})
			}
		})
	}
}

func TestOpcodeBridgeFFIEightConcurrentCalls(t *testing.T) {
	fixture := loadOpcodePlanFixture(t)
	for _, adapter := range opcodeBridgeFFIAdapters {
		t.Run(adapter.name, func(t *testing.T) {
			var workers sync.WaitGroup
			errorsByIndex := make(chan error, 8)
			for index := 0; index < 8; index++ {
				index := index
				workers.Add(1)
				go func() {
					defer workers.Done()
					testCase := fixture.Cases[index]
					tags, err := opcodespike.TagsFromOpcodes(testCase.Input)
					if err != nil {
						errorsByIndex <- err
						return
					}
					got, err := adapter.evaluate(context.Background(), tags, uint64(index+1))
					if err == nil {
						_, err = verifyOpcodeBridgeResult(got, goOpcodeHelperOracle(testCase.Input))
					}
					errorsByIndex <- err
				}()
			}
			workers.Wait()
			close(errorsByIndex)
			for err := range errorsByIndex {
				if err != nil {
					t.Error(err)
				}
			}

		})
	}
}
