package executor

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/state"
)

const benchmarkOpcodePlanRunID = "01ARZ3NDEKTSV4RRFFQ69G5FAV"

var opcodePlanBenchmarkSink struct {
	Groups  OpcodeGroups
	IDs     []string
	HasPlan bool
	Key     string
}

type opcodePlanBenchmarkWorkload struct {
	name    string
	decoded []*state.GeneratorOpcode
	encoded []byte
}

func BenchmarkOpcodePlan(b *testing.B) {
	workloads := []opcodePlanBenchmarkWorkload{
		makeOpcodePlanWorkload(b, "small", []*state.GeneratorOpcode{
			{Op: enums.OpcodeStepRun, ID: "step-1"},
		}),
		makeOpcodePlanWorkload(b, "mixed", []*state.GeneratorOpcode{
			{Op: enums.OpcodeSleep, ID: "sleep-1"},
			{Op: enums.OpcodeWaitForEvent, ID: "wait-1"},
			{Op: enums.OpcodeRunComplete, ID: "complete-1"},
			{Op: enums.OpcodeDeferAdd, ID: "defer-add"},
			{Op: enums.OpcodeDeferAbort, ID: "defer-abort"},
			{Op: enums.OpcodeStepPlanned, ID: "step-2"},
			{Op: enums.OpcodeInvokeFunction, ID: "invoke-1"},
			{Op: enums.OpcodeWaitForSignal, ID: "signal-1"},
		}),
		makeOpcodePlanWorkload(b, "large_1000", largeOpcodePlanInput()),
	}

	for _, workload := range workloads {
		// The first path starts with already decoded input. The second includes
		// response JSON decoding before the same pure planning work.
		b.Run("pure_grouping/"+workload.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				benchmarkOpcodePlan(workload.decoded)
			}
		})

		b.Run("decode_end_to_end/"+workload.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var response state.DriverResponse
				if err := json.Unmarshal(workload.encoded, &response); err != nil {
					b.Fatal(err)
				}
				benchmarkOpcodePlan(response.Generator)
			}
		})
	}
}

func makeOpcodePlanWorkload(b *testing.B, name string, ops []*state.GeneratorOpcode) opcodePlanBenchmarkWorkload {
	b.Helper()
	encoded, err := json.Marshal(state.DriverResponse{Generator: ops})
	if err != nil {
		b.Fatal(err)
	}
	return opcodePlanBenchmarkWorkload{name: name, decoded: ops, encoded: encoded}
}

func largeOpcodePlanInput() []*state.GeneratorOpcode {
	pattern := []enums.Opcode{
		enums.OpcodeStepPlanned,
		enums.OpcodeStepRun,
		enums.OpcodeWaitForEvent,
		enums.OpcodeDeferAdd,
		enums.OpcodeSleep,
		enums.OpcodeInvokeFunction,
		enums.OpcodeDeferAbort,
	}
	ops := make([]*state.GeneratorOpcode, 1_000)
	for i := range ops {
		ops[i] = &state.GeneratorOpcode{
			Op: pattern[i%len(pattern)],
			ID: fmt.Sprintf("step-%04d", i),
		}
	}
	return ops
}

func benchmarkOpcodePlan(ops []*state.GeneratorOpcode) {
	groups := opGroups(ops)
	ids := groups.NonLazyIDs()
	hasPlan := hasPlanOp(ops)
	key := ""
	if hasPlan && len(ids) > 1 {
		key = computeParallelCoalesceKey(benchmarkOpcodePlanRunID, ids)
	}
	opcodePlanBenchmarkSink.Groups = groups
	opcodePlanBenchmarkSink.IDs = ids
	opcodePlanBenchmarkSink.HasPlan = hasPlan
	opcodePlanBenchmarkSink.Key = key
}
