package executor

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/state"
)

var stagedOutputSink string

// These are component baselines through current pure owners, not a copied
// planner or timing of mocked effects. Projection/validation of an unaccepted
// intention representation is deliberately not invented here.
func BenchmarkStagedBatchComponents(b *testing.B) {
	for _, tc := range []struct {
		name       string
		n, payload int
	}{
		{"small1", 1, 0}, {"mixed8", 8, 0}, {"large1000", 1000, 0}, {"payload8_16KiB", 8, 16384},
	} {
		ops := make([]*state.GeneratorOpcode, tc.n)
		pattern := []enums.Opcode{enums.OpcodeStepRun, enums.OpcodeStep, enums.OpcodeNone, enums.OpcodeStepError, enums.OpcodeStepFailed, enums.OpcodeStepPlanned, enums.OpcodeSleep, enums.OpcodeStepRun}
		var outputOps []*state.GeneratorOpcode
		rawBytes, outputBytes := 0, 0
		for n := range ops {
			g := &state.GeneratorOpcode{Op: pattern[n%len(pattern)], ID: fmt.Sprintf("s-%04d", n)}
			switch g.Op {
			case enums.OpcodeStep, enums.OpcodeStepRun:
				g.Data = json.RawMessage(`null`)
				if tc.payload > 0 {
					g.Data = json.RawMessage(`"` + strings.Repeat("x", tc.payload) + `"`)
				}
			case enums.OpcodeStepError, enums.OpcodeStepFailed:
				g.Error = &state.UserError{Name: "E", Message: "x"}
			case enums.OpcodeSleep:
				g.Name = "1s"
			}
			ops[n] = g
			rawBytes += len(g.Data)
			switch g.Op {
			case enums.OpcodeStep, enums.OpcodeStepRun, enums.OpcodeStepError, enums.OpcodeStepFailed:
				outputOps = append(outputOps, g)
				out, err := g.Output()
				if err != nil {
					b.Fatal(err)
				}
				outputBytes += len(out)
			}
		}
		encoded, err := json.Marshal(state.DriverResponse{Generator: ops})
		if err != nil {
			b.Fatal(err)
		}
		for _, component := range []string{"grouping_hash", "output_materialization"} {
			b.Run(component+"/"+tc.name, func(b *testing.B) {
				b.ReportAllocs()
				b.ResetTimer()
				for n := 0; n < b.N; n++ {
					if component == "grouping_hash" {
						benchmarkOpcodePlan(ops)
					} else {
						for _, g := range outputOps {
							out, err := g.Output()
							if err != nil {
								b.Fatal(err)
							}
							stagedOutputSink = out
						}
					}
				}
				b.StopTimer()
				b.ReportMetric(float64(len(encoded)), "response-B")
				b.ReportMetric(float64(rawBytes), "raw-data-B")
				b.ReportMetric(float64(outputBytes), "saved-output-B")
			})
		}
	}
}
