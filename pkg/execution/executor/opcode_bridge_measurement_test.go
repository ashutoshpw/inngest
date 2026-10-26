//go:build linux && opcode_bridge_spike

package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inngest/inngest/internal/opcodespike"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/state"
)

const opcodeSpikeRSSInterval = 10 * time.Millisecond

type opcodeSpikeWorkload struct {
	name         string
	ops          []*state.GeneratorOpcode
	responseJSON []byte
}

type opcodeSpikeLatency struct {
	Lane                    string                        `json:"lane"`
	Workload                string                        `json:"workload"`
	Concurrency             int                           `json:"concurrency"`
	Samples                 int                           `json:"samples"`
	P50Nanoseconds          int64                         `json:"p50Nanoseconds"`
	P95Nanoseconds          int64                         `json:"p95Nanoseconds"`
	P99Nanoseconds          int64                         `json:"p99Nanoseconds"`
	ThroughputPerSec        float64                       `json:"throughputPerSecond"`
	StartRSSBytes           uint64                        `json:"aggregateStartRSSBytes"`
	PeakRSSBytes            uint64                        `json:"aggregatePeakRSSBytes"`
	FinalRSSBytes           uint64                        `json:"aggregateFinalRSSBytes"`
	GrowthRSSBytes          uint64                        `json:"aggregatePeakMinusInitialRSSBytes"`
	RSSScope                string                        `json:"rssScope"`
	WorkerProcesses         int                           `json:"workerProcesses"`
	StartupReadyNanoseconds *int64                        `json:"startupReadyNanoseconds,omitempty"`
	FirstRequestNanoseconds *int64                        `json:"firstRequestNanoseconds,omitempty"`
	WarmupRequests          int                           `json:"warmupRequestsExcluded,omitempty"`
	Resources               []opcodeSpikeResourceSnapshot `json:"resources,omitempty"`
	RSSPeaks                *opcodeSpikeRSSPeaks          `json:"sampledRSSPeaks,omitempty"`
}

type opcodeSpikeStartup struct {
	Lane         string             `json:"lane"`
	Workload     string             `json:"workload"`
	Ready        opcodeSpikeLatency `json:"spawnThroughReady"`
	FirstRequest opcodeSpikeLatency `json:"firstCompletedRequest"`
}

type opcodeSpikeReport struct {
	SchemaVersion       int                  `json:"schemaVersion"`
	GoVersion           string               `json:"goVersion"`
	RustVersion         string               `json:"rustVersion"`
	GOOS                string               `json:"goos"`
	GOARCH              string               `json:"goarch"`
	SamplesPerRow       int                  `json:"samplesPerLaneWorkloadConcurrency"`
	StartupSamples      int                  `json:"startupSamples"`
	RSSSampleIntervalNS int64                `json:"rssSampleIntervalNanoseconds"`
	LatencyScope        string               `json:"latencyScope"`
	ResourceScope       string               `json:"resourceScope"`
	ResourceLimitations string               `json:"resourceLimitations"`
	Startup             []opcodeSpikeStartup `json:"processStartup,omitempty"`
	RowOrderSeed        *int64               `json:"rowOrderSeed,omitempty"`
	SkippedLanes        []string             `json:"skippedLanes,omitempty"`
	Results             []opcodeSpikeLatency `json:"results"`
}

func TestOpcodeBridgeMeasurementHarness(t *testing.T) {
	if os.Getenv("OPCODE_SPIKE_MEASURE") != "1" {
		t.Skip("set OPCODE_SPIKE_MEASURE=1 to run the separate 10k-sample measurement campaign")
	}
	samples := positiveEnvInt(t, "OPCODE_SPIKE_MEASURE_SAMPLES", 10_000)
	startupSamples := positiveEnvInt(t, "OPCODE_SPIKE_STARTUP_SAMPLES", min(samples, 500))
	if lane := os.Getenv("OPCODE_SPIKE_MEASURE_CHILD_LANE"); lane != "" {
		measureOpcodeSpikeChild(t, lane, samples)
		return
	}
	lanes := selectOpcodeSpikeNames(t, "OPCODE_SPIKE_MEASURE_LANES", opcodeSpikeMeasurementLanes())
	workloads := selectedOpcodeSpikeWorkloads(t, false)
	concurrencies := selectOpcodeSpikeNames(t, "OPCODE_SPIKE_MEASURE_CONCURRENCIES", []string{"1", "8"})

	report := opcodeSpikeReport{
		SchemaVersion:       3,
		GoVersion:           runtime.Version(),
		RustVersion:         "1.94.0",
		GOOS:                runtime.GOOS,
		GOARCH:              runtime.GOARCH,
		SamplesPerRow:       samples,
		RSSSampleIntervalNS: int64(opcodeSpikeRSSInterval),
		LatencyScope:        "individual synchronous calls, from before request ID assignment and input decoding through final groups/IDs/flags; includes pool admission and transport; excludes workload dispatch, preflight, pool startup and shutdown; throughput is closed-loop wall throughput at the stated caller concurrency",
		ResourceScope:       "fresh test subprocess per lane/workload/concurrency; steady-state host RSS plus only that lane's live workers; baseline before pool creation; warmup/steady initial after first request, preflight, GC and measurement-buffer allocation",
		ResourceLimitations: "10ms sampled RSS can miss short peaks and sampler contention can affect latency; aggregate is summed at each observation, not a sum of independent peaks, and double-counts shared pages; host includes test runtime and workload fixtures; Rust workers have no Go goroutine count; threads and RSS may remain retained after close; startup/first-request distributions are serial fresh size-one pools and have no sampled resource peaks; row startup is through all pool workers READY; the single first request in each row is before preflight; steady samples exclude both and preflight warmup",
	}
	if !opcodeSpikeFFIEnabled() {
		report.SkippedLanes = []string{"ffi_decoded: requires opcode_bridge_ffi and cgo", "ffi_full_driver_response_json: requires opcode_bridge_ffi and cgo"}
	}
	type row struct {
		lane        string
		workload    opcodeSpikeWorkload
		concurrency int
	}
	var rows []row
	for _, lane := range lanes {
		for _, workload := range workloads {
			if strings.HasPrefix(lane, "process_") && opcodeSpikeRepresentativeWorkload(workload.name) {
				report.Startup = append(report.Startup, measureOpcodeWorkerStartup(t, lane, workload, startupSamples))
				report.StartupSamples = startupSamples
			}
			for _, value := range concurrencies {
				concurrency, _ := strconv.Atoi(value) // Values validated against the exact allowed list.
				rows = append(rows, row{lane, workload, concurrency})
			}
		}
	}
	if value := os.Getenv("OPCODE_SPIKE_MEASURE_SEED"); value != "" {
		seed, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatalf("OPCODE_SPIKE_MEASURE_SEED: %v", err)
		}
		report.RowOrderSeed = &seed
		rand.New(rand.NewSource(seed)).Shuffle(len(rows), func(i, j int) { rows[i], rows[j] = rows[j], rows[i] })
	}
	for _, row := range rows {
		report.Results = append(report.Results, runOpcodeSpikeMeasurementChild(t, row.lane, row.workload.name, row.concurrency, samples))
	}

	if t.Failed() {
		t.Fatal("measurement stages failed; refusing report")
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if outputPath := os.Getenv("OPCODE_SPIKE_MEASURE_OUTPUT"); outputPath != "" {
		absolute, err := filepath.Abs(outputPath)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(absolute, encoded, 0o600); err != nil {
			t.Fatal(err)
		}
		t.Logf("measurement report written to %s", absolute)
	} else {
		t.Logf("measurement report: %s", encoded)
	}
}

// Each row gets a fresh host process, so a previous lane cannot leave its heap
// or worker pool in the next lane's resource baseline.
func runOpcodeSpikeMeasurementChild(t *testing.T, lane, workload string, concurrency, samples int) opcodeSpikeLatency {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(t.TempDir(), "row.json")
	command := exec.Command(executable, "-test.run=^TestOpcodeBridgeMeasurementHarness$", "-test.count=1", "-test.timeout=5m")
	command.Env = opcodeSpikeChildEnv(os.Environ(), map[string]string{
		"OPCODE_SPIKE_MEASURE":                   "1",
		"OPCODE_SPIKE_MEASURE_CHILD_LANE":        lane,
		"OPCODE_SPIKE_MEASURE_CHILD_WORKLOAD":    workload,
		"OPCODE_SPIKE_MEASURE_CHILD_CONCURRENCY": strconv.Itoa(concurrency),
		"OPCODE_SPIKE_MEASURE_SAMPLES":           strconv.Itoa(samples),
		"OPCODE_SPIKE_MEASURE_OUTPUT":            output,
	})
	if logs, err := command.CombinedOutput(); err != nil {
		t.Fatalf("measurement child %s/%s/%d: %v\n%s", lane, workload, concurrency, err, logs)
	}
	encoded, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var result opcodeSpikeLatency
	if err := json.Unmarshal(encoded, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func opcodeSpikeChildEnv(environment []string, overrides map[string]string) []string {
	result := make([]string, 0, len(environment)+len(overrides))
	for _, entry := range environment {
		key, _, _ := strings.Cut(entry, "=")
		if _, replaced := overrides[key]; !replaced {
			result = append(result, entry)
		}
	}
	for key, value := range overrides {
		result = append(result, key+"="+value)
	}
	return result
}

func opcodeSpikeMeasurementLanes() []string {
	lanes := []string{"go_helper_decoded", "go_helper_full_driver_response_json", "process_decoded", "process_full_driver_response_json"}
	if opcodeSpikeFFIEnabled() {
		lanes = append(lanes, "ffi_decoded", "ffi_full_driver_response_json")
	}
	return lanes
}

func opcodeSpikeKnownLane(lane string) bool {
	for _, available := range opcodeSpikeMeasurementLanes() {
		if lane == available {
			return true
		}
	}
	return false
}

func newOpcodeSpikeLanePool(lane string, concurrency int) (*opcodespike.Pool, error) {
	if !opcodeSpikeKnownLane(lane) {
		return nil, fmt.Errorf("unknown or unavailable measurement lane %q", lane)
	}
	if !strings.HasPrefix(lane, "process_") {
		return nil, nil
	}
	path, err := opcodeBridgeWorkerPath()
	if err != nil {
		return nil, err
	}
	return opcodespike.NewPool(context.Background(), opcodespike.PoolConfig{
		Worker: opcodespike.WorkerConfig{BinaryPath: path}, Size: concurrency, MaxQueued: concurrency,
	})
}

func measureOpcodeSpikeChild(t *testing.T, lane string, samples int) {
	t.Helper()
	if !opcodeSpikeKnownLane(lane) {
		t.Fatalf("unknown or unavailable child lane %q", lane)
	}
	concurrency := positiveEnvInt(t, "OPCODE_SPIKE_MEASURE_CHILD_CONCURRENCY", 1)
	if concurrency != 1 && concurrency != 8 {
		t.Fatal("measurement concurrency must be 1 or 8")
	}
	var workload opcodeSpikeWorkload
	for _, candidate := range opcodeSpikeWorkloads(t) {
		if candidate.name == os.Getenv("OPCODE_SPIKE_MEASURE_CHILD_WORKLOAD") {
			workload = candidate
			break
		}
	}
	if workload.name == "" {
		t.Fatal("unknown measurement workload")
	}
	// Initialize runtime pipe polling and GC before the resource baseline, so
	// lazy runtime descriptors are not mistaken for leaked worker pipes.
	prepareOpcodeSpikeResourceBaseline(t)
	baseline := captureOpcodeSpikeResources(t, "baseline", nil, false)
	began := time.Now()
	pool, err := newOpcodeSpikeLanePool(lane, concurrency)
	ready := time.Since(began).Nanoseconds()
	if err != nil {
		t.Fatalf("startup READY %s: %v", lane, err)
	}
	if pool != nil {
		defer func() {
			if err := pool.Close(); err != nil {
				t.Errorf("close measurement pool: %v", err)
			}
		}()
	}
	var pids []int
	if pool != nil {
		pids = pool.PIDs()
		if len(pids) != concurrency {
			t.Fatalf("READY worker count = %d, want %d", len(pids), concurrency)
		}
	}
	readyResources := captureOpcodeSpikeResources(t, "ready", pids, false)
	// The first request has no oracle or preflight before it. FFI alone needs
	// a caller ID; steady IDs start after the preflight IDs 1 and 2.
	began = time.Now()
	var firstID uint64
	if strings.HasPrefix(lane, "ffi_") {
		firstID = 3
	}
	first, err := runOpcodeSpikeLane(lane, workload, pool, firstID)
	firstRequest := time.Since(began).Nanoseconds()
	if err != nil {
		t.Fatalf("first request %s/%s: %v", lane, workload.name, err)
	}
	ops, err := decodeOpcodeSpikeWorkload(lane, workload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(first, opcodeSpikeGoOutput(ops)) {
		t.Fatal("first request output mismatch")
	}
	if err := preflightOpcodeSpikeLane(lane, workload, pool); err != nil {
		t.Fatalf("preflight %s/%s: %v", lane, workload.name, err)
	}
	// Serial pool admission rotates through the available slots. One call per
	// slot makes every worker warm, including a size-eight pool whose first
	// request and preflight alone would only reach three workers.
	for index := 0; index < concurrency; index++ {
		var requestID uint64
		if strings.HasPrefix(lane, "ffi_") {
			requestID = uint64(index) + 4
		}
		output, err := runOpcodeSpikeLane(lane, workload, pool, requestID)
		if err != nil {
			t.Fatalf("warmup %s/%s request %d: %v", lane, workload.name, index, err)
		}
		runtime.KeepAlive(output)
	}
	// Oracle allocations are outside timing and collected before steady RSS.
	runtime.GC()
	result := measureOpcodeSpikeLane(t, lane, workload, concurrency, samples, pool)
	result.WarmupRequests = concurrency + 2 // Bridge preflight sends two requests.
	if strings.HasPrefix(lane, "go_helper_") {
		result.WarmupRequests-- // Go preflight calls the measured helper path once.
	}
	result.FirstRequestNanoseconds = &firstRequest
	if pool != nil {
		result.StartupReadyNanoseconds = &ready
	}
	result.Resources = append([]opcodeSpikeResourceSnapshot{baseline, readyResources}, result.Resources...)
	if pool != nil {
		if !reflect.DeepEqual(pids, pool.PIDs()) {
			t.Fatal("worker PID set changed during measurement")
		}
		if err := pool.Close(); err != nil {
			t.Fatalf("close measurement pool: %v", err)
		}
	}
	after := closedOpcodeSpikeResources(t, baseline, pids)
	result.Resources = append(result.Resources, after)

	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(os.Getenv("OPCODE_SPIKE_MEASURE_OUTPUT"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

// Campaign commands select one exact lane/workload per fresh benchmark process.
// These allocation/throughput benchmarks are serial, with a size-one process
// pool. Matched concurrency 1/8 wall throughput comes from the latency harness.
func BenchmarkOpcodeBridgeSpikeSmoke(b *testing.B) {
	lanes := selectOpcodeSpikeNames(b, "OPCODE_SPIKE_MEASURE_LANES", opcodeSpikeAllMeasurementLanes())
	for _, lane := range lanes {
		for _, workload := range selectedOpcodeSpikeWorkloads(b, true) {
			b.Run(lane+"/"+workload.name+"/serial", func(b *testing.B) {
				if strings.HasPrefix(lane, "ffi_") && !opcodeSpikeFFIEnabled() {
					b.Skip("FFI lane requires opcode_bridge_ffi and cgo")
				}
				pool, err := newOpcodeSpikeLanePool(lane, 1)
				if err != nil {
					b.Fatal(err)
				}
				if pool != nil {
					defer func() {
						if err := pool.Close(); err != nil {
							b.Error(err)
						}
					}()
				}
				if err := preflightOpcodeSpikeLane(lane, workload, pool); err != nil {
					b.Fatal(err)
				}
				isFFI := strings.HasPrefix(lane, "ffi_")
				b.ReportAllocs()
				b.ResetTimer()
				for index := 0; index < b.N; index++ {
					var requestID uint64
					if isFFI {
						requestID = uint64(index) + 3
					}
					output, err := runOpcodeSpikeLane(lane, workload, pool, requestID)
					if err != nil {
						b.Fatal(err)
					}
					runtime.KeepAlive(output)
				}
				b.StopTimer()
			})
		}
	}
}

func opcodeSpikeRepresentativeWorkload(name string) bool {
	return name == "small1" || name == "mixed8" || name == "large1000"
}

func opcodeSpikeAllMeasurementLanes() []string {
	return []string{"go_helper_decoded", "go_helper_full_driver_response_json", "process_decoded", "process_full_driver_response_json", "ffi_decoded", "ffi_full_driver_response_json"}
}

func selectOpcodeSpikeNames(t testing.TB, name string, allowed []string) []string {
	t.Helper()
	selected, err := opcodeSpikeSelectNames(os.Getenv(name), allowed)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return selected
}

func opcodeSpikeSelectNames(value string, allowed []string) ([]string, error) {
	if value == "" {
		return allowed, nil
	}
	selected := strings.Split(value, ",")
	seen := make(map[string]bool)
	for _, name := range selected {
		found := false
		for _, candidate := range allowed {
			if name == candidate {
				found = true
				break
			}
		}
		if !found || seen[name] {
			return nil, fmt.Errorf("unknown, unavailable or duplicate exact selection %q (allowed: %s)", name, strings.Join(allowed, ","))
		}
		seen[name] = true
	}
	return selected, nil
}

func selectedOpcodeSpikeWorkloads(t testing.TB, representativeOnly bool) []opcodeSpikeWorkload {
	t.Helper()
	workloads := opcodeSpikeWorkloads(t)
	if representativeOnly {
		var representative []opcodeSpikeWorkload
		for _, workload := range workloads {
			if opcodeSpikeRepresentativeWorkload(workload.name) {
				representative = append(representative, workload)
			}
		}
		workloads = representative
	}
	names := make([]string, len(workloads))
	for i := range workloads {
		names[i] = workloads[i].name
	}
	var selected []opcodeSpikeWorkload
	for _, name := range selectOpcodeSpikeNames(t, "OPCODE_SPIKE_MEASURE_WORKLOADS", names) {
		for _, workload := range workloads {
			if workload.name == name {
				selected = append(selected, workload)
				break
			}
		}
	}
	return selected
}

func opcodeSpikeWorkloads(t testing.TB) []opcodeSpikeWorkload {
	t.Helper()
	workloads := opcodeSpikeWorkloadsForBenchmark()
	for index := range workloads {
		var err error
		response := state.DriverResponse{Generator: workloads[index].ops, Output: map[string]any{"opaque": true}}
		workloads[index].responseJSON, err = json.Marshal(response)
		if err != nil {
			t.Fatalf("workload %s response JSON: %v", workloads[index].name, err)
		}
	}
	return workloads
}

func opcodeSpikeWorkloadsForBenchmark() []opcodeSpikeWorkload {
	makeOpcode := func(op enums.Opcode, id, name string) *state.GeneratorOpcode {
		return &state.GeneratorOpcode{Op: op, ID: id, Name: name}
	}

	small := []*state.GeneratorOpcode{makeOpcode(enums.OpcodeStepRun, "small", "small")}
	mixed := []*state.GeneratorOpcode{
		makeOpcode(enums.OpcodeStepRun, "a", "a"),
		makeOpcode(enums.OpcodeWaitForEvent, "b", "b"),
		makeOpcode(enums.OpcodeSleep, "c", "c"),
		makeOpcode(enums.OpcodeDeferAdd, "d", "d"),
		makeOpcode(enums.OpcodeStepPlanned, "e", "e"),
		makeOpcode(enums.OpcodeDeferAbort, "f", "f"),
		makeOpcode(enums.OpcodeGateway, "g", "g"),
		makeOpcode(enums.OpcodeRunComplete, "h", "h"),
	}
	large := make([]*state.GeneratorOpcode, 1000)
	choices := []enums.Opcode{enums.OpcodeStepRun, enums.OpcodeWaitForEvent, enums.OpcodeDeferAdd, enums.OpcodeSleep, enums.OpcodeStepError}
	for index := range large {
		large[index] = makeOpcode(choices[index%len(choices)], fmt.Sprintf("large-%d", index%40), fmt.Sprintf("large-%d", index))
	}
	lazy := []*state.GeneratorOpcode{
		makeOpcode(enums.OpcodeDeferAdd, "lazy-a", "lazy-a"),
		makeOpcode(enums.OpcodeDeferAbort, "lazy-b", "lazy-b"),
	}
	priority := []*state.GeneratorOpcode{
		makeOpcode(enums.OpcodeWaitForEvent, "wait-a", "wait-a"),
		makeOpcode(enums.OpcodeStepRun, "step-a", "step-a"),
		makeOpcode(enums.OpcodeDeferAdd, "defer-a", "defer-a"),
		makeOpcode(enums.OpcodeWaitForEvent, "wait-b", "wait-b"),
	}
	duplicates := []*state.GeneratorOpcode{
		makeOpcode(enums.OpcodeWaitForEvent, "", "empty-id"),
		makeOpcode(enums.OpcodeStepRun, "duplicate", "first duplicate"),
		makeOpcode(enums.OpcodeStepRun, "duplicate", "second duplicate"),
		makeOpcode(enums.OpcodeDeferAbort, "duplicate", "lazy duplicate"),
	}
	duplicates[1].Data = json.RawMessage(`{"value":1}`)
	duplicates[1].Opts = map[string]any{"opaque": "first"}
	duplicates[2].Data = json.RawMessage(`{"value":2}`)
	duplicates[2].Opts = map[string]any{"opaque": "second"}
	opaque := []*state.GeneratorOpcode{
		{
			Op:   enums.OpcodeStepRun,
			ID:   "opaque",
			Name: "opaque payload",
			Data: json.RawMessage(`{"bytes":[0,1,2],"unicode":"π"}`),
			Opts: map[string]any{"nested": map[string]any{"raw": "value", "count": 12}},
		},
		makeOpcode(enums.OpcodeWaitForSignal, "signal", "signal"),
	}

	return []opcodeSpikeWorkload{
		{name: "small1", ops: small},
		{name: "mixed8", ops: mixed},
		{name: "large1000", ops: large},
		{name: "empty", ops: []*state.GeneratorOpcode{}},
		{name: "lazy", ops: lazy},
		{name: "priority", ops: priority},
		{name: "duplicates", ops: duplicates},
		{name: "opaque_payload", ops: opaque},
	}
}

// The comparable output is the actual executor groups, non-lazy IDs and plan
// flag. In particular, the Go baseline does not construct an index projection.
type opcodeSpikeOutput struct {
	groups     OpcodeGroups
	nonLazyIDs []string
	hasPlan    bool
}

func decodeOpcodeSpikeWorkload(lane string, workload opcodeSpikeWorkload) ([]*state.GeneratorOpcode, error) {
	if strings.Contains(lane, "full_driver_response_json") {
		var response state.DriverResponse
		if err := json.Unmarshal(workload.responseJSON, &response); err != nil {
			return nil, fmt.Errorf("decode DriverResponse: %w", err)
		}
		return response.Generator, nil
	}
	return workload.ops, nil
}

func opcodeSpikeGoOutput(ops []*state.GeneratorOpcode) opcodeSpikeOutput {
	groups := opGroups(ops)
	return opcodeSpikeOutput{groups: groups, nonLazyIDs: groups.NonLazyIDs(), hasPlan: hasPlanOp(ops)}
}

func opcodeSpikeBridgeProjection(lane string, ops []*state.GeneratorOpcode, pool *opcodespike.Pool, requestID uint64) (opcodespike.Projection, error) {
	// Projection allocation, input checks, codec copies, transport and semantic
	// result validation in DecodeResult are deliberately part of bridge timing.
	tags, err := opcodespike.TagsFromOpcodes(ops)
	if err != nil {
		return opcodespike.Projection{}, err
	}
	switch {
	case strings.HasPrefix(lane, "process_"):
		if pool == nil {
			return opcodespike.Projection{}, errors.New("process lane has no worker pool")
		}
		return pool.Evaluate(context.Background(), tags)
	case strings.HasPrefix(lane, "ffi_"):
		return opcodeSpikeEvaluateFFI(context.Background(), tags, requestID)
	default:
		return opcodespike.Projection{}, fmt.Errorf("unknown bridge lane %q", lane)
	}
}

func opcodeSpikeMaterializeOutput(ops []*state.GeneratorOpcode, projection opcodespike.Projection) opcodeSpikeOutput {
	output := opcodeSpikeOutput{
		groups: OpcodeGroups{
			PriorityGroup: OpcodeGroup{Opcodes: make([]*state.GeneratorOpcode, 0, len(projection.PriorityIndices)), ShouldStartHistoryGroup: projection.HistoryGroup},
			OtherGroup:    OpcodeGroup{Opcodes: make([]*state.GeneratorOpcode, 0, len(projection.OtherIndices)), ShouldStartHistoryGroup: projection.HistoryGroup},
		},
		nonLazyIDs: make([]string, 0, len(projection.NonLazyIndices)),
		hasPlan:    projection.HasPlanOp,
	}
	for _, index := range projection.PriorityIndices {
		output.groups.PriorityGroup.Opcodes = append(output.groups.PriorityGroup.Opcodes, ops[index])
	}
	for _, index := range projection.OtherIndices {
		output.groups.OtherGroup.Opcodes = append(output.groups.OtherGroup.Opcodes, ops[index])
	}
	for _, index := range projection.NonLazyIndices {
		output.nonLazyIDs = append(output.nonLazyIDs, ops[index].ID)
	}
	return output
}

func runOpcodeSpikeLane(lane string, workload opcodeSpikeWorkload, pool *opcodespike.Pool, requestID uint64) (opcodeSpikeOutput, error) {
	ops, err := decodeOpcodeSpikeWorkload(lane, workload)
	if err != nil {
		return opcodeSpikeOutput{}, err
	}
	if strings.HasPrefix(lane, "go_helper_") {
		return opcodeSpikeGoOutput(ops), nil
	}
	projection, err := opcodeSpikeBridgeProjection(lane, ops, pool, requestID)
	if err != nil {
		return opcodeSpikeOutput{}, err
	}
	return opcodeSpikeMaterializeOutput(ops, projection), nil
}

func preflightOpcodeSpikeLane(lane string, workload opcodeSpikeWorkload, pool *opcodespike.Pool) error {
	ops, err := decodeOpcodeSpikeWorkload(lane, workload)
	if err != nil {
		return err
	}
	// Preconditions and the independent copying/index oracle are preflight only.
	if _, err := opcodespike.TagsFromOpcodes(ops); err != nil {
		return err
	}
	if !strings.HasPrefix(lane, "go_helper_") {
		projection, err := opcodeSpikeBridgeProjection(lane, ops, pool, 1)
		if err != nil {
			return err
		}
		if _, err := verifyOpcodeBridgeResult(projection, goOpcodeHelperOracle(ops)); err != nil {
			return err
		}
	}
	got, err := runOpcodeSpikeLane(lane, workload, pool, 2)
	if err != nil {
		return err
	}
	if want := opcodeSpikeGoOutput(ops); !reflect.DeepEqual(got, want) {
		return fmt.Errorf("measurement output disagrees with Go groups/IDs/flags: got=%+v want=%+v", got, want)
	}
	return nil
}

// Each paired sample uses a fresh worker and representative end-to-end input.
// READY excludes the request; first request excludes spawn, READY and shutdown.
func measureOpcodeWorkerStartup(t *testing.T, lane string, workload opcodeSpikeWorkload, samples int) opcodeSpikeStartup {
	t.Helper()
	ready := make([]time.Duration, samples)
	first := make([]time.Duration, samples)
	for index := range ready {
		began := time.Now()
		pool, err := newOpcodeSpikeLanePool(lane, 1)
		ready[index] = time.Since(began)
		if err != nil {
			t.Fatalf("startup %s/%s sample %d: %v", lane, workload.name, index, err)
		}
		// Ensure cleanup even if a validation stage fails.
		func() {
			defer func() {
				if err := pool.Close(); err != nil {
					t.Errorf("startup shutdown sample %d: %v", index, err)
				}
			}()
			began = time.Now()
			got, err := runOpcodeSpikeLane(lane, workload, pool, 0)
			first[index] = time.Since(began)
			if err != nil {
				t.Fatalf("startup first request %s/%s sample %d: %v", lane, workload.name, index, err)
			}
			// Materialized equality is checked after the timer, with no prior request.
			ops, err := decodeOpcodeSpikeWorkload(lane, workload)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, opcodeSpikeGoOutput(ops)) {
				t.Fatal("startup first request output mismatch")
			}
		}()
	}
	result := opcodeSpikeStartup{Lane: lane, Workload: workload.name,
		Ready:        summarizeOpcodeSpikeLatency("process_startup_ready", workload.name, 1, ready, sumDurations(ready), 0, 0, 0),
		FirstRequest: summarizeOpcodeSpikeLatency("process_first_request", workload.name, 1, first, sumDurations(first), 0, 0, 0),
	}
	result.Ready.RSSScope = "not_sampled"
	result.FirstRequest.RSSScope = "not_sampled"
	return result
}

func measureOpcodeSpikeLane(
	t *testing.T,
	lane string,
	workload opcodeSpikeWorkload,
	concurrency int,
	samples int,
	pool *opcodespike.Pool,
) opcodeSpikeLatency {
	t.Helper()
	isFFI := strings.HasPrefix(lane, "ffi_")
	var ffiRequestID atomic.Uint64
	if isFFI {
		// Preflight uses 1/2, first request 3, warmup 4 through 3+concurrency.
		// Process workers assign their own IDs and Go helpers need none.
		ffiRequestID.Store(uint64(3 + concurrency))
	}
	durations := make([]time.Duration, samples)
	jobs := make(chan int, samples)
	for index := 0; index < samples; index++ {
		jobs <- index
	}
	close(jobs)
	start := make(chan struct{})
	abort := make(chan struct{})
	var workers sync.WaitGroup
	var errOnce sync.Once
	var firstErr error
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			select {
			case <-start:
			case <-abort:
				return
			}
			for index := range jobs {
				began := time.Now()
				var requestID uint64
				if isFFI {
					requestID = ffiRequestID.Add(1)
				}
				output, err := runOpcodeSpikeLane(lane, workload, pool, requestID)
				runtime.KeepAlive(output)
				durations[index] = time.Since(began)
				if err != nil {
					errOnce.Do(func() { firstErr = err })
					return
				}
			}
		}()
	}

	var pids []int
	if pool != nil {
		pids = pool.PIDs()
	}
	tracker, err := newOpcodeSpikeResourceTracker(func() (opcodeSpikeResourceSnapshot, error) { return readOpcodeSpikeResources(pids, false) }, opcodeSpikeRSSInterval)
	if err != nil {
		close(abort)
		workers.Wait()
		t.Fatalf("steady initial resources: %v", err)
	}
	wallStart := time.Now()
	close(start)
	workers.Wait()
	wall := time.Since(wallStart)
	initial, peaks, final, err := tracker.Stop()
	if err != nil {
		t.Fatalf("steady resource sampling: %v", err)
	}
	startRSS, peakRSS, finalRSS := initial.AggregateRSSBytes, peaks.AggregateRSSBytes, final.AggregateRSSBytes
	if firstErr != nil {
		t.Fatalf("measurement %s/%s/%d: %v", lane, workload.name, concurrency, firstErr)
	}
	result := summarizeOpcodeSpikeLatency(lane, workload.name, concurrency, durations, wall, startRSS, peakRSS, finalRSS)
	initial.Stage = "warmup"
	final.Stage = "end"
	result.Resources = []opcodeSpikeResourceSnapshot{initial, final}
	result.RSSPeaks = &peaks
	result.RSSScope = "isolated_host"
	if pool != nil {
		result.RSSScope = "isolated_host_plus_lane_workers"
		result.WorkerProcesses = len(pool.PIDs())
	}
	return result
}

func summarizeOpcodeSpikeLatency(lane, workload string, concurrency int, durations []time.Duration, wall time.Duration, startRSS, peakRSS, finalRSS uint64) opcodeSpikeLatency {
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	throughput := 0.0
	if wall > 0 {
		throughput = float64(len(durations)) / wall.Seconds()
	}
	growth := uint64(0)
	if peakRSS > startRSS {
		growth = peakRSS - startRSS
	}
	return opcodeSpikeLatency{
		Lane:             lane,
		Workload:         workload,
		Concurrency:      concurrency,
		Samples:          len(durations),
		P50Nanoseconds:   percentileNanoseconds(sorted, 0.50),
		P95Nanoseconds:   percentileNanoseconds(sorted, 0.95),
		P99Nanoseconds:   percentileNanoseconds(sorted, 0.99),
		ThroughputPerSec: throughput,
		StartRSSBytes:    startRSS,
		PeakRSSBytes:     peakRSS,
		FinalRSSBytes:    finalRSS,
		GrowthRSSBytes:   growth,
	}
}

func percentileNanoseconds(sorted []time.Duration, percentile float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	index := int(math.Ceil(percentile*float64(len(sorted)))) - 1
	index = max(0, min(index, len(sorted)-1))
	return sorted[index].Nanoseconds()
}

func sumDurations(durations []time.Duration) time.Duration {
	var total time.Duration
	for _, duration := range durations {
		total += duration
	}
	return total
}

func positiveEnvInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		t.Fatalf("%s must be a positive integer: %q", name, value)
	}
	return parsed
}
