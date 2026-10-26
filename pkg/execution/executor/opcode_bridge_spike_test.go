//go:build linux && opcode_bridge_spike

package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"sync"
	"testing"

	"github.com/inngest/inngest/internal/opcodespike"
	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/state"
)

var errOpcodeBridgeOracleMismatch = errors.New("opcode bridge disagrees with the Go helper oracle")

func TestOpcodeBridgeSpikeFixtureDifferential(t *testing.T) {
	pool := newOpcodeBridgeSpikePool(t, 1, 8)
	defer func() {
		if err := pool.Close(); err != nil {
			t.Errorf("close Rust worker: %v", err)
		}
	}()

	fixture := loadOpcodePlanFixture(t)
	if len(fixture.Cases) != 27 {
		t.Fatalf("fixture cases = %d, want exact accepted corpus of 27", len(fixture.Cases))
	}
	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			tags, err := opcodespike.TagsFromOpcodes(tc.Input)
			if err != nil {
				t.Fatalf("pre-bridge input validation: %v", err)
			}
			got, err := pool.Evaluate(context.Background(), tags)
			if err != nil {
				t.Fatalf("Rust process bridge: %v", err)
			}
			wantGo := goOpcodeHelperOracle(tc.Input)
			verified, err := verifyOpcodeBridgeResult(got, wantGo)
			if err != nil {
				t.Fatal(err)
			}
			wantFixture := opcodespike.Projection{
				PriorityIndices: uint32Indices(tc.Expected.PriorityIndices),
				OtherIndices:    uint32Indices(tc.Expected.OtherIndices),
				NonLazyIndices:  uint32Indices(tc.Expected.NonLazyIndices),
				HistoryGroup:    tc.Expected.HistoryGroup,
				HasPlanOp:       tc.Expected.HasPlanOp,
			}
			if !reflect.DeepEqual(verified, wantFixture) {
				t.Fatalf("Rust result = %+v, literal fixture = %+v", verified, wantFixture)
			}

			materialized := materializeOpcodeProjection(tc.Input, verified)
			if tc.Name == "nonlazy-ids-retain-duplicates-and-empty-values" {
				requireOpcodePayloadIdentity(t, tc.Input, materialized)
			}
		})
	}
}

func TestOpcodeBridgeSpikeRepeatedPointerRetainsTwoPositions(t *testing.T) {
	pool := newOpcodeBridgeSpikePool(t, 1, 1)
	defer func() { _ = pool.Close() }()

	same := &state.GeneratorOpcode{Op: enums.OpcodeStepRun, ID: "same"}
	ops := []*state.GeneratorOpcode{same, same}
	tags, err := opcodespike.TagsFromOpcodes(ops)
	if err != nil {
		t.Fatal(err)
	}
	got, err := pool.Evaluate(context.Background(), tags)
	if err != nil {
		t.Fatal(err)
	}
	want := goOpcodeHelperOracle(ops)
	got, err = verifyOpcodeBridgeResult(got, want)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.OtherIndices, []uint32{0, 1}) || !reflect.DeepEqual(got.NonLazyIndices, []uint32{0, 1}) {
		t.Fatalf("same pointer lost a position: %+v", got)
	}
	materialized := materializeOpcodeProjection(ops, got)
	if len(materialized.other) != 2 || materialized.other[0] != same || materialized.other[1] != same {
		t.Fatalf("reconstruction did not preserve both original positions: %+v", materialized.other)
	}
}

func TestOpcodeBridgeSpikeEightWorkerPoolConcurrentDifferential(t *testing.T) {
	pool := newOpcodeBridgeSpikePool(t, 8, 8)
	defer func() {
		if err := pool.Close(); err != nil {
			t.Errorf("close eight-worker pool: %v", err)
		}
	}()

	fixture := loadOpcodePlanFixture(t)
	if len(fixture.Cases) < 8 {
		t.Fatal("fixture is too small for concurrent samples")
	}
	errorsByIndex := make(chan error, 8)
	var workers sync.WaitGroup
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
			got, err := pool.Evaluate(context.Background(), tags)
			if err != nil {
				errorsByIndex <- err
				return
			}
			_, err = verifyOpcodeBridgeResult(got, goOpcodeHelperOracle(testCase.Input))
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
}

func TestOpcodeBridgeSpikeMismatchReturnsNoUsableDecision(t *testing.T) {
	want := opcodespike.Projection{
		PriorityIndices: []uint32{1},
		OtherIndices:    []uint32{0},
		NonLazyIndices:  []uint32{1, 0},
		HistoryGroup:    true,
		HasPlanOp:       true,
	}
	bad := want
	bad.HasPlanOp = false
	got, err := verifyOpcodeBridgeResult(bad, want)
	if !errors.Is(err, errOpcodeBridgeOracleMismatch) {
		t.Fatalf("mismatch error = %v", err)
	}
	if !isZeroOpcodeBridgeProjection(got) {
		t.Fatalf("mismatched result remained usable: %+v", got)
	}
}

func TestOpcodeBridgeSpikeSeededDifferential(t *testing.T) {
	pool := newOpcodeBridgeSpikePool(t, 1, 8)
	defer func() { _ = pool.Close() }()

	choices := []enums.Opcode{
		enums.OpcodeNone,
		enums.OpcodeStep,
		enums.OpcodeStepRun,
		enums.OpcodeStepError,
		enums.OpcodeStepPlanned,
		enums.OpcodeSleep,
		enums.OpcodeWaitForEvent,
		enums.OpcodeInvokeFunction,
		enums.OpcodeAIGateway,
		enums.OpcodeGateway,
		enums.OpcodeWaitForSignal,
		enums.OpcodeRunComplete,
		enums.OpcodeStepFailed,
		enums.OpcodeSyncRunComplete,
		enums.OpcodeDiscoveryRequest,
		enums.OpcodeDeferAdd,
		enums.OpcodeDeferAbort,
	}
	random := rand.New(rand.NewSource(0x4f50434f4445))
	for sample := 0; sample < 256; sample++ {
		length := random.Intn(96)
		ops := make([]*state.GeneratorOpcode, length)
		for index := range ops {
			ops[index] = &state.GeneratorOpcode{
				Op:   choices[random.Intn(len(choices))],
				ID:   fmt.Sprintf("step-%d", random.Intn(12)),
				Name: fmt.Sprintf("opaque-name-%d", sample),
				Data: json.RawMessage(`{"payload":[1,"x",null]}`),
				Opts: map[string]any{"opaque": sample, "index": index},
			}
		}
		tags, err := opcodespike.TagsFromOpcodes(ops)
		if err != nil {
			t.Fatalf("sample %d pre-bridge validation: %v", sample, err)
		}
		got, err := pool.Evaluate(context.Background(), tags)
		if err != nil {
			t.Fatalf("sample %d process result: %v", sample, err)
		}
		if _, err := verifyOpcodeBridgeResult(got, goOpcodeHelperOracle(ops)); err != nil {
			t.Fatalf("sample %d: %v", sample, err)
		}
	}
}

type materializedOpcodeProjection struct {
	priority []*state.GeneratorOpcode
	other    []*state.GeneratorOpcode
	nonLazy  []*state.GeneratorOpcode
}

func goOpcodeHelperOracle(opcodes []*state.GeneratorOpcode) opcodespike.Projection {
	positionOps := make([]*state.GeneratorOpcode, len(opcodes))
	for index, opcode := range opcodes {
		copied := *opcode
		copied.ID = strconv.Itoa(index)
		positionOps[index] = &copied
	}
	groups := opGroups(positionOps)
	priority := helperPositionIndices(groups.PriorityGroup.Opcodes)
	other := helperPositionIndices(groups.OtherGroup.Opcodes)
	nonLazy := make([]uint32, 0, len(opcodes))
	for _, id := range groups.NonLazyIDs() {
		index, err := strconv.ParseUint(id, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("position-labeled Go helper emitted invalid ID %q: %v", id, err))
		}
		nonLazy = append(nonLazy, uint32(index))
	}
	if groups.PriorityGroup.ShouldStartHistoryGroup != groups.OtherGroup.ShouldStartHistoryGroup {
		panic("Go helper history flags differ across groups")
	}
	return opcodespike.Projection{
		PriorityIndices: priority,
		OtherIndices:    other,
		NonLazyIndices:  nonLazy,
		HistoryGroup:    groups.PriorityGroup.ShouldStartHistoryGroup,
		HasPlanOp:       hasPlanOp(opcodes),
	}
}

func helperPositionIndices(opcodes []*state.GeneratorOpcode) []uint32 {
	indices := make([]uint32, 0, len(opcodes))
	for _, opcode := range opcodes {
		index, err := strconv.ParseUint(opcode.ID, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("position-labeled Go helper emitted invalid ID %q: %v", opcode.ID, err))
		}
		indices = append(indices, uint32(index))
	}
	return indices
}

func verifyOpcodeBridgeResult(got, want opcodespike.Projection) (opcodespike.Projection, error) {
	if !reflect.DeepEqual(got, want) {
		return opcodespike.Projection{}, fmt.Errorf("%w: rust=%+v go=%+v", errOpcodeBridgeOracleMismatch, got, want)
	}
	return got, nil
}

func materializeOpcodeProjection(opcodes []*state.GeneratorOpcode, projection opcodespike.Projection) materializedOpcodeProjection {
	materialized := materializedOpcodeProjection{
		priority: make([]*state.GeneratorOpcode, 0, len(projection.PriorityIndices)),
		other:    make([]*state.GeneratorOpcode, 0, len(projection.OtherIndices)),
		nonLazy:  make([]*state.GeneratorOpcode, 0, len(projection.NonLazyIndices)),
	}
	for _, index := range projection.PriorityIndices {
		materialized.priority = append(materialized.priority, opcodes[index])
	}
	for _, index := range projection.OtherIndices {
		materialized.other = append(materialized.other, opcodes[index])
	}
	for _, index := range projection.NonLazyIndices {
		materialized.nonLazy = append(materialized.nonLazy, opcodes[index])
	}
	return materialized
}

func requireOpcodePayloadIdentity(t *testing.T, input []*state.GeneratorOpcode, got materializedOpcodeProjection) {
	t.Helper()
	if len(input) != 4 || len(got.priority) != 2 || len(got.other) != 2 || len(got.nonLazy) != 3 {
		t.Fatalf("unexpected duplicate payload case shape: input=%d projection=%+v", len(input), got)
	}
	if got.priority[0] != input[0] || got.priority[1] != input[3] || got.other[0] != input[1] || got.other[1] != input[2] {
		t.Fatal("materialization did not use original indices")
	}
	if got.other[0] == got.other[1] || got.other[0].Name != "first duplicate" || got.other[1].Name != "second duplicate" {
		t.Fatal("duplicate IDs collapsed distinct opcode payloads")
	}
	if !reflect.DeepEqual(got.other[0].Data, input[1].Data) || !reflect.DeepEqual(got.other[1].Data, input[2].Data) {
		t.Fatalf("opaque data changed: %q and %q", got.other[0].Data, got.other[1].Data)
	}
	first, firstOK := got.other[0].Opts.(map[string]any)
	second, secondOK := got.other[1].Opts.(map[string]any)
	if !firstOK || !secondOK || !reflect.DeepEqual(got.other[0].Opts, input[1].Opts) || !reflect.DeepEqual(got.other[1].Opts, input[2].Opts) || first["opaque"] != "first" || second["opaque"] != "second" {
		t.Fatalf("opaque options changed: %#v and %#v", got.other[0].Opts, got.other[1].Opts)
	}
}

func uint32Indices(indices []int) []uint32 {
	output := make([]uint32, len(indices))
	for index, value := range indices {
		output[index] = uint32(value)
	}
	return output
}

func isZeroOpcodeBridgeProjection(projection opcodespike.Projection) bool {
	return len(projection.PriorityIndices) == 0 &&
		len(projection.OtherIndices) == 0 &&
		len(projection.NonLazyIndices) == 0 &&
		!projection.HistoryGroup &&
		!projection.HasPlanOp
}

func newOpcodeBridgeSpikePool(t *testing.T, size, maxQueued int) *opcodespike.Pool {
	t.Helper()
	path, err := opcodeBridgeWorkerPath()
	if err != nil {
		t.Fatal(err)
	}
	pool, err := opcodespike.NewPool(context.Background(), opcodespike.PoolConfig{
		Worker:    opcodespike.WorkerConfig{BinaryPath: path},
		Size:      size,
		MaxQueued: maxQueued,
	})
	if err != nil {
		t.Fatal(err)
	}
	return pool
}

func opcodeBridgeWorkerPath() (string, error) {
	path := os.Getenv("OPCODE_WORKER_BIN")
	if path == "" {
		_, source, _, ok := runtime.Caller(0)
		if !ok {
			return "", errors.New("cannot locate opcode bridge test source")
		}
		root := filepath.Clean(filepath.Join(filepath.Dir(source), "../../.."))
		path = filepath.Join(root, "rust", "target", "debug", "opcode-worker")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	if _, err := os.Stat(absolute); err != nil {
		return "", fmt.Errorf("build Rust worker before tagged Go tests (%s): %w", absolute, err)
	}
	return absolute, nil
}
