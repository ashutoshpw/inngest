//go:build opcode_bridge_spike

package opcodespike

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/state"
)

func rawResultFrame(t *testing.T, requestID uint64, projection Projection, flags uint32) []byte {
	t.Helper()
	payload := make([]byte, 16+4*(len(projection.PriorityIndices)+len(projection.OtherIndices)+len(projection.NonLazyIndices)))
	binary.LittleEndian.PutUint32(payload[0:4], uint32(len(projection.PriorityIndices)))
	binary.LittleEndian.PutUint32(payload[4:8], uint32(len(projection.OtherIndices)))
	binary.LittleEndian.PutUint32(payload[8:12], uint32(len(projection.NonLazyIndices)))
	binary.LittleEndian.PutUint32(payload[12:16], flags)
	position := 16
	for _, group := range [][]uint32{projection.PriorityIndices, projection.OtherIndices, projection.NonLazyIndices} {
		for _, index := range group {
			binary.LittleEndian.PutUint32(payload[position:position+4], index)
			position += 4
		}
	}
	frame, err := EncodeFrame(KindResult, requestID, 0, payload)
	if err != nil {
		t.Fatal(err)
	}
	return frame
}

func projectionFlags(projection Projection) uint32 {
	flags := uint32(0)
	if projection.HistoryGroup {
		flags |= ResultFlagHistory
	}
	if projection.HasPlanOp {
		flags |= ResultFlagHasPlan
	}
	return flags
}

func TestExplicitWireTagMapCoversRecognizedOpcodeSet(t *testing.T) {
	goOpcodes := []enums.Opcode{
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
	for expected, opcode := range goOpcodes {
		got, ok := TagForOpcode(opcode)
		if !ok || got != Tag(expected) {
			t.Fatalf("opcode %v mapped to (%d, %t), want explicit tag %d", opcode, got, ok, expected)
		}
	}
	if _, ok := TagForOpcode(enums.Opcode(17)); ok {
		t.Fatal("unknown Go opcode was accepted")
	}
}

func TestTagsFromOpcodesRejectsInvalidExperimentInputsBeforeBridge(t *testing.T) {
	if _, err := TagsFromOpcodes([]*state.GeneratorOpcode{nil}); !errors.Is(err, ErrNilOpcode) {
		t.Fatalf("nil opcode error = %v", err)
	}
	if _, err := TagsFromOpcodes([]*state.GeneratorOpcode{{Op: enums.Opcode(17)}}); !errors.Is(err, ErrUnknownOpcode) {
		t.Fatalf("unknown opcode error = %v", err)
	}
	tooMany := make([]*state.GeneratorOpcode, MaxOpcodes+1)
	for index := range tooMany {
		tooMany[index] = &state.GeneratorOpcode{Op: enums.OpcodeNone}
	}
	if _, err := TagsFromOpcodes(tooMany); !errors.Is(err, ErrTooManyOpcodes) {
		t.Fatalf("oversize input error = %v", err)
	}
}

func TestPlanFrameAndHandshakeAreExact(t *testing.T) {
	request, err := EncodePlan(1, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(request) != HeaderSize+4 || len(request) > MaxRequestFrameSize {
		t.Fatalf("empty request frame length = %d", len(request))
	}
	frame, err := DecodeFrame(request)
	if err != nil {
		t.Fatal(err)
	}
	if frame.Kind != KindPlan || frame.RequestID != 1 || binary.LittleEndian.Uint32(frame.Payload) != 0 {
		t.Fatalf("unexpected empty plan frame: %+v", frame)
	}
	if len(HandshakeFrame()) != HeaderSize+len(handshakePayload) {
		t.Fatal("HELLO payload length changed")
	}
	ready, err := EncodeFrame(KindReady, 0, 0, handshakePayload[:])
	if err != nil {
		t.Fatal(err)
	}
	if err := ValidateReady(ready); err != nil {
		t.Fatal(err)
	}
	ready[len(ready)-1] ^= 1
	if err := ValidateReady(ready); err == nil {
		t.Fatal("mismatched capability handshake was accepted")
	}
	bye, err := EncodeFrame(KindBye, 0, 0, nil)
	if err != nil || ValidateBye(bye) != nil {
		t.Fatalf("valid BYE was rejected: frame error=%v, validation=%v", err, ValidateBye(bye))
	}
}

func TestResultValidatorAcceptsExactCoverageAndRejectsMalformedAnswers(t *testing.T) {
	tags := []Tag{TagSleep, TagWaitForEvent, TagDeferAdd, TagStepRun}
	valid := Projection{
		PriorityIndices: []uint32{1, 2},
		OtherIndices:    []uint32{0, 3},
		NonLazyIndices:  []uint32{1, 0, 3},
		HistoryGroup:    true,
		HasPlanOp:       true,
	}
	frame := rawResultFrame(t, 44, valid, projectionFlags(valid))
	decoded, err := DecodeResult(44, tags, frame)
	if err != nil {
		t.Fatal(err)
	}
	if !equalProjection(decoded, valid) {
		t.Fatalf("decoded result = %+v, want %+v", decoded, valid)
	}

	cases := []struct {
		name       string
		tags       []Tag
		projection Projection
		flags      uint32
	}{
		{"out-of-range", tags, Projection{PriorityIndices: []uint32{1, 2}, OtherIndices: []uint32{0, 9}, NonLazyIndices: []uint32{1, 0, 3}, HistoryGroup: true, HasPlanOp: true}, 3},
		{"duplicate", tags, Projection{PriorityIndices: []uint32{1, 2}, OtherIndices: []uint32{0, 0}, NonLazyIndices: []uint32{1, 0, 3}, HistoryGroup: true, HasPlanOp: true}, 3},
		{"missing", tags, Projection{PriorityIndices: []uint32{1, 2}, OtherIndices: []uint32{0}, NonLazyIndices: []uint32{1, 0, 3}, HistoryGroup: true, HasPlanOp: true}, 3},
		{"group-order", tags, Projection{PriorityIndices: []uint32{2, 1}, OtherIndices: []uint32{0, 3}, NonLazyIndices: []uint32{2, 0, 3}, HistoryGroup: true, HasPlanOp: true}, 3},
		{"priority-membership", tags, Projection{PriorityIndices: []uint32{0, 1, 2}, OtherIndices: []uint32{3}, NonLazyIndices: []uint32{0, 1, 3}, HistoryGroup: true, HasPlanOp: true}, 3},
		{"non-lazy-order", tags, Projection{PriorityIndices: []uint32{1, 2}, OtherIndices: []uint32{0, 3}, NonLazyIndices: []uint32{1, 3, 0}, HistoryGroup: true, HasPlanOp: true}, 3},
		{"lazy-in-non-lazy", tags, Projection{PriorityIndices: []uint32{1, 2}, OtherIndices: []uint32{0, 3}, NonLazyIndices: []uint32{1, 2, 0, 3}, HistoryGroup: true, HasPlanOp: true}, 3},
		{"history-flag", tags, Projection{PriorityIndices: []uint32{1, 2}, OtherIndices: []uint32{0, 3}, NonLazyIndices: []uint32{1, 0, 3}, HasPlanOp: true}, 2},
		{"plan-flag", tags, Projection{PriorityIndices: []uint32{1, 2}, OtherIndices: []uint32{0, 3}, NonLazyIndices: []uint32{1, 0, 3}, HistoryGroup: true}, 1},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if _, err := DecodeResult(44, test.tags, rawResultFrame(t, 44, test.projection, test.flags)); err == nil {
				t.Fatal("malformed projection was accepted")
			}
		})
	}

	reserved := rawResultFrame(t, 44, valid, 7)
	if _, err := DecodeResult(44, tags, reserved); err == nil {
		t.Fatal("reserved result flag was accepted")
	}
	if _, err := DecodeResult(45, tags, frame); err == nil {
		t.Fatal("mismatched request ID was accepted")
	}
	if _, err := DecodeResult(44, tags, append(append([]byte(nil), frame...), 0)); err == nil {
		t.Fatal("trailing response byte was accepted")
	}
	if _, err := DecodeResult(44, tags, frame[:len(frame)-1]); err == nil {
		t.Fatal("truncated response was accepted")
	}
}

func TestFrameReaderRejectsMalformedHeadersBeforePayloadAllocation(t *testing.T) {
	valid, err := EncodePlan(1, []Tag{TagNone})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ReadFrame(bytes.NewReader(valid[:HeaderSize])); err == nil {
		t.Fatal("truncated frame was accepted")
	}
	if _, err := ReadFrame(bytes.NewReader(append(append([]byte(nil), valid...), 0))); err != nil {
		t.Fatalf("first frame should parse; stream trailing byte is a later partial header: %v", err)
	}
	oversizedHeader := append([]byte(nil), valid[:HeaderSize]...)
	binary.LittleEndian.PutUint32(oversizedHeader[16:20], MaxOpcodes+5)
	if _, err := ReadFrame(bytes.NewReader(oversizedHeader)); err == nil {
		t.Fatal("oversized PLAN declaration was accepted")
	}
	badStatus := append([]byte(nil), valid[:HeaderSize]...)
	binary.LittleEndian.PutUint32(badStatus[20:24], 1)
	if _, err := ReadFrame(bytes.NewReader(badStatus)); err == nil {
		t.Fatal("nonzero PLAN status was accepted")
	}
	unknownKind := append([]byte(nil), valid[:HeaderSize]...)
	binary.LittleEndian.PutUint16(unknownKind[6:8], 99)
	if _, err := ReadFrame(bytes.NewReader(unknownKind)); err == nil {
		t.Fatal("unknown frame kind was accepted")
	}
	unknownVersion := append([]byte(nil), valid[:HeaderSize]...)
	binary.LittleEndian.PutUint16(unknownVersion[4:6], 99)
	if _, err := ReadFrame(bytes.NewReader(unknownVersion)); err == nil {
		t.Fatal("unknown protocol version was accepted")
	}
	if _, err := ReadFrame(bytes.NewReader(nil)); !errors.Is(err, ErrCleanEOF) {
		t.Fatalf("clean EOF = %v", err)
	}
}

func TestWriteFullHandlesShortAndZeroWriters(t *testing.T) {
	short := shortWriter{limit: 2}
	if err := WriteFull(&short, []byte("abcdef")); err != nil || string(short.data) != "abcdef" {
		t.Fatalf("short writer result = %q, %v", short.data, err)
	}
	if err := WriteFull(zeroWriter{}, []byte("x")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("zero writer error = %v", err)
	}
}

type shortWriter struct {
	limit int
	data  []byte
}

func (w *shortWriter) Write(data []byte) (int, error) {
	if len(data) > w.limit {
		data = data[:w.limit]
	}
	w.data = append(w.data, data...)
	return len(data), nil
}

type zeroWriter struct{}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func equalProjection(left, right Projection) bool {
	if left.HistoryGroup != right.HistoryGroup || left.HasPlanOp != right.HasPlanOp {
		return false
	}
	return equalUint32s(left.PriorityIndices, right.PriorityIndices) &&
		equalUint32s(left.OtherIndices, right.OtherIndices) &&
		equalUint32s(left.NonLazyIndices, right.NonLazyIndices)
}

func equalUint32s(left, right []uint32) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
