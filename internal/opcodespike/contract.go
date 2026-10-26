//go:build opcode_bridge_spike

// Package opcodespike contains the build-tagged Go side of the isolated Rust
// opcode bridge experiment. Production execution does not import this package.
package opcodespike

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/inngest/inngest/pkg/enums"
	"github.com/inngest/inngest/pkg/execution/state"
)

const (
	ProtocolVersion      uint16 = 1
	HeaderSize                  = 24
	MaxOpcodes                  = 65_536
	MaxFrameSize                = 1_048_576
	MaxRequestFrameSize         = 65_564
	MaxResultFrameSize          = 524_328
	MaxResultPayloadSize        = MaxResultFrameSize - HeaderSize
	ResultFlagHistory           = uint32(1)
	ResultFlagHasPlan           = uint32(2)
)

var (
	frameMagic          = [4]byte{'I', 'O', 'P', 'C'}
	ErrCleanEOF         = errors.New("clean end of opcode bridge stream")
	ErrNilOpcode        = errors.New("opcode bridge input contains nil opcode")
	ErrUnknownOpcode    = errors.New("opcode bridge input contains unknown opcode")
	ErrTooManyOpcodes   = errors.New("opcode bridge input exceeds experiment limit")
	ErrInvalidRequestID = errors.New("opcode bridge request ID must be nonzero")
	ErrWorkerBusy       = errors.New("opcode bridge worker is busy")
	ErrPoolClosed       = errors.New("opcode bridge worker pool is closed")
)

// Tag is a protocol-v1 opcode tag. Values are explicit and independent of the
// Go enum's iota representation.
type Tag uint8

const (
	TagNone             Tag = 0
	TagStep             Tag = 1
	TagStepRun          Tag = 2
	TagStepError        Tag = 3
	TagStepPlanned      Tag = 4
	TagSleep            Tag = 5
	TagWaitForEvent     Tag = 6
	TagInvokeFunction   Tag = 7
	TagAIGateway        Tag = 8
	TagGateway          Tag = 9
	TagWaitForSignal    Tag = 10
	TagRunComplete      Tag = 11
	TagStepFailed       Tag = 12
	TagSyncRunComplete  Tag = 13
	TagDiscoveryRequest Tag = 14
	TagDeferAdd         Tag = 15
	TagDeferAbort       Tag = 16
)

func (tag Tag) valid() bool {
	return tag <= TagDeferAbort
}

func tagPriority(tag Tag) bool {
	return tag == TagWaitForEvent || tag == TagDeferAdd || tag == TagDeferAbort
}

func tagLazy(tag Tag) bool {
	return tag == TagDeferAdd || tag == TagDeferAbort
}

func tagHasPlan(tag Tag) bool {
	switch tag {
	case TagAIGateway, TagGateway, TagInvokeFunction, TagSleep, TagStepPlanned, TagWaitForEvent, TagWaitForSignal:
		return true
	default:
		return false
	}
}

// TagForOpcode uses an explicit mapping; casting an enum to a wire byte is not
// part of this experiment's contract.
func TagForOpcode(op enums.Opcode) (Tag, bool) {
	switch op {
	case enums.OpcodeNone:
		return TagNone, true
	case enums.OpcodeStep:
		return TagStep, true
	case enums.OpcodeStepRun:
		return TagStepRun, true
	case enums.OpcodeStepError:
		return TagStepError, true
	case enums.OpcodeStepPlanned:
		return TagStepPlanned, true
	case enums.OpcodeSleep:
		return TagSleep, true
	case enums.OpcodeWaitForEvent:
		return TagWaitForEvent, true
	case enums.OpcodeInvokeFunction:
		return TagInvokeFunction, true
	case enums.OpcodeAIGateway:
		return TagAIGateway, true
	case enums.OpcodeGateway:
		return TagGateway, true
	case enums.OpcodeWaitForSignal:
		return TagWaitForSignal, true
	case enums.OpcodeRunComplete:
		return TagRunComplete, true
	case enums.OpcodeStepFailed:
		return TagStepFailed, true
	case enums.OpcodeSyncRunComplete:
		return TagSyncRunComplete, true
	case enums.OpcodeDiscoveryRequest:
		return TagDiscoveryRequest, true
	case enums.OpcodeDeferAdd:
		return TagDeferAdd, true
	case enums.OpcodeDeferAbort:
		return TagDeferAbort, true
	default:
		return 0, false
	}
}

// TagsFromOpcodes applies only experiment preconditions. Runtime validation
// and policy remain with the existing Go response handler.
func TagsFromOpcodes(opcodes []*state.GeneratorOpcode) ([]Tag, error) {
	if len(opcodes) > MaxOpcodes {
		return nil, ErrTooManyOpcodes
	}
	tags := make([]Tag, len(opcodes))
	for index, opcode := range opcodes {
		if opcode == nil {
			return nil, fmt.Errorf("%w at position %d", ErrNilOpcode, index)
		}
		tag, ok := TagForOpcode(opcode.Op)
		if !ok {
			return nil, fmt.Errorf("%w at position %d: %d", ErrUnknownOpcode, index, opcode.Op)
		}
		tags[index] = tag
	}
	return tags, nil
}

type Kind uint16

const (
	KindHello    Kind = 1
	KindReady    Kind = 2
	KindPlan     Kind = 3
	KindResult   Kind = 4
	KindError    Kind = 5
	KindShutdown Kind = 6
	KindBye      Kind = 7
)

type ErrorCode uint32

const (
	CodeInvalidFrame       ErrorCode = 1
	CodeUnsupportedVersion ErrorCode = 2
	CodeUnknownKind        ErrorCode = 3
	CodeInvalidHandshake   ErrorCode = 4
	CodeInvalidPlan        ErrorCode = 5
	CodeUnknownOpcode      ErrorCode = 6
	CodeTooManyOpcodes     ErrorCode = 7
	CodeFrameTooLarge      ErrorCode = 8
	CodeAllocationFailed   ErrorCode = 9
	CodePanic              ErrorCode = 10
)

type Frame struct {
	Kind      Kind
	RequestID uint64
	Status    ErrorCode
	Payload   []byte
}

type Projection struct {
	PriorityIndices []uint32
	OtherIndices    []uint32
	NonLazyIndices  []uint32
	HistoryGroup    bool
	HasPlanOp       bool
}

type RemoteError struct {
	RequestID uint64
	Code      ErrorCode
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("opcode worker returned error code %d for request %d", e.Code, e.RequestID)
}

func payloadLengthValid(kind Kind, status ErrorCode, payloadLen uint64) bool {
	if kind == KindError {
		return status >= CodeInvalidFrame && status <= CodePanic && payloadLen == 0
	}
	if status != 0 {
		return false
	}
	switch kind {
	case KindHello, KindReady:
		return payloadLen == 16
	case KindPlan:
		return payloadLen >= 4 && payloadLen <= 4+MaxOpcodes
	case KindResult:
		return payloadLen >= 16 && payloadLen <= MaxResultPayloadSize
	case KindShutdown, KindBye:
		return payloadLen == 0
	default:
		return false
	}
}

func parseHeader(header []byte) (Frame, uint32, error) {
	if len(header) != HeaderSize {
		return Frame{}, 0, fmt.Errorf("invalid opcode frame header length %d", len(header))
	}
	if string(header[:4]) != string(frameMagic[:]) {
		return Frame{}, 0, errors.New("invalid opcode frame magic")
	}
	version := binary.LittleEndian.Uint16(header[4:6])
	if version != ProtocolVersion {
		return Frame{}, 0, fmt.Errorf("unsupported opcode protocol version %d", version)
	}
	kind := Kind(binary.LittleEndian.Uint16(header[6:8]))
	if kind < KindHello || kind > KindBye {
		return Frame{}, 0, fmt.Errorf("unknown opcode frame kind %d", kind)
	}
	requestID := binary.LittleEndian.Uint64(header[8:16])
	payloadLen := binary.LittleEndian.Uint32(header[16:20])
	status := ErrorCode(binary.LittleEndian.Uint32(header[20:24]))
	if !payloadLengthValid(kind, status, uint64(payloadLen)) {
		return Frame{}, 0, errors.New("invalid opcode frame status or payload length")
	}
	if uint64(HeaderSize)+uint64(payloadLen) > MaxFrameSize {
		return Frame{}, 0, errors.New("opcode frame exceeds complete-frame limit")
	}
	return Frame{Kind: kind, RequestID: requestID, Status: status}, payloadLen, nil
}

func EncodeFrame(kind Kind, requestID uint64, status ErrorCode, payload []byte) ([]byte, error) {
	if len(payload) > MaxFrameSize-HeaderSize || !payloadLengthValid(kind, status, uint64(len(payload))) {
		return nil, errors.New("invalid opcode frame fields")
	}
	totalLen := HeaderSize + len(payload)
	if totalLen > MaxFrameSize {
		return nil, errors.New("opcode frame exceeds complete-frame limit")
	}
	frame := make([]byte, totalLen)
	copy(frame[:4], frameMagic[:])
	binary.LittleEndian.PutUint16(frame[4:6], ProtocolVersion)
	binary.LittleEndian.PutUint16(frame[6:8], uint16(kind))
	binary.LittleEndian.PutUint64(frame[8:16], requestID)
	binary.LittleEndian.PutUint32(frame[16:20], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[20:24], uint32(status))
	copy(frame[HeaderSize:], payload)
	return frame, nil
}

func DecodeFrame(frame []byte) (Frame, error) {
	if len(frame) < HeaderSize || len(frame) > MaxFrameSize {
		return Frame{}, errors.New("invalid opcode complete-frame length")
	}
	parsed, payloadLen, err := parseHeader(frame[:HeaderSize])
	if err != nil {
		return Frame{}, err
	}
	if uint64(HeaderSize)+uint64(payloadLen) != uint64(len(frame)) {
		return Frame{}, errors.New("opcode frame has truncated or trailing bytes")
	}
	parsed.Payload = frame[HeaderSize:]
	return parsed, nil
}

// ReadFrame reads one exact frame and rejects its declared size before
// allocating the payload. ErrCleanEOF means EOF occurred at a frame boundary.
func ReadFrame(reader io.Reader) ([]byte, error) {
	var header [HeaderSize]byte
	n, err := io.ReadFull(reader, header[:])
	if err != nil {
		if n == 0 && errors.Is(err, io.EOF) {
			return nil, ErrCleanEOF
		}
		return nil, fmt.Errorf("read opcode frame header: %w", err)
	}
	_, payloadLen, err := parseHeader(header[:])
	if err != nil {
		return nil, err
	}
	frameLen := HeaderSize + int(payloadLen)
	frame := make([]byte, frameLen)
	copy(frame, header[:])
	if _, err := io.ReadFull(reader, frame[HeaderSize:]); err != nil {
		return nil, fmt.Errorf("read opcode frame payload: %w", err)
	}
	if _, err := DecodeFrame(frame); err != nil {
		return nil, err
	}
	return frame, nil
}

func WriteFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		written, err := writer.Write(data)
		if err != nil {
			return err
		}
		if written == 0 {
			return io.ErrShortWrite
		}
		data = data[written:]
	}
	return nil
}

func HandshakeFrame() []byte {
	frame, _ := EncodeFrame(KindHello, 0, 0, handshakePayload[:])
	return frame
}

func ValidateReady(frame []byte) error {
	parsed, err := DecodeFrame(frame)
	if err != nil {
		return err
	}
	if parsed.Kind != KindReady || parsed.RequestID != 0 || parsed.Status != 0 || !equalBytes(parsed.Payload, handshakePayload[:]) {
		return errors.New("opcode worker handshake schema or capabilities do not match")
	}
	return nil
}

func ShutdownFrame() []byte {
	frame, _ := EncodeFrame(KindShutdown, 0, 0, nil)
	return frame
}

func ValidateBye(frame []byte) error {
	parsed, err := DecodeFrame(frame)
	if err != nil {
		return err
	}
	if parsed.Kind != KindBye || parsed.RequestID != 0 || parsed.Status != 0 || len(parsed.Payload) != 0 {
		return errors.New("invalid opcode worker shutdown acknowledgement")
	}
	return nil
}

var handshakePayload = [16]byte{
	1, 0, // schema version
	1, 0, // opcode tag set version
	4, 0, // index width
	3, 0, // supported result flags
	0, 0, 1, 0, // maximum opcode count: 65,536
	0, 0, 16, 0, // maximum complete frame: 1 MiB
}

func EncodePlan(requestID uint64, tags []Tag) ([]byte, error) {
	if requestID == 0 {
		return nil, ErrInvalidRequestID
	}
	if len(tags) > MaxOpcodes {
		return nil, ErrTooManyOpcodes
	}
	payload := make([]byte, 4+len(tags))
	binary.LittleEndian.PutUint32(payload[:4], uint32(len(tags)))
	for index, tag := range tags {
		if !tag.valid() {
			return nil, fmt.Errorf("%w: tag %d at position %d", ErrUnknownOpcode, tag, index)
		}
		payload[4+index] = byte(tag)
	}
	frame, err := EncodeFrame(KindPlan, requestID, 0, payload)
	if err != nil {
		return nil, err
	}
	if len(frame) > MaxRequestFrameSize {
		return nil, ErrTooManyOpcodes
	}
	return frame, nil
}

func DecodeResult(requestID uint64, tags []Tag, encoded []byte) (Projection, error) {
	frame, err := DecodeFrame(encoded)
	if err != nil {
		return Projection{}, err
	}
	if frame.RequestID != requestID {
		return Projection{}, fmt.Errorf("opcode request ID mismatch: got %d, want %d", frame.RequestID, requestID)
	}
	if frame.Kind == KindError {
		return Projection{}, &RemoteError{RequestID: requestID, Code: frame.Status}
	}
	if frame.Kind != KindResult {
		return Projection{}, fmt.Errorf("expected RESULT frame, got kind %d", frame.Kind)
	}
	if len(tags) > MaxOpcodes || len(frame.Payload) < 16 {
		return Projection{}, errors.New("opcode result exceeds input bounds")
	}
	priorityCount := binary.LittleEndian.Uint32(frame.Payload[0:4])
	otherCount := binary.LittleEndian.Uint32(frame.Payload[4:8])
	nonLazyCount := binary.LittleEndian.Uint32(frame.Payload[8:12])
	flags := binary.LittleEndian.Uint32(frame.Payload[12:16])
	if flags & ^uint32(ResultFlagHistory|ResultFlagHasPlan) != 0 {
		return Projection{}, errors.New("opcode result contains reserved flags")
	}
	for _, count := range []uint32{priorityCount, otherCount, nonLazyCount} {
		if uint64(count) > uint64(len(tags)) {
			return Projection{}, errors.New("opcode result count exceeds input size")
		}
	}
	if uint64(priorityCount)+uint64(otherCount) != uint64(len(tags)) {
		return Projection{}, errors.New("opcode groups do not cover the input exactly")
	}
	indexCount := uint64(priorityCount) + uint64(otherCount) + uint64(nonLazyCount)
	payloadLen := uint64(16) + indexCount*4
	if payloadLen > MaxResultPayloadSize || payloadLen != uint64(len(frame.Payload)) {
		return Projection{}, errors.New("opcode result count arithmetic does not match payload")
	}

	projection := Projection{
		PriorityIndices: make([]uint32, int(priorityCount)),
		OtherIndices:    make([]uint32, int(otherCount)),
		NonLazyIndices:  make([]uint32, int(nonLazyCount)),
		HistoryGroup:    flags&ResultFlagHistory != 0,
		HasPlanOp:       flags&ResultFlagHasPlan != 0,
	}
	position := 16
	for _, group := range [][]uint32{projection.PriorityIndices, projection.OtherIndices, projection.NonLazyIndices} {
		for index := range group {
			group[index] = binary.LittleEndian.Uint32(frame.Payload[position : position+4])
			position += 4
		}
	}
	if err := ValidateProjection(tags, projection); err != nil {
		return Projection{}, err
	}
	return projection, nil
}

// ValidateProjection enforces the transport contract. The executor test
// harness separately compares the accepted result with the real Go helpers.
func ValidateProjection(tags []Tag, got Projection) error {
	if len(tags) > MaxOpcodes {
		return ErrTooManyOpcodes
	}
	for index, tag := range tags {
		if !tag.valid() {
			return fmt.Errorf("%w: tag %d at position %d", ErrUnknownOpcode, tag, index)
		}
	}
	if len(got.PriorityIndices)+len(got.OtherIndices) != len(tags) {
		return errors.New("opcode groups do not cover the input exactly")
	}
	seen := make([]bool, len(tags))
	for _, part := range []struct {
		indices  []uint32
		priority bool
	}{{got.PriorityIndices, true}, {got.OtherIndices, false}} {
		for index, original := range part.indices {
			if uint64(original) >= uint64(len(tags)) {
				return fmt.Errorf("opcode group index %d is out of bounds", original)
			}
			if index > 0 && part.indices[index-1] >= original {
				return errors.New("opcode group indices are not strictly increasing")
			}
			if seen[original] {
				return fmt.Errorf("opcode group index %d is repeated", original)
			}
			if tagPriority(tags[original]) != part.priority {
				return fmt.Errorf("opcode group index %d has incorrect priority membership", original)
			}
			seen[original] = true
		}
	}
	for index, present := range seen {
		if !present {
			return fmt.Errorf("opcode groups omit original index %d", index)
		}
	}

	wantNonLazy := make([]uint32, 0, len(tags))
	for _, group := range [][]uint32{got.PriorityIndices, got.OtherIndices} {
		for _, original := range group {
			if !tagLazy(tags[original]) {
				wantNonLazy = append(wantNonLazy, original)
			}
		}
	}
	if len(got.NonLazyIndices) != len(wantNonLazy) {
		return errors.New("non-lazy indices do not match the grouped input")
	}
	for index := range wantNonLazy {
		if got.NonLazyIndices[index] != wantNonLazy[index] {
			return errors.New("non-lazy indices do not match group order")
		}
	}
	wantHistory := len(wantNonLazy) > 1
	if got.HistoryGroup != wantHistory {
		return errors.New("history flag does not match non-lazy count")
	}
	wantHasPlan := false
	for _, tag := range tags {
		wantHasPlan = wantHasPlan || tagHasPlan(tag)
	}
	if got.HasPlanOp != wantHasPlan {
		return errors.New("plan capability flag does not match opcode tags")
	}
	return nil
}

func equalBytes(left, right []byte) bool {
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
