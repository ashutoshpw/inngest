//go:build native_step_integration

package executor

// Experimental reduced-domain comparator. Every bypass and residual mismatch is
// listed in response-owner-oneshot-readiness.md. This is NOT the Go handler or a
// production latency baseline. Production Go owners are unchanged; the native clock correction is separately documented.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/inngest/inngest/pkg/consts"
	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/jonboulle/clockwork"
	"github.com/oklog/ulid/v2"
	"github.com/redis/rueidis"
)

// This comparator consumes Go-generated descriptors on Linux amd64. All native
// usize fields are restricted to [0, 2^63-1], the Go int domain; native wire
// inputs above that bound are outside comparator parity (see boundary controls).
// The wire shape remains the accepted native descriptor, not a second ABI.
// Integers, rather than []byte, deliberately encode response as a JSON array.
type oneshotDescriptor struct {
	Store struct {
		Address          string  `json:"address"`
		Username         *string `json:"username"`
		Password         string  `json:"password"`
		ConnectTimeoutMS uint64  `json:"connectTimeoutMs"`
		ReadTimeoutMS    uint64  `json:"readTimeoutMs"`
		WriteTimeoutMS   uint64  `json:"writeTimeoutMs"`
	} `json:"store"`
	State struct {
		Prefix   string `json:"prefix"`
		Layout   string `json:"layout"`
		Identity struct {
			RunID      string `json:"runId"`
			FunctionID string `json:"functionId"`
			AccountID  string `json:"accountId"`
			EnvID      string `json:"envId"`
			AppID      string `json:"appId"`
		} `json:"identity"`
	} `json:"state"`
	BudgetMS        uint64 `json:"budgetMs"`
	DisabledRetries bool   `json:"disabledRetries"`
	Operation       struct {
		Kind  string `json:"kind"`
		Input struct {
			Domain struct {
				ObserverRegistrations int    `json:"observerRegistrations"`
				Tracing               string `json:"tracing"`
				CapacityLease         string `json:"capacityLease"`
				MetadataWriters       int    `json:"metadataWriters"`
				ResponseOwners        int    `json:"responseOwners"`
				MetadataGeneration    string `json:"metadataGeneration"`
			} `json:"domain"`
			Response        []uint16   `json:"response"`
			ResponseVersion int32      `json:"responseVersion"`
			NoRetry         bool       `json:"noRetry"`
			RetryAt         *time.Time `json:"retryAt"`
			Loaded          struct {
				RequestVersion     int32 `json:"requestVersion"`
				ForceStepPlan      bool  `json:"forceStepPlan"`
				StateBytes         int   `json:"stateBytes"`
				MetadataSize       int   `json:"metadataSize"`
				MetadataSizeLoaded int   `json:"metadataSizeLoaded"`
			} `json:"loaded"`
			StateLimit *int `json:"stateLimit"`
			Queue      struct {
				Prefix string `json:"prefix"`
				Mode   string `json:"mode"`
			} `json:"queue"`
			IdempotencyKey           string   `json:"idempotencyKey"`
			Incoming                 string   `json:"incoming"`
			PriorityFactor           *int64   `json:"priorityFactor"`
			IdentifierPriorityFactor *int64   `json:"identifierPriorityFactor"`
			MaxAttempts              *int     `json:"maxAttempts"`
			FunctionVersion          uint32   `json:"functionVersion"`
			EventID                  string   `json:"eventId"`
			EventIDs                 []string `json:"eventIds"`
		} `json:"input"`
	} `json:"operation"`
}

// Outcomes retain native raw fields as diagnostics; only shared semantic fields
// are compared. Go cannot invent the native transport's attempts/reply certainty.
type oneshotOutcome struct {
	Disposition        string          `json:"disposition"`
	RetryAt            *time.Time      `json:"retryAt"`
	PreparationError   *string         `json:"preparationError"`
	Save               json.RawMessage `json:"save"`
	Enqueue            json.RawMessage `json:"enqueue"`
	MetadataSizeLoaded int             `json:"metadataSizeLoaded"`
	GroupID            *string         `json:"groupId"`
	NowMS              *int64          `json:"nowMs"`
	Failure            *string         `json:"failure"`
	GoError            string          `json:"goError,omitempty"`
}

func oneshotDecode(raw []byte) (oneshotDescriptor, error) {
	var d oneshotDescriptor
	if err := oneshotRequired(raw, reflect.TypeOf(d)); err != nil {
		return d, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&d); err != nil {
		return d, err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return d, errors.New("trailing descriptor")
	}
	// Readiness deliberately supports one fixed accepted profile. Refuse wider
	// native configurations instead of silently selecting a different Go profile.
	x := d.Operation.Input
	host, _, err := net.SplitHostPort(d.Store.Address)
	if err != nil || net.ParseIP(host) == nil || d.Store.Password == "" ||
		d.Store.ConnectTimeoutMS != 500 || d.Store.ReadTimeoutMS != 500 || d.Store.WriteTimeoutMS != 500 ||
		d.BudgetMS != 5000 || !d.DisabledRetries || d.Operation.Kind != "response" ||
		d.State.Prefix != redis_state.StateDefaultKey || d.State.Layout != "NeverShard" ||
		x.Queue.Prefix != redis_state.QueueDefaultKey || x.Queue.Mode != "Partition" {
		return d, errors.New("unsupported readiness profile")
	}
	if x.Domain.ObserverRegistrations < 0 || x.Domain.MetadataWriters < 0 || x.Domain.ResponseOwners < 0 || x.Loaded.StateBytes < 0 || (x.MaxAttempts != nil && (*x.MaxAttempts < 0 || uint64(*x.MaxAttempts) > uint64(1<<32-1))) || (x.StateLimit != nil && *x.StateLimit < 0) {
		return d, errors.New("invalid unsigned descriptor field")
	}

	for _, b := range x.Response {
		if b > 255 {
			return d, errors.New("response byte range")
		}
	}
	return d, nil
}

// serde's descriptor rejects duplicate and missing non-Option fields. Do not
// let encoding/json's zero values or last-key-wins silently widen that protocol.
func oneshotRequired(raw []byte, typ reflect.Type) error {
	if typ.Kind() != reflect.Struct || typ == reflect.TypeOf(time.Time{}) {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return errors.New("descriptor object")
	}
	allowed := map[string]bool{}
	for j := 0; j < typ.NumField(); j++ {
		allowed[strings.Split(typ.Field(j).Tag.Get("json"), ",")[0]] = true
	}
	fields := map[string]json.RawMessage{}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return err
		}
		name := key.(string)
		if !allowed[name] {
			return errors.New("unknown descriptor field")
		}
		if _, ok := fields[name]; ok {
			return errors.New("duplicate descriptor field")
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return err
		}
		fields[name] = value
	}
	for j := 0; j < typ.NumField(); j++ {
		f := typ.Field(j)
		name := strings.Split(f.Tag.Get("json"), ",")[0]
		value, ok := fields[name]
		if (!ok || bytes.Equal(bytes.TrimSpace(value), []byte("null"))) && f.Type.Kind() != reflect.Pointer {
			return fmt.Errorf("missing descriptor field: %s", name)
		}
		if ok {
			if err := oneshotRequired(value, f.Type); err != nil {
				return err
			}
		}
	}
	return nil
}

// Admission only, with RawMessage preserving the actual output owner's input.
// Duplicate keys inside Data remain valid and are owned by gen.Output().
func oneshotGenerator(raw []byte) error {
	var list []json.RawMessage
	if err := json.Unmarshal(raw, &list); err != nil {
		return err
	}
	if len(list) != 1 {
		return errors.New("one StepRun required")
	}
	dec := json.NewDecoder(bytes.NewReader(list[0]))
	tok, err := dec.Token()
	if err != nil || tok != json.Delim('{') {
		return errors.New("generator object")
	}
	seen := map[string]bool{}
	for dec.More() {
		tok, err = dec.Token()
		if err != nil {
			return err
		}
		key := tok.(string)
		if seen[key] {
			return errors.New("duplicate generator field")
		}
		seen[key] = true
		var v json.RawMessage
		if err := dec.Decode(&v); err != nil {
			return err
		}
		switch key {
		case "op", "id", "name":
			var value string
			if json.Unmarshal(v, &value) != nil || (key == "op" && value != "StepRun") || (key == "id" && value == "") {
				return errors.New("generator identity/name")
			}
		case "data":
		case "opts":
			var compact bytes.Buffer
			if json.Compact(&compact, v) != nil || (compact.String() != "null" && compact.String() != "{}") {
				return errors.New("generator opts")
			}
		case "metadata":
			var compact bytes.Buffer
			if json.Compact(&compact, v) != nil || (compact.String() != "null" && compact.String() != "[]") {
				return errors.New("generator metadata")
			}
		case "error", "displayName", "userland":
			if string(bytes.TrimSpace(v)) != "null" {
				return errors.New("generator extension")
			}
		default:
			return errors.New("unknown generator field")
		}
	}
	if !seen["op"] || !seen["id"] {
		return errors.New("generator identity")
	}
	return nil
}

func oneshotSource(d oneshotDescriptor) (*constructedResponseSource, error) {
	s := newConstructedResponseSource()
	x, id := d.Operation.Input, d.State.Identity
	var err error
	s.run.md.ID.RunID, err = ulid.ParseStrict(id.RunID)
	if err != nil {
		return nil, err
	}
	ids := []*uuid.UUID{&s.run.md.ID.FunctionID, &s.run.md.ID.Tenant.AccountID, &s.run.md.ID.Tenant.EnvID, &s.run.md.ID.Tenant.AppID}
	for j, raw := range []string{id.FunctionID, id.AccountID, id.EnvID, id.AppID} {
		*ids[j], err = uuid.Parse(raw)
		if err != nil || ids[j].String() != raw {
			return nil, errors.New("identity")
		}
	}
	if s.run.md.ID.FunctionID == uuid.Nil {
		return nil, errors.New("function identity")
	}
	event, err := ulid.ParseStrict(x.EventID)
	if err != nil {
		return nil, err
	}
	s.run.md.Config.EventIDs = []ulid.ULID{event}
	s.run.md.Config.Idempotency = x.IdempotencyKey
	s.run.md.Config.FunctionVersion = int(x.FunctionVersion)
	s.run.md.Config.PriorityFactor = x.IdentifierPriorityFactor
	s.run.md.Metrics.StateSize = x.Loaded.StateBytes
	s.run.md.Metrics.MetadataSize = x.Loaded.MetadataSize
	s.run.md.Metrics.MetadataSizeLoaded = x.Loaded.MetadataSizeLoaded
	s.run.item.PriorityFactor, s.run.item.MaxAttempts = x.PriorityFactor, x.MaxAttempts
	edge := s.run.item.Payload.(queue.PayloadEdge)
	edge.Edge.Incoming = x.Incoming
	s.run.item.Payload = edge
	s.response.RequestVersion, s.response.NoRetry, s.response.RetryAt = int(x.ResponseVersion), x.NoRetry, x.RetryAt
	s.raw = make([]byte, len(x.Response))
	for j, b := range x.Response {
		if b > 255 {
			return nil, errors.New("response byte")
		}
		s.raw[j] = byte(b)
	}
	if err := oneshotGenerator(s.raw); err != nil {
		return nil, err
	}
	priority := s.run.PriorityFactor()
	if priority != nil {
		p := *priority
		if p > 9007199254740 || p < -9007199254740 {
			return nil, errors.New("priority range")
		}
		score := int64(s.run.md.ID.RunID.Time()) - p*1000
		if score > 9007199254740991 || score < -9007199254740991 {
			return nil, errors.New("score range")
		}
	}
	return s, nil
}

func oneshotState(ctx context.Context, client rueidis.Client) (sv2.RunService, error) {
	u := redis_state.NewUnshardedClient(client, redis_state.StateDefaultKey, redis_state.QueueDefaultKey)
	s := redis_state.NewShardedClient(redis_state.ShardedClientOpts{UnshardedClient: u, FunctionRunStateClient: client, BatchClient: client, StateDefaultKey: redis_state.StateDefaultKey, QueueDefaultKey: redis_state.QueueDefaultKey, FnRunIsSharded: redis_state.NeverShardOnRun})
	m, err := redis_state.New(ctx, redis_state.WithShardedClient(s))
	if err != nil {
		return nil, err
	}
	return redis_state.MustRunServiceV2(m, redis_state.WithDisabledRetries()), nil
}

func oneshotClient(d oneshotDescriptor) (rueidis.Client, error) {
	username := ""
	if d.Store.Username != nil {
		username = *d.Store.Username
	}
	return rueidis.NewClient(rueidis.ClientOption{InitAddress: []string{d.Store.Address}, Username: username, Password: d.Store.Password,
		DisableCache: true, DisableRetry: true, ForceSingleClient: true, AlwaysPipelining: false,
		Dialer: net.Dialer{Timeout: 500 * time.Millisecond}, ConnWriteTimeout: 500 * time.Millisecond})
}

func oneshotQueue(ctx context.Context, c rueidis.Client) (queue.Queue, error) {
	opts := []queue.QueueOpt{queue.WithClock(clockwork.NewRealClock()), queue.WithRunMode(queue.QueueRunMode{Partition: true}), queue.WithAllowKeyQueues(func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID) bool { return false })}
	shard := redis_state.NewQueueShard(consts.DefaultQueueShardName, redis_state.NewQueueClient(c, redis_state.QueueDefaultKey), opts...)
	registry, err := queue.NewSingleShardRegistry(shard)
	if err != nil {
		return nil, err
	}
	return queue.New(ctx, "oneshot-reduced-comparator", registry, opts...)
}

// Only the sequencing necessarily absent from the reduced owner is local here.
// Output, validation, accounting, SaveStep, discovery item and enqueue semantics
// remain calls to their actual Go owners, never a copied native planner/oracle.
func oneshotGo(ctx context.Context, d oneshotDescriptor) (out oneshotOutcome, resultErr error) {
	defer func() {
		// Same response preference precedence; preserve the real Go chain locally.
		switch {
		case resultErr == nil:
			out.Disposition = "Success"
		case errors.Is(resultErr, state.ErrStateOverflowed):
			out.Disposition = "StateOverflow"
		case queue.IsAlwaysRetryable(resultErr):
			out.Disposition = "AlwaysRetry"
		case d.Operation.Input.NoRetry:
			resultErr = queue.NeverRetryError(resultErr)
			out.Disposition = "NeverRetry"
		case d.Operation.Input.RetryAt != nil:
			resultErr = queue.RetryAtError(resultErr, d.Operation.Input.RetryAt)
			out.Disposition = "RetryAt"
			out.RetryAt = d.Operation.Input.RetryAt
		}
		if resultErr != nil {
			out.GoError = resultErr.Error()
		}
	}()
	out.Disposition = "Error"
	out.MetadataSizeLoaded = d.Operation.Input.Loaded.MetadataSizeLoaded
	x, domain := d.Operation.Input, d.Operation.Input.Domain
	if domain.ObserverRegistrations != 0 || domain.Tracing != "Absent" || domain.CapacityLease != "Absent" || domain.MetadataWriters != 1 || domain.ResponseOwners != 1 || domain.MetadataGeneration != "Absent" || x.Loaded.RequestVersion != 2 || x.Loaded.ForceStepPlan || x.Loaded.StateBytes < 0 || x.Loaded.MetadataSize < 0 || x.Loaded.MetadataSizeLoaded < 0 || len(x.EventIDs) != 1 || x.EventIDs[0] != x.EventID || x.Incoming == "" {
		reason := "Ineligible"
		out.PreparationError = &reason
		return out, errors.New("ineligible constructed domain")
	}
	s, err := oneshotSource(d)
	if err != nil {
		return out, err
	}
	err = s.withInput(ctx, false, func(map[string]any) {
		var gen []*state.GeneratorOpcode
		if resultErr = json.Unmarshal(s.raw, &gen); resultErr != nil {
			return
		}
		x := d.Operation.Input
		// A literal static option only, applied after constructed-source admission.
		if x.StateLimit != nil {
			s.e.stateSizeLimit = func(sv2.ID) int { return *x.StateLimit }
		}
		g := goResponseEffects{executor: s.e}
		step := responseStepBinding{runCtx: &s.run, gen: *gen[0], edge: s.run.item.Payload.(queue.PayloadEdge)}
		var output string
		output, resultErr = gen[0].Output()
		if resultErr != nil {
			return
		}
		resultErr = s.e.validateStateSize(len(output), s.run.md)
		if resultErr != nil {
			return
		}
		// Like native CallContext, the adapter budget starts after preparation,
		// before cold connection/protocol initialization and either write.
		ctx, cancel := context.WithTimeout(ctx, time.Duration(d.BudgetMS)*time.Millisecond)
		defer cancel()
		// Consume the real accounting claim at the save boundary even when cold
		// client bootstrap fails before SaveStep can return a Go result.
		ctx, request := step.saveRequest(ctx, output)
		out.MetadataSizeLoaded = s.run.md.Metrics.MetadataSizeLoaded
		var c rueidis.Client
		c, resultErr = oneshotClient(d)
		if resultErr != nil {
			return
		}
		defer c.Close()
		s.e.smv2, resultErr = oneshotState(ctx, c)
		if resultErr != nil {
			return
		}
		s.e.queue, resultErr = oneshotQueue(ctx, c)
		if resultErr != nil {
			return
		}
		saved := g.saveStep(ctx, request)
		saveClass := "None"
		switch {
		case errors.Is(saved.err, state.ErrIdempotentResponse):
			saveClass = "ErrIdempotentResponse"
		case errors.Is(saved.err, state.ErrDuplicateResponse):
			saveClass = "ErrDuplicateResponse"
		case saved.err != nil:
			saveClass = "GoError"
		}
		out.Save, _ = json.Marshal(map[string]any{"error": saveClass, "hasPending": saved.hasPending})
		if saved.err != nil && saveClass == "GoError" {
			resultErr = saved.err
			return
		}
		group, now := uuid.New().String(), time.Now()
		out.GroupID, out.NowMS = &group, new(now.UnixMilli())
		b := g.discoveryBinding(state.WithGroupID(ctx, group), step, group, now, nil, false)
		if shouldEnqueueDiscovery(saved.hasPending, gen[0].ParallelMode()) {
			resultErr = g.enqueue(b)
			class := "Enqueued"
			if errors.Is(resultErr, queue.ErrQueueItemExists) {
				class = "Duplicate"
				resultErr = nil
			} else if resultErr != nil {
				class = "Failed"
				resultErr = retryableEnqueueError(resultErr)
			}
			out.Enqueue, _ = json.Marshal(map[string]any{"class": class})
		}
	})
	if err != nil {
		resultErr = err
	}

	return out, resultErr
}

// Child process only: no test runner PASS text in the accepted helper protocol.
// os.Exit follows all comparator defers, including the cold client's Close.
func TestResponseOneShotGoChild(t *testing.T) {
	if os.Getenv("RESPONSE_ONESHOT_CHILD") != "descriptor-v1" {
		t.Skip("one-shot artifact only")
	}
	raw, err := io.ReadAll(io.LimitReader(os.Stdin, 32*1024*1024+1))
	if err != nil || len(raw) > 32*1024*1024 {
		os.Exit(2)
	}
	d, err := oneshotDecode(raw)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid helper input")
		os.Exit(2)
	}
	out, _ := oneshotGo(context.Background(), d)
	if json.NewEncoder(os.Stdout).Encode(out) != nil {
		os.Exit(3)
	}
	os.Exit(0)
}
