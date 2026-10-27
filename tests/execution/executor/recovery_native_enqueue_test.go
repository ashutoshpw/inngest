//go:build native_step_integration

package executor

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	statev2 "github.com/inngest/inngest/pkg/execution/state/v2"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
)

func TestRecoveryReplaysNativeEnqueue(t *testing.T) {
	runRecoveryNativeStep(t, func(t *testing.T, v *queuehelper.ValkeyContainer, id statev2.ID, keys bool, s *nativeReplaySave, q *nativeReplayDiscovery) {
		// Original HTTP fixture bytes are source input. No fixture supplies native
		// prepared output, item bytes, KEYS, ARGV or a queue score.
		response := recoveryFixture(t, "native-checkpoint.http").Body
		input := func(address string, op map[string]any) map[string]any {
			return map[string]any{
				"store":    map[string]any{"address": strings.Replace(address, "localhost:", "127.0.0.1:", 1), "password": v.Password, "connectTimeoutMs": 1000, "readTimeoutMs": 2000, "writeTimeoutMs": 1000},
				"state":    map[string]any{"prefix": redis_state.StateDefaultKey, "layout": "AlwaysShard", "identity": map[string]any{"runId": id.RunID.String(), "functionId": id.FunctionID.String(), "accountId": id.Tenant.AccountID.String(), "envId": id.Tenant.EnvID.String(), "appId": id.Tenant.AppID.String()}},
				"budgetMs": 5000, "disabledRetries": true, "operation": op,
			}
		}
		operation := func(kind string) map[string]any {
			b := make([]uint16, len(response))
			for i, v := range response {
				b[i] = uint16(v)
			}
			return map[string]any{"kind": kind, "response": b, "config": map[string]any{"requestVersion": 2, "forceStepPlan": false, "traceDisabled": true, "hasAi": false, "sdkMetadata": []any{}, "experiments": []any{}, "loadedStateBytes": 0, "stateLimit": nil}}
		}
		var nativeSaved bool
		s.nativeSave = func(ctx context.Context, gotID statev2.ID, step string, data []byte) (bool, error) {
			require.Equal(t, id, gotID)
			require.Equal(t, "checkpoint-output", step)
			op := operation("prepareSave")
			op["delta"] = state.MetadataSizeDeltaFromContext(ctx)
			out := callRecoveryNative(t, ctx, input(v.Addr, op), nil, nil)
			require.Equal(t, "None", out.Error)
			require.Equal(t, "Confirmed", out.WriteStatus)
			require.False(t, out.HasPending)
			nativeSaved = true
			// Independent later read/assertions compare this stored result against both
			// the literal fixture and the real Go-prepared value observed by this wrapper.
			return out.HasPending, nil
		}
		q.nativeEnqueue = func(ctx context.Context, item queue.Item, at time.Time, opts queue.EnqueueOpts) (bool, error) {
			require.True(t, nativeSaved, "native save must complete first")
			require.Empty(t, item.CustomConcurrencyKeys)
			require.Empty(t, item.Semaphores)
			require.Nil(t, item.Throttle)
			require.Nil(t, item.Singleton)
			require.Nil(t, item.QueueName)
			require.Nil(t, item.ParallelCoalesceKey)
			require.Zero(t, item.ParallelMode)
			require.Empty(t, item.Identifier.CustomConcurrencyKeys)
			require.Empty(t, item.Identifier.Semaphores)
			require.Nil(t, item.Identifier.BatchID)
			require.Len(t, item.Identifier.EventIDs, 1, "native discovery excludes batches before projecting metadata")
			require.Nil(t, item.Identifier.OriginalRunID)
			require.Nil(t, item.Identifier.ReplayID)
			require.Zero(t, item.Attempt)
			require.Equal(t, queue.EnqueueOpts{}, opts)
			before := q.read(ctx)
			require.NoError(t, before.Err)
			var original state.Identifier
			require.NoError(t, json.Unmarshal([]byte(before.Metadata["id"]), &original))
			mode := "Partition"
			if keys {
				mode = "Key"
			}
			carrier, ok := item.Metadata["user-otel-ctx"].(string)
			require.True(t, ok)
			require.Len(t, item.Metadata, 1, "opaque Go-generated propagation carrier only")
			op := operation("enqueue")
			events := make([]string, len(item.Identifier.EventIDs))
			for i, e := range item.Identifier.EventIDs {
				events[i] = e.String()
			}
			op["discovery"] = map[string]any{
				"queue": map[string]any{"prefix": redis_state.QueueDefaultKey, "mode": mode}, "saveCompleted": nativeSaved, "hasPending": false,
				"idempotencyKey": original.Key, "incoming": item.Payload.(queue.PayloadEdge).Edge.Incoming, "groupId": item.GroupID, "carrier": carrier,
				"nowMs": at.UnixMilli(), "priorityFactor": item.PriorityFactor, "identifierPriorityFactor": item.Identifier.PriorityFactor,
				"maxAttempts": item.MaxAttempts, "functionVersion": item.Identifier.WorkflowVersion, "eventId": item.Identifier.EventID.String(), "eventIds": events,
			}
			relay := startRecoveryNativeRelay(t, v.Addr, q.mode)
			var observed bool
			var observe func()
			if q.mode == "after" {
				observe = func() {
					independent := q.read(ctx)
					require.NoError(t, independent.Err)
					require.Equal(t, before.Actions, independent.Actions)
					require.Equal(t, before.Stack, independent.Stack)
					require.Equal(t, before.Metadata, independent.Metadata)
					require.Len(t, independent.Items, len(before.Items)+1, "commit independently visible while native caller is live")
					observed = true
				}
			}
			out := callRecoveryNative(t, ctx, input(relay.address(), op), relay, observe)
			relay.assertSingleAttempt(t)
			if q.mode != "" {
				require.Equal(t, "Failed", out.Class)
				require.Equal(t, "MayHaveCommitted", out.WriteStatus)
				require.Nil(t, out.Result)
				require.Equal(t, q.mode == "after", observed)
			}
			err := nativeDiscoveryInteropError(out)
			if q.mode == "" {
				require.NoError(t, err)
				require.Equal(t, "Enqueued", out.Class)
				require.NotNil(t, out.Result)
				require.Zero(t, *out.Result)
			}
			return observed || out.Class == "Enqueued", err
		}
	})
}

type recoveryNativeOutcome struct {
	Class            string  `json:"class"`
	Error            string  `json:"error"`
	WriteStatus      string  `json:"writeStatus"`
	HasPending       bool    `json:"hasPending"`
	Result           *int64  `json:"result"`
	StoredID         string  `json:"storedId"`
	ExistingRunID    *string `json:"existingRunId"`
	LastFailure      string  `json:"lastFailure"`
	PreparationError string  `json:"preparationError"`
}

// Native classes have no Go sentinel identity. Reconstruct Go's public queue
// duplicate error only at this interop boundary; the unchanged Go discovery
// owner treats that as success and wraps every other error as AlwaysRetry.
func nativeDiscoveryInteropError(out recoveryNativeOutcome) error {
	switch out.Class {
	case "Enqueued":
		return nil
	case "Duplicate":
		// This replay does not inject duplicates; live/tombstone lookup is owned by
		// the native socket interop test. Preserve RunID if one is supplied.
		var run *ulid.ULID
		if out.ExistingRunID != nil {
			v, err := ulid.ParseStrict(*out.ExistingRunID)
			if err != nil {
				return err
			}
			run = &v
		}
		return queue.QueueItemExists(out.StoredID, run)
	default:
		return fmt.Errorf("native discovery %s: %s", out.Class, out.LastFailure)
	}
}

func callRecoveryNative(t *testing.T, ctx context.Context, input map[string]any, relay *recoveryNativeRelay, observe func()) recoveryNativeOutcome {
	t.Helper()
	binary := os.Getenv("NATIVE_STEP_TEST_BINARY")
	require.True(t, filepath.IsAbs(binary))
	require.FileExists(t, binary)
	raw, err := json.Marshal(input)
	require.NoError(t, err)
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(callCtx, binary)
	cmd.Stdin = bytes.NewReader(raw)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Start())
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	if observe != nil {
		select {
		case <-relay.committed:
			select {
			case err := <-done:
				t.Fatalf("native exited before independent read: %v", err)
			default:
			}
			observe()
			close(relay.release)
		case err := <-done:
			t.Fatalf("native exited without real commit: %v stderr=%s", err, stderr.String())
		case <-callCtx.Done():
			t.Fatal("native commit observation deadline")
		}
	}
	require.NoError(t, <-done, "native helper: %s", stderr.String())
	var out recoveryNativeOutcome
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &out))
	require.Empty(t, out.PreparationError)
	return out
}

// RESP2 transport fault control, local to this recovery boundary. All replies
// originate in the authenticated AOF Valkey. Never synthesizes successful I/O.
type recoveryNativeFrame struct {
	raw   []byte
	value string
	items []recoveryNativeFrame
}

func readRecoveryNativeFrame(r *bufio.Reader) (f recoveryNativeFrame, err error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return f, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return f, fmt.Errorf("bad RESP header")
	}
	f.raw = line
	f.value = string(line[1 : len(line)-2])
	switch line[0] {
	case '*':
		n, e := strconv.Atoi(f.value)
		if e != nil || n < 0 || n > 64 {
			return f, fmt.Errorf("bad RESP array")
		}
		for range n {
			v, e := readRecoveryNativeFrame(r)
			if e != nil {
				return f, e
			}
			f.items = append(f.items, v)
			f.raw = append(f.raw, v.raw...)
		}
	case '$':
		n, e := strconv.Atoi(f.value)
		if e != nil || n < -1 || n > 16<<20 {
			return f, fmt.Errorf("bad RESP bulk")
		}
		if n < 0 {
			return f, nil
		}
		b := make([]byte, n+2)
		if _, e = io.ReadFull(r, b); e != nil {
			return f, e
		}
		if string(b[n:]) != "\r\n" {
			return f, fmt.Errorf("bad RESP terminator")
		}
		f.raw = append(f.raw, b...)
		f.value = string(b[:n])
	case '+', '-', ':':
	default:
		return f, fmt.Errorf("bad RESP type")
	}
	return f, nil
}

type recoveryNativeRelay struct {
	listener  net.Listener
	committed chan struct{}
	release   chan struct{}
	mu        sync.Mutex
	sockets   []net.Conn
	scripts   []string
	replies   []string
	errs      []error
	wg        sync.WaitGroup
}

func (r *recoveryNativeRelay) address() string { return r.listener.Addr().String() }
func startRecoveryNativeRelay(t *testing.T, upstream, mode string) *recoveryNativeRelay {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	r := &recoveryNativeRelay{listener: l, committed: make(chan struct{}, 1), release: make(chan struct{})}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			r.mu.Lock()
			r.sockets = append(r.sockets, c)
			r.mu.Unlock()
			r.wg.Add(1)
			go func() {
				defer r.wg.Done()
				defer c.Close()
				u, err := net.DialTimeout("tcp", upstream, time.Second)
				if err != nil {
					r.fail(err)
					return
				}
				defer u.Close()
				r.mu.Lock()
				r.sockets = append(r.sockets, u)
				r.mu.Unlock()
				_ = u.SetDeadline(time.Now().Add(8 * time.Second))
				_ = c.SetDeadline(time.Now().Add(8 * time.Second))
				front, back := bufio.NewReader(c), bufio.NewReader(u)
				for {
					request, err := readRecoveryNativeFrame(front)
					if err != nil {
						if err != io.EOF {
							r.fail(err)
						}
						return
					}
					if len(request.items) == 0 {
						r.fail(fmt.Errorf("empty command"))
						return
					}
					name := request.items[0].value
					script := name == "EVALSHA" || name == "EVAL"
					if script {
						r.mu.Lock()
						r.scripts = append(r.scripts, name)
						r.mu.Unlock()
						if mode == "before" {
							return
						}
					}
					if _, err = u.Write(request.raw); err != nil {
						r.fail(err)
						return
					}
					reply, err := readRecoveryNativeFrame(back)
					if err != nil {
						r.fail(err)
						return
					}
					if script {
						r.mu.Lock()
						r.replies = append(r.replies, string(reply.raw))
						r.mu.Unlock()
					}
					if script && mode == "after" && reply.raw[0] == ':' && reply.value == "0" {
						r.committed <- struct{}{}
						<-r.release
						return
					}
					if _, err = c.Write(reply.raw); err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = l.Close()
		r.mu.Lock()
		for _, c := range r.sockets {
			_ = c.Close()
		}
		r.mu.Unlock()
		select {
		case <-r.release:
		default:
			close(r.release)
		}
		r.wg.Wait()
		r.mu.Lock()
		defer r.mu.Unlock()
		require.Empty(t, r.errs)
	})
	return r
}
func (r *recoveryNativeRelay) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errs = append(r.errs, err)
}
func (r *recoveryNativeRelay) assertSingleAttempt(t *testing.T) {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	require.Empty(t, r.errs)
	require.NotEmpty(t, r.scripts)
	require.Equal(t, "EVALSHA", r.scripts[0])
	require.LessOrEqual(t, len(r.scripts), 2)
	if len(r.scripts) == 2 {
		require.Equal(t, "EVAL", r.scripts[1])
		require.True(t, strings.HasPrefix(r.replies[0], "-NOSCRIPT "))
	}
}
