//go:build native_step_integration

package executor

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/inngest/inngest/pkg/execution/queue"
	"github.com/inngest/inngest/pkg/execution/state"
	"github.com/inngest/inngest/pkg/execution/state/redis_state"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
	"github.com/inngest/inngest/pkg/inngest"
	queuehelper "github.com/inngest/inngest/tests/execution/queue/helper"
	"github.com/redis/rueidis"
	"github.com/stretchr/testify/require"
)

type oneshotSelection struct {
	ID          int    `json:"id"`
	Seed        int    `json:"seed"`
	Payload     int    `json:"payload"`
	Lane        string `json:"lane"`
	Observation int    `json:"observation"`
	Phase       string `json:"phase"`
	Scenario    string `json:"scenario"`
}
type oneshotRecord struct {
	epoch     time.Time
	Selection oneshotSelection `json:"selection"`
	// Offsets use one monotonic epoch. Unix anchors correlate external samples;
	// wall clock is never the latency denominator.
	EpochUnixNS           int64            `json:"epoch_unix_ns"`
	Times                 map[string]int64 `json:"times"`
	ParentPID             int              `json:"parent_pid"`
	ChildPID              int              `json:"child_pid"`
	ChildIdentity         string           `json:"child_identity"`
	ChildUsage            any              `json:"child_usage"`
	Argv                  []string         `json:"argv"`
	Exit                  int              `json:"exit"`
	Outcome               oneshotOutcome   `json:"outcome"`
	Stdout                string           `json:"stdout"`
	Stderr                string           `json:"stderr"`
	Error                 string           `json:"error"`
	Cleanup               bool             `json:"cleanup"`
	Readback              string           `json:"readback,omitempty"`
	ReadbackSHA256        string           `json:"readback_sha256,omitempty"`
	Validated             bool             `json:"validated"`
	ObserverRegistrations int              `json:"observer_registrations"`
	ObserverDeliveries    int              `json:"observer_deliveries"`
	ValkeyMemory          string           `json:"valkey_memory"`
	Blockers              []string         `json:"blockers"`
}

// Bounded output still drains the pipe, so overflow cannot deadlock a child.
type oneshotOutput struct {
	bytes.Buffer
	overflow bool
}

func (b *oneshotOutput) Write(p []byte) (int, error) {
	n := len(p)
	left := 64*1024 - b.Len()
	if left > 0 {
		_, _ = b.Buffer.Write(p[:min(left, n)])
	}
	if n > left {
		b.overflow = true
	}
	return n, nil
}

func oneshotCommand(ctx context.Context, lane string, raw []byte, r *oneshotRecord, stamp func(string)) error {
	binary := os.Getenv("NATIVE_STEP_TEST_BINARY")
	var args []string
	if lane == "go" {
		var err error
		binary, err = os.Executable()
		if err != nil {
			return err
		}
		args = []string{"-test.run=^TestResponseOneShotGoChild$", "-test.timeout=8s"}
	} else if lane != "native" {
		return errors.New("invalid lane")
	}
	if binary == "" {
		return errors.New("missing native helper")
	}
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), "RESPONSE_ONESHOT_CHILD=descriptor-v1", "EXPERIMENTAL_KEY_QUEUES_ENABLE=false", "GOMAXPROCS=2")
	cmd.Stdin = bytes.NewReader(raw)
	var stdout, stderr oneshotOutput
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	cmd.WaitDelay = 500 * time.Millisecond
	r.Argv = append([]string{binary}, args...)
	stamp("spawn_start")
	if err := cmd.Start(); err != nil {
		return err
	}
	r.ChildPID = cmd.Process.Pid
	// /proc stat start ticks disambiguate PID reuse. A very short-lived child may
	// disappear before sampling; keep the failure, never invent an identity/RSS.
	if stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", r.ChildPID)); err == nil {
		r.ChildIdentity = string(stat)
	}
	stamp("spawn_return")
	// Collector control projection only. The supervisor never infers owner role
	// from recursive discovery; publication races must fail closed there.
	var projectionErr error
	if path := os.Getenv("RESPONSE_ONESHOT_SPAWN"); path != "" {
		parentStat, readErr := os.ReadFile(fmt.Sprintf("/proc/%d/stat", os.Getpid()))
		projectionErr = readErr
		if projectionErr == nil {
			var raw []byte
			raw, projectionErr = json.Marshal(map[string]any{"parent_pid": os.Getpid(), "parent_identity": string(parentStat),
				"child_pid": r.ChildPID, "child_identity": r.ChildIdentity, "selection": r.Selection, "argv": r.Argv, "unix_ns": time.Now().UnixNano(),
				"capture_grant":            os.Getenv("RESPONSE_ONESHOT_AUTHORIZATION"),
				"runner_generation":        os.Getenv("RESPONSE_ONESHOT_RUNNER_GENERATION"),
				"invocation_epoch_unix_ns": r.EpochUnixNS})
			if projectionErr == nil {
				projectionErr = os.WriteFile(path+".tmp", raw, 0600)
			}
			if projectionErr == nil {
				projectionErr = os.Rename(path+".tmp", path)
			}
		}
	}
	err := cmd.Wait()
	stamp("reaped")
	r.Exit = cmd.ProcessState.ExitCode()
	r.ChildUsage = cmd.ProcessState.SysUsage()
	r.Stdout, r.Stderr = stdout.String(), stderr.String()
	// Wait closes os/exec's connection pipes and joins its copy goroutines.
	r.Cleanup = true
	if killErr := syscall.Kill(r.ChildPID, 0); killErr != syscall.ESRCH {
		r.Cleanup = false
		return fmt.Errorf("child still present after wait: %v", killErr)
	}
	if err != nil {
		return err
	}
	if projectionErr != nil {
		return fmt.Errorf("collector spawn projection: %w", projectionErr)
	}
	if stdout.overflow || stderr.overflow {
		return errors.New("child output retention exceeded")
	}
	if err := json.Unmarshal(stdout.Bytes(), &r.Outcome); err != nil {
		return fmt.Errorf("outcome decode: %w", err)
	}
	stamp("decoded")
	return nil
}

func oneshotDescriptorBytes(s *constructedResponseSource, v *queuehelper.ValkeyContainer, input map[string]any) ([]byte, error) {
	id := s.run.md.ID
	var username any
	if v.Username != "" {
		username = v.Username
	}
	return json.Marshal(map[string]any{"store": map[string]any{"address": strings.Replace(v.Addr, "localhost:", "127.0.0.1:", 1), "username": username, "password": v.Password, "connectTimeoutMs": 500, "readTimeoutMs": 500, "writeTimeoutMs": 500},
		"state": map[string]any{"prefix": redis_state.StateDefaultKey, "layout": "NeverShard", "identity": map[string]any{"runId": id.RunID.String(), "functionId": id.FunctionID.String(), "accountId": id.Tenant.AccountID.String(), "envId": id.Tenant.EnvID.String(), "appId": id.Tenant.AppID.String()}}, "budgetMs": 5000, "disabledRetries": true, "operation": map[string]any{"kind": "response", "input": input}})
}

func oneshotPayload(savedBytes int) []byte {
	// The saved envelope is exactly 64 or 65,536 bytes: 9 envelope + 2 quotes.
	return []byte(`[{"op":"StepRun","id":"s","data":"` + strings.Repeat("x", savedBytes-11) + `"}]`)
}

// Verify and preload precisely the accepted manifest's actual source/includes.
// This is setup, outside owner latency, with no generated/copied Lua fixture.
func oneshotPreload(t *testing.T, ctx context.Context, c rueidis.Client) {
	t.Helper()
	var f struct {
		Scripts map[string]struct {
			Path          string
			RawSHA256     string
			Includes      map[string]string `json:"includesSHA256"`
			SHA1          string
			SHA256        string
			ExpandedBytes int
		}
	}
	raw, err := os.ReadFile("../state/redis_state/testdata/compatibility/native-step-effects-v1.json")
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(raw, &f))
	include := regexp.MustCompile(`-- \$include\(([\w.]+)\)`)
	for _, m := range f.Scripts {
		raw, err := os.ReadFile(filepath.Join("../state/redis_state", m.Path))
		require.NoError(t, err)
		require.Equal(t, m.RawSHA256, fmt.Sprintf("%x", sha256.Sum256(raw)))
		expanded := string(raw)
		for _, match := range include.FindAllStringSubmatch(string(raw), -1) {
			b, err := os.ReadFile("../state/redis_state/lua/includes/" + match[1])
			require.NoError(t, err)
			require.Equal(t, m.Includes[match[1]], fmt.Sprintf("%x", sha256.Sum256(b)))
			expanded = strings.ReplaceAll(expanded, match[0], string(b))
		}
		require.Len(t, expanded, m.ExpandedBytes)
		require.Equal(t, m.SHA1, fmt.Sprintf("%x", sha1.Sum([]byte(expanded))))
		require.Equal(t, m.SHA256, fmt.Sprintf("%x", sha256.Sum256([]byte(expanded))))
		got, err := c.Do(ctx, c.B().ScriptLoad().Script(expanded).Build()).ToString()
		require.NoError(t, err)
		require.Equal(t, m.SHA1, got)
	}
}

// A fresh independent durable run, equal initial facts in both lanes. The
// dedicated helper's DB is reset between invocations; SCRIPT LOAD survives reset.
func oneshotSetup(t *testing.T, ctx context.Context, c rueidis.Client, sel oneshotSelection) *constructedResponseSource {
	t.Helper()
	require.NoError(t, c.Do(ctx, c.B().Flushdb().Build()).Error())
	s := newConstructedResponseSource()
	if sel.Payload > 0 {
		s.raw = oneshotPayload(sel.Payload)
	}
	if sel.Payload == 0 {
		s.raw = []byte(`[{"op":"StepRun","id":"s","data":9007199254740993}]`)
	}
	svc, err := oneshotState(ctx, c)
	require.NoError(t, err)
	created, err := svc.Create(ctx, sv2.CreateState{Metadata: s.run.md, Events: s.run.events})
	require.NoError(t, err)
	s.run.md = created.Metadata
	s.run.md.Metrics.MetadataSize = 17
	s.run.md.Metrics.MetadataSizeLoaded = 6
	metadataKey := "{" + redis_state.StateDefaultKey + "}:metadata:" + s.run.md.ID.RunID.String()
	require.NoError(t, c.Do(ctx, c.B().Hset().Key(metadataKey).FieldValue().FieldValue("metadata_size", "6").Build()).Error())
	switch sel.Scenario {
	case "pending":
		require.NoError(t, svc.SavePending(ctx, s.run.md.ID, []string{"s", "held"}))
	case "idempotent", "duplicate":
		data := []byte(`{"data":9007199254740993}`)
		if sel.Scenario == "duplicate" {
			data = []byte(`{"data":"previous"}`)
		}
		_, err := svc.SaveStep(ctx, s.run.md.ID, "s", data)
		require.NoError(t, err)
	case "savefail":
		// Wrong type at the first Lua read, so this control cannot partially save.
		require.NoError(t, c.Do(ctx, c.B().Set().Key("{"+redis_state.StateDefaultKey+"}:actions:"+s.run.md.ID.FunctionID.String()+":"+s.run.md.ID.RunID.String()).Value("wrongtype").Build()).Error())
	case "enqueuefail":
		require.NoError(t, c.Do(ctx, c.B().Set().Key("{"+redis_state.QueueDefaultKey+"}:queue:item").Value("wrongtype").Build()).Error())
	case "zero":
		s.run.md.Metrics.MetadataSize = 6
	case "negative":
		s.run.md.Metrics.MetadataSize = 2
	case "reject", "fresh", "overflow":
	default:
		t.Fatalf("unknown scenario %q", sel.Scenario)
	}
	return s
}

func oneshotInvoke(parentCtx context.Context, t *testing.T, v *queuehelper.ValkeyContainer, s *constructedResponseSource, sel oneshotSelection, epoch time.Time) oneshotRecord {
	t.Helper()
	r := oneshotRecord{epoch: epoch, Selection: sel, EpochUnixNS: epoch.UnixNano(), Times: map[string]int64{}, ParentPID: os.Getpid(), Exit: -1}
	r.ObserverRegistrations = len(s.e.lifecycles) + len(s.e.syncLifecycles)
	stamp := func(phase string) { r.Times[phase] = time.Since(epoch).Nanoseconds() }
	r.Times["slot_start"] = 1
	stamp("construction_end")
	stamp("outer_start")
	ctx, cancel := context.WithTimeout(parentCtx, 7*time.Second)
	var childErr error
	err := s.withInput(ctx, false, func(input map[string]any) {
		stamp("admitted_projected")
		if sel.Scenario == "reject" {
			input["domain"].(map[string]any)["observerRegistrations"] = 1
		}
		if sel.Scenario == "overflow" {
			input["stateLimit"] = 1
		}
		if sel.Scenario == "savefail" || sel.Scenario == "enqueuefail" {
			input["noRetry"] = true
		}
		raw, err := oneshotDescriptorBytes(s, v, input)
		if err != nil {
			childErr = err
			return
		}
		stamp("descriptor_encoded")
		childErr = oneshotCommand(ctx, sel.Lane, raw, &r, stamp)
		clear(raw)
	})
	// withInput returned only after release of both exclusive source bindings.
	cancel()
	stamp("outer_end")
	if err != nil {
		r.Error = err.Error()
	} else if childErr != nil {
		r.Error = childErr.Error()
	}
	if sel.Phase == "functional" {
		b, _ := json.Marshal(r)
		t.Logf("FUNCTIONAL_CHILD %s", b)
	}
	return r
}

func oneshotReadback(t *testing.T, v *queuehelper.ValkeyContainer, s *constructedResponseSource, r *oneshotRecord) responseSnapshot {
	t.Helper()
	r.Times["validation_start"] = time.Since(r.epoch).Nanoseconds()
	c, err := queuehelper.NewValkeyClient(v.Addr, v.Username, v.Password, false)
	require.NoError(t, err)
	defer c.Close()
	snapshot := snapshotResponseStore(t, t.Context(), c)
	// Preserve unnormalized store bytes and bind them to this actual child receipt.
	raw, err := json.Marshal(map[string]any{"schema": "oneshot-durable-v1", "selection": r.Selection, "child_pid": r.ChildPID, "child_identity": r.ChildIdentity, "epoch_unix_ns": r.EpochUnixNS, "snapshot": snapshot})
	require.NoError(t, err)
	require.LessOrEqual(t, len(raw), 512*1024)
	r.Readback = string(raw)
	r.ReadbackSHA256 = fmt.Sprintf("%x", sha256.Sum256(raw))
	scenario := r.Selection.Scenario
	if scenario != "reject" && scenario != "overflow" && scenario != "savefail" {
		var save struct {
			Error      string
			HasPending bool
		}
		require.NoError(t, json.Unmarshal(r.Outcome.Save, &save))
		expectedSave := map[string]string{"idempotent": "ErrIdempotentResponse", "duplicate": "ErrDuplicateResponse"}[scenario]
		if expectedSave == "" {
			expectedSave = "None"
		}
		require.Equal(t, expectedSave, save.Error)
		require.Equal(t, scenario == "pending", save.HasPending)
		if scenario == "pending" {
			require.True(t, len(r.Outcome.Enqueue) == 0 || string(r.Outcome.Enqueue) == "null", "enqueue must be absent")
		} else {
			var enqueue struct{ Class string }
			require.NoError(t, json.Unmarshal(r.Outcome.Enqueue, &enqueue))
			want := "Enqueued"
			if scenario == "enqueuefail" {
				want = "Failed"
			}
			require.Equal(t, want, enqueue.Class)
		}
	} else {
		if scenario == "reject" || scenario == "overflow" {
			require.True(t, len(r.Outcome.Save) == 0 || string(r.Outcome.Save) == "null", "save must be absent")
		} else {
			var save struct {
				Error      string
				HasPending bool
			}
			require.NoError(t, json.Unmarshal(r.Outcome.Save, &save))
			want := "GoError"
			if r.Selection.Lane == "native" {
				want = "ErrMaxAttemptReached"
			}
			require.Equal(t, want, save.Error, "save failure must retain its actual failed result")
			require.False(t, save.HasPending)
		}
		require.Nil(t, r.Outcome.GroupID)
		require.Nil(t, r.Outcome.NowMS)
		require.True(t, len(r.Outcome.Enqueue) == 0 || string(r.Outcome.Enqueue) == "null", "enqueue must be absent")
	}

	if r.Selection.Scenario != "savefail" {
		svc, err := oneshotState(t.Context(), c)
		require.NoError(t, err)
		steps, err := svc.LoadSteps(t.Context(), s.run.md.ID)
		require.NoError(t, err)
		if r.Selection.Scenario == "reject" || r.Selection.Scenario == "overflow" {
			require.Empty(t, steps)
		} else {
			want := `{"data":9007199254740993}`
			if r.Selection.Payload > 0 {
				want = `{"data":"` + strings.Repeat("x", r.Selection.Payload-11) + `"}`
			}
			if r.Selection.Scenario == "duplicate" {
				want = `{"data":"previous"}`
			}
			require.Len(t, steps, 1)
			require.Equal(t, want, string(steps["s"]))
			md, err := svc.LoadMetadata(t.Context(), s.run.md.ID)
			require.NoError(t, err)
			wantCounter := 17
			if r.Selection.Scenario == "zero" || r.Selection.Scenario == "negative" || r.Selection.Scenario == "idempotent" || r.Selection.Scenario == "duplicate" {
				wantCounter = 6
			}
			require.Equal(t, wantCounter, md.Metrics.MetadataSize)
			require.Equal(t, 34+len(want), md.Metrics.StateSize)
		}
	}
	r.ValkeyMemory, err = c.Do(t.Context(), c.B().Info().Section("memory").Build()).ToString()
	require.NoError(t, err)
	c.Close()
	require.NoError(t, oneshotDurableShape(snapshot, r.Selection.Scenario))
	metadata := snapshot["{estate}:metadata:01ARZ3NDEKTSV4RRFFQ69G5FAV"].Value.(map[string]string)
	counter, stepCount := "6", "0"
	saved := scenario != "reject" && scenario != "overflow" && scenario != "savefail"
	if saved {
		stepCount = "1"
		if scenario != "zero" && scenario != "negative" && scenario != "idempotent" && scenario != "duplicate" {
			counter = "17"
		}
	}
	require.Equal(t, counter, metadata["metadata_size"])
	require.Equal(t, stepCount, metadata["step_count"])
	if saved {
		require.Equal(t, []string{"s"}, snapshot["{estate}:stack:01ARZ3NDEKTSV4RRFFQ69G5FAV"].Value)
	}
	if scenario == "savefail" {
		require.Equal(t, responseStoredValue{"string", "wrongtype", -1}, snapshot["{estate}:actions:00000000-0000-0000-0000-000000000001:01ARZ3NDEKTSV4RRFFQ69G5FAV"])
	}

	oneshotNormalizeSnapshot(t, snapshot, r)
	r.Validated = true
	receipt, err := json.Marshal(r)
	require.NoError(t, err)
	t.Logf("FUNCTIONAL_DURABLE_RECEIPT %s", receipt)
	r.Times["validation_end"] = time.Since(r.epoch).Nanoseconds()
	return snapshot
}

// Functional invocations only. Existing native owner tests own its full fault
// corpus; this test guards the NEW executable comparator and subprocess boundary.
func TestResponseOneShotFunctional(t *testing.T) {
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	c, err := queuehelper.NewValkeyClient(v.Addr, v.Username, v.Password, false)
	require.NoError(t, err)
	defer c.Close()
	oneshotPreload(t, t.Context(), c)
	for _, scenario := range []string{"fresh", "pending", "idempotent", "duplicate", "overflow", "savefail", "enqueuefail", "zero", "negative"} {
		t.Run(scenario, func(t *testing.T) {
			var snapshots []responseSnapshot
			for _, lane := range []string{"go", "native"} {
				sel := oneshotSelection{Lane: lane, Scenario: scenario, Phase: "functional"}
				epoch := time.Now()
				s := oneshotSetup(t, t.Context(), c, sel)
				r := oneshotInvoke(t.Context(), t, v, s, sel, epoch)
				require.Empty(t, r.Error)
				require.True(t, r.Cleanup)
				require.Equal(t, 0, r.Exit)
				want := "Success"
				switch scenario {
				case "overflow":
					want = "StateOverflow"
				case "savefail":
					want = "NeverRetry"
				case "enqueuefail":
					want = "AlwaysRetry"
				}
				require.Equal(t, want, r.Outcome.Disposition)
				if scenario != "overflow" {
					require.Equal(t, s.run.md.Metrics.MetadataSize, r.Outcome.MetadataSizeLoaded)
				}
				if scenario == "enqueuefail" && lane == "go" {
					require.Contains(t, r.Outcome.GoError, "error enqueueing discovery step:")
				}
				snapshot := oneshotReadback(t, v, s, &r)
				// Only post-save generated values vary; preserve all keys and other
				// values. Store time scores are checked separately, not erased here.
				require.Empty(t, r.Blockers, "accepted reviews must not become result blockers")
				if scenario == "fresh" {
					// A reported Enqueued result cannot stand in for durable storage.
					require.NoError(t, c.Do(t.Context(), c.B().Hdel().Key("{queue}:queue:item").Field("3k6yiivdqom1o").Build()).Error())
					missing := snapshotResponseStore(t, t.Context(), c)
					require.ErrorContains(t, oneshotDurableShape(missing, scenario), "expected durable queue item")
				}
				snapshots = append(snapshots, snapshot)
			}
			require.Equal(t, snapshots[0], snapshots[1], "independent real-store states")
		})
	}
	for _, size := range []int{64, 65536} {
		for _, lane := range []string{"go", "native"} {
			t.Run(fmt.Sprintf("saved%d/%s", size, lane), func(t *testing.T) {
				sel := oneshotSelection{Lane: lane, Scenario: "fresh", Payload: size, Phase: "functional"}
				epoch := time.Now()
				s := oneshotSetup(t, t.Context(), c, sel)
				r := oneshotInvoke(t.Context(), t, v, s, sel, epoch)
				require.Empty(t, r.Error)
				require.Equal(t, "Success", r.Outcome.Disposition)
				oneshotReadback(t, v, s, &r)
			})
		}
	}
	t.Log("FUNCTIONAL ONLY: 22 one-shot owner children; no performance claim")
}

// Strict executable-protocol controls have no reachable store. A decoder
// regression would try I/O and return an outcome instead of the required exit 2.
func TestResponseOneShotProtocol(t *testing.T) {
	s := newConstructedResponseSource()
	v := &queuehelper.ValkeyContainer{Addr: "127.0.0.1:1", Password: "functional-control-only"}
	var raw []byte
	require.NoError(t, s.withInput(t.Context(), false, func(input map[string]any) {
		var err error
		raw, err = oneshotDescriptorBytes(s, v, input)
		require.NoError(t, err)
	}))
	for _, change := range []string{"unknown", "missing", "trailing", "duplicate", "response-low", "response-high", "function-negative", "function-high", "loaded-low", "loaded-high", "bytes-high", "attempts-high", "domain-negative", "state-negative"} {
		bad := append([]byte(nil), raw...)
		switch change {
		case "response-low", "response-high", "function-negative", "function-high", "loaded-low", "loaded-high", "bytes-high", "attempts-high", "domain-negative", "state-negative":
			var object map[string]any
			require.NoError(t, json.Unmarshal(bad, &object))
			input := object["operation"].(map[string]any)["input"].(map[string]any)
			switch change {
			case "response-low":
				input["responseVersion"] = -2147483649
			case "response-high":
				input["responseVersion"] = 2147483648
			case "function-negative":
				input["functionVersion"] = -1
			case "function-high":
				input["functionVersion"] = uint64(4294967296)
			case "loaded-low":
				input["loaded"].(map[string]any)["requestVersion"] = -2147483649
			case "loaded-high":
				input["loaded"].(map[string]any)["requestVersion"] = 2147483648
			case "bytes-high":
				input["response"] = []int{256}
			case "attempts-high":
				input["maxAttempts"] = uint64(4294967296)
			case "domain-negative":
				input["domain"].(map[string]any)["observerRegistrations"] = -1
			case "state-negative":
				input["loaded"].(map[string]any)["stateBytes"] = -1
			}
			var err error
			bad, err = json.Marshal(object)
			require.NoError(t, err)
		case "unknown":
			bad = append([]byte(`{"unknown":0,`), bad[1:]...)
		case "missing":
			var object map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(bad, &object))
			delete(object, "disabledRetries")
			var err error
			bad, err = json.Marshal(object)
			require.NoError(t, err)
		case "trailing":
			bad = append(bad, []byte(` {}`)...)
		case "duplicate":
			bad = append([]byte(`{"disabledRetries":true,`), bad[1:]...)
		}
		for _, lane := range []string{"go", "native"} {
			t.Run(change+"/"+lane, func(t *testing.T) {
				r := oneshotRecord{Exit: -1}
				err := oneshotCommand(t.Context(), lane, bad, &r, func(string) {})
				require.Error(t, err)
				require.Equal(t, 2, r.Exit)
				require.True(t, r.Cleanup)
				require.Empty(t, r.Stdout)
				t.Logf("FUNCTIONAL_PROTOCOL_CHILD lane=%s control=%s exit=%d pid=%d", lane, change, r.Exit, r.ChildPID)
			})
		}
	}
	// Semantic rejection is a decoded result, not malformed-protocol exit.
	// Representable loaded versions still face the separate loaded-v2 admission.
	for _, version := range []int64{-2147483648, 2147483647} {
		var object map[string]any
		require.NoError(t, json.Unmarshal(raw, &object))
		input := object["operation"].(map[string]any)["input"].(map[string]any)
		input["responseVersion"] = version
		input["functionVersion"] = uint64(4294967295)
		if version < 0 {
			input["functionVersion"] = 0
		}
		input["loaded"].(map[string]any)["requestVersion"] = version
		boundary, err := json.Marshal(object)
		require.NoError(t, err)
		d, err := oneshotDecode(boundary)
		require.NoError(t, err)
		require.Equal(t, int32(version), d.Operation.Input.Loaded.RequestVersion)
		for _, lane := range []string{"go", "native"} {
			r := oneshotRecord{Exit: -1}
			require.NoError(t, oneshotCommand(t.Context(), lane, boundary, &r, func(string) {}))
			require.Equal(t, 0, r.Exit)
			require.Equal(t, "Error", r.Outcome.Disposition)
			require.Equal(t, "Ineligible", *r.Outcome.PreparationError)
			require.Nil(t, r.Outcome.GroupID)
			t.Logf("FUNCTIONAL_BOUNDARY_CHILD lane=%s version=%d pid=%d", lane, version, r.ChildPID)
		}
	}

	// Exercise every usize field at the Go-generated maximum, then disclose the
	// wider native wire range. Unsupported loaded version prevents all store I/O.
	for _, field := range []string{"observerRegistrations", "metadataWriters", "responseOwners", "stateBytes", "stateLimit"} {
		d, err := oneshotDecode(raw)
		require.NoError(t, err)
		d.Operation.Input.Loaded.RequestVersion = 0
		max := int(1<<63 - 1)
		switch field {
		case "observerRegistrations":
			d.Operation.Input.Domain.ObserverRegistrations = max
		case "metadataWriters":
			d.Operation.Input.Domain.MetadataWriters = max
		case "responseOwners":
			d.Operation.Input.Domain.ResponseOwners = max
		case "stateBytes":
			d.Operation.Input.Loaded.StateBytes = max
		case "stateLimit":
			d.Operation.Input.StateLimit = &max
		}
		generated, err := json.Marshal(d)
		require.NoError(t, err)
		for _, bound := range []string{"9223372036854775807", "9223372036854775808", "18446744073709551615"} {
			boundary := bytes.ReplaceAll(generated, []byte("9223372036854775807"), []byte(bound))
			for _, lane := range []string{"go", "native"} {
				t.Run(field+"/"+bound+"/"+lane, func(t *testing.T) {
					r := oneshotRecord{Exit: -1}
					err := oneshotCommand(t.Context(), lane, boundary, &r, func(string) {})
					if lane == "go" && bound != "9223372036854775807" {
						require.Error(t, err)
						require.Equal(t, 2, r.Exit)
						require.Empty(t, r.Stdout)
					} else {
						require.NoError(t, err)
						require.Equal(t, 0, r.Exit)
						require.Equal(t, "Error", r.Outcome.Disposition)
						require.Equal(t, "Ineligible", *r.Outcome.PreparationError)
						require.Nil(t, r.Outcome.GroupID)
					}
					require.True(t, r.Cleanup)
					t.Logf("FUNCTIONAL_BOUNDARY_CHILD lane=%s field=%s bound=%s exit=%d pid=%d", lane, field, bound, r.Exit, r.ChildPID)
				})
			}
		}
	}

	for _, lane := range []string{"go", "native"} {
		t.Run("domain/"+lane, func(t *testing.T) {
			r := oneshotInvoke(t.Context(), t, v, newConstructedResponseSource(), oneshotSelection{Lane: lane, Scenario: "reject", Phase: "functional"}, time.Now())
			require.Empty(t, r.Error)
			require.True(t, r.Cleanup)
			require.Equal(t, "Error", r.Outcome.Disposition)
			require.Nil(t, r.Outcome.GroupID)
			require.Nil(t, r.Outcome.NowMS)
			require.Equal(t, 0, r.Outcome.MetadataSizeLoaded)
		})
	}
	t.Log("FUNCTIONAL ONLY: 28 malformed-protocol children + 4 representable-boundary children + 30 usize scope children + 2 domain rejection children; zero store effects")
}

// JSON cannot preserve Go wrapper interfaces/sentinel identity. These four
// in-process calls guard that distinct contract using errors from real adapters.
func TestResponseOneShotGoErrorIdentity(t *testing.T) {
	v, err := queuehelper.StartValkey(t)
	require.NoError(t, err)
	c, err := queuehelper.NewValkeyClient(v.Addr, v.Username, v.Password, false)
	require.NoError(t, err)
	defer c.Close()
	oneshotPreload(t, t.Context(), c)
	for _, scenario := range []string{"overflow", "enqueuefail", "savefail-never", "savefail-at"} {
		t.Run(scenario, func(t *testing.T) {
			setup := scenario
			if strings.HasPrefix(setup, "savefail-") {
				setup = "savefail"
			}
			s := oneshotSetup(t, t.Context(), c, oneshotSelection{Scenario: setup})
			var raw []byte
			require.NoError(t, s.withInput(t.Context(), false, func(input map[string]any) {
				input["noRetry"] = scenario != "savefail-at"
				input["retryAt"] = "2026-10-28T12:00:00Z"
				if scenario == "overflow" {
					input["stateLimit"] = 1
				}
				var err error
				raw, err = oneshotDescriptorBytes(s, v, input)
				require.NoError(t, err)
			}))
			d, err := oneshotDecode(raw)
			require.NoError(t, err)
			out, err := oneshotGo(t.Context(), d)
			require.Error(t, err)
			switch scenario {
			case "overflow":
				require.ErrorIs(t, err, state.ErrStateOverflowed)
				require.Equal(t, "StateOverflow", out.Disposition)
				require.Nil(t, out.Save)
				require.Equal(t, 6, out.MetadataSizeLoaded)
			case "enqueuefail":
				require.True(t, queue.IsAlwaysRetryable(err))
				require.True(t, queue.ShouldRetry(err, 100, 1))
				require.Contains(t, err.Error(), "error enqueueing discovery step:")
				require.Equal(t, "AlwaysRetry", out.Disposition)
			case "savefail-never":
				require.False(t, queue.ShouldRetry(err, 0, 10))
				require.Nil(t, queue.AsRetryAtError(err))
				require.Equal(t, "NeverRetry", out.Disposition)
			case "savefail-at":
				require.NotNil(t, queue.AsRetryAtError(err))
				require.Equal(t, *d.Operation.Input.RetryAt, *queue.AsRetryAtError(err).NextRetryAt())
				require.Equal(t, "RetryAt", out.Disposition)
			}
		})
	}
	t.Log("FUNCTIONAL ONLY: 4 in-process reduced-Go owner invocations; no performance claim")
}

// This contract starts from the declared case, never from whichever keys exist.
func oneshotDurableShape(snapshot responseSnapshot, scenario string) error {
	const state = "{estate}:"
	const run = "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const function = "00000000-0000-0000-0000-000000000001:"
	keys := []string{state + "metadata:" + run, state + "bulk-events:" + function + run, state + "key:source-key"}
	saved := scenario != "reject" && scenario != "overflow" && scenario != "savefail"
	if saved || scenario == "savefail" {
		keys = append(keys, state+"actions:"+function+run)
	}
	if saved {
		keys = append(keys, state+"stack:"+run)
	}
	if scenario == "pending" {
		key := state + "pending:" + function + run
		keys = append(keys, key)
		if !reflect.DeepEqual(snapshot[key].Value, []string{"held"}) {
			return errors.New("expected durable pending held")
		}
	}
	queued := saved && scenario != "pending" && scenario != "enqueuefail"
	if queued {
		item, ok := snapshot["{queue}:queue:item"]
		if !ok || item.Type != "hash" {
			return errors.New("expected durable queue item")
		}
		hash, ok := item.Value.(map[string]string)
		if !ok || len(hash) != 1 || hash["3k6yiivdqom1o"] == "" {
			return errors.New("expected exactly one durable queue item 3k6yiivdqom1o")
		}
		keys = append(keys, "{queue}:queue:item", "{queue}:partition:item", "{queue}:partition:sorted", "{queue}:accounts:sorted", "{queue}:accounts:00000000-0000-0000-0000-000000000003:partition:sorted", "{queue}:queue:sorted:"+strings.TrimSuffix(function, ":"), "{queue}:idx:run:"+run, "{queue}:queue:status:"+function+"in-progress")
	} else if scenario == "enqueuefail" {
		keys = append(keys, "{queue}:queue:item")
		if snapshot["{queue}:queue:item"].Value != "wrongtype" {
			return errors.New("expected enqueue failure sentinel")
		}
	}
	actual := make([]string, 0, len(snapshot))
	for key := range snapshot {
		actual = append(actual, key)
	}
	sort.Strings(keys)
	sort.Strings(actual)
	if !reflect.DeepEqual(keys, actual) {
		return fmt.Errorf("durable keys: expected %v, got %v", keys, actual)
	}
	return nil
}

func oneshotNormalizeSnapshot(t *testing.T, snapshot responseSnapshot, r *oneshotRecord) {
	t.Helper()
	f := loadResponsePersistenceFixture(t)
	pointerKeys := map[string]bool{"{queue}:partition:sorted": true, "{queue}:accounts:sorted": true, "{queue}:accounts:00000000-0000-0000-0000-000000000003:partition:sorted": true}
	for key, value := range snapshot {
		if value.PTTL > 0 {
			require.InDelta(t, 86400000, value.PTTL, 5000)
			value.PTTL = 86400000
		}
		if strings.Contains(key, ":queue:item") && value.Type == "hash" {
			for field, raw := range value.Value.(map[string]string) {
				var item queue.QueueItem
				require.NoError(t, json.Unmarshal([]byte(raw), &item))
				require.NotNil(t, r.Outcome.GroupID)
				require.Equal(t, *r.Outcome.GroupID, item.Data.GroupID)
				require.Empty(t, item.Data.Metadata)
				require.Equal(t, "source-incoming", item.Data.Payload.(queue.PayloadEdge).Edge.Incoming)
				require.Equal(t, "s", item.Data.Payload.(queue.PayloadEdge).Edge.Outgoing)
				require.EqualValues(t, 19, *item.Data.PriorityFactor)
				require.Equal(t, 5, *item.Data.MaxAttempts)
				require.NotNil(t, r.Outcome.NowMS)
				require.GreaterOrEqual(t, item.WallTimeMS, *r.Outcome.NowMS)
				require.LessOrEqual(t, item.WallTimeMS, (r.EpochUnixNS+r.Times["outer_end"])/int64(time.Millisecond))
				// Bounds check this invocation only. The deterministic advancing
				// store oracle owns clock parity; normalization cannot prove it.
				// Review acceptance is admission state, never a durable effect blocker.
				require.Equal(t, *r.Outcome.NowMS-19000, item.AtMS)
				require.Regexp(t, `^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`, item.Data.GroupID)
				require.Equal(t, item.WallTimeMS, item.EnqueuedAt, "old-run FIFO score precedes Redis enqueue clock in this fixed profile")
				// Rebind only the disclosed constructed sources and post-save UUID/
				// times in the ORIGINAL accepted literal item. No Go planner answer.
				var expected queue.QueueItem
				require.NoError(t, json.Unmarshal(f.Queue.Item, &expected))
				expected.ID = "3k6yiivdqom1o"
				expected.AtMS = *r.Outcome.NowMS - 19000
				expected.WallTimeMS = item.WallTimeMS
				expected.EnqueuedAt = item.EnqueuedAt
				expected.Data.GroupID = *r.Outcome.GroupID
				expected.Data.Metadata = nil
				expected.Data.PriorityFactor = new(int64(19))
				expected.Data.MaxAttempts = new(5)
				expected.Data.Payload = queue.PayloadEdge{Edge: inngest.Edge{Incoming: "source-incoming", Outgoing: "s"}}
				require.Equal(t, expected, item)
				item.AtMS = 0
				item.WallTimeMS = 0
				item.EnqueuedAt = 0
				item.Data.GroupID = "generated"
				b, err := json.Marshal(item)
				require.NoError(t, err)
				value.Value.(map[string]string)[field] = string(b)
			}
		}
		// Queue indexes contain event/run-derived scores, except global ready
		// pointers, whose seconds are separately bounded by generated wall time.
		if value.Type == "zset" {
			for j, z := range value.Value.([]rueidis.ZScore) {
				if pointerKeys[key] {
					require.NotNil(t, r.Outcome.NowMS)
					require.GreaterOrEqual(t, z.Score, float64(*r.Outcome.NowMS/1000))
					require.LessOrEqual(t, z.Score, float64((r.EpochUnixNS+r.Times["outer_end"])/int64(time.Second)))
					value.Value.([]rueidis.ZScore)[j].Score = 0
				} else {
					require.Equal(t, float64(1469922831259), z.Score, "accepted run FIFO score with priority 19")
				}
			}
		}
		snapshot[key] = value
	}
	if r.Selection.Scenario != "pending" && r.Selection.Scenario != "overflow" && r.Selection.Scenario != "reject" && r.Selection.Scenario != "savefail" && r.Selection.Scenario != "enqueuefail" {
		raw := snapshot["{queue}:queue:item"].Value.(map[string]string)["3k6yiivdqom1o"]
		expected := expectedResponseQueueSnapshot(t, f, "queue", false, raw, "3k6yiivdqom1o")
		actual := responseSnapshot{}
		for key, v := range snapshot {
			if strings.HasPrefix(key, "{queue}:") {
				actual[key] = v
			}
		}
		for key, v := range expected {
			if v.Type == "zset" {
				zs := v.Value.([]rueidis.ZScore)
				for j := range zs {
					if pointerKeys[key] {
						zs[j].Score = 0
					} else {
						zs[j].Score = 1469922831259
					}
				}
				v.Value = zs
				expected[key] = v
			}
		}
		require.Equal(t, expected, actual, "exact durable queue keys, members and indexes")
	}
}

// This entry is executable future support, never selected by ordinary tests.
// The Python caller enforces declaration/provenance/package stop and sealing.
func TestResponseOneShotPilot(t *testing.T) {
	if os.Getenv("RESPONSE_ONESHOT_AUTHORIZATION") == "" {
		t.Skip("future pilot requires separate parent authorization")
	}
	grant, err := oneshotAdmission(oneshotAuthorizationState, os.Getenv("RESPONSE_ONESHOT_AUTHORIZATION"), filepath.Dir(os.Getenv("RESPONSE_ONESHOT_ROWS")), time.Now().Unix())
	require.NoError(t, err)
	require.NoError(t, oneshotClaimRunner(grant))
	ctx, stop := signal.NotifyContext(t.Context(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	inputFile, err := os.Open(os.Getenv("RESPONSE_ONESHOT_SELECTION"))
	require.NoError(t, err)
	raw, err := io.ReadAll(io.LimitReader(inputFile, 64*1024+1))
	require.NoError(t, err)
	require.NoError(t, inputFile.Close())
	require.LessOrEqual(t, len(raw), 64*1024)
	selection, err := oneshotValidateDeclaration(raw)
	require.NoError(t, err)
	budget, err := strconv.Atoi(os.Getenv("RESPONSE_ONESHOT_REMAINING_MS"))
	require.NoError(t, err)
	require.Greater(t, budget, 0)
	require.LessOrEqual(t, budget, 170000)
	deadline := time.Now().Add(time.Duration(budget) * time.Millisecond)
	_, err = oneshotAdmission(oneshotAuthorizationState, grant.ID, grant.Out, time.Now().Unix())
	require.NoError(t, err)
	v := oneshotAttachStore(t)
	inspect, err := v.Container.Inspect(ctx)
	require.NoError(t, err)
	storeFile, err := os.OpenFile(os.Getenv("RESPONSE_ONESHOT_ROWS")+".store", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	require.NoError(t, err)
	require.NoError(t, json.NewEncoder(storeFile).Encode(map[string]any{"container_id": v.Container.GetContainerID(), "pid": inspect.State.Pid, "image": inspect.Image, "address": v.Addr, "unix_ns": time.Now().UnixNano()}))
	require.NoError(t, storeFile.Sync())
	require.NoError(t, storeFile.Close())
	c, err := queuehelper.NewValkeyClient(v.Addr, v.Username, v.Password, false)
	require.NoError(t, err)
	defer c.Close()
	oneshotPreload(t, t.Context(), c)
	file, err := os.OpenFile(os.Getenv("RESPONSE_ONESHOT_ROWS"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	require.NoError(t, err)
	defer file.Close()
	starts, err := os.OpenFile(os.Getenv("RESPONSE_ONESHOT_ROWS")+".starts", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	require.NoError(t, err)
	defer starts.Close()
	readbacks, err := os.OpenFile(os.Getenv("RESPONSE_ONESHOT_ROWS")+".readbacks", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	require.NoError(t, err)
	defer readbacks.Close()
	var source *constructedResponseSource
	oneshotDispatch(t, selection, file, starts, readbacks, func(t *testing.T, sel oneshotSelection, epoch time.Time) oneshotRecord {
		if ctx.Err() != nil {
			t.Fatal("STOP: cooperative parent stop")
		}
		if time.Until(deadline) < 8*time.Second {
			t.Fatal("STOP: dispatch budget reserve")
		}
		_, err := oneshotAdmission(oneshotAuthorizationState, grant.ID, grant.Out, time.Now().Unix())
		require.NoError(t, err, "authorization before dispatch")
		source = oneshotSetup(t, ctx, c, sel)
		return oneshotInvoke(ctx, t, v, source, sel, epoch)
	}, func(t *testing.T, r *oneshotRecord) {
		defer func() { source = nil }() // no completed source retained between slots
		oneshotReadback(t, v, source, r)
	})
	require.NoError(t, starts.Close())
	require.NoError(t, readbacks.Close())
	require.NoError(t, file.Close())
}

// Binding without Listen reserves the port while every TCP connect is refused.
// Closing a listener would leave a race in which another process can claim it.
func TestResponseOneShotRefusedConnect(t *testing.T) {
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	require.NoError(t, err)
	defer syscall.Close(fd)
	require.NoError(t, syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}))
	addr, err := syscall.Getsockname(fd)
	require.NoError(t, err)
	v := &queuehelper.ValkeyContainer{Addr: net.JoinHostPort("127.0.0.1", strconv.Itoa(addr.(*syscall.SockaddrInet4).Port)), Password: "functional-control-only"}
	for _, lane := range []string{"go", "native"} {
		t.Run(lane, func(t *testing.T) {
			s := newConstructedResponseSource()
			s.run.md.Metrics.MetadataSize, s.run.md.Metrics.MetadataSizeLoaded = 17, 6
			r := oneshotInvoke(t.Context(), t, v, s, oneshotSelection{Lane: lane, Phase: "functional"}, time.Now())
			require.Empty(t, r.Error)
			require.Equal(t, 0, r.Exit)
			require.True(t, r.Cleanup)
			require.Equal(t, 17, r.Outcome.MetadataSizeLoaded)
			require.Equal(t, "Error", r.Outcome.Disposition)
			require.Nil(t, r.Outcome.GroupID)
			require.Nil(t, r.Outcome.NowMS)
			require.True(t, len(r.Outcome.Enqueue) == 0 || string(r.Outcome.Enqueue) == "null", "enqueue must be absent")
			if lane == "go" {
				require.Contains(t, r.Outcome.GoError, "connection refused")
				require.JSONEq(t, "null", string(r.Outcome.Save), "bootstrap failed before a Go SaveStep result exists")
			} else {
				var save struct {
					WriteStatus string `json:"writeStatus"`
					Attempts    int    `json:"attempts"`
					Results     []any  `json:"results"`
					LastFailure string `json:"lastFailure"`
				}
				require.NoError(t, json.Unmarshal(r.Outcome.Save, &save))
				require.Equal(t, "NotSent", save.WriteStatus)
				require.Equal(t, 1, save.Attempts)
				require.Empty(t, save.Results)
				require.Equal(t, "connect-or-auth", save.LastFailure)
			}
		})
	}
	t.Log("FUNCTIONAL ONLY: 2 refused-connect children; reserved nonlistening port, no store")
}

//go:embed testdata/response-oneshot-declaration.json
var oneshotAcceptedDeclaration []byte

type oneshotDeclaration struct {
	Schema         int                `json:"schema"`
	Status         string             `json:"status"`
	Blockers       []string           `json:"blockers"`
	Observations   int                `json:"observations"`
	Controls       int                `json:"controls"`
	PackageSeconds int                `json:"package_seconds"`
	StopSeconds    int                `json:"stop_seconds"`
	SealSeconds    int                `json:"seal_seconds"`
	Gates          string             `json:"gates"`
	Rows           []oneshotSelection `json:"rows"`
}

func oneshotValidateDeclaration(raw []byte) ([]oneshotSelection, error) {
	var want, got oneshotDeclaration
	reject := errors.New("exact fixed declaration required")
	if len(raw) > 64*1024 {
		return nil, reject
	}
	if err := oneshotRequired(raw, reflect.TypeOf(got)); err != nil {
		return nil, fmt.Errorf("%w: %v", reject, err)
	}
	var rowObjects struct {
		Rows []json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal(raw, &rowObjects); err != nil {
		return nil, reject
	}
	for _, row := range rowObjects.Rows {
		if err := oneshotRequired(row, reflect.TypeOf(oneshotSelection{})); err != nil {
			return nil, reject
		}
	}
	if err := json.Unmarshal(oneshotAcceptedDeclaration, &want); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		return nil, reject
	}
	if !reflect.DeepEqual(want, got) {
		return nil, reject
	}
	return got.Rows, nil
}

func oneshotAttachStore(t *testing.T) *queuehelper.ValkeyContainer {
	t.Helper()
	file, err := os.Open(os.Getenv("RESPONSE_ONESHOT_STORE"))
	require.NoError(t, err)
	raw, err := io.ReadAll(io.LimitReader(file, 4097))
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.LessOrEqual(t, len(raw), 4096)
	var identity struct {
		ContainerID string `json:"container_id"`
		Name        string `json:"name"`
		Owner       string `json:"owner"`
	}
	require.NoError(t, json.Unmarshal(raw, &identity))
	require.Regexp(t, `^[0-9a-f]{64}$`, identity.ContainerID)
	require.Regexp(t, `^[0-9a-f]{32}$`, identity.Owner)
	require.Equal(t, "oneshot-"+identity.Owner, identity.Name)
	v, err := queuehelper.AttachOwnedValkey(t, identity.ContainerID, identity.Name, "inngest.oneshot.owner", identity.Owner)
	require.NoError(t, err)
	return v
}

func oneshotValidateInvocation(t *testing.T, r *oneshotRecord) {
	t.Helper()
	require.Empty(t, r.Blockers, "result blockers")
	require.Empty(t, r.Error, "child error")
	require.True(t, r.Cleanup, "child cleanup")
	require.Zero(t, r.Exit, "child exit")
	require.Zero(t, r.ObserverRegistrations, "observer registrations")
	require.Zero(t, r.ObserverDeliveries, "observer deliveries")
	// Verify /proc identity, rather than accepting any nonempty string.
	close := strings.LastIndex(r.ChildIdentity, ")")
	open := strings.Index(r.ChildIdentity, " (")
	require.Greater(t, open, 0, "child lifetime identity missing")
	require.Greater(t, close, open, "child lifetime identity malformed")
	pid, err := strconv.Atoi(r.ChildIdentity[:open])
	require.NoError(t, err)
	require.Positive(t, r.ChildPID)
	require.Equal(t, r.ChildPID, pid, "child lifetime PID binding")
	fields := strings.Fields(r.ChildIdentity[close+1:])
	require.GreaterOrEqual(t, len(fields), 20, "child lifetime stat fields")
	ticks, err := strconv.ParseUint(fields[19], 10, 64)
	require.NoError(t, err)
	require.Positive(t, ticks, "child lifetime start ticks")
	want, ok := map[string]string{"fresh": "Success", "pending": "Success", "reject": "Error", "savefail": "NeverRetry", "enqueuefail": "AlwaysRetry"}[r.Selection.Scenario]
	require.True(t, ok, "undeclared scenario")
	require.Equal(t, want, r.Outcome.Disposition, "declared outcome")
	require.Nil(t, r.Outcome.RetryAt, "fixed declaration has no retry-at")
	require.Nil(t, r.Outcome.Failure)
	if r.Selection.Scenario == "reject" {
		require.Equal(t, new("Ineligible"), r.Outcome.PreparationError)
		require.Zero(t, r.Outcome.MetadataSizeLoaded)
	} else {
		require.Nil(t, r.Outcome.PreparationError)
		require.Equal(t, 17, r.Outcome.MetadataSizeLoaded, "fixed loaded accounting")
	}
	if r.Selection.Lane == "go" && r.Selection.Scenario == "savefail" {
		require.Contains(t, r.Outcome.GoError, "maximum retry attempts reached: error saving response:")
	} else if r.Selection.Lane == "go" && r.Selection.Scenario == "enqueuefail" {
		require.Contains(t, r.Outcome.GoError, "error enqueueing discovery step:")
	} else if r.Selection.Lane == "go" && r.Selection.Scenario == "reject" {
		require.Equal(t, "ineligible constructed domain", r.Outcome.GoError)
	} else {
		require.Empty(t, r.Outcome.GoError)
	}
	if r.Selection.Scenario == "fresh" || r.Selection.Scenario == "pending" {
		require.Nil(t, r.Outcome.PreparationError)
		require.Nil(t, r.Outcome.Failure)
		require.Empty(t, r.Outcome.GoError)
		if r.Selection.Lane == "native" {
			var save struct {
				WriteStatus string
				Attempts    int
				Results     [][]int
				LastFailure *string
			}
			require.NoError(t, json.Unmarshal(r.Outcome.Save, &save))
			require.Equal(t, "Confirmed", save.WriteStatus)
			require.Equal(t, 1, save.Attempts)
			pending := 0
			if r.Selection.Scenario == "pending" {
				pending = 1
			}
			require.Equal(t, [][]int{{pending}}, save.Results)
			require.Nil(t, save.LastFailure)
			if r.Selection.Scenario == "fresh" {
				var enqueue struct {
					WriteStatus string
					Result      *int
					StoredID    string
					LastFailure *string
				}
				require.NoError(t, json.Unmarshal(r.Outcome.Enqueue, &enqueue))
				require.Equal(t, "Confirmed", enqueue.WriteStatus)
				require.NotNil(t, enqueue.Result)
				require.Zero(t, *enqueue.Result)
				require.NotEmpty(t, enqueue.StoredID)
				require.Nil(t, enqueue.LastFailure)
			}
		}
	}
}

// This is the dispatch boundary used by the blocked entry and functional fault
// controls. A failed subtest returns before the next setup/child can be reached.
func oneshotDispatch(t *testing.T, selection []oneshotSelection, rows, starts, readbacks *os.File,
	invoke func(*testing.T, oneshotSelection, time.Time) oneshotRecord,
	validate func(*testing.T, *oneshotRecord)) {
	t.Helper()
	emit := func(file *os.File, value any) {
		require.NoError(t, json.NewEncoder(file).Encode(value))
		require.NoError(t, file.Sync())
	}
	for _, sel := range selection {
		epoch := time.Now()
		r := oneshotRecord{epoch: epoch, Selection: sel, EpochUnixNS: epoch.UnixNano(), ParentPID: os.Getpid(), Times: map[string]int64{}}
		emit(starts, map[string]any{"selection": sel, "epoch_unix_ns": epoch.UnixNano(), "parent_pid": os.Getpid(),
			"capture_grant":     os.Getenv("RESPONSE_ONESHOT_AUTHORIZATION"),
			"runner_generation": os.Getenv("RESPONSE_ONESHOT_RUNNER_GENERATION")})
		invoked := false
		accepted := t.Run(fmt.Sprintf("slot-%d", sel.ID), func(t *testing.T) {
			defer func() {
				if !invoked {
					emit(rows, map[string]any{"kind": "invocation", "record": r})
				}
				if r.Readback != "" {
					emit(readbacks, json.RawMessage(r.Readback))
				}
				kind := "validated"
				if t.Failed() {
					kind = "failure"
					r.Validated = false
				}
				r.Times["validation_end"] = time.Since(epoch).Nanoseconds()
				r.Times["slot_end"] = time.Since(epoch).Nanoseconds()
				emit(rows, map[string]any{"kind": kind, "record": r, "failure": t.Failed()})
			}()
			r = invoke(t, sel, epoch)
			emit(rows, map[string]any{"kind": "invocation", "record": r})
			invoked = true
			// Raw readback is retained before assertions by oneshotReadback.
			validate(t, &r)
			oneshotValidateInvocation(t, &r)
			require.True(t, r.Validated, "durable validation incomplete")
		})
		if !accepted {
			return
		}
	}
}

func TestResponseOneShotDispatchControls(t *testing.T) {
	if mode := os.Getenv("RESPONSE_ONESHOT_DISPATCH_CONTROL"); mode != "" {
		dir := os.Getenv("RESPONSE_ONESHOT_CONTROL_OUT")
		files := make([]*os.File, 3)
		for i, name := range []string{"rows", "starts", "readbacks"} {
			f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			require.NoError(t, err)
			defer f.Close()
			files[i] = f
		}
		selection := []oneshotSelection{{ID: 0, Lane: "go", Scenario: "fresh"}, {ID: 1, Lane: "go", Scenario: "fresh"}}
		oneshotDispatch(t, selection, files[0], files[1], files[2], func(t *testing.T, sel oneshotSelection, epoch time.Time) oneshotRecord {
			stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", os.Getpid()))
			require.NoError(t, err)
			r := oneshotRecord{epoch: epoch, Selection: sel, Times: map[string]int64{}, ChildPID: os.Getpid(), ChildIdentity: string(stat), Cleanup: true, Outcome: oneshotOutcome{Disposition: "Success", MetadataSizeLoaded: 17}}
			switch mode {
			case "result-blocker":
				r.Blockers = []string{"genuine effect blocker"}
			case "disposition":
				r.Outcome.Disposition = "NeverRetry"
			case "loaded":
				r.Outcome.MetadataSizeLoaded = 6
			case "preparation":
				r.Outcome.PreparationError = new("Ineligible")
			case "failure":
				r.Outcome.Failure = new("injected failure")
			case "go-error":
				r.Outcome.GoError = "injected error"
			case "retry-at":
				r.Outcome.RetryAt = new(time.Now())
			case "observer-registration":
				r.ObserverRegistrations = 1
			case "observer-delivery":
				r.ObserverDeliveries = 1
			case "identity":
				r.ChildIdentity = ""
			case "identity-pid":
				r.ChildPID++
			case "identity-malformed":
				r.ChildIdentity = "unverified"
			case "error":
				r.Error = "injected child failure"
			case "cleanup":
				r.Cleanup = false
			case "exit":
				r.Exit = 2
			}
			return r
		}, func(t *testing.T, r *oneshotRecord) {
			r.Readback = `{"schema":"synthetic-control","snapshot":{}}`
			if mode == "durable" {
				t.Fatal("injected independent durable failure")
			}
			r.Validated = true
		})
		return
	}
	binary, err := os.Executable()
	require.NoError(t, err)
	for _, mode := range []string{"valid", "result-blocker", "disposition", "loaded", "preparation", "failure", "go-error", "retry-at", "observer-registration", "observer-delivery", "identity", "identity-pid", "identity-malformed", "error", "cleanup", "exit", "durable"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			cmd := exec.Command(binary, "-test.run=^TestResponseOneShotDispatchControls$", "-test.v", "-test.timeout=5s")
			cmd.Env = append(os.Environ(), "RESPONSE_ONESHOT_DISPATCH_CONTROL="+mode, "RESPONSE_ONESHOT_CONTROL_OUT="+dir)
			output, err := cmd.CombinedOutput()
			if mode == "valid" {
				require.NoError(t, err, "%s", output)
			} else {
				require.Error(t, err, "%s", output)
				reason := map[string]string{
					"result-blocker": "result blockers", "disposition": "declared outcome", "loaded": "fixed loaded accounting", "preparation": "Expected nil", "failure": "Expected nil", "go-error": "Should be empty", "retry-at": "fixed declaration has no retry-at",
					"observer-registration": "observer registrations", "observer-delivery": "observer deliveries", "identity": "child lifetime identity missing", "identity-pid": "child lifetime PID binding", "identity-malformed": "child lifetime identity missing",
					"error": "child error", "cleanup": "child cleanup", "exit": "child exit", "durable": "injected independent durable failure",
				}[mode]
				require.Contains(t, string(output), reason, "control must reject for the intended reason")
			}
			read := func(name string) []map[string]json.RawMessage {
				raw, err := os.ReadFile(filepath.Join(dir, name))
				require.NoError(t, err)
				var events []map[string]json.RawMessage
				for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
					var event map[string]json.RawMessage
					require.NoError(t, json.Unmarshal(line, &event))
					events = append(events, event)
				}
				return events
			}
			rows, starts, readbacks := read("rows"), read("starts"), read("readbacks")
			count := 1
			if mode == "valid" {
				count = 2
			}
			require.Len(t, starts, count, "invalid slot must prevent next dispatch")
			require.Len(t, readbacks, count, "raw partial durable evidence retained")
			require.Len(t, rows, count*2)
			require.JSONEq(t, `"invocation"`, string(rows[0]["kind"]))
			if mode != "valid" {
				require.JSONEq(t, `"failure"`, string(rows[1]["kind"]))
				require.JSONEq(t, `true`, string(rows[1]["failure"]))
			}
			t.Logf("FUNCTIONAL_DISPATCH_CONTROL mode=%s starts=%d rows=%d readbacks=%d child_exit=%v", mode, len(starts), len(rows), len(readbacks), cmd.ProcessState.ExitCode())
		})
	}
}

func TestResponseOneShotFixedDeclaration(t *testing.T) {
	rows, err := oneshotValidateDeclaration(oneshotAcceptedDeclaration)
	require.NoError(t, err)
	require.Len(t, rows, 68)
	for _, raw := range [][]byte{
		bytes.Replace(oneshotAcceptedDeclaration, []byte(`"schema": 1`), []byte(`"schema": 2, "schema": 1`), 1),
		bytes.Replace(oneshotAcceptedDeclaration, []byte(`"seed": 26011`), []byte(`"seed": 1, "seed": 26011`), 1),
		append(append([]byte(nil), oneshotAcceptedDeclaration...), []byte(`{}`)...),
	} {
		_, err := oneshotValidateDeclaration(raw)
		require.ErrorContains(t, err, "exact fixed declaration")
	}
	// Mutations enter the same direct-Go validator before any Docker/child work.
	for _, field := range []string{"seed", "payload", "lane", "observation", "phase", "scenario", "id", "order", "count", "package_seconds", "stop_seconds", "seal_seconds", "controls", "observations", "extra", "typed-seed", "row-extra", "missing-zero-observation"} {
		t.Run(field, func(t *testing.T) {
			var object map[string]any
			require.NoError(t, json.Unmarshal(oneshotAcceptedDeclaration, &object))
			list := object["rows"].([]any)
			switch field {
			case "typed-seed":
				list[0].(map[string]any)["seed"] = 26099
			case "row-extra":
				list[0].(map[string]any)["selector"] = "arbitrary"
			case "missing-zero-observation":
				require.Equal(t, float64(0), list[0].(map[string]any)["observation"])
				delete(list[0].(map[string]any), "observation")
			case "order":
				list[0], list[1] = list[1], list[0]
			case "count":
				object["rows"] = list[:67]
			case "package_seconds", "stop_seconds", "seal_seconds", "controls", "observations":
				object[field] = 999
			case "extra":
				object["selector"] = "arbitrary"
			default:
				list[0].(map[string]any)[field] = "changed"
			}
			raw, err := json.Marshal(object)
			require.NoError(t, err)
			if field == "typed-seed" || field == "row-extra" || field == "missing-zero-observation" {
				// These valid typed inputs must reach admission beyond JSON decoding.
				var declaration oneshotDeclaration
				require.NoError(t, json.Unmarshal(raw, &declaration))
			}
			_, err = oneshotValidateDeclaration(raw)
			require.ErrorContains(t, err, "exact fixed declaration")
		})
	}
}

// Explicit functional control ONLY: four real Go children, then a forced stop
// while the helper owns the parent-created store. Never a pilot/latency entry.
func TestResponseOneShotOwnedStoreControl(t *testing.T) {
	if os.Getenv("RESPONSE_ONESHOT_STORE_CONTROL") != "forced-stop-functional-v1" {
		t.Skip("explicit disposable owned-store control only")
	}
	signal.Ignore(syscall.SIGTERM)
	v := oneshotAttachStore(t)
	c, err := queuehelper.NewValkeyClient(v.Addr, v.Username, v.Password, false)
	require.NoError(t, err)
	defer c.Close()
	oneshotPreload(t, t.Context(), c)
	out := os.Getenv("RESPONSE_ONESHOT_CONTROL_OUT")
	for _, mode := range []string{"disposition", "identity", "observer", "durable"} {
		t.Run(mode, func(t *testing.T) {
			files := make([]*os.File, 3)
			for i, suffix := range []string{"rows", "starts", "readbacks"} {
				files[i], err = os.OpenFile(filepath.Join(out, mode+"."+suffix), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
				require.NoError(t, err)
				defer files[i].Close()
			}
			selection := []oneshotSelection{{ID: 0, Seed: 26011, Payload: 64, Lane: "go", Phase: "functional", Scenario: "fresh"}, {ID: 1, Seed: 26011, Payload: 64, Lane: "go", Phase: "functional", Scenario: "fresh"}}
			oneshotDispatch(t, selection, files[0], files[1], files[2], func(t *testing.T, sel oneshotSelection, epoch time.Time) oneshotRecord {
				s := oneshotSetup(t, t.Context(), c, sel)
				r := oneshotInvoke(t.Context(), t, v, s, sel, epoch)
				require.Empty(t, r.Error)
				require.Equal(t, "Success", r.Outcome.Disposition)
				switch mode {
				case "disposition":
					r.Outcome.Disposition = "NeverRetry"
				case "identity":
					r.ChildIdentity = ""
				case "observer":
					r.ObserverDeliveries = 1
				case "durable":
					require.NoError(t, c.Do(t.Context(), c.B().Hdel().Key("{queue}:queue:item").Field("3k6yiivdqom1o").Build()).Error())
				}
				return r
			}, func(t *testing.T, r *oneshotRecord) {
				oneshotReadback(t, v, newConstructedResponseSource(), r)
			})
		})
	}
	ready, err := os.OpenFile(filepath.Join(out, "ready"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require.NoError(t, err)
	require.NoError(t, ready.Sync())
	require.NoError(t, ready.Close())
	// The supervising functional control MUST kill/reap this process and
	// independently remove/inspect its exact store. No helper cleanup can run.
	select {}
}

//go:embed testdata/response-oneshot-authorization.json
var oneshotAuthorizationState []byte

const oneshotReviewAcceptance = "parent-accepted:readinessreview03+clockaccountingreview02+P3precedencereview01+runnerstopreview01+declarationreview02"

type oneshotGrant struct {
	ID                string `json:"id"`
	Scope             string `json:"scope"`
	DeclarationSHA256 string `json:"declaration_sha256"`
	Out               string `json:"out"`
	Claim             string `json:"claim"`
	ExpiresUnix       int64  `json:"expires_unix"`
	Attempts          int    `json:"attempts"`
}

type oneshotAdmissionState struct {
	Schema           int           `json:"schema"`
	ReviewAcceptance string        `json:"review_acceptance"`
	Authorization    *oneshotGrant `json:"authorization"`
}

func oneshotDecodeStrict(raw []byte, value any) error {
	if len(raw) > 4096 {
		return errors.New("authorization capacity")
	}
	if err := oneshotRequired(raw, reflect.TypeOf(value).Elem()); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("authorization trailing data")
	}
	return nil
}

func oneshotAdmission(raw []byte, requested, out string, now int64) (*oneshotGrant, error) {
	var state oneshotAdmissionState
	if err := oneshotDecodeStrict(raw, &state); err != nil {
		return nil, err
	}
	if state.Schema != 1 || state.ReviewAcceptance != oneshotReviewAcceptance {
		return nil, errors.New("unrecognized review acceptance")
	}
	grant := state.Authorization
	if grant == nil {
		return nil, errors.New("capture requires separate parent authorization; none issued")
	}
	// Required fields and duplicate detection also apply inside the grant.
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return nil, err
	}
	if err := oneshotRequired(object["authorization"], reflect.TypeOf(oneshotGrant{})); err != nil {
		return nil, err
	}
	resolved, err := filepath.Abs(out)
	if err != nil {
		return nil, err
	}
	rel, err := filepath.Rel(grant.Out, grant.Claim)
	if err != nil {
		return nil, err
	}
	if !regexp.MustCompile(`^[a-z0-9-]{16,80}$`).MatchString(grant.ID) || requested != grant.ID ||
		grant.Scope != "oneshot-capability-60-plus-8" || grant.Attempts != 1 || grant.ExpiresUnix <= now ||
		grant.DeclarationSHA256 != fmt.Sprintf("%x", sha256.Sum256(oneshotAcceptedDeclaration)) ||
		!filepath.IsAbs(grant.Out) || resolved != grant.Out || !filepath.IsAbs(grant.Claim) ||
		(rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))) {
		return nil, errors.New("capture authorization expired or scope mismatch")
	}
	return grant, nil
}

// The receipt and exclusive runner marker prevent direct-entry reuse of a claim.
func oneshotClaimRunner(grant *oneshotGrant) error {
	file, err := os.Open(grant.Claim)
	if err != nil {
		return err
	}
	raw, err := io.ReadAll(io.LimitReader(file, 4097))
	closeErr := file.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	var claimed oneshotGrant
	if err := oneshotDecodeStrict(raw, &claimed); err != nil {
		return err
	}
	if claimed != *grant {
		return errors.New("capture authorization receipt mismatch")
	}
	runner, err := os.OpenFile(grant.Claim+".runner", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("capture authorization exhausted: %w", err)
	}
	defer runner.Close()
	if _, err := runner.WriteString(grant.ID); err != nil {
		return err
	}
	return runner.Sync()
}

func TestResponseOneShotAuthorizationState(t *testing.T) {
	dir := t.TempDir()
	grant := oneshotGrant{ID: "synthetic-future-grant-01", Scope: "oneshot-capability-60-plus-8", DeclarationSHA256: fmt.Sprintf("%x", sha256.Sum256(oneshotAcceptedDeclaration)), Out: filepath.Join(dir, "out"), Claim: filepath.Join(dir, "claim"), ExpiresUnix: 2000, Attempts: 1}
	state := oneshotAdmissionState{Schema: 1, ReviewAcceptance: oneshotReviewAcceptance, Authorization: &grant}
	raw, err := json.Marshal(state)
	require.NoError(t, err)
	admitted, err := oneshotAdmission(raw, grant.ID, grant.Out, 1000)
	require.NoError(t, err)
	require.Equal(t, grant, *admitted)
	nullState, err := json.Marshal(oneshotAdmissionState{Schema: 1, ReviewAcceptance: oneshotReviewAcceptance})
	require.NoError(t, err)
	_, err = oneshotAdmission(nullState, grant.ID, grant.Out, 1000)
	require.ErrorContains(t, err, "none issued")
	// The embedded registry has its own lifecycle; it need not be null.
	var current oneshotAdmissionState
	require.NoError(t, oneshotDecodeStrict(oneshotAuthorizationState, &current))
	if current.Authorization == nil {
		_, err = oneshotAdmission(oneshotAuthorizationState, grant.ID, grant.Out, 1000)
		require.ErrorContains(t, err, "none issued")
	} else {
		_, err = oneshotAdmission(oneshotAuthorizationState, grant.ID, grant.Out, 1000)
		require.ErrorContains(t, err, "expired or scope mismatch")
		// Check the embedded grant at its expiry without touching its claim or
		// depending on a host cache, wall clock, or historical registry being null.
		_, err = oneshotAdmission(oneshotAuthorizationState, current.Authorization.ID,
			current.Authorization.Out, current.Authorization.ExpiresUnix)
		require.ErrorContains(t, err, "expired or scope mismatch")
	}
	for _, mutation := range []string{"expired", "scope", "declaration", "attempts", "review", "out", "missing", "unknown", "duplicate"} {
		t.Run(mutation, func(t *testing.T) {
			candidate := state
			changed := grant
			candidate.Authorization = &changed
			switch mutation {
			case "expired":
				changed.ExpiresUnix = 1000
			case "scope":
				changed.Scope = "other"
			case "declaration":
				changed.DeclarationSHA256 = "wrong"
			case "attempts":
				changed.Attempts = 2
			case "review":
				candidate.ReviewAcceptance = "external assertion"
			case "out":
				changed.Out = filepath.Join(dir, "other")
			}
			input, err := json.Marshal(candidate)
			require.NoError(t, err)
			if mutation == "missing" {
				input = bytes.Replace(input, []byte(`"attempts":1`), []byte(`"unused":1`), 1)
			}
			if mutation == "unknown" {
				input = bytes.Replace(input, []byte(`"attempts":1`), []byte(`"attempts":1,"unused":1`), 1)
			}
			if mutation == "duplicate" {
				input = bytes.Replace(input, []byte(`"attempts":1`), []byte(`"attempts":1,"attempts":1`), 1)
			}
			_, err = oneshotAdmission(input, grant.ID, grant.Out, 1000)
			require.Error(t, err)
		})
	}
	_, err = oneshotAdmission(raw, "wrong-grant", grant.Out, 1000)
	require.Error(t, err)
	require.Error(t, oneshotClaimRunner(&grant), "missing Python claim")
	wrong := grant
	wrong.Scope = "other"
	encoded, err := json.Marshal(wrong)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(grant.Claim, encoded, 0600))
	require.ErrorContains(t, oneshotClaimRunner(&grant), "receipt mismatch")
	encoded, err = json.Marshal(grant)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(grant.Claim, encoded, 0600))
	require.NoError(t, oneshotClaimRunner(&grant))
	require.ErrorContains(t, oneshotClaimRunner(&grant), "exhausted")
}

func TestResponseOneShotAcceptedReviewDurableFixture(t *testing.T) {
	// Independent literal queue fixture; no store or owner runs. This catches
	// the old unconditional pending-review append at the real normalization boundary.
	f := loadResponsePersistenceFixture(t)
	var item queue.QueueItem
	require.NoError(t, json.Unmarshal(f.Queue.Item, &item))
	now := int64(1700000001000)
	group := "00000000-0000-4000-8000-000000000005"
	item.ID = "3k6yiivdqom1o"
	item.AtMS = now - 19000
	item.WallTimeMS = now
	item.EnqueuedAt = now
	item.Data.GroupID = group
	item.Data.Metadata = nil
	item.Data.PriorityFactor = new(int64(19))
	item.Data.MaxAttempts = new(5)
	item.Data.Payload = queue.PayloadEdge{Edge: inngest.Edge{Incoming: "source-incoming", Outgoing: "s"}}
	encoded, err := json.Marshal(item)
	require.NoError(t, err)
	snapshot := expectedResponseQueueSnapshot(t, f, "queue", false, string(encoded), item.ID)
	pointers := map[string]bool{"{queue}:partition:sorted": true, "{queue}:accounts:sorted": true, "{queue}:accounts:00000000-0000-0000-0000-000000000003:partition:sorted": true}
	for key, value := range snapshot {
		if value.Type == "zset" {
			for j := range value.Value.([]rueidis.ZScore) {
				score := float64(1469922831259)
				if pointers[key] {
					score = float64(now / 1000)
				}
				value.Value.([]rueidis.ZScore)[j].Score = score
			}
		}
	}
	r := oneshotRecord{Selection: oneshotSelection{Scenario: "fresh"}, EpochUnixNS: now * int64(time.Millisecond), Times: map[string]int64{"outer_end": 1}, Outcome: oneshotOutcome{NowMS: &now, GroupID: &group}}
	// Existing genuine blockers must survive the accepted review state verbatim.
	oneshotNormalizeSnapshot(t, snapshot, &r)
	require.Empty(t, r.Blockers)
	r.Blockers = []string{"genuine retained effect blocker"}
	// The snapshot is already normalized; repeat from independent raw bytes.
	var original queue.QueueItem
	require.NoError(t, json.Unmarshal(encoded, &original))
	snapshot = expectedResponseQueueSnapshot(t, f, "queue", false, string(encoded), original.ID)
	for key, value := range snapshot {
		if value.Type == "zset" {
			for j := range value.Value.([]rueidis.ZScore) {
				score := float64(1469922831259)
				if pointers[key] {
					score = float64(now / 1000)
				}
				value.Value.([]rueidis.ZScore)[j].Score = score
			}
		}
	}
	oneshotNormalizeSnapshot(t, snapshot, &r)
	require.Equal(t, []string{"genuine retained effect blocker"}, r.Blockers)
}
