//go:build linux && response_continuation_spike && cgo

package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
)

var measureMode = flag.String("response-measure-mode", "", "plan, smoke, campaign, screen or diagnostic; execution requires separate authorization")
var measureDeclareOnly = flag.Bool("response-measure-declare-only", false, "screen declaration only; execute no children")
var measurePackageRemaining = flag.Duration("response-measure-package-remaining", 15*time.Minute, "screen package budget remaining after outer setup")
var measureOut = flag.String("response-measure-out", "", "new artifact directory (required)")
var measureLaneSelection = flag.String("response-measure-lanes", "go,native", "exact comma-separated lanes")
var measureProfileSelection = flag.String("response-measure-profiles", "controlled,real-valkey-off,real-valkey-on", "exact profiles; missing Valkey is an error")
var measureWorkloadSelection = flag.String("response-measure-workloads", strings.Join(measureWorkloads, ","), "exact workloads")
var measureConcurrencySelection = flag.String("response-measure-concurrency", "1,8", "exact concurrency")

// Named, bounded capture profiles; diagnostic cannot alter campaign defaults.
type measureBudget struct {
	Watchdog        time.Duration `json:"post_setup_watchdog_ns"`
	RowContext      time.Duration `json:"row_context_ns"`
	TestTimeout     time.Duration `json:"test_timeout_ns"`
	OuterKill       time.Duration `json:"outer_kill_ns"`
	Request         time.Duration `json:"request_deadline_ns"`
	ResourceRecords int           `json:"resource_records"`
}

func measureCaptureBudget(mode string) (measureBudget, error) {
	b := measureBudget{110 * time.Second, 120 * time.Second, 140 * time.Second, 150 * time.Second, 20 * time.Second, 120000}
	switch mode {
	case "", "plan", "smoke", "campaign":
	case "diagnostic":
		b = measureBudget{210 * time.Second, 240 * time.Second, 260 * time.Second, 270 * time.Second, 20 * time.Second, 240000}
	case "screen":
		b = measureBudget{60 * time.Second, 90 * time.Second, 110 * time.Second, 120 * time.Second, 20 * time.Second, 120000}
	default:
		return b, fmt.Errorf("invalid capture budget mode %q", mode)
	}
	return b, nil
}

type measureRow struct {
	Preflight   bool   `json:"preflight"`
	Seed        int    `json:"seed"`
	Lane        string `json:"lane"`
	Profile     string `json:"profile"`
	Workload    string `json:"workload"`
	Concurrency int    `json:"concurrency"`
	Count       int    `json:"count"`
	Warmups     int    `json:"warmups_per_slot"`
	FirstOnly   bool   `json:"first_only"`
	Pair        int    `json:"first_pair"`
	Order       int    `json:"order"`
}

const measureThroughputScope = "closed-loop input preparation, owner/resource completion, validation and input release; backend runs precreated"

type measureSample struct {
	SlotStart     int64  `json:"slot_start_ns"`
	SlotEnd       int64  `json:"slot_end_ns"`
	ID            int    `json:"id"`
	Worker        int    `json:"worker"`
	Phase         string `json:"phase"`
	Start         int64  `json:"start_ns"`
	Owner         int64  `json:"owner_return_ns"`
	Delivered     int64  `json:"delivered_ns"`
	Resource      int64  `json:"resource_ns"`
	Result        string `json:"result_class"`
	CallbackCount int    `json:"callback_count"`
	Timeout       bool   `json:"timeout"`
	Complete      bool   `json:"complete"`
	Error         string `json:"error,omitempty"`
}
type measureResource struct {
	Stage        string `json:"stage"`
	Offset       int64  `json:"offset_ns"`
	UTC          string `json:"utc"`
	RSS          uint64 `json:"rss_bytes"`
	Highwater    uint64 `json:"vmhwm_bytes"`
	FD           int    `json:"fd"`
	Threads      int    `json:"threads"`
	Goroutines   int    `json:"goroutines"`
	ValkeyMemory string `json:"valkey_memory_info,omitempty"`
}

// This counts harness-owned input roots and their distinct Data backing capacity,
// not OS memory or native allocations. Samples contain scalar outcomes only.
type measureInputRetention struct {
	Prepared         int `json:"prepared"`
	Validated        int `json:"validated"`
	Released         int `json:"released"`
	Remaining        int `json:"remaining"`
	PeakInputs       int `json:"peak_inputs"`
	PeakPayloadBytes int `json:"peak_payload_capacity_bytes"`
	PrecreatedRuns   int `json:"precreated_backend_runs"`
}

type measureInputs struct {
	mu    sync.Mutex
	live  map[*measureInput]struct{}
	stats measureInputRetention
}

func (m *measureInputs) retain(in *measureInput) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.live[in] = struct{}{}
	m.stats.Prepared++
	m.stats.PeakInputs = max(m.stats.PeakInputs, len(m.live))
	// Some descriptors within one response share Data; count backing arrays once.
	data := map[*byte]int{}
	for input := range m.live {
		for _, op := range input.i.resp.Generator {
			if cap(op.Data) > 0 {
				data[&op.Data[:cap(op.Data)][0]] = cap(op.Data)
			}
		}
	}
	bytes := 0
	for _, capacity := range data {
		bytes += capacity
	}
	m.stats.PeakPayloadBytes = max(m.stats.PeakPayloadBytes, bytes)
}

func (m *measureInputs) release(in *measureInput, validated bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if validated {
		m.stats.Validated++
	}
	delete(m.live, in)
	m.stats.Released++
}

func (m *measureInputs) snapshot() measureInputRetention {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.stats
	stats.Remaining = len(m.live)
	return stats
}

type measureResult struct {
	Budget                 measureBudget         `json:"capture_budget"`
	StopReason             string                `json:"stop_reason,omitempty"`
	ThroughputScope        string                `json:"throughput_scope"`
	InputRetention         measureInputRetention `json:"input_retention"`
	RequestRecordCapacity  int                   `json:"request_record_capacity"`
	ResourceRecordCapacity int                   `json:"resource_record_capacity"`
	Schema                 int                   `json:"schema"`
	Row                    measureRow            `json:"row"`
	PID                    int                   `json:"pid"`
	UTC                    string                `json:"utc"`
	Units                  string                `json:"units"`
	Method                 string                `json:"method"`
	SetupNS                int64                 `json:"setup_ns"`
	SetupHeap              uint64                `json:"setup_heap_bytes"`
	Samples                []measureSample       `json:"samples"`
	Resources              []measureResource     `json:"resources"`
	LoadBefore             string                `json:"load_before"`
	LoadAfter              string                `json:"load_after"`
	CPUAvailability        string                `json:"cpu_availability"`
	ValkeyPingNS           int64                 `json:"valkey_ping_ns"`
	RSSIntervalNS          int64                 `json:"rss_interval_ns"`
	RSSStatus              string                `json:"rss_status"`
	Error                  string                `json:"error,omitempty"`
}

func measureSelect(value string, allowed []string) ([]string, error) {
	result := strings.Split(value, ",")
	seen := map[string]bool{}
	for _, v := range result {
		valid := false
		for _, a := range allowed {
			if a == v {
				valid = true
			}
		}
		if !valid || seen[v] {
			return nil, fmt.Errorf("invalid/duplicate exact selector %q (allowed %v)", v, allowed)
		}
		seen[v] = true
	}
	return result, nil
}
func measureRows(mode, lanes, profiles, workloads, concurrency string) ([]measureRow, error) {
	if mode == "screen" && (lanes != "go,native" || profiles != "controlled" || workloads != "run64,mixed8,run64k" || concurrency != "1") {
		return nil, errors.New("screen requires exact go,native/controlled/run64,mixed8,run64k/C1 selectors")
	}
	if mode == "diagnostic" {
		if lanes != "native" || profiles != "controlled" || workloads != "run64k" || concurrency != "1" {
			return nil, errors.New("diagnostic requires exact native/controlled/run64k/C1 selectors")
		}
		return []measureRow{{Seed: 26011, Lane: "native", Profile: "controlled", Workload: "run64k", Concurrency: 1, Count: 5000, Warmups: 32}}, nil
	}
	if mode != "smoke" && mode != "campaign" && mode != "plan" && mode != "screen" {
		return nil, fmt.Errorf("invalid mode %q", mode)
	}
	ls, err := measureSelect(lanes, []string{"go", "native"})
	if err != nil {
		return nil, err
	}
	ps, err := measureSelect(profiles, []string{"controlled", "real-valkey-off", "real-valkey-on"})
	if err != nil {
		return nil, err
	}
	ws, err := measureSelect(workloads, measureWorkloads)
	if err != nil {
		return nil, err
	}
	cs, err := measureSelect(concurrency, []string{"1", "8"})
	if err != nil {
		return nil, err
	}
	if mode != "smoke" && mode != "screen" && (len(ls) != 2 || len(ps) != 3 || len(ws) != 5 || len(cs) != 2) {
		return nil, errors.New("plan/campaign requires the complete declared matrix")
	}
	seeds := []int{26011, 26012, 26013}
	if mode == "smoke" {
		seeds = seeds[:1]
	}
	rows := []measureRow{}
	for _, seed := range seeds {
		rng := rand.New(rand.NewSource(int64(seed)))
		cells := []measureRow{}
		for _, p := range ps {
			for _, w := range ws {
				for _, c := range cs {
					n, _ := strconv.Atoi(c)
					count := 5000
					if p != "controlled" || mode == "screen" {
						count = 500
					}
					warm := 32
					if mode == "smoke" {
						count = max(2, n)
						warm = 1
					}
					cells = append(cells, measureRow{Seed: seed, Profile: p, Workload: w, Concurrency: n, Count: count, Warmups: warm})
				}
			}
		}
		rng.Shuffle(len(cells), func(i, j int) { cells[i], cells[j] = cells[j], cells[i] })
		for _, cell := range cells {
			order := append([]string(nil), ls...)
			rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
			for _, lane := range order {
				r := cell
				r.Lane = lane
				r.Order = len(rows)
				rows = append(rows, r)
			}
		}
	}
	if mode == "screen" {
		return rows, nil // First request belongs to each row; no extra first children.
	}
	// Separate fresh-process first observations; never label them cold-cache.
	rng := rand.New(rand.NewSource(26011))
	pairs := 10
	if mode == "smoke" {
		pairs = 1
	}
	for _, p := range ps {
		for _, w := range ws {
			for pair := 0; pair < pairs; pair++ {
				order := append([]string(nil), ls...)
				rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
				for _, lane := range order {
					rows = append(rows, measureRow{Seed: 26011, Lane: lane, Profile: p, Workload: w, Concurrency: 1, Count: 0, FirstOnly: true, Pair: pair, Order: len(rows)})
				}
			}
		}
	}
	return rows, nil
}

// The timestamps have ONE start. Native construction/projection is inside it;
// Go enters the actual owner directly. Slot reuse waits for Resource, not Owner.
func measureRequest(ctx context.Context, lane string, in *measureInput, epoch time.Time, id, worker int, phase string) (sample measureSample, result error) {
	sample = measureSample{ID: id, Worker: worker, Phase: phase, Start: time.Since(epoch).Nanoseconds()}
	if in.startOffset != nil {
		in.startOffset.Store(sample.Start)
	}
	var d *continuationDriver
	if lane == "go" {
		result = in.e.HandleGeneratorResponse(in.ctx, in.i, in.i.resp)
	} else if lane == "native" {
		d, result = newContinuationDriver(in.ctx, in.e, in.i, in.i.resp)
		if result == nil {
			result = d.run()
		}
	} else {
		result = fmt.Errorf("invalid lane %q", lane)
	}
	sample.Owner = time.Since(epoch).Nanoseconds()
	drainErr := in.callbacks.wait(ctx)
	if d != nil && drainErr == nil {
		d.callbacks.Wait()
	}
	sample.Delivered = time.Since(epoch).Nanoseconds()
	var closeErr error
	if d != nil {
		closeErr = d.drain()
	}
	timedOut := ctx.Err() != nil
	in.span.End()
	if in.cleanup != nil {
		in.cleanup()
	}
	if d != nil {
		d.branches = nil
		d.original = nil
		d.errors = nil
		d = nil
	}
	resourceAt := time.Now()
	sample.Resource = resourceAt.Sub(epoch).Nanoseconds()
	if deadline, ok := ctx.Deadline(); ok && !resourceAt.Before(deadline) {
		timedOut = true
	}
	in.callbacks.mu.Lock()
	sample.CallbackCount = in.callbacks.completed
	in.callbacks.mu.Unlock()
	sample.Timeout = timedOut || errors.Is(result, context.DeadlineExceeded) || errors.Is(drainErr, context.DeadlineExceeded)
	sample.Complete = drainErr == nil && closeErr == nil
	sample.Result = "ok"
	if result != nil {
		sample.Result = "unexpected_error"
		sample.Error = result.Error()
		if errors.Is(result, ErrHandledStepError) {
			sample.Result = "retryable_step_error"
		}
		if strings.Contains(result.Error(), measureCommitError.Error()) {
			sample.Result = "enqueue_commit_error"
		}
	}
	if sample.Timeout {
		sample.Result = "timeout"
	}
	if drainErr != nil || closeErr != nil {
		sample.Error += fmt.Sprint(errors.Join(drainErr, closeErr))
	}
	return sample, result
}
func measureReadResource(stage string, epoch time.Time) (measureResource, error) {
	r := measureResource{Stage: stage, Offset: time.Since(epoch).Nanoseconds(), UTC: time.Now().UTC().Format(time.RFC3339Nano), Goroutines: runtime.NumGoroutine()}
	b, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return r, err
	}
	for _, line := range strings.Split(string(b), "\n") {
		f := strings.Fields(line)
		if len(f) < 2 {
			continue
		}
		n, _ := strconv.ParseUint(f[1], 10, 64)
		switch f[0] {
		case "VmRSS:":
			r.RSS = n * 1024
		case "VmHWM:":
			r.Highwater = n * 1024
		case "Threads:":
			r.Threads = int(n)
		}
	}
	fds, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return r, err
	}
	r.FD = len(fds)
	if r.RSS == 0 || r.Threads == 0 {
		return r, errors.New("missing RSS/thread counters")
	}
	return r, nil
}
func measureLoad() string { b, _ := os.ReadFile("/proc/loadavg"); return strings.TrimSpace(string(b)) }
func measureCPU() string {
	b, _ := os.ReadFile("/proc/self/status")
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, "Cpus_allowed_list:") {
			return l
		}
	}
	return "unavailable"
}
func measureWrite(path string, v any) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(append(b, '\n'))
	err = errors.Join(writeErr, file.Sync(), file.Close())
	if err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

// Read only published immutable completions. Watchdog expiry never fabricates
// a request timeout; an observed request-deadline expiry is recorded separately.
func measureFailureSamples(res *measureResult, completed []atomic.Pointer[measureSample], starts []atomic.Int64, reason string) {
	for j := range completed {
		if sample := completed[j].Load(); sample != nil {
			res.Samples[j] = *sample
		}
		if !res.Samples[j].Complete {
			if start := starts[j].Load(); start > 0 {
				res.Samples[j].Start = start
				res.Samples[j].Result = "in_flight_at_" + reason
				res.Samples[j].Timeout = reason == "request_deadline_expired"
			} else if res.Samples[j].SlotStart > 0 {
				res.Samples[j].Result += "_at_" + reason
			}
		}
	}
}

// Failure-only, outside measurement. Native unwinding is best effort: tool,
// ptrace permission, symbols and unwind metadata may be unavailable.
func measureFailureStacks(path string) error {
	const bound = 2 << 20
	buf := make([]byte, bound)
	n := runtime.Stack(buf, true)
	goErr := os.WriteFile(path+"-go-stacks.txt", buf[:n], 0600)
	meta := map[string]any{"go_byte_limit": bound, "go_bytes": n, "go_possibly_truncated": n == bound, "go_write_error": fmt.Sprint(goErr), "native_limit": "best effort; no install/elevation; 3s, 2MiB; ptrace/symbol/unwind limitations"}
	if tool, err := exec.LookPath("gdb"); err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, tool, "--batch", "--nx", "-iex", "set auto-load off", "-ex", "set pagination off", "-ex", "thread apply all bt 64", "-p", strconv.Itoa(os.Getpid()))
		output := &measureBoundedOutput{remaining: bound}
		cmd.Stdout, cmd.Stderr = output, output
		cmd.WaitDelay = time.Second
		err := cmd.Run()
		meta["native_argv"], meta["native_error"], meta["native_truncated"] = cmd.Args, fmt.Sprint(err), output.truncated
		nativeErr := os.WriteFile(path+"-native-stacks.txt", output.Bytes(), 0600)
		meta["native_write_error"] = fmt.Sprint(nativeErr)
		goErr = errors.Join(goErr, nativeErr)
	} else {
		meta["native_status"] = "unavailable: gdb not installed; no native backtrace established"
	}
	return errors.Join(goErr, measureWrite(path+"-stacks.json", meta))
}

type measureBoundedOutput struct {
	bytes.Buffer
	remaining int
	truncated bool
}

func (w *measureBoundedOutput) Write(p []byte) (int, error) {
	n := len(p)
	if len(p) > w.remaining {
		p = p[:w.remaining]
		w.truncated = true
	}
	_, _ = w.Buffer.Write(p)
	w.remaining -= len(p)
	return n, nil
}

// Test-scope barriers exercise the actual parent/row protocol without a screen.
// No production ports, workload semantics or timing functions are substituted.
type measureControlBarrier struct {
	stage, path string
	attempted   chan struct{}
	once        sync.Once
}

func newMeasureControlBarrier(row measureRow, path string) *measureControlBarrier {
	stage := os.Getenv("RESPONSE_MEASURE_CONTROL_BARRIER")
	if stage == "" || row.Preflight || (os.Getenv("RESPONSE_MEASURE_COMBINED_CONTROL") != "1" && os.Getenv("RESPONSE_MEASURE_CONTROL_HELPER") != "row") {
		return nil
	}
	return &measureControlBarrier{stage: stage, path: path, attempted: make(chan struct{})}
}

func (b *measureControlBarrier) point(ctx context.Context, point string) {
	if b == nil {
		return
	}
	if point == "attempted" {
		b.once.Do(func() { close(b.attempted) })
		return
	}
	if point == "watchdog" && b.stage == "setup" {
		<-b.attempted
		return
	}
	if point != b.stage {
		return
	}
	if err := measureWrite(b.path+".ready", map[string]any{"stage": point, "pid": os.Getpid(), "parent_pid": os.Getppid()}); err != nil {
		panic(err)
	}
	fmt.Println("control barrier:", point)
	<-ctx.Done()
	if point != "setup" {
		select {}
	}
}

// This is an explicit, tiny test configuration of the real measurement parent.
// Python selects this test, which still runs the real preflight/command runner
// and launches TestResponseContinuationMeasure in a separate row process.
func TestResponseMeasureCombinedControl(t *testing.T) {
	if os.Getenv("RESPONSE_MEASURE_COMBINED_CONTROL") != "1" {
		t.Skip("combined control requires explicit helper configuration")
	}
	fmt.Println("control parent starting")
	TestResponseContinuationMeasure(t)
}

func measureControlRows(rows []measureRow) []measureRow {
	if os.Getenv("RESPONSE_MEASURE_COMBINED_CONTROL") == "1" {
		return []measureRow{{Seed: 26011, Lane: "native", Profile: "controlled", Workload: "run64", Concurrency: 1, Count: 3, Warmups: 1}}
	}
	return rows
}

func runMeasureRow(row measureRow, path string) (retErr error) {
	budget, err := measureCaptureBudget(*measureMode)
	if err != nil {
		return err
	}
	epoch := time.Now()
	for value, allowed := range map[string][]string{row.Lane: {"go", "native"}, row.Profile: {"controlled", "real-valkey-off", "real-valkey-on"}, row.Workload: measureWorkloads} {
		if _, err := measureSelect(value, allowed); err != nil {
			return err
		}
	}
	if (row.Concurrency != 1 && row.Concurrency != 8) || row.Count < 0 || row.Count > 5000 || row.Warmups < 0 || row.Warmups > 32 {
		return errors.New("invalid private row bounds")
	}
	res := measureResult{Budget: budget, Schema: 2, ThroughputScope: measureThroughputScope, Row: row, PID: os.Getpid(), UTC: epoch.UTC().Format(time.RFC3339Nano), Units: "all offsets/durations nanoseconds; memory bytes; absolute UTC RFC3339Nano", Method: "nearest-rank p95 over raw resource_ns-start_ns; closed-loop first steady slot start to last slot end including preparation/validation; no outlier removal", LoadBefore: measureLoad(), CPUAvailability: measureCPU(), RSSIntervalNS: int64(time.Millisecond)}
	defer func() {
		res.LoadAfter = measureLoad()
		if retErr != nil {
			res.Error = retErr.Error()
			if res.StopReason == "" {
				res.StopReason = "request_validation_or_capture_error"
			}
			// Save known observations before best-effort stacks consume grace.
			retErr = errors.Join(retErr, measureWrite(path, res))
			retErr = errors.Join(retErr, measureFailureStacks(path))
			return
		}
		retErr = errors.Join(retErr, measureWrite(path, res))
	}()
	if runtime.GOMAXPROCS(0) != 8 {
		return errors.New("rows require GOMAXPROCS=8")
	}
	// Expiry is a capture stop, not proof of a native hang or interruptible C calls.
	setupStart := time.Now()
	barrier := newMeasureControlBarrier(row, path)
	// SIGUSR1 requests a failure-only snapshot, never a graceful native join.
	// Register before setup so a stop cannot arrive with the default disposition.
	stopCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGUSR1)
	defer stopSignals()
	ctx, cancel := context.WithTimeout(stopCtx, budget.RowContext)
	defer cancel()
	total := 1 + row.Warmups*row.Concurrency + row.Count
	inputs := &measureInputs{live: map[*measureInput]struct{}{}}
	defer func() { res.InputRetention = inputs.snapshot() }()
	res.RequestRecordCapacity = total
	res.ResourceRecordCapacity = budget.ResourceRecords
	var completedSamples []atomic.Pointer[measureSample]
	var startedOffsets []atomic.Int64
	var resourceMu sync.Mutex
	var liveBoundaries []measureResource
	var rowWriteMu sync.Mutex
	watchdogBase := res
	watchdogBase.Resources = nil
	watchdogBase.Samples = append([]measureSample(nil), res.Samples...)
	watchdogDone := make(chan struct{})
	watchdogExited := make(chan struct{})
	var sampled []measureResource
	// A request is admitted before input preparation. The admitted operation
	// includes preparation, owner dispatch, validation and release. Work admitted
	// before STOP may still run; after the gate closes no new operation can start.
	// This linearizes observable cancellation, not the external signal instant.
	var admissionMu sync.Mutex
	stopped, snapshotOwned, finished := false, false, false
	ready := make(chan struct{})
	stop, done := make(chan struct{}), make(chan struct{})
	var sampleErr error
	fatalCapture := make(chan error, 1)
	// Final artifact ownership is handed off under the same gate as admission.
	// Cancellation already observed here must be diagnosed, even if setup returns
	// before the coordinator has been scheduled. Never overwrite its snapshot.
	defer func() {
		admissionMu.Lock()
		if ctx.Err() != nil || snapshotOwned {
			admissionMu.Unlock()
			<-watchdogExited // failure coordinator exits the process after bounded writes
			return
		}
		finished = true
		close(watchdogDone)
		admissionMu.Unlock()
		<-watchdogExited
	}()
	go func() {
		defer close(watchdogExited)
		barrier.point(ctx, "watchdog")
		timer := time.NewTimer(time.Hour)
		timer.Stop()
		var watchdog <-chan time.Time
		setupReady := (<-chan struct{})(ready)
		defer timer.Stop()
		// Request deadline observation cannot interrupt native work, but bounds capture.
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		reason, detail := "", ""
		for reason == "" {
			select {
			case <-watchdogDone:
				return
			case <-setupReady:
				setupReady = nil
				timer.Reset(budget.Watchdog)
				watchdog = timer.C
			case <-watchdog:
				reason, detail = "row_watchdog_expired", "in flight when row watchdog expired; request timeout/native hang not established"
			case e := <-fatalCapture:
				reason, detail = "resource_sampling_error", e.Error()
			case <-stopCtx.Done():
				reason, detail = "cooperative_stop", "package/provenance stop requested; unfinished request not joined"
			case <-ctx.Done():
				reason, detail = "row_context_expired", ctx.Err().Error()
				if stopCtx.Err() != nil {
					reason, detail = "cooperative_stop", "package/provenance stop requested; unfinished request not joined"
				}
			case <-tick.C:
				admissionMu.Lock()
				for j := range startedOffsets {
					if start := startedOffsets[j].Load(); start > 0 && (completedSamples[j].Load() == nil || completedSamples[j].Load().Resource == 0) && time.Since(epoch).Nanoseconds()-start >= int64(budget.Request) {
						reason, detail = "request_deadline_expired", fmt.Sprintf("request %d still in flight after request deadline; native hang not established", j)
						break
					}
				}
				admissionMu.Unlock()
			}
		}
		admissionMu.Lock()
		if finished {
			admissionMu.Unlock()
			return
		}
		stopped, snapshotOwned = true, true
		resourceMu.Lock()
		watchdogBase.Resources = append(watchdogBase.Resources, liveBoundaries...)
		watchdogBase.Resources = append(watchdogBase.Resources, sampled...)
		resourceMu.Unlock()
		if len(watchdogBase.Samples) == 0 {
			watchdogBase.Samples = make([]measureSample, total)
			for j := range watchdogBase.Samples {
				phase, worker := "first", 0
				if j > 0 && j <= row.Warmups*row.Concurrency {
					phase, worker = "warmup", (j-1)%row.Concurrency
				} else if j > 0 {
					phase, worker = "steady", (j-1-row.Warmups*row.Concurrency)%row.Concurrency
				}
				watchdogBase.Samples[j] = measureSample{ID: j, Worker: worker, Phase: phase, Result: "not_started"}
			}
		}
		measureFailureSamples(&watchdogBase, completedSamples, startedOffsets, reason)
		watchdogBase.InputRetention = inputs.snapshot()
		admissionMu.Unlock()
		watchdogBase.LoadAfter = measureLoad()
		watchdogBase.StopReason = reason
		watchdogBase.Error = "STOP: " + detail
		// All data belongs to this snapshot: no live res slice, scheduler join,
		// native teardown, callback reclamation or disk I/O on request owners.
		rowWriteMu.Lock()
		writeErr := measureWrite(path, watchdogBase)
		rowWriteMu.Unlock()
		if writeErr != nil {
			fmt.Fprintln(os.Stderr, "STOP: diagnostic row flush failed:", writeErr)
		}
		if err := measureFailureStacks(path); err != nil {
			fmt.Fprintln(os.Stderr, "STOP: diagnostic stacks write failed:", err)
		}
		os.Exit(2)
	}()
	profile := "controlled"
	if row.Profile != "controlled" {
		profile = "real-valkey"
	}
	ports, err := newMeasurePorts(ctx, profile, row.Profile == "real-valkey-on", os.Getenv("RESPONSE_MEASURE_VALKEY_ADDR"))
	if err != nil {
		return err
	}
	defer func() {
		admissionMu.Lock()
		closePorts := !snapshotOwned && ctx.Err() == nil
		admissionMu.Unlock()
		if closePorts {
			ports.Close()
		}
	}()
	stage := func(name string) error {
		r, err := measureReadResource(name, epoch)
		if err != nil {
			return err
		}
		if ports.client != nil {
			r.ValkeyMemory, err = ports.client.Do(ctx, ports.client.B().Info().Section("memory").Build()).ToString()
			if err != nil {
				return err
			}
		}
		resourceMu.Lock()
		res.Resources = append(res.Resources, r)
		liveBoundaries = append(liveBoundaries, r)
		resourceMu.Unlock()
		return nil
	}
	if err = stage("baseline"); err != nil {
		return err
	}
	if ports.client != nil {
		t := time.Now()
		err = ports.client.Do(ctx, ports.client.B().Ping().Build()).Error()
		res.ValkeyPingNS = time.Since(t).Nanoseconds()
		if err != nil {
			return err
		}
	}
	if ports.client != nil {
		for j := 0; j < total; j++ {
			if err = ports.prepareRun(ctx, measureInstance(row.Workload, j)); err != nil {
				return err
			}
			inputs.mu.Lock()
			inputs.stats.PrecreatedRuns++
			inputs.mu.Unlock()
		}
		ports.precreated = true
	}
	admissionMu.Lock()
	completedSamples = make([]atomic.Pointer[measureSample], total)
	startedOffsets = make([]atomic.Int64, total)
	res.Samples = make([]measureSample, total)
	for j := 0; j < total; j++ {
		phase, worker := "first", 0
		if j > 0 && j <= row.Warmups*row.Concurrency {
			phase = "warmup"
			worker = (j - 1) % row.Concurrency
		} else if j > 0 {
			phase = "steady"
			worker = (j - 1 - row.Warmups*row.Concurrency) % row.Concurrency
		}
		res.Samples[j] = measureSample{ID: j, Worker: worker, Phase: phase, Result: "not_started"}
	}
	admissionMu.Unlock()
	res.SetupNS = time.Since(setupStart).Nanoseconds()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	res.SetupHeap = mem.HeapAlloc
	if err = stage("ready"); err != nil {
		return err
	}
	barrier.point(ctx, "setup")
	// Persist planned IDs before owner timing. A Go watchdog snapshots completed
	// immutable samples if FFI/ports get stuck; no disk writes during timed work.
	rowWriteMu.Lock()
	admissionMu.Lock()
	initialWrite := !snapshotOwned
	if initialWrite {
		watchdogBase = res
		watchdogBase.Resources = nil
		watchdogBase.Samples = append([]measureSample(nil), res.Samples...)
	}
	admissionMu.Unlock()
	if initialWrite {
		err = measureWrite(path, res)
	}
	rowWriteMu.Unlock()
	if err != nil {
		return err
	}
	resourceMu.Lock()
	sampled = make([]measureResource, 0, budget.ResourceRecords)
	resourceMu.Unlock()
	close(ready)
	// Preallocated bounded resource sampling, with live failure snapshots.
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		fail := func(e error) {
			sampleErr = e
			admissionMu.Lock()
			stopped = true
			admissionMu.Unlock()
			fatalCapture <- e
		}
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				r, e := measureReadResource("sample", epoch)
				if e != nil {
					fail(e)
					return
				}
				resourceMu.Lock()
				if len(sampled) == cap(sampled) {
					resourceMu.Unlock()
					fail(errors.New("resource sample bound exceeded"))
					return
				}
				sampled = append(sampled, r)
				resourceMu.Unlock()
			}
		}
	}()
	samplerStopped := false
	defer func() {
		if !samplerStopped {
			close(stop)
			<-done
			resourceMu.Lock()
			res.Resources = append(res.Resources, sampled...)
			resourceMu.Unlock()
		}
		if sampleErr != nil {
			res.StopReason = "resource_sampling_error"
			retErr = errors.Join(retErr, sampleErr)
		}
	}()
	var runErr error
	var runErrOnce sync.Once
	run := func(index, worker int, phase string) bool {
		defer barrier.point(ctx, "attempted")
		admissionMu.Lock()
		if stopped || ctx.Err() != nil {
			stopped = true
			admissionMu.Unlock()
			runErrOnce.Do(func() { runErr = errors.New("STOP: request admission closed") })
			return false
		}
		slotStart := time.Since(epoch).Nanoseconds()
		admitted := res.Samples[index]
		admitted.SlotStart, admitted.Result = slotStart, "preparation"
		res.Samples[index] = admitted
		completedSamples[index].Store(&admitted)
		admissionMu.Unlock()
		err := func() error {
			if barrier != nil {
				fmt.Println("control input preparation", index)
			}
			barrier.point(ctx, "preparation")
			in, err := ports.input(ctx, row.Workload, index)
			if err != nil {
				return err
			}
			admissionMu.Lock()
			inputs.retain(in)
			prepared := admitted
			prepared.Result = "prepared"
			completedSamples[index].Store(&prepared)
			admissionMu.Unlock()
			barrier.point(ctx, "prepared")
			validated := false
			defer func() { admissionMu.Lock(); inputs.release(in, validated); admissionMu.Unlock() }()
			in.startOffset = &startedOffsets[index]
			callCtx, callCancel := context.WithTimeout(ctx, budget.Request)
			defer callCancel()
			in.ctx = measureDeadlineContext(in.ctx, callCtx)
			in.cleanup = callCancel
			sample, result := measureRequest(callCtx, row.Lane, in, epoch, index, worker, phase)
			sample.SlotStart = slotStart
			res.Samples[index] = sample
			admissionMu.Lock()
			completedSamples[index].Store(&sample)
			admissionMu.Unlock()
			if sample.Timeout {
				return context.DeadlineExceeded
			}
			if !sample.Complete {
				return errors.New("incomplete drain")
			}
			if sample.Result == "unexpected_error" {
				return fmt.Errorf("unexpected result: %s", sample.Error)
			}
			// Validation is outside request timing but inside the closed-loop slot.
			if err := ports.validate(ctx, in, row.Workload, result); err != nil {
				return err
			}
			validated = true
			return nil
		}()
		// The helper has returned: no payload, error wrapper or input closure is
		// retained by samples or by the next call on this worker.
		res.Samples[index].SlotEnd = time.Since(epoch).Nanoseconds()
		sample := res.Samples[index]
		admissionMu.Lock()
		completedSamples[index].Store(&sample)
		admissionMu.Unlock()
		if index == 1 {
			barrier.point(ctx, "published")
		}
		if err != nil {
			runErrOnce.Do(func() {
				runErr = fmt.Errorf("request %d: %w", index, err)
				admissionMu.Lock()
				stopped = true
				admissionMu.Unlock()
				if errors.Is(err, context.DeadlineExceeded) {
					res.StopReason = "request_deadline_expired"
					if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						res.StopReason = "row_context_expired"
					}
				}
			})
		}
		return err == nil
	}
	run(0, 0, "first")
	if runErr != nil {
		return runErr
	}
	if !res.Samples[0].Complete || res.Samples[0].Timeout {
		return errors.New("first response did not drain")
	}
	// No sibling barriers: each worker owns a closed-loop slot through cleanup.
	batch := func(begin, count int, phase string) {
		var wg sync.WaitGroup
		for worker := 0; worker < row.Concurrency; worker++ {
			wg.Add(1)
			go func(worker int) {
				defer wg.Done()
				for j := worker; j < count; j += row.Concurrency {
					if !run(begin+j, worker, phase) {
						return
					}
				}
			}(worker)
		}
		wg.Wait()
	}
	batch(1, row.Warmups*row.Concurrency, "warmup")
	if runErr != nil {
		return runErr
	}
	if err = stage("warmup"); err != nil {
		return err
	}
	batch(1+row.Warmups*row.Concurrency, row.Count, "steady")
	if runErr != nil {
		return runErr
	}
	close(stop)
	<-done
	samplerStopped = true
	resourceMu.Lock()
	res.Resources = append(res.Resources, sampled...)
	resourceMu.Unlock()
	if sampleErr != nil {
		return sampleErr
	}
	if err = ctx.Err(); err != nil {
		res.StopReason = "row_context_expired"
		return err
	}
	if err = stage("end"); err != nil {
		return err
	}
	res.RSSStatus = "INCONCLUSIVE"
	// Minimum coverage is a necessary condition, not proof of catching peaks.
	if row.Count > 0 {
		first := int64(1<<63 - 1)
		last := int64(0)
		for _, s := range res.Samples {
			if s.Phase == "steady" {
				first = min(first, s.SlotStart)
				last = max(last, s.SlotEnd)
			}
		}
		observations := 0
		lastObservation := first
		maxGap := int64(0)
		for _, r := range sampled {
			if r.Offset >= first && r.Offset <= last {
				observations++
				maxGap = max(maxGap, r.Offset-lastObservation)
				lastObservation = r.Offset
			}
		}
		maxGap = max(maxGap, last-lastObservation)
		if observations >= 10 && last-first >= 10*res.RSSIntervalNS && maxGap <= 5*res.RSSIntervalNS {
			res.RSSStatus = "SAMPLED"
		}

	}
	res.InputRetention = inputs.snapshot()
	if res.InputRetention.PeakInputs > row.Concurrency || res.InputRetention.Remaining != 0 || res.InputRetention.Validated != total || res.InputRetention.Released != total {
		return fmt.Errorf("input retention accounting: %+v", res.InputRetention)
	}
	if err = ports.validateRow(ctx, row.Workload, total); err != nil {
		return err
	}
	if err = stage("validated"); err != nil {
		return err
	}
	ports.Close()
	ports.client = nil
	ports.readClient = nil
	return stage("after_close")
}

// Retain span/value context while using the equally timed request deadline.
// Deadline context is constructed outside the owner start in both lanes.
type measureJoinedContext struct {
	context.Context
	values context.Context
}

func (c measureJoinedContext) Value(key any) any { return c.values.Value(key) }
func measureDeadlineContext(values, deadline context.Context) context.Context {
	return measureJoinedContext{Context: deadline, values: values}
}

type measureChild struct {
	row  measureRow
	name string
}

func measureChildren(rows []measureRow) []measureChild {
	children := []measureChild{}
	seen := map[string]bool{}
	for _, r := range rows {
		key := r.Profile + "/" + r.Workload + "/" + r.Lane
		if seen[key] {
			continue
		}
		seen[key] = true
		r.Preflight, r.FirstOnly = true, true
		r.Concurrency, r.Count, r.Warmups, r.Pair = 1, 0, 0, 0
		r.Order = len(children)
		children = append(children, measureChild{r, fmt.Sprintf("port-preflight-%03d", len(children))})
	}
	for j, r := range rows {
		children = append(children, measureChild{r, fmt.Sprintf("row-%03d", j)})
	}
	return children
}

// Stream output as it arrives and record STARTED before launch. Hiding *os.File
// from exec makes its copying goroutine return log write failures from Wait.
// This is capture control, outside owner request timing.
const measureCommandGrace = 5 * time.Second

type measureLogWriter struct {
	io.Writer
	mu  sync.Mutex
	err error
}

func (w *measureLogWriter) Write(p []byte) (int, error) {
	n, err := w.Writer.Write(p)
	if n != len(p) && err == nil {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.mu.Lock()
		w.err = errors.Join(w.err, err)
		w.mu.Unlock()
	}
	return n, err
}

func measureRunCommand(cmd *exec.Cmd, base string) error {
	start := time.Now()
	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	record := map[string]any{
		"argv": cmd.Args, "cwd": cwd,
		"start_utc": start.UTC().Format(time.RFC3339Nano),
		"status":    "STARTED", "error": "child incomplete; diagnostic flush unconfirmed",
	}
	overrides := []string{}
	for _, value := range cmd.Env {
		if strings.HasPrefix(value, "GOMAXPROCS=") || strings.HasPrefix(value, "RESPONSE_MEASURE_CHILD=") || strings.HasPrefix(value, "RESPONSE_MEASURE_ROW_PATH=") {
			overrides = append(overrides, value)
		}
	}
	record["env_overrides"] = overrides
	path := base + "-command.json"
	if err := measureWrite(path, record); err != nil {
		return fmt.Errorf("initial command ledger: %w", err)
	}
	log, err := os.OpenFile(base+".log", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fmt.Errorf("child log open: %w", err)
	}
	writer := &measureLogWriter{Writer: log}
	cmd.Stdout, cmd.Stderr = writer, writer
	// CommandContext already owns the timeout. Request a snapshot first; Wait
	// escalates after a bounded grace if native work or diagnostics cannot exit.
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGUSR1) }
	cmd.WaitDelay = measureCommandGrace
	runErr := cmd.Run()
	// Wait can prefer a child exit error (e.g. SIGPIPE) over its copy error.
	// Retain the actual write error independently so neither is hidden.
	writer.mu.Lock()
	logErr := errors.Join(writer.err, log.Sync(), log.Close())
	writer.mu.Unlock()
	record["status"] = "INCOMPLETE"
	if runErr == nil && logErr == nil {
		record["status"] = "COMPLETE"
	}
	record["error"] = fmt.Sprint(errors.Join(runErr, logErr))
	record["end_utc"] = time.Now().UTC().Format(time.RFC3339Nano)
	record["launch_setup_run_ns"] = time.Since(start).Nanoseconds()
	if runErr != nil {
		record["flush_status"] = "unconfirmed; inspect row/stacks; hard kill or OS failure may lose unpublished observations"
	}
	ledgerErr := measureWrite(path, record)
	return errors.Join(runErr, logErr, ledgerErr)
}

func TestResponseContinuationMeasure(t *testing.T) {
	packageStart := time.Now()
	if *measureMode == "screen" {
		// Defaults for every other mode remain the original full declaration.
		defaults := map[string]string{"lanes": "go,native", "profiles": "controlled", "workloads": "run64,mixed8,run64k", "concurrency": "1"}
		flag.Visit(func(f *flag.Flag) { delete(defaults, strings.TrimPrefix(f.Name, "response-measure-")) })
		for key, value := range defaults {
			if err := flag.Set("response-measure-"+key, value); err != nil {
				t.Fatal(err)
			}
		}
		if *measurePackageRemaining <= 0 || *measurePackageRemaining > 15*time.Minute || runtime.GOMAXPROCS(0) != 8 {
			t.Fatal("screen requires GOMAXPROCS=8 and positive remaining package budget <=15m")
		}
	}
	if raw := os.Getenv("RESPONSE_MEASURE_CHILD"); raw != "" {
		if *measureMode == "diagnostic" {
			t.Fatal("diagnostic disallows private child marker")
		}
		var row measureRow
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			t.Fatal(err)
		}
		if *measureMode == "screen" {
			rows, err := measureRows("screen", *measureLaneSelection, *measureProfileSelection, *measureWorkloadSelection, *measureConcurrencySelection)
			valid := false
			for _, child := range measureChildren(measureControlRows(rows)) {
				valid = valid || child.row == row
			}
			if err != nil || !valid || *measureDeclareOnly {
				t.Fatal("invalid private screen child")
			}
		}
		if err := runMeasureRow(row, os.Getenv("RESPONSE_MEASURE_ROW_PATH")); err != nil {
			t.Fatal(err)
		}
		return
	}
	if *measureMode == "" {
		t.Skip("explicit measurement mode required")
	}
	if *measureDeclareOnly && *measureMode != "screen" {
		t.Fatal("declare-only requires screen mode")
	}
	packageCtx, stopParent := signal.NotifyContext(context.Background(), syscall.SIGUSR1)
	defer stopParent()
	if *measureMode == "screen" {
		var cancel context.CancelFunc
		packageCtx, cancel = context.WithDeadline(packageCtx, packageStart.Add(*measurePackageRemaining))
		defer cancel()
	}
	rows, err := measureRows(*measureMode, *measureLaneSelection, *measureProfileSelection, *measureWorkloadSelection, *measureConcurrencySelection)
	rows = measureControlRows(rows)
	if err != nil {
		t.Fatal(err)
	}
	if *measureOut == "" {
		t.Fatal("response-measure-out is required")
	}
	if os.Getenv("RESPONSE_MEASURE_CHILD") != "" {
		t.Fatal("parent cannot inherit a child marker")
	}
	if err = os.Mkdir(*measureOut, 0700); err != nil {
		t.Fatal(err)
	}
	if err = measureWrite(filepath.Join(*measureOut, "selection.json"), rows); err != nil {
		t.Fatal(err)
	}
	if *measureMode == "plan" || *measureDeclareOnly {
		return
	}
	if *measureMode == "diagnostic" {
		if runtime.GOMAXPROCS(0) != 8 {
			t.Fatal("diagnostic requires GOMAXPROCS=8")
		}
		if err = runMeasureRow(rows[0], filepath.Join(*measureOut, "row-000.json")); err != nil {
			t.Fatal(err)
		}
		return
	}
	// Preflight is a distinct process before ALL rows. It retains the literal,
	// poison, admission, cancellation and lifetime suite (no Valkey helper setup).
	preflightCtx, preflightCancel := context.WithCancel(packageCtx)
	if *measureMode == "screen" {
		preflightCancel()
		preflightCtx, preflightCancel = context.WithTimeout(packageCtx, 90*time.Second-measureCommandGrace)
	}
	defer preflightCancel()
	preflight := exec.CommandContext(preflightCtx, os.Args[0], "-test.run=^TestResponseContinuation(Contract|Integrated(Contract|RejectsWholeBatch|SiblingProgressAndJoin|Close.*|RecoveredPanic.*|Poison.*|RetainsFullDescriptor|ReleasePort|Activation|AsyncPanicSemantics))$", "-test.timeout=90s")
	preflight.Env = append(os.Environ(), "GOMAXPROCS=8")
	if err := measureRunCommand(preflight, filepath.Join(*measureOut, "preflight")); err != nil {
		t.Fatalf("STOP: preflight: %v", err)
	}
	children := measureChildren(rows)
	for j, child := range children {
		row := child.row
		path := filepath.Join(*measureOut, child.name+".json")
		raw, _ := json.Marshal(row)
		// Preserve planned contaminated rows even if the OS watchdog must kill a
		// stuck native call. Missing request completion is STOP, never filtered.
		if err := measureWrite(path, map[string]any{"row": row, "error": "child incomplete", "status": "STARTED"}); err != nil {
			t.Fatal("STOP: initial row write:", err)
		}
		budget, _ := measureCaptureBudget(*measureMode)
		// Reserve command shutdown within the existing outer bound.
		ctx, cancel := context.WithTimeout(packageCtx, budget.OuterKill-measureCommandGrace)
		if err := ctx.Err(); err != nil {
			cancel()
			t.Fatalf("STOP: package budget expired before child %d; retained %s", j, path)
		}
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestResponseContinuationMeasure$", "-test.timeout="+budget.TestTimeout.String())
		if *measureMode == "screen" {
			deadline, _ := packageCtx.Deadline()
			cmd.Args = append(cmd.Args, "-response-measure-mode=screen", "-response-measure-package-remaining="+time.Until(deadline).String())
		}
		cmd.Env = append(os.Environ(), "GOMAXPROCS=8", "RESPONSE_MEASURE_CHILD="+string(raw), "RESPONSE_MEASURE_ROW_PATH="+path)
		err = measureRunCommand(cmd, strings.TrimSuffix(path, ".json"))
		cancel()
		if err != nil {
			t.Fatalf("STOP row %d (%+v): %v; retained %s", j, row, err, path)
		}
	}
}

// Authoring gate: these distinguish independent clock and resource contracts
// from functional parity. No production hooks; a real callback stays blocked
// after owner return and cleanup remains part of closed-loop reuse.
type measureBlockedAsync struct {
	*measureAsyncListener
	entered, release chan struct{}
}

func (l *measureBlockedAsync) OnStepScheduled(ctx context.Context, md sv2.Metadata, item queue.Item, name *string) {
	close(l.entered)
	<-l.release
	l.measureAsyncListener.OnStepScheduled(ctx, md, item, name)
}
func TestResponseMeasureCompletionBoundaries(t *testing.T) {
	for _, lane := range []string{"go", "native"} {
		t.Run(lane, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			p, err := newMeasurePorts(ctx, "controlled", false, "")
			if err != nil {
				t.Fatal(err)
			}
			defer p.Close()
			requestCtx, requestCancel := context.WithCancel(ctx)
			defer requestCancel()
			in, err := p.input(requestCtx, "run64", 0)
			if err != nil {
				t.Fatal(err)
			}
			listener := &measureBlockedAsync{measureAsyncListener: &measureAsyncListener{c: in.callbacks}, entered: make(chan struct{}), release: make(chan struct{})}
			in.e.lifecycles = []execution.LifecycleListener{listener}
			cleanupEntered, cleanupRelease := make(chan struct{}), make(chan struct{})
			in.cleanup = func() { close(cleanupEntered); <-cleanupRelease; requestCancel() }
			var callbackOnce, cleanupOnce sync.Once
			releaseCallback := func() { callbackOnce.Do(func() { close(listener.release) }) }
			releaseCleanup := func() { cleanupOnce.Do(func() { close(cleanupRelease) }) }
			defer releaseCallback()
			defer releaseCleanup()
			epoch := time.Now()
			result := make(chan measureSample, 1)
			errs := make(chan error, 1)
			go func() {
				sample, err := measureRequest(requestCtx, lane, in, epoch, 0, 0, "regression")
				errs <- err
				result <- sample
			}()
			select {
			case <-listener.entered:
			case <-ctx.Done():
				t.Fatal("callback not launched")
			}
			select {
			case <-cleanupEntered:
				t.Fatal("cleanup began before callback delivery")
			case <-time.After(5 * time.Millisecond):
			}
			deliveredRelease := time.Since(epoch).Nanoseconds()
			releaseCallback()
			select {
			case <-cleanupEntered:
			case <-ctx.Done():
				t.Fatal("cleanup never began")
			}
			select {
			case <-result:
				t.Fatal("slot reusable before cleanup returned")
			case <-time.After(5 * time.Millisecond):
			}
			resourceRelease := time.Since(epoch).Nanoseconds()
			releaseCleanup()
			select {
			case sample := <-result:
				if err := <-errs; err != nil {
					t.Fatal(err)
				}
				if sample.Timeout {
					t.Fatal("own cleanup cancellation counted as timeout")
				}
				if sample.Delivered < deliveredRelease || sample.Resource < resourceRelease || sample.Owner > sample.Delivered || sample.Delivered > sample.Resource {
					t.Fatalf("early completion timestamps: %+v delivery release=%d cleanup release=%d", sample, deliveredRelease, resourceRelease)
				}
			case <-ctx.Done():
				t.Fatal("measurement stuck")
			}
		})
	}
}

// Authoring gate: actual row execution must bound live inputs as sample count
// grows; parity and completion tests do not observe retention between requests.
// No OS RSS threshold or production seam is needed for this ownership contract.
func TestResponseMeasureInputRetention(t *testing.T) {
	previous := runtime.GOMAXPROCS(8)
	defer runtime.GOMAXPROCS(previous)
	for _, lane := range []string{"go", "native"} {
		for _, concurrency := range []int{1, 8} {
			for _, count := range []int{8, 80} {
				t.Run(fmt.Sprintf("%s/C%d/N%d", lane, concurrency, count), func(t *testing.T) {
					row := measureRow{Lane: lane, Profile: "controlled", Workload: "run64k", Concurrency: concurrency, Warmups: 2, Count: count}
					path := filepath.Join(t.TempDir(), "row.json")
					if err := runMeasureRow(row, path); err != nil {
						t.Fatal(err)
					}
					data, err := os.ReadFile(path)
					if err != nil {
						t.Fatal(err)
					}
					var result measureResult
					if err := json.Unmarshal(data, &result); err != nil {
						t.Fatal(err)
					}
					r := result.InputRetention
					total := 1 + row.Warmups*concurrency + count
					if r.Prepared != total || r.Validated != total || r.Released != total || r.Remaining != 0 {
						t.Fatalf("unaccounted input lifecycle: %+v", r)
					}
					if r.PeakInputs < 1 || r.PeakInputs > concurrency || r.PeakPayloadBytes < 65527 || r.PeakPayloadBytes > concurrency*65536 {
						t.Fatalf("sample-count-dependent input retention: %+v", r)
					}
					for _, sample := range result.Samples {
						if sample.SlotStart <= 0 || sample.SlotStart > sample.Start || sample.SlotEnd < sample.Resource || !sample.Complete {
							t.Fatalf("incorrect setup/validation boundary: %+v", sample)
						}
					}
					t.Logf("N=%d C=%d total=%d retention=%+v setup_heap_bytes=%d", count, concurrency, total, r, result.SetupHeap)
				})
			}
		}
	}
}

func TestResponseMeasureSelectors(t *testing.T) {
	for _, bad := range []string{"", "native,go,go", "Native", "ffi", "go,"} {
		if _, err := measureSelect(bad, []string{"go", "native"}); err == nil {
			t.Fatalf("accepted selector %q", bad)
		}
	}
	if _, err := newMeasurePorts(context.Background(), "real-valkey", false, ""); err == nil {
		t.Fatal("missing real Valkey silently substituted")
	}
	rows, err := measureRows("plan", "go,native", "controlled,real-valkey-off,real-valkey-on", strings.Join(measureWorkloads, ","), "1,8")
	if err != nil {
		t.Fatal(err)
	}
	steady, total, first := 0, 0, 0
	for _, r := range rows {
		if r.FirstOnly {
			first++
		} else {
			steady++
			total += r.Count
		}
	}
	if steady != 180 || total != 360000 || first != 300 {
		t.Fatalf("declared campaign lost work: rows=%d responses=%d first=%d", steady, total, first)
	}
}

// Capture-control contracts: exact authorization, unchanged campaign budget,
// and truthful failure artifacts. These do not execute measurement rows or add
// production seams; functional/boundary behavior has its existing owners.
func TestResponseMeasureCaptureControl(t *testing.T) {
	if helper := os.Getenv("RESPONSE_MEASURE_CONTROL_HELPER"); helper != "" {
		path := os.Getenv("RESPONSE_MEASURE_CONTROL_PATH")
		fmt.Println("control child output before measurements")
		switch helper {
		case "row":
			if err := runMeasureRow(measureRow{Seed: 26011, Lane: "native", Profile: "controlled", Workload: "run64", Concurrency: 1, Count: 3, Warmups: 1}, path); err != nil {
				t.Fatal(err)
			}
		case "unresponsive":
			signal.Ignore(syscall.SIGUSR1)
			if err := os.WriteFile(path+".ready", []byte("ready"), 0600); err != nil {
				t.Fatal(err)
			}
			for {
				time.Sleep(time.Second)
			}
		case "final-ledger":
			if err := os.Mkdir(path+"-command.json.tmp", 0700); err != nil {
				t.Fatal(err)
			}
		case "large-output":
			fmt.Print(strings.Repeat("x", 8192))
		case "log-write-failure":
			// Isolated OS limit: actual temp-file write fails after STARTED.
			signal.Ignore(syscall.SIGXFSZ)
			if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: 4096, Max: 4096}); err != nil {
				t.Fatal(err)
			}
			cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^TestResponseMeasureCaptureControl$")
			cmd.Env = append(os.Environ(), "RESPONSE_MEASURE_CONTROL_HELPER=large-output")
			if err := measureRunCommand(cmd, path); !errors.Is(err, syscall.EFBIG) {
				t.Fatalf("wrong log write failure: %v", err)
			}
		}
		return
	}
	// Actual row signal flushing owns the in-memory publication/lifetime risk;
	// Python's process control regression separately owns expiry/provenance seals.
	t.Run("signal preserves published row samples and streaming output", func(t *testing.T) {
		for _, failFlush := range []bool{false, true} {
			t.Run(fmt.Sprint(failFlush), func(t *testing.T) {
				base := filepath.Join(t.TempDir(), "row")
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestResponseMeasureCaptureControl$", "-test.timeout=15s")
				cmd.Env = append(os.Environ(), "GOMAXPROCS=8", "RESPONSE_MEASURE_CONTROL_HELPER=row", "RESPONSE_MEASURE_CONTROL_BARRIER=published", "RESPONSE_MEASURE_CONTROL_PATH="+base+".json")
				observed := make(chan error, 1)
				go func() {
					defer cancel()
					for {
						if _, err := os.Stat(base + ".json.ready"); err == nil {
							break
						}
						select {
						case <-ctx.Done():
							observed <- ctx.Err()
							return
						case <-time.After(5 * time.Millisecond):
						}
					}
					var err error
					if failFlush {
						err = os.Mkdir(base+".json.tmp", 0700)
					}
					observed <- err
				}()
				err := measureRunCommand(cmd, base)
				if waitErr := <-observed; waitErr != nil {
					t.Fatal(waitErr)
				}
				if err == nil {
					t.Fatal("stopped child accepted")
				}
				output, readErr := os.ReadFile(base + ".log")
				if readErr != nil || !bytes.Contains(output, []byte("control child output before measurements")) {
					t.Fatalf("lost output: %s %v", output, readErr)
				}
				data, readErr := os.ReadFile(base + ".json")
				if readErr != nil {
					t.Fatal(readErr)
				}
				var result measureResult
				if err := json.Unmarshal(data, &result); err != nil {
					t.Fatal(err)
				}
				if failFlush {
					if !bytes.Contains(output, []byte("diagnostic row flush failed")) {
						t.Fatalf("silent flush failure: %s", output)
					}
				} else {
					complete := 0
					for _, sample := range result.Samples {
						if sample.Complete && sample.SlotEnd > 0 {
							complete++
						}
					}
					if complete < 2 || complete >= len(result.Samples) || result.StopReason != "cooperative_stop" || result.Error == "" || len(result.Resources) < 3 {
						t.Fatalf("lost partial observations: completed=%d/%d stop=%s resources=%d error=%s", complete, len(result.Samples), result.StopReason, len(result.Resources), result.Error)
					}
					if _, err := os.Stat(base + ".json-go-stacks.txt"); err != nil {
						t.Fatal(err)
					}
				}
				ledger, err := os.ReadFile(base + "-command.json")
				if err != nil {
					t.Fatal(err)
				}
				var command map[string]any
				if err := json.Unmarshal(ledger, &command); err != nil {
					t.Fatal(err)
				}
				if command["status"] != "INCOMPLETE" || command["end_utc"] == nil {
					t.Fatalf("lost final command evidence: %s", ledger)
				}
			})
		}
	})
	t.Run("unresponsive child has bounded hard kill and incomplete evidence", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "child")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestResponseMeasureCaptureControl$", "-test.timeout=30s")
		cmd.Env = append(os.Environ(), "RESPONSE_MEASURE_CONTROL_HELPER=unresponsive", "RESPONSE_MEASURE_CONTROL_PATH="+base)
		ready := make(chan bool, 1)
		go func() {
			defer cancel()
			for {
				if _, err := os.Stat(base + ".ready"); err == nil {
					ready <- true
					return
				}
				select {
				case <-ctx.Done():
					ready <- false
					return
				case <-time.After(5 * time.Millisecond):
				}
			}
		}()
		start := time.Now()
		err := measureRunCommand(cmd, base)
		if !<-ready || err == nil || time.Since(start) > 8*time.Second {
			t.Fatalf("unbounded/accepted child: %v %s", err, time.Since(start))
		}
		data, err := os.ReadFile(base + "-command.json")
		if err != nil {
			t.Fatal(err)
		}
		var record map[string]any
		if err := json.Unmarshal(data, &record); err != nil {
			t.Fatal(err)
		}
		if record["status"] != "INCOMPLETE" || record["flush_status"] == nil {
			t.Fatalf("hard kill concealed: %s", data)
		}
	})
	t.Run("artifact failures are fatal at command boundary", func(t *testing.T) {
		for _, failure := range []string{"initial-ledger", "log-open", "final-ledger"} {
			t.Run(failure, func(t *testing.T) {
				base := filepath.Join(t.TempDir(), "child")
				switch failure {
				case "initial-ledger":
					if err := os.Mkdir(base+"-command.json.tmp", 0700); err != nil {
						t.Fatal(err)
					}
				case "log-open":
					if err := os.Mkdir(base+".log", 0700); err != nil {
						t.Fatal(err)
					}
				}
				cmd := exec.CommandContext(context.Background(), os.Args[0], "-test.run=^TestResponseMeasureCaptureControl$")
				cmd.Env = append(os.Environ(), "RESPONSE_MEASURE_CONTROL_HELPER=final-ledger", "RESPONSE_MEASURE_CONTROL_PATH="+base)
				err := measureRunCommand(cmd, base)
				expected := base + "-command.json.tmp"
				if failure == "log-open" {
					expected = base + ".log"
				}
				if err == nil || !strings.Contains(err.Error(), expected) {
					t.Fatalf("wrong artifact failure: %v", err)
				}
				if failure != "initial-ledger" {
					data, err := os.ReadFile(base + "-command.json")
					if err != nil {
						t.Fatal(err)
					}
					var record map[string]any
					if err := json.Unmarshal(data, &record); err != nil {
						t.Fatal(err)
					}
					if record["status"] != "STARTED" {
						t.Fatalf("failed write lost incomplete ledger: %s", data)
					}
				}
			})
		}
	})

	t.Run("initial row artifact failure dispatches no request", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "row.json")
		if err := os.Mkdir(path+".tmp", 0700); err != nil {
			t.Fatal(err)
		}
		err := runMeasureRow(measureRow{Seed: 26011, Lane: "go", Profile: "controlled", Workload: "run64", Concurrency: 1}, path)
		if err == nil || !strings.Contains(err.Error(), path+".tmp") {
			t.Fatalf("wrong initial row failure: %v", err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("failed initial record accepted: %v", err)
		}
	})
	t.Run("streaming log write failure cannot accept successful child", func(t *testing.T) {
		base := filepath.Join(t.TempDir(), "child")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestResponseMeasureCaptureControl$", "-test.timeout=8s")
		cmd.Env = append(os.Environ(), "RESPONSE_MEASURE_CONTROL_HELPER=log-write-failure", "RESPONSE_MEASURE_CONTROL_PATH="+base)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("log failure control: %v %s", err, output)
		}
		data, err := os.ReadFile(base + "-command.json")
		if err != nil {
			t.Fatal(err)
		}
		var record map[string]any
		if err := json.Unmarshal(data, &record); err != nil {
			t.Fatal(err)
		}
		if record["status"] != "INCOMPLETE" || !strings.Contains(fmt.Sprint(record["error"]), "file too large") {
			t.Fatalf("log write failure accepted: %s", data)
		}
	})
	t.Run("fixed screen declaration and adjacent seed pairs", func(t *testing.T) {
		rows, err := measureRows("screen", "go,native", "controlled", "run64,mixed8,run64k", "1")
		if err != nil || len(rows) != 18 {
			t.Fatalf("screen declaration: %v %v", rows, err)
		}
		again, _ := measureRows("screen", "go,native", "controlled", "run64,mixed8,run64k", "1")
		a, _ := json.Marshal(rows)
		b, _ := json.Marshal(again)
		if !bytes.Equal(a, b) {
			t.Fatal("screen order is not reproducible before execution")
		}
		seen := map[string]bool{}
		steady, total, nativeFirst := 0, 0, 0
		for j, r := range rows {
			key := fmt.Sprintf("%d/%s/%s", r.Seed, r.Workload, r.Lane)
			if seen[key] || r.Order != j || r.Seed != 26011+j/6 || r.Profile != "controlled" || r.Concurrency != 1 || r.FirstOnly || r.Preflight || r.Count != 500 || r.Warmups != 32 {
				t.Fatalf("wrong fixed screen work: %+v", r)
			}
			seen[key] = true
			steady += r.Count
			total += 1 + r.Warmups + r.Count
			if j%2 == 0 {
				next := rows[j+1]
				if r.Seed != next.Seed || r.Workload != next.Workload || r.Lane == next.Lane {
					t.Fatalf("unpaired adjacent lanes: %+v %+v", r, next)
				}
				if r.Lane == "native" {
					nativeFirst++
				}
			}
		}
		children := measureChildren(rows)
		if steady != 9000 || total != 9594 || len(children) != 24 || nativeFirst == 0 || nativeFirst == 9 {
			t.Fatalf("screen counts/order: steady=%d total=%d children=%d nativeFirst=%d", steady, total, len(children), nativeFirst)
		}
		for j, child := range children {
			if j < 6 && (!child.row.Preflight || 1+child.row.Count+child.row.Warmups != 1 || !child.row.FirstOnly) {
				t.Fatalf("port preflight expanded: %+v", child)
			}
			if j >= 6 && child.row != rows[j-6] {
				t.Fatalf("screen row changed after declaration: %+v", child)
			}
		}
		for _, selectors := range [][4]string{{"native", "controlled", "run64,mixed8,run64k", "1"}, {"go,native", "real-valkey-on", "run64,mixed8,run64k", "1"}, {"go,native", "controlled", "run64k", "1"}, {"go,native", "controlled", "run64,mixed8,run64k", "1,8"}} {
			if _, err := measureRows("screen", selectors[0], selectors[1], selectors[2], selectors[3]); err == nil {
				t.Fatalf("changed screen accepted: %v", selectors)
			}
		}
		budget, err := measureCaptureBudget("screen")
		if err != nil || budget.Watchdog != 60*time.Second || budget.RowContext != 90*time.Second || budget.TestTimeout != 110*time.Second || budget.OuterKill != 120*time.Second || budget.Request != 20*time.Second || budget.ResourceRecords != 120000 {
			t.Fatalf("uncoordinated screen budget: %+v %v", budget, err)
		}
	})
	t.Run("bounded authorization", func(t *testing.T) {
		rows, err := measureRows("diagnostic", "native", "controlled", "run64k", "1")
		if err != nil || len(rows) != 1 {
			t.Fatalf("diagnostic selection: %v %v", rows, err)
		}
		r := rows[0]
		if r.Seed != 26011 || r.Count != 5000 || r.Warmups != 32 || r.FirstOnly || r.Preflight || 1+r.Count+r.Warmups*r.Concurrency != 5033 {
			t.Fatalf("wrong authorized work: %+v", r)
		}
		for _, selectors := range [][4]string{{"go,native", "controlled", "run64k", "1"}, {"native", "real-valkey-on", "run64k", "1"}, {"native", "controlled", "run64", "1"}, {"native", "controlled", "run64k", "1,8"}} {
			if _, err := measureRows("diagnostic", selectors[0], selectors[1], selectors[2], selectors[3]); err == nil {
				t.Fatalf("expanded diagnostic accepted: %v", selectors)
			}
		}
		campaign, _ := measureCaptureBudget("campaign")
		diagnostic, _ := measureCaptureBudget("diagnostic")
		if campaign.Watchdog != 110*time.Second || campaign.RowContext != 120*time.Second || campaign.TestTimeout != 140*time.Second || campaign.OuterKill != 150*time.Second || campaign.ResourceRecords != 120000 {
			t.Fatalf("campaign budget changed: %+v", campaign)
		}
		if diagnostic.Watchdog != 210*time.Second || diagnostic.RowContext != 240*time.Second || diagnostic.TestTimeout != 260*time.Second || diagnostic.OuterKill != 270*time.Second || diagnostic.ResourceRecords != 240000 || diagnostic.Request != 20*time.Second || campaign.Request != diagnostic.Request {
			t.Fatalf("uncoordinated diagnostic budget: %+v", diagnostic)
		}
		if _, err := measureCaptureBudget("resume"); err == nil {
			t.Fatal("unknown budget accepted")
		}
	})
	t.Run("failure preserves observations without inventing request timeout", func(t *testing.T) {
		for _, reason := range []string{"row_watchdog_expired", "request_deadline_expired", "resource_sampling_error", "row_context_expired"} {
			completed := make([]atomic.Pointer[measureSample], 3)
			starts := make([]atomic.Int64, 3)
			closed := measureSample{ID: 0, Start: 10, Resource: 20, SlotEnd: 30, Complete: true, Result: "ok"}
			completed[0].Store(&closed)
			starts[1].Store(40)
			res := measureResult{Samples: []measureSample{{ID: 0}, {ID: 1}, {ID: 2, Result: "not_started"}}}
			measureFailureSamples(&res, completed, starts, reason)
			if res.Samples[0] != closed || res.Samples[1].Start != 40 || res.Samples[1].Complete || res.Samples[1].Timeout != (reason == "request_deadline_expired") || res.Samples[1].Result != "in_flight_at_"+reason || res.Samples[2].Result != "not_started" {
				t.Fatalf("misleading failure snapshot: %+v", res.Samples)
			}
		}
	})
	t.Run("bounded failure output", func(t *testing.T) {
		output := &measureBoundedOutput{remaining: 4}
		for _, s := range []string{"abc", "def", "ghi"} {
			if n, err := output.Write([]byte(s)); n != 3 || err != nil {
				t.Fatal(n, err)
			}
		}
		if output.String() != "abcd" || !output.truncated {
			t.Fatalf("unbounded/lost output: %+v", output)
		}
	})
}
