//go:build linux && response_continuation_spike && cgo

package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/inngest/inngest/pkg/execution"
	"github.com/inngest/inngest/pkg/execution/queue"
	sv2 "github.com/inngest/inngest/pkg/execution/state/v2"
)

var measureMode = flag.String("response-measure-mode", "", "plan, smoke, campaign or diagnostic; execution requires separate authorization")
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
	if mode == "diagnostic" {
		if lanes != "native" || profiles != "controlled" || workloads != "run64k" || concurrency != "1" {
			return nil, errors.New("diagnostic requires exact native/controlled/run64k/C1 selectors")
		}
		return []measureRow{{Seed: 26011, Lane: "native", Profile: "controlled", Workload: "run64k", Concurrency: 1, Count: 5000, Warmups: 32}}, nil
	}
	if mode != "smoke" && mode != "campaign" && mode != "plan" {
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
	if mode != "smoke" && (len(ls) != 2 || len(ps) != 3 || len(ws) != 5 || len(cs) != 2) {
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
					if p != "controlled" {
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
	return os.WriteFile(path, append(b, '\n'), 0600)
}

// Read only published immutable completions. Watchdog expiry never fabricates
// a request timeout; an observed request-deadline expiry is recorded separately.
func measureFailureSamples(res *measureResult, completed []atomic.Pointer[measureSample], starts []atomic.Int64, reason string) {
	for j := range completed {
		if sample := completed[j].Load(); sample != nil {
			res.Samples[j] = *sample
		} else if start := starts[j].Load(); start > 0 {
			res.Samples[j].Start = start
			res.Samples[j].Result = "in_flight_at_" + reason
			res.Samples[j].Timeout = reason == "request_deadline_expired"
		}
	}
}

// Failure-only, outside measurement. Native unwinding is best effort: tool,
// ptrace permission, symbols and unwind metadata may be unavailable.
func measureFailureStacks(path string) {
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
		meta["native_write_error"] = fmt.Sprint(os.WriteFile(path+"-native-stacks.txt", output.Bytes(), 0600))
	} else {
		meta["native_status"] = "unavailable: gdb not installed; no native backtrace established"
	}
	_ = measureWrite(path+"-stacks.json", meta)
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
			measureFailureStacks(path)
		}
		retErr = errors.Join(retErr, measureWrite(path, res))
	}()
	if runtime.GOMAXPROCS(0) != 8 {
		return errors.New("rows require GOMAXPROCS=8")
	}
	// Expiry is a capture stop, not proof of a native hang or interruptible C calls.
	setupStart := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), budget.RowContext)
	defer cancel()
	profile := "controlled"
	if row.Profile != "controlled" {
		profile = "real-valkey"
	}
	ports, err := newMeasurePorts(ctx, profile, row.Profile == "real-valkey-on", os.Getenv("RESPONSE_MEASURE_VALKEY_ADDR"))
	if err != nil {
		return err
	}
	defer ports.Close()
	var resourceMu sync.Mutex
	var liveBoundaries []measureResource
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
	total := 1 + row.Warmups*row.Concurrency + row.Count
	inputs := &measureInputs{live: map[*measureInput]struct{}{}}
	defer func() { res.InputRetention = inputs.snapshot() }()
	res.RequestRecordCapacity = total
	res.ResourceRecordCapacity = budget.ResourceRecords
	if ports.client != nil {
		for j := 0; j < total; j++ {
			if err = ports.prepareRun(ctx, measureInstance(row.Workload, j)); err != nil {
				return err
			}
			inputs.stats.PrecreatedRuns++
		}
		ports.precreated = true
	}
	completedSamples := make([]atomic.Pointer[measureSample], total)
	startedOffsets := make([]atomic.Int64, total)
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
	res.SetupNS = time.Since(setupStart).Nanoseconds()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	res.SetupHeap = mem.HeapAlloc
	if err = stage("ready"); err != nil {
		return err
	}
	// Persist planned IDs before owner timing. A Go watchdog snapshots completed
	// immutable samples if FFI/ports get stuck; no disk writes during timed work.
	if err = measureWrite(path, res); err != nil {
		return err
	}
	watchdogBase := res
	watchdogBase.Resources = nil
	watchdogBase.Samples = append([]measureSample(nil), res.Samples...)
	watchdogDone := make(chan struct{})
	watchdogExited := make(chan struct{})
	defer func() { close(watchdogDone); <-watchdogExited }()
	sampled := make([]measureResource, 0, budget.ResourceRecords)
	var stopWork atomic.Bool
	stop, done := make(chan struct{}), make(chan struct{})
	var sampleErr error
	fatalCapture := make(chan error, 1)
	go func() {
		defer close(watchdogExited)
		timer := time.NewTimer(budget.Watchdog)
		defer timer.Stop()
		// Request deadline observation cannot interrupt native work, but bounds capture.
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		reason, detail := "", ""
		for reason == "" {
			select {
			case <-watchdogDone:
				return
			case <-timer.C:
				reason, detail = "row_watchdog_expired", "in flight when row watchdog expired; request timeout/native hang not established"
			case e := <-fatalCapture:
				reason, detail = "resource_sampling_error", e.Error()
			case <-ctx.Done():
				reason, detail = "row_context_expired", ctx.Err().Error()
			case <-tick.C:
				for j := range startedOffsets {
					if start := startedOffsets[j].Load(); start > 0 && completedSamples[j].Load() == nil && time.Since(epoch).Nanoseconds()-start >= int64(budget.Request) {
						reason, detail = "request_deadline_expired", fmt.Sprintf("request %d still in flight after request deadline; native hang not established", j)
						break
					}
				}
			}
		}
		stopWork.Store(true)
		resourceMu.Lock()
		watchdogBase.Resources = append(watchdogBase.Resources, liveBoundaries...)
		watchdogBase.Resources = append(watchdogBase.Resources, sampled...)
		resourceMu.Unlock()
		measureFailureSamples(&watchdogBase, completedSamples, startedOffsets, reason)
		watchdogBase.InputRetention = inputs.snapshot()
		watchdogBase.LoadAfter = measureLoad()
		watchdogBase.StopReason = reason
		watchdogBase.Error = "STOP: " + detail
		_ = measureWrite(path, watchdogBase)
		measureFailureStacks(path)
		os.Exit(2)
	}()
	// Preallocated bounded resource sampling, with live failure snapshots.
	go func() {
		defer close(done)
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		fail := func(e error) {
			sampleErr = e
			stopWork.Store(true)
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
	run := func(index, worker int, phase string) {
		if stopWork.Load() {
			return
		}
		slotStart := time.Since(epoch).Nanoseconds()
		err := func() error {
			in, err := ports.input(ctx, row.Workload, index)
			if err != nil {
				return err
			}
			inputs.retain(in)
			validated := false
			defer func() { inputs.release(in, validated) }()
			in.startOffset = &startedOffsets[index]
			callCtx, callCancel := context.WithTimeout(ctx, budget.Request)
			defer callCancel()
			in.ctx = measureDeadlineContext(in.ctx, callCtx)
			in.cleanup = callCancel
			sample, result := measureRequest(callCtx, row.Lane, in, epoch, index, worker, phase)
			sample.SlotStart = slotStart
			res.Samples[index] = sample
			completedSamples[index].Store(&sample)
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
		completedSamples[index].Store(&sample)
		if err != nil {
			runErrOnce.Do(func() {
				runErr = fmt.Errorf("request %d: %w", index, err)
				stopWork.Store(true)
				if errors.Is(err, context.DeadlineExceeded) {
					res.StopReason = "request_deadline_expired"
					if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						res.StopReason = "row_context_expired"
					}
				}
			})
		}
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
					if stopWork.Load() || ctx.Err() != nil {
						return
					}
					run(begin+j, worker, phase)
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

func TestResponseContinuationMeasure(t *testing.T) {
	if raw := os.Getenv("RESPONSE_MEASURE_CHILD"); raw != "" {
		if *measureMode == "diagnostic" {
			t.Fatal("diagnostic disallows private child marker")
		}
		var row measureRow
		if err := json.Unmarshal([]byte(raw), &row); err != nil {
			t.Fatal(err)
		}
		if err := runMeasureRow(row, os.Getenv("RESPONSE_MEASURE_ROW_PATH")); err != nil {
			t.Fatal(err)
		}
		return
	}
	if *measureMode == "" {
		t.Skip("explicit measurement mode required")
	}
	rows, err := measureRows(*measureMode, *measureLaneSelection, *measureProfileSelection, *measureWorkloadSelection, *measureConcurrencySelection)
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
	if *measureMode == "plan" {
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
	preflight := exec.Command(os.Args[0], "-test.run=^TestResponseContinuation(Contract|Integrated(Contract|RejectsWholeBatch|SiblingProgressAndJoin|Close.*|RecoveredPanic.*|Poison.*|RetainsFullDescriptor|ReleasePort|Activation|AsyncPanicSemantics))$", "-test.timeout=90s")
	preflight.Env = append(os.Environ(), "GOMAXPROCS=8")
	preflightStart := time.Now()
	output, err := preflight.CombinedOutput()
	if writeErr := measureWrite(filepath.Join(*measureOut, "preflight-command.json"), map[string]any{"argv": preflight.Args, "env_overrides": []string{"GOMAXPROCS=8"}, "cwd": func() string { dir, _ := os.Getwd(); return dir }(), "start_utc": preflightStart.UTC().Format(time.RFC3339Nano), "end_utc": time.Now().UTC().Format(time.RFC3339Nano), "error": fmt.Sprint(err)}); writeErr != nil {
		t.Fatal(writeErr)
	}
	_ = os.WriteFile(filepath.Join(*measureOut, "preflight.log"), output, 0600)
	if err != nil {
		t.Fatalf("preflight: %v", err)
	}
	type child struct {
		row  measureRow
		name string
	}
	children := []child{}
	seen := map[string]bool{}
	for _, r := range rows {
		key := r.Profile + "/" + r.Workload + "/" + r.Lane
		if seen[key] {
			continue
		}
		seen[key] = true
		r.Preflight = true
		r.Concurrency = 1
		r.Count = 0
		r.Warmups = 0
		r.FirstOnly = true
		r.Pair = 0
		r.Order = len(children)
		children = append(children, child{r, fmt.Sprintf("port-preflight-%03d", len(children))})
	}
	for j, r := range rows {
		children = append(children, child{r, fmt.Sprintf("row-%03d", j)})
	}
	for j, child := range children {
		row := child.row
		path := filepath.Join(*measureOut, child.name+".json")
		raw, _ := json.Marshal(row)
		// Preserve planned contaminated rows even if the OS watchdog must kill a
		// stuck native call. Missing request completion is STOP, never filtered.
		_ = measureWrite(path, map[string]any{"row": row, "error": "child incomplete", "status": "STARTED"})
		budget, _ := measureCaptureBudget(*measureMode)
		ctx, cancel := context.WithTimeout(context.Background(), budget.OuterKill)
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestResponseContinuationMeasure$", "-test.timeout="+budget.TestTimeout.String())
		cmd.Env = append(os.Environ(), "GOMAXPROCS=8", "RESPONSE_MEASURE_CHILD="+string(raw), "RESPONSE_MEASURE_ROW_PATH="+path)
		start := time.Now()
		output, err = cmd.CombinedOutput()
		cancel()
		_ = os.WriteFile(strings.TrimSuffix(path, ".json")+".log", output, 0600)
		_ = measureWrite(strings.TrimSuffix(path, ".json")+"-command.json", map[string]any{"argv": cmd.Args, "env_overrides": []string{"GOMAXPROCS=8", "RESPONSE_MEASURE_CHILD=" + string(raw), "RESPONSE_MEASURE_ROW_PATH=" + path}, "start_utc": start.UTC().Format(time.RFC3339Nano), "launch_setup_run_ns": time.Since(start).Nanoseconds(), "error": fmt.Sprint(err)})
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
