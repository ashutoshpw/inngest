//go:build linux && opcode_bridge_spike

package opcodespike

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	defaultStartupTimeout = 2 * time.Second
	defaultRequestTimeout = 1 * time.Second
	defaultShutdownWait   = 250 * time.Millisecond
	defaultCleanupWait    = 1 * time.Second
	maxStderrBytes        = 64 * 1024
)

var (
	errWorkerCleanup      = errors.New("opcode worker cleanup incomplete")
	errStderrReaderForced = errors.New("opcode worker stderr reader forced closed before EOF")
)

type WorkerConfig struct {
	BinaryPath      string
	Args            []string
	Env             []string
	StartupTimeout  time.Duration
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration
	CleanupTimeout  time.Duration
}

type exchangeResult struct {
	frame []byte
	err   error
}

type Worker struct {
	mu sync.Mutex

	cmd          *exec.Cmd
	stdin        *os.File
	stdout       *os.File
	stderrReader *os.File
	stderr       *limitedStderr

	stderrDone chan struct{}
	waitDone   chan struct{}
	ioDone     chan struct{}
	waitMu     sync.Mutex
	waitErr    error

	stderrErrMu  sync.Mutex
	stderrErr    error
	stderrForced bool

	requestID  uint64
	config     WorkerConfig
	closed     bool
	poisoned   bool
	cleanupErr error
	closeErr   error
}

func NewWorker(config WorkerConfig) (*Worker, error) {
	return newWorker(context.Background(), config)
}

func newWorker(parent context.Context, config WorkerConfig) (*Worker, error) {
	if !filepath.IsAbs(config.BinaryPath) {
		return nil, fmt.Errorf("opcode worker path must be absolute: %q", config.BinaryPath)
	}
	if config.StartupTimeout <= 0 {
		config.StartupTimeout = defaultStartupTimeout
	}
	if config.RequestTimeout <= 0 {
		config.RequestTimeout = defaultRequestTimeout
	}
	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = defaultShutdownWait
	}
	if config.CleanupTimeout <= 0 {
		config.CleanupTimeout = defaultCleanupWait
	}

	stdinReader, stdinWriter, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("create opcode worker stdin pipe: %w", err)
	}
	stdoutReader, stdoutWriter, err := os.Pipe()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create opcode worker stdout pipe: %w", err), closeFiles(stdinReader, stdinWriter))
	}
	stderrReader, stderrWriter, err := os.Pipe()
	if err != nil {
		return nil, errors.Join(fmt.Errorf("create opcode worker stderr pipe: %w", err), closeFiles(stdinReader, stdinWriter, stdoutReader, stdoutWriter))
	}

	cmd := exec.Command(config.BinaryPath, config.Args...)
	if config.Env != nil {
		cmd.Env = append(os.Environ(), config.Env...)
	}
	cmd.Stdin = stdinReader
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter
	// Setpgid lets cleanup signal ordinary descendants as a group. A descendant
	// can escape with setsid; direct-child reaping is tracked independently.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		return nil, errors.Join(fmt.Errorf("start opcode worker: %w", err), closeFiles(stdinReader, stdinWriter, stdoutReader, stdoutWriter, stderrReader, stderrWriter))
	}

	worker := &Worker{
		cmd:          cmd,
		stdin:        stdinWriter,
		stdout:       stdoutReader,
		stderrReader: stderrReader,
		stderr:       &limitedStderr{},
		stderrDone:   make(chan struct{}),
		waitDone:     make(chan struct{}),
		config:       config,
	}
	// These are the child ends of the manually-owned pipes. Keeping only the
	// parent ends means cmd.Wait never has to coordinate with our stream readers.
	childPipeErr := closeFiles(stdinReader, stdoutWriter, stderrWriter)
	go worker.drainStderr()
	go worker.waitForProcess()
	if childPipeErr != nil {
		cleanupErr := worker.poisonAndCleanup()
		return nil, errors.Join(fmt.Errorf("close opcode worker child pipe ends: %w", childPipeErr), cleanupErr)
	}

	ctx, cancel := context.WithTimeout(parent, config.StartupTimeout)
	defer cancel()
	frame, err := worker.exchange(ctx, HandshakeFrame())
	if err == nil {
		err = ValidateReady(frame)
	}
	if err != nil {
		cleanupErr := worker.poisonAndCleanup()
		return nil, errors.Join(fmt.Errorf("opcode worker startup handshake: %w", err), cleanupErr)
	}
	return worker, nil
}

func closeFiles(files ...*os.File) error {
	var closeErrors []error
	for _, file := range files {
		if file == nil {
			continue
		}
		if err := file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			closeErrors = append(closeErrors, err)
		}
	}
	return errors.Join(closeErrors...)
}

func (w *Worker) waitForProcess() {
	err := w.cmd.Wait()
	w.waitMu.Lock()
	w.waitErr = err
	w.waitMu.Unlock()
	close(w.waitDone)
}

func (w *Worker) drainStderr() {
	defer close(w.stderrDone)
	buffer := make([]byte, 4096)
	for {
		count, err := w.stderrReader.Read(buffer)
		if count > 0 {
			w.stderr.Write(buffer[:count])
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				w.stderrErrMu.Lock()
				w.stderrErr = err
				w.stderrErrMu.Unlock()
			}
			return
		}
		if count == 0 {
			return
		}
	}
}

func (w *Worker) exchange(ctx context.Context, request []byte) ([]byte, error) {
	return w.exchangeWithExitPolicy(ctx, request, false)
}

// Shutdown may flush its BYE immediately before exiting. Keep waiting for the
// response after Wait completes so the caller can validate that acknowledgement.
func (w *Worker) exchangeShutdown(ctx context.Context) ([]byte, error) {
	return w.exchangeWithExitPolicy(ctx, ShutdownFrame(), true)
}

func (w *Worker) exchangeWithExitPolicy(ctx context.Context, request []byte, allowExitBeforeResponse bool) ([]byte, error) {
	result := make(chan exchangeResult, 1)
	done := make(chan struct{})
	w.ioDone = done
	go func() {
		defer close(done)
		if err := WriteFull(w.stdin, request); err != nil {
			result <- exchangeResult{err: fmt.Errorf("write opcode worker request: %w", err)}
			return
		}
		frame, err := ReadFrame(w.stdout)
		if err != nil {
			result <- exchangeResult{err: fmt.Errorf("read opcode worker response: %w", err)}
			return
		}
		result <- exchangeResult{frame: frame}
	}()

	select {
	case outcome := <-result:
		<-done
		if ctx.Err() != nil {
			return nil, errors.Join(ctx.Err(), w.stopProcess())
		}
		if outcome.err != nil {
			return nil, outcome.err
		}
		if !allowExitBeforeResponse {
			select {
			case <-w.waitDone:
				return nil, fmt.Errorf("opcode worker exited after response: %w", w.processExitError())
			default:
			}
		}
		return outcome.frame, nil
	case <-ctx.Done():
		return nil, errors.Join(ctx.Err(), w.stopProcess())
	case <-w.waitDone:
		if allowExitBeforeResponse {
			select {
			case outcome := <-result:
				<-done
				if ctx.Err() != nil {
					return nil, errors.Join(ctx.Err(), w.stopProcess())
				}
				if outcome.err != nil {
					return nil, outcome.err
				}
				return outcome.frame, nil
			case <-ctx.Done():
				return nil, errors.Join(ctx.Err(), w.stopProcess())
			}
		}
		return nil, errors.Join(fmt.Errorf("opcode worker exited while request was in flight: %w", w.processExitError()), w.stopProcess())
	}
}

func (w *Worker) Evaluate(ctx context.Context, tags []Tag) (Projection, error) {
	if err := ctx.Err(); err != nil {
		return Projection{}, err
	}
	if !w.mu.TryLock() {
		return Projection{}, ErrWorkerBusy
	}
	defer w.mu.Unlock()
	if w.closed || w.poisoned {
		return Projection{}, errors.New("opcode worker is not usable")
	}
	if err := ctx.Err(); err != nil {
		return Projection{}, err
	}
	if w.requestID == ^uint64(0) {
		return Projection{}, errors.New("opcode worker request ID exhausted")
	}
	w.requestID++
	requestID := w.requestID
	request, err := EncodePlan(requestID, tags)
	if err != nil {
		return Projection{}, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, w.config.RequestTimeout)
	defer cancel()
	response, err := w.exchange(requestCtx, request)
	if err != nil {
		cleanupErr := w.poisonAndCleanup()
		return Projection{}, errors.Join(err, cleanupErr)
	}
	projection, err := DecodeResult(requestID, tags, response)
	if err != nil {
		cleanupErr := w.poisonAndCleanup()
		return Projection{}, errors.Join(err, cleanupErr)
	}
	// A response that raced cancellation is never allowed to become usable.
	if err := requestCtx.Err(); err != nil {
		cleanupErr := w.poisonAndCleanup()
		return Projection{}, errors.Join(err, cleanupErr)
	}
	return projection, nil
}

func (w *Worker) poisonAndCleanup() error {
	w.poisoned = true
	w.closed = true
	w.cleanupErr = errors.Join(w.cleanupErr, w.cleanupProcess(false))
	return w.cleanupErr
}

func (w *Worker) stopProcess() error {
	var stopErrors []error
	if err := closeFiles(w.stdin, w.stdout); err != nil {
		stopErrors = append(stopErrors, fmt.Errorf("close opcode worker request pipes: %w", err))
	}
	if w.cmd != nil && w.cmd.Process != nil {
		pid := w.cmd.Process.Pid
		if err := syscall.Kill(-pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			groupErr := fmt.Errorf("kill opcode worker process group %d: %w", pid, err)
			if processErr := w.cmd.Process.Kill(); processErr != nil && !errors.Is(processErr, os.ErrProcessDone) {
				stopErrors = append(stopErrors, errors.Join(groupErr, fmt.Errorf("kill opcode worker process %d: %w", pid, processErr)))
			} else {
				stopErrors = append(stopErrors, groupErr)
			}
		}
	}
	return errors.Join(stopErrors...)
}

func (w *Worker) cleanupProcess(graceful bool) error {
	deadline := time.Now().Add(w.config.CleanupTimeout)
	var cleanupErrors []error
	if graceful {
		if err := closeFiles(w.stdin); err != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("close opcode worker stdin: %w", err))
		}
	} else if err := w.stopProcess(); err != nil {
		cleanupErrors = append(cleanupErrors, err)
	}

	forceDelay := w.config.CleanupTimeout / 4
	if forceDelay > 50*time.Millisecond {
		forceDelay = 50 * time.Millisecond
	}
	if forceDelay <= 0 {
		forceDelay = time.Nanosecond
	}
	forceAt := deadline.Add(-forceDelay)
	if forceAt.Before(time.Now()) {
		forceAt = time.Now()
	}
	timer := time.NewTimer(max(time.Until(forceAt), 0))
	defer timer.Stop()
	forced := false
	waitJoined := false
	stderrJoined := false
	ioJoined := w.ioDone == nil
	for !waitJoined || !stderrJoined || !ioJoined {
		select {
		case <-w.waitDone:
			waitJoined = true
		case <-w.stderrDone:
			stderrJoined = true
		case <-w.ioDone:
			ioJoined = true
		case <-timer.C:
			waitJoined = waitJoined || isChannelClosed(w.waitDone)
			stderrJoined = stderrJoined || isChannelClosed(w.stderrDone)
			ioJoined = ioJoined || isChannelClosed(w.ioDone)
			if waitJoined && stderrJoined && ioJoined {
				goto joined
			}
			if !forced {
				forced = true
				if !waitJoined || !ioJoined {
					if err := w.stopProcess(); err != nil {
						cleanupErrors = append(cleanupErrors, err)
					}
				}
				if !stderrJoined {
					w.stderrForced = true
					cleanupErrors = append(cleanupErrors, errStderrReaderForced)
					if err := closeFiles(w.stderrReader); err != nil {
						cleanupErrors = append(cleanupErrors, fmt.Errorf("close opcode worker stderr reader: %w", err))
					}
				}
				timer.Reset(max(time.Until(deadline), 0))
				continue
			}
			if !waitJoined {
				cleanupErrors = append(cleanupErrors, errors.New("opcode worker direct child was not reaped before cleanup deadline"))
			}
			if !stderrJoined {
				cleanupErrors = append(cleanupErrors, errors.New("opcode worker stderr drainer did not join before cleanup deadline"))
			}
			if !ioJoined {
				cleanupErrors = append(cleanupErrors, errors.New("opcode worker request I/O did not join before cleanup deadline"))
			}
			goto joined
		}
	}

joined:
	if stderrJoined && !w.stderrForced {
		w.stderrErrMu.Lock()
		stderrErr := w.stderrErr
		w.stderrErrMu.Unlock()
		if stderrErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("read opcode worker stderr: %w", stderrErr))
		}
	}
	if err := closeFiles(w.stdin, w.stdout, w.stderrReader); err != nil {
		cleanupErrors = append(cleanupErrors, fmt.Errorf("close opcode worker pipes: %w", err))
	}
	if len(cleanupErrors) == 0 {
		return nil
	}
	return errors.Join(errWorkerCleanup, errors.Join(cleanupErrors...))
}

func (w *Worker) processError() error {
	w.waitMu.Lock()
	defer w.waitMu.Unlock()
	return w.waitErr
}

func (w *Worker) processExitError() error {
	if err := w.processError(); err != nil {
		return err
	}
	return errors.New("worker exited with status 0")
}

func (w *Worker) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return errors.Join(w.closeErr, w.cleanupErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), w.config.ShutdownTimeout)
	defer cancel()
	frame, err := w.exchangeShutdown(ctx)
	if err == nil {
		err = ValidateBye(frame)
	}
	if err != nil {
		w.poisoned = true
		w.closed = true
		w.closeErr = fmt.Errorf("opcode worker shutdown: %w", err)
		w.cleanupErr = errors.Join(w.cleanupErr, w.cleanupProcess(false))
		return errors.Join(w.closeErr, w.cleanupErr)
	}

	w.closed = true
	w.cleanupErr = errors.Join(w.cleanupErr, w.cleanupProcess(true))
	if isChannelClosed(w.waitDone) {
		if waitErr := w.processError(); waitErr != nil {
			w.closeErr = fmt.Errorf("opcode worker exited unsuccessfully: %w; stderr: %s", waitErr, w.stderr.String())
		}
	}
	return errors.Join(w.closeErr, w.cleanupErr)
}

func isChannelClosed(channel <-chan struct{}) bool {
	if channel == nil {
		return true
	}
	select {
	case <-channel:
		return true
	default:
		return false
	}
}

func (w *Worker) cleanupFailure() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.cleanupErr
}

func (w *Worker) Poisoned() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.poisoned
}

func (w *Worker) PID() int {
	if w.cmd == nil || w.cmd.Process == nil {
		return 0
	}
	return w.cmd.Process.Pid
}

func (w *Worker) Stderr() string {
	return w.stderr.String()
}

type limitedStderr struct {
	mu   sync.Mutex
	data []byte
}

func (b *limitedStderr) Write(data []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	remaining := maxStderrBytes - len(b.data)
	if remaining > 0 {
		if len(data) > remaining {
			data = data[:remaining]
		}
		b.data = append(b.data, data...)
	}
}

func (b *limitedStderr) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return strings.TrimSpace(string(b.data))
}
