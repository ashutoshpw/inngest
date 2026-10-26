//go:build linux && opcode_bridge_spike

package opcodespike

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	maxRestartAttemptsPerMinute = 3
	restartAttemptWindow        = time.Minute
	minRestartBackoff           = 100 * time.Millisecond
	maxRestartBackoff           = 5 * time.Second
)

var errPoolUnavailable = errors.New("all opcode workers are unavailable")

type PoolConfig struct {
	Worker    WorkerConfig
	Size      int
	MaxQueued int
}

type poolSlot struct {
	worker *Worker
	failed bool
}

// Pool bounds admitted work to one in-flight request per worker plus the
// configured number of queued requests. It never retries a failed request.
type Pool struct {
	mu sync.Mutex

	ctx         context.Context
	cancel      context.CancelFunc
	config      PoolConfig
	slots       []*poolSlot
	available   chan *poolSlot
	admission   chan struct{}
	unavailable chan struct{}
	failedSlots int

	restartAttempts []time.Time
	restartErrors   []error

	closed          bool
	unavailableSent bool
	closeDone       chan struct{}
	closeErr        error
	calls           sync.WaitGroup
	restarts        sync.WaitGroup
}

func NewPool(ctx context.Context, config PoolConfig) (*Pool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if config.Size <= 0 {
		config.Size = 1
	}
	if config.Size > 8 {
		return nil, errors.New("opcode worker pool size is limited to eight")
	}
	if config.MaxQueued < 0 {
		return nil, errors.New("opcode worker queue size cannot be negative")
	}
	if config.MaxQueued == 0 {
		config.MaxQueued = config.Size
	}

	poolCtx, cancel := context.WithCancel(context.Background())
	pool := &Pool{
		ctx:         poolCtx,
		cancel:      cancel,
		config:      config,
		slots:       make([]*poolSlot, 0, config.Size),
		available:   make(chan *poolSlot, config.Size),
		admission:   make(chan struct{}, config.Size+config.MaxQueued),
		unavailable: make(chan struct{}),
		closeDone:   make(chan struct{}),
	}

	for index := 0; index < config.Size; index++ {
		if err := ctx.Err(); err != nil {
			return nil, errors.Join(err, pool.Close())
		}
		worker, err := newWorker(ctx, config.Worker)
		if err != nil {
			cleanupErr := pool.Close()
			return nil, errors.Join(fmt.Errorf("start opcode worker %d: %w", index, err), cleanupErr)
		}
		slot := &poolSlot{worker: worker}
		pool.slots = append(pool.slots, slot)
		pool.available <- slot
	}
	return pool, nil
}

func (p *Pool) Evaluate(ctx context.Context, tags []Tag) (Projection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return Projection{}, err
	}
	select {
	case p.admission <- struct{}{}:
		defer func() { <-p.admission }()
	default:
		return Projection{}, ErrWorkerBusy
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return Projection{}, ErrPoolClosed
	}
	p.calls.Add(1)
	p.mu.Unlock()
	defer p.calls.Done()

	var slot *poolSlot
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return Projection{}, ErrPoolClosed
		}
		if p.failedSlots == len(p.slots) && len(p.slots) > 0 {
			p.mu.Unlock()
			return Projection{}, errPoolUnavailable
		}
		unavailable := p.unavailable
		p.mu.Unlock()

		select {
		case slot = <-p.available:
			p.mu.Lock()
			if p.closed {
				p.mu.Unlock()
				return Projection{}, ErrPoolClosed
			}
			p.mu.Unlock()
			goto acquired
		case <-ctx.Done():
			return Projection{}, ctx.Err()
		case <-p.ctx.Done():
			return Projection{}, ErrPoolClosed
		case <-unavailable:
			return Projection{}, errPoolUnavailable
		}
	}

acquired:
	requestCtx, cancel := context.WithCancel(ctx)
	stopClose := context.AfterFunc(p.ctx, cancel)
	projection, evalErr := slot.worker.Evaluate(requestCtx, tags)
	stopClose()
	cancel()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return Projection{}, ErrPoolClosed
	}
	if slot.worker.Poisoned() {
		p.markSlotFailedLocked(slot)
		cleanupErr := slot.worker.cleanupFailure()
		if cleanupErr == nil {
			p.restarts.Add(1)
			go p.restart(slot)
		}
		p.mu.Unlock()
		return Projection{}, errors.Join(evalErr, cleanupErr)
	}
	p.available <- slot
	p.mu.Unlock()
	return projection, evalErr
}

func (p *Pool) markSlotFailedLocked(slot *poolSlot) {
	if slot.failed {
		return
	}
	slot.failed = true
	p.failedSlots++
	if p.failedSlots == len(p.slots) && !p.unavailableSent {
		close(p.unavailable)
		p.unavailableSent = true
	}
}

func (p *Pool) restoreSlotLocked(slot *poolSlot) {
	if !slot.failed {
		return
	}
	slot.failed = false
	p.failedSlots--
	if p.unavailableSent {
		p.unavailable = make(chan struct{})
		p.unavailableSent = false
	}
}

func (p *Pool) restart(slot *poolSlot) {
	defer p.restarts.Done()
	attempt := 0
	for {
		if !waitRestartDelay(p.ctx, restartBackoff(attempt)) {
			return
		}

		waitFor, reserved := p.reserveRestartAttempt()
		if !reserved {
			if !waitRestartDelay(p.ctx, waitFor) {
				return
			}
			continue
		}

		worker, err := newWorker(p.ctx, p.config.Worker)
		if err != nil {
			if errors.Is(err, errWorkerCleanup) {
				p.mu.Lock()
				p.restartErrors = append(p.restartErrors, err)
				p.mu.Unlock()
				return
			}
			if p.ctx.Err() != nil {
				return
			}
			attempt++
			continue
		}

		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			if closeErr := worker.Close(); closeErr != nil {
				p.mu.Lock()
				p.restartErrors = append(p.restartErrors, closeErr)
				p.mu.Unlock()
			}
			return
		}
		slot.worker = worker
		p.restoreSlotLocked(slot)
		p.mu.Unlock()

		select {
		case p.available <- slot:
			return
		case <-p.ctx.Done():
			return
		}
	}
}

func (p *Pool) reserveRestartAttempt() (time.Duration, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return 0, false
	}
	now := time.Now()
	cutoff := now.Add(-restartAttemptWindow)
	firstRecent := 0
	for firstRecent < len(p.restartAttempts) && !p.restartAttempts[firstRecent].After(cutoff) {
		firstRecent++
	}
	if firstRecent > 0 {
		p.restartAttempts = append([]time.Time(nil), p.restartAttempts[firstRecent:]...)
	}
	if len(p.restartAttempts) >= maxRestartAttemptsPerMinute {
		return p.restartAttempts[0].Add(restartAttemptWindow).Sub(now), false
	}
	p.restartAttempts = append(p.restartAttempts, now)
	return 0, true
}

func restartBackoff(attempt int) time.Duration {
	backoff := minRestartBackoff
	for index := 0; index < attempt && backoff < maxRestartBackoff; index++ {
		if backoff > maxRestartBackoff/2 {
			return maxRestartBackoff
		}
		backoff *= 2
	}
	if backoff > maxRestartBackoff {
		return maxRestartBackoff
	}
	return backoff
}

func waitRestartDelay(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return ctx.Err() == nil
	case <-ctx.Done():
		return false
	}
}

func (p *Pool) Close() error {
	p.mu.Lock()
	if p.closed {
		done := p.closeDone
		p.mu.Unlock()
		<-done
		return p.closeErr
	}
	p.closed = true
	p.cancel()
	p.mu.Unlock()

	// Calls own their workers until they discard any late result. Restart
	// attempts are cancelled and joined before the final worker set is closed.
	p.calls.Wait()
	p.restarts.Wait()

	var closeWait sync.WaitGroup
	var errorMu sync.Mutex
	var closeErrors []error
	p.mu.Lock()
	closeErrors = append(closeErrors, p.restartErrors...)
	workers := make([]*Worker, 0, len(p.slots))
	for _, slot := range p.slots {
		if slot.worker != nil {
			workers = append(workers, slot.worker)
		}
	}
	p.mu.Unlock()
	for _, worker := range workers {
		closeWait.Add(1)
		go func(worker *Worker) {
			defer closeWait.Done()
			if err := worker.Close(); err != nil {
				errorMu.Lock()
				closeErrors = append(closeErrors, err)
				errorMu.Unlock()
			}
		}(worker)
	}
	closeWait.Wait()

	p.mu.Lock()
	p.closeErr = errors.Join(closeErrors...)
	close(p.closeDone)
	closeErr := p.closeErr
	p.mu.Unlock()
	return closeErr
}

func (p *Pool) PIDs() []int {
	p.mu.Lock()
	defer p.mu.Unlock()
	pids := make([]int, 0, len(p.slots))
	for _, slot := range p.slots {
		if slot.worker != nil {
			pids = append(pids, slot.worker.PID())
		}
	}
	return pids
}
