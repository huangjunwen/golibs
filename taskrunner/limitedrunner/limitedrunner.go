package limitedrunner

import (
	"fmt"
	"sync"
	"time"

	. "github.com/huangjunwen/golibs/taskrunner"
)

const (
	// Default minimum go routines to handle tasks.
	DefaultMinWorkers = 2

	// Default maximum go routines to handle tasks.
	DefaultMaxWorkers = 4096

	// Default queue size.
	DefaultQueueSize = 4 * 4096

	// Default idle time for non-persistent worker before quit.
	DefaultIdleTime = 10 * time.Second
)

var (
	nop            = func() {}
	_   TaskRunner = (*LimitedRunner)(nil)
)

// LimitedRunner implements taskrunner interface. It starts with some persistent
// worker go routines (MinWorkers) which will not exit until Close.
// New worker go routines (up to MaxWorkers - MinWorkers)
// maybe created when workload increases, and will exit after some
// idle time (IdleTime).
//
// Tasks are submitted to a buffered channel (size is QueueSize) and distrubuted to all workers.
type LimitedRunner struct {
	minWorkers int // at least 1
	maxWorkers int // at least minWorkers
	queueSize  int // at least 1
	idleTime   time.Duration

	workerCh chan struct{} // to limit the number of workers
	taskCh   chan func()   // buffered task channel
	wg       sync.WaitGroup

	mu     sync.RWMutex
	closed bool
}

// Must creates a LimitedRunner or panic.
func Must(opts ...Option) *LimitedRunner {
	ret, err := New(opts...)
	if err != nil {
		panic(err)
	}
	return ret
}

// New creates a new LimitedRunner.
func New(opts ...Option) (*LimitedRunner, error) {
	r := &LimitedRunner{
		minWorkers: DefaultMinWorkers,
		maxWorkers: DefaultMaxWorkers,
		queueSize:  DefaultQueueSize,
		idleTime:   DefaultIdleTime,
	}

	for _, opt := range opts {
		if err := opt(r); err != nil {
			return nil, err
		}
	}

	if r.maxWorkers < r.minWorkers {
		return nil, fmt.Errorf("New: MaxWorkers(%d) < MinWorkers(%d)", r.maxWorkers, r.minWorkers)
	}

	r.workerCh = make(chan struct{}, r.maxWorkers)
	r.taskCh = make(chan func(), r.queueSize)

	for i := 0; i < r.minWorkers; i++ {
		r.workerCh <- struct{}{}
		r.wg.Add(1)
		go r.workerLoop(true, nop)
	}

	if r.maxWorkers > r.minWorkers {
		r.wg.Add(1)
		go r.managerLoop()
	}

	return r, nil
}

// managerLoop is used to fork non-persistent worker go routines.
func (r *LimitedRunner) managerLoop() {
	defer r.wg.Done()

	for {
		r.workerCh <- struct{}{} // We need worker quota.
		task := <-r.taskCh
		if task == nil {
			<-r.workerCh
			return
		}
		r.wg.Add(1)
		go r.workerLoop(false, task)
	}
}

// workerLoop handles task until taskCh closed, or idle long enough
// for non-persistent workers.
func (r *LimitedRunner) workerLoop(persistent bool, task func()) {

	defer func() {
		// Release worker quota.
		<-r.workerCh
		r.wg.Done()
	}()

	// NOTE: idleCh is nil for persistent worker so that it will never trigger a idle timeout (and exit).
	// stopTimer should be invoked only when idleCh has NOT yet drained.
	// resetTimer should be invoked only on stopped or expired timers with drained channels.
	idleCh := (<-chan time.Time)(nil)
	stopTimer := nop
	resetTimer := nop

	if !persistent {
		idleTimer := time.NewTimer(r.idleTime)

		idleCh = idleTimer.C
		stopTimer = func() {
			if !idleTimer.Stop() {
				<-idleTimer.C
			}
		}
		resetTimer = func() {
			idleTimer.Reset(r.idleTime)
		}

		stopTimer()
	}

	for {
		func() {
			defer func() {
				// Recover silently.
				recover()
			}()
			task()
		}()

		resetTimer()
		select {
		case task = <-r.taskCh:
			stopTimer()
			if task == nil {
				return
			}

		case <-idleCh:
			// This branch is impossible for persistent worker.
			return
		}
	}

}

// Submit implements taskrunner interface. Returns ErrTooBusy if task queue
// (the buffered channel) is full at this moment.
func (r *LimitedRunner) Submit(task func()) error {
	if task == nil {
		panic(fmt.Errorf("LimitedRunner.Submit(nil)"))
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.closed {
		return ErrClosed
	}

	select {
	case r.taskCh <- task:
		return nil

	default:
		return ErrTooBusy
	}

}

// Close implements taskrunner interface. Returns when all submitted task finished.
func (r *LimitedRunner) Close() {

	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.taskCh)
	}
	r.mu.Unlock()

	// Wait workers and manager.
	r.wg.Wait()

	if l := len(r.taskCh); l != 0 {
		panic(fmt.Errorf("len(taskCh) = %d in Close()", l))
	}
	if l := len(r.workerCh); l != 0 {
		panic(fmt.Errorf("len(workerCh) = %d in Close()", l))
	}
}
