package taskrunner

import (
	"fmt"
	"sync"
	"time"

	. "github.com/huangjunwen/golibs/taskrunner"
)

const (
	// Default minimum go routines to handle tasks.
	DefaultLimitedRunnerMinWorkers = 2

	// Default maximum go routines to handle tasks.
	DefaultLimitedRunnerMaxWorkers = 4096

	// Default queue size.
	DefaultLimitedRunnerQueueSize = 16 * 1024

	// Default idle time for worker before quit.
	DefaultLimitedRunnerIdleTime = 30 * time.Second
)

var (
	nop            = func() {}
	_   TaskRunner = (*LimitedRunner)(nil)
)

// LimitedRunner implements taskrunner interface. It starts with some persistent
// worker go routines (LimitedRunnerMinWorkers) which will not exit until Close.
// New worker go routines (up to LimitedRunnerMaxWorkers - LimitedRunnerMinWorkers)
// maybe created when workload increases. These workers will exit after some
// idle time (LimitedRunnerIdleTime).
//
// Tasks are submitted to a buffered channel and distrubuted to all workers.
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

// LimitedRunnerOption is the option in creating LimitedRunner.
type LimitedRunnerOption func(*LimitedRunner) error

// NewLimitedRunner creates a new LimitedRunner.
func NewLimitedRunner(opts ...LimitedRunnerOption) (*LimitedRunner, error) {
	r := &LimitedRunner{
		minWorkers: DefaultLimitedRunnerMinWorkers,
		maxWorkers: DefaultLimitedRunnerMaxWorkers,
		queueSize:  DefaultLimitedRunnerQueueSize,
		idleTime:   DefaultLimitedRunnerIdleTime,
	}

	for _, opt := range opts {
		if err := opt(r); err != nil {
			return nil, err
		}
	}

	if r.maxWorkers < r.minWorkers {
		return nil, fmt.Errorf("NewLimitedRunner: MaxWorkers(%d) < MinWorkers(%d)", r.maxWorkers, r.minWorkers)
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
		r.workerCh <- struct{}{}
		task := <-r.taskCh
		if task == nil {
			<-r.workerCh
			return
		}
		r.wg.Add(1)
		go r.workerLoop(false, task)
	}
}

func (r *LimitedRunner) workerLoop(persistent bool, task func()) {

	defer func() {
		// Release worker quota.
		<-r.workerCh
		r.wg.Done()
	}()

	if task == nil {
		task = nop
	}

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

// LimitedRunnerMinWorkers sets minimum worker go routines. n should be at least 1.
func LimitedRunnerMinWorkers(n int) LimitedRunnerOption {
	return func(r *LimitedRunner) error {
		if n < 1 {
			return fmt.Errorf("LimitedRunnerMinWorkers < 1")
		}
		r.minWorkers = n
		return nil
	}
}

// LimitedRunnerMaxWorkers sets maximum worker go routines. n should be at least MinWorkers.
func LimitedRunnerMaxWorkers(n int) LimitedRunnerOption {
	return func(r *LimitedRunner) error {
		if n < 1 {
			return fmt.Errorf("LimitedRunnerMaxWorkers < 1")
		}
		r.maxWorkers = n
		return nil
	}
}

// LimitedRunnerQueueSize sets the task buffered channel. n should be at least 1.
// n should be set to a large enough value to handle burst.
// Memory overhead: sizeof(function pointer) * n (e.g. 8M if n is 1 million on 64 bit machine)
func LimitedRunnerQueueSize(n int) LimitedRunnerOption {
	return func(r *LimitedRunner) error {
		if n < 1 {
			return fmt.Errorf("LimitedRunnerQueueSize < 1")
		}
		r.queueSize = n
		return nil
	}
}

// LimitedRunnerIdleTime sets the idle time after which a non-persistent worker go routine
// should exit.
func LimitedRunnerIdleTime(t time.Duration) LimitedRunnerOption {
	return func(r *LimitedRunner) error {
		if t < 0 {
			return fmt.Errorf("LimitedRunnerIdleTime < 0")
		}
		r.idleTime = t
		return nil
	}
}
