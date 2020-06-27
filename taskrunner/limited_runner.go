package taskrunner

import (
	"fmt"
	"sync"
	"time"
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

type LimitedRunner struct {
	minWorkers int // at least 1
	maxWorkers int // at least minWorkers
	queueSize  int // at least 1
	idleTime   time.Duration

	workerCh chan struct{} // to limit the number of workers
	taskCh   chan func()   // buffered task channel
	closeCh  chan struct{}
	wg       sync.WaitGroup // to wait workers

	mu     sync.RWMutex
	closed bool
}

type LimitedRunnerOption func(*LimitedRunner) error

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
	r.closeCh = make(chan struct{})

	// Prefork persistent worker go routines.
	for i := 0; i < r.minWorkers; i++ {
		r.workerCh <- struct{}{}
		r.wg.Add(1)
		go r.workerLoop(true, nop)
	}

	// Manager go routine: fork non-persistent worker go routines.
	if r.maxWorkers > r.minWorkers {
		r.wg.Add(1)
		go r.managerLoop()
	}

	return r, nil
}

func (r *LimitedRunner) managerLoop() {
	defer r.wg.Done()

	for {
		// Wait a quota to start another go routine or exit.
		select {
		case r.workerCh <- struct{}{}:
		case <-r.closeCh:
			return
		}

		// Wait another task or exit.
		task := <-r.taskCh
		if task == nil {
			// Release worker quota.
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
		// XXX: maybe panic.
		task()

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

func (r *LimitedRunner) Close() {

	r.mu.Lock()
	if !r.closed {
		r.closed = true
		close(r.taskCh)
		close(r.closeCh)
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

func LimitedRunnerMinWorkers(n int) LimitedRunnerOption {
	return func(r *LimitedRunner) error {
		if n < 1 {
			return fmt.Errorf("LimitedRunnerMinWorkers < 1")
		}
		r.minWorkers = n
		return nil
	}
}

func LimitedRunnerMaxWorkers(n int) LimitedRunnerOption {
	return func(r *LimitedRunner) error {
		if n < 1 {
			return fmt.Errorf("LimitedRunnerMaxWorkers < 1")
		}
		r.maxWorkers = n
		return nil
	}
}

func LimitedRunnerQueueSize(n int) LimitedRunnerOption {
	return func(r *LimitedRunner) error {
		if n < 1 {
			return fmt.Errorf("LimitedRunnerQueueSize < 1")
		}
		r.queueSize = n
		return nil
	}
}

func LimitedRunnerIdleTime(t time.Duration) LimitedRunnerOption {
	return func(r *LimitedRunner) error {
		if t < 0 {
			return fmt.Errorf("LimitedRunnerIdleTime < 0")
		}
		r.idleTime = t
		return nil
	}
}
