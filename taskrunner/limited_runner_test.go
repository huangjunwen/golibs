package taskrunner

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewLimitedRunner(t *testing.T) {
	assert := assert.New(t)

	{
		_, err := NewLimitedRunner(LimitedRunnerMinWorkers(0))
		assert.Error(err)
	}
	{
		_, err := NewLimitedRunner(LimitedRunnerMaxWorkers(0))
		assert.Error(err)
	}
	{
		_, err := NewLimitedRunner(LimitedRunnerQueueSize(0))
		assert.Error(err)
	}
	{
		_, err := NewLimitedRunner(LimitedRunnerMinWorkers(2), LimitedRunnerMaxWorkers(1))
		assert.Error(err)
	}
	{
		_, err := NewLimitedRunner(LimitedRunnerIdleTime(-time.Second))
		assert.Error(err)
	}
}

func TestLimitedRunnerSubmit(t *testing.T) {
	assert := assert.New(t)

	r, err := NewLimitedRunner(
		LimitedRunnerMinWorkers(1),
		LimitedRunnerMaxWorkers(2),
		LimitedRunnerQueueSize(1),
	)
	assert.NoError(err)
	defer r.Close()

	// The task returns only when close(stopCh)
	syncCh := make(chan struct{})
	stopCh := make(chan struct{})
	task := func() {
		syncCh <- struct{}{}
		<-stopCh
	}

	assert.NoError(r.Submit(task)) // 1
	<-syncCh                       // sync 1

	assert.NoError(r.Submit(task)) // 2
	<-syncCh                       // sync 2

	assert.NoError(r.Submit(task)) // 3 enqueued but no worker can handle this
	//<-syncCh                     // sync 3 will be dead

	assert.Equal(ErrTooBusy, r.Submit(task)) // 4 fail since queue full

	close(stopCh) // 1 and 2 done
	<-syncCh      // sync 3

	r.Close()

	assert.Equal(ErrClosed, r.Submit(task)) // 5 fail since closed
}

func TestLimitedRunnerClose(t *testing.T) {
	assert := assert.New(t)

	r, err := NewLimitedRunner(
		LimitedRunnerMinWorkers(1),
		LimitedRunnerMaxWorkers(15),
	)
	assert.NoError(err)
	defer r.Close()

	mu := &sync.Mutex{}
	count := 0
	task := func() {
		mu.Lock()
		count++
		c := count
		mu.Unlock()
		fmt.Printf("task %d\n", c)
		time.Sleep(time.Second)
	}

	start := time.Now()
	n := 30
	for i := 0; i < n; i++ {
		assert.NoError(r.Submit(task))
	}
	fmt.Printf("tasks submitted\n")
	r.Close() // Close should wait all submitted tasks finished
	dur := time.Since(start)

	assert.Equal(n, count)
	fmt.Printf("%d tasks in %s\n", n, dur.String())

}
