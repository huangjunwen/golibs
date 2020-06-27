package taskrunner

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/huangjunwen/golibs/taskrunner"
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

	maxWorkers := 15
	taskCount := 30

	r, err := NewLimitedRunner(
		LimitedRunnerMaxWorkers(maxWorkers),
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
	for i := 0; i < taskCount; i++ {
		assert.NoError(r.Submit(task))
	}
	fmt.Printf("tasks submitted\n")
	r.Close() // Close should wait all submitted tasks finished
	dur := time.Since(start)

	assert.Equal(taskCount, count)
	fmt.Printf("%d tasks in %s\n", count, dur.String())

}

func TestLimitedRunnerIdleTime(t *testing.T) {
	assert := assert.New(t)

	r, err := NewLimitedRunner(
		LimitedRunnerMaxWorkers(4294967296),         // Unlimited workers.
		LimitedRunnerIdleTime(500*time.Millisecond), // Short idle time.
		//LimitedRunnerIdleTime(10*time.Millisecond), // Short idle time.
	)
	assert.NoError(err)
	defer r.Close()

	wg := &sync.WaitGroup{}
	stopCh := make(chan struct{})

	// Print number of go routines for each interval.
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case t := <-ticker.C:
				fmt.Printf("[%s] go routines: %d\n", t.String(), runtime.NumGoroutine())

			case <-stopCh:
				return
			}
		}

	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stopCh)

		taskCountPerSecond := 20480
		interval := 100 * time.Millisecond

		tickerInSecond := int(time.Second / interval)
		taskCountPerInterval := taskCountPerSecond / tickerInSecond

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		i := 0
		for taskCountPerInterval > 0 {
			<-ticker.C

			// Submit taskCountPerInterval tasks to the runner.
			for j := 0; j < taskCountPerInterval; j++ {
				assert.NoError(r.Submit(nop))
			}

			// Each second reduce taskCountPerInterval to a half.
			if i%tickerInSecond == 0 {
				taskCountPerInterval /= 2
			}
			i++

		}

	}()

	wg.Wait()

	fmt.Printf("[%s] go routines before close: %d\n", time.Now().String(), runtime.NumGoroutine())
	r.Close()
	fmt.Printf("[%s] go routines after close: %d\n", time.Now().String(), runtime.NumGoroutine())

}
