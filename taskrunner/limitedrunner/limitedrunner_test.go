package limitedrunner

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	. "github.com/huangjunwen/golibs/taskrunner"
)

func TestNew(t *testing.T) {
	assert := assert.New(t)

	{
		_, err := New(MinWorkers(0))
		assert.Error(err)
	}
	{
		_, err := New(MaxWorkers(0))
		assert.Error(err)
	}
	{
		_, err := New(QueueSize(0))
		assert.Error(err)
	}
	{
		_, err := New(MinWorkers(2), MaxWorkers(1))
		assert.Error(err)
	}
	{
		_, err := New(IdleTime(-time.Second))
		assert.Error(err)
	}
}

func TestSubmit(t *testing.T) {
	assert := assert.New(t)

	r, err := New(
		MinWorkers(1),
		MaxWorkers(2),
		QueueSize(1),
	)
	assert.NoError(err)
	defer r.Close()

	// The task returns only when close(stopCh)
	syncCh := make(chan int)
	stopCh := make(chan struct{})
	task := func(i int) func() {
		return func() {
			syncCh <- i
			<-stopCh
		}
	}

	assert.NoError(r.Submit(task(1))) // 1
	assert.Equal(1, <-syncCh)         // sync 1

	assert.NoError(r.Submit(task(2))) // 2
	assert.Equal(2, <-syncCh)         // sync 2

	assert.NoError(r.Submit(task(3))) // 3 enqueued but no worker can handle this
	//<-syncCh                        // sync 3 will be dead

	assert.Equal(ErrTooBusy, r.Submit(task(4))) // 4 fail since queue full

	close(stopCh)             // 1 and 2 done
	assert.Equal(3, <-syncCh) // sync 3

	r.Close()

	assert.Equal(ErrClosed, r.Submit(task(5))) // 5 fail since closed
}

func TestClose(t *testing.T) {
	assert := assert.New(t)

	maxWorkers := 15
	taskCount := 30

	r, err := New(
		MaxWorkers(maxWorkers),
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

func TestIdleTime(t *testing.T) {
	assert := assert.New(t)

	r, err := New(
		MaxWorkers(4294967296),         // Unlimited workers.
		IdleTime(500*time.Millisecond), // Short idle time.
		//IdleTime(10*time.Millisecond), // Short idle time.
	)
	assert.NoError(err)
	defer r.Close()

	wg := &sync.WaitGroup{}
	stopCh := make(chan struct{})

	// Print number of go routines each interval.
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

			// Each second reduce taskCountPerInterval to half.
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

func TestPanic(t *testing.T) {
	assert := assert.New(t)

	r, err := New(
		MinWorkers(1),
		MaxWorkers(1),
		QueueSize(1),
	)
	assert.NoError(err)
	defer r.Close()

	// Task can't be nil
	assert.Panics(func() {
		r.Submit(nil)
	})

	// The first panic task should no affect the later one.
	syncCh := make(chan struct{})
	assert.NoError(r.Submit(func() {
		syncCh <- struct{}{}
		panic(fmt.Errorf("xxxx"))
	}))

	<-syncCh
	laterCalled := false
	assert.NoError(r.Submit(func() {
		fmt.Println("task after panic")
		laterCalled = true
	}))

	r.Close()
	assert.True(laterCalled)
}

func TestSubmitInTask(t *testing.T) {
	assert := assert.New(t)

	r, err := New(
		MinWorkers(1),
		MaxWorkers(1),
		QueueSize(1),
	)
	assert.NoError(err)
	defer r.Close()

	laterCalled := false
	syncCh := make(chan struct{})

	assert.NoError(r.Submit(func() {
		assert.NoError(r.Submit(func() {
			fmt.Println("submit inside task")
			laterCalled = true
		}))
		syncCh <- struct{}{}
	}))

	<-syncCh
	r.Close()
	assert.True(laterCalled)
}
