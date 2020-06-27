package taskrunner

// TaskRunner is an interface to run tasks.
type TaskRunner interface {
	// Submit submits a task to run. The call must not block.
	// Return an error if the task can't be run.
	Submit(task func()) error

	// Close stops the TaskRunner and waits for all tasks finish.
	// Any Submit after Close should return an error.
	Close()
}
