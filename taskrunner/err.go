package taskrunner

import (
	"errors"
)

var (
	// ErrClosed is returned when task is submitted after closed.
	ErrClosed = errors.New("TaskRunner: Closed")

	// ErrTooBusy is returned when task is submitted but the task runner is too busy to handle.
	ErrTooBusy = errors.New("TaskRunner: Too busy")
)
