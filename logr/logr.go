// Package logr defines Logger interface.
package logr

// Logger is a sub-interface of github.com/go-logr/logr::Logger,
// which i think it's enough in most cases.
type Logger interface {
	// Info logs a non-error message with the given key/value pairs as context.
	Info(msg string, keysAndValues ...interface{})

	// Error logs an error, with the given message and key/value pairs as context.
	Error(err error, msg string, keysAndValues ...interface{})

	// WithValues adds some key-value pairs of context to a logger.
	WithValues(keysAndValues ...interface{}) Logger
}

type nopLogger struct{}

func (l nopLogger) Info(msg string, keysAndValues ...interface{}) {}

func (l nopLogger) Error(err error, msg string, keysAndValues ...interface{}) {}

func (l nopLogger) WithValues(keysAndValues ...interface{}) Logger { return nopLogger{} }

var (
	// Nop does nothing.
	Nop Logger = nopLogger{}
)
