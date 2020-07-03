package logr

// Logger is a sub-interface of github.com/go-logr/logr::Logger,
// which i think it's enough in most cases.
type Logger interface {
	// Info logs a non-error message with the given key/value pairs as context.
	Info(msg string, keysAndValues ...interface{})

	// Error logs an error, with the given message and key/value pairs as context.
	Error(err error, msg string, keysAndValues ...interface{})
}

type nopLogger struct{}

func (l nopLogger) Info(msg string, keysAndValues ...interface{}) {}

func (l nopLogger) Error(err error, msg string, keysAndValues ...interface{}) {}

var (
	// Nop does nothing.
	Nop Logger = nopLogger{}
)
