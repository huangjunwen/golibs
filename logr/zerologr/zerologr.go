package zerologr

import (
	"github.com/rs/zerolog"

	"github.com/huangjunwen/golibs/logr"
)

// Logger is implements github.com/huangjunwen/golibs/logr::Logger interface using
// github.com/rs/zerolog::Logger.
type Logger zerolog.Logger

var (
	_ logr.Logger = (*Logger)(nil)
)

func (logger *Logger) Info(msg string, keysAndValues ...interface{}) {
	l := (*zerolog.Logger)(logger)
	ev := l.Info()
	for i := 0; i < len(keysAndValues); i += 2 {
		ev = ev.Interface(keysAndValues[i].(string), keysAndValues[i+1])
	}
	ev.Msg(msg)
}

func (logger *Logger) Error(err error, msg string, keysAndValues ...interface{}) {
	l := (*zerolog.Logger)(logger)
	ev := l.Error().Err(err)
	for i := 0; i < len(keysAndValues); i += 2 {
		ev = ev.Interface(keysAndValues[i].(string), keysAndValues[i+1])
	}
	ev.Msg(msg)
}
