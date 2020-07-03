package zerologr

import (
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestZerologr(t *testing.T) {
	assert := assert.New(t)

	{
		buf := &strings.Builder{}
		logger := zerolog.New(buf)
		l := (*Logger)(&logger)
		l.Info("msg", "k", "v")
		assert.Equal(`{"level":"info","k":"v","message":"msg"}`, strings.TrimSpace(buf.String()))
	}

	{
		buf := &strings.Builder{}
		logger := zerolog.New(buf)
		l := (*Logger)(&logger)
		l.Error(fmt.Errorf("err"), "msg", "k", "v")
		assert.Equal(`{"level":"error","error":"err","k":"v","message":"msg"}`, strings.TrimSpace(buf.String()))
	}
}
