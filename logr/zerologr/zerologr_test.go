package zerologr

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func jsonEqual(assert *assert.Assertions, j1, j2 string) {
	v1 := map[string]interface{}{}
	v2 := map[string]interface{}{}

	if err := json.NewDecoder(strings.NewReader(j1)).Decode(&v1); err != nil {
		panic(err)
	}

	if err := json.NewDecoder(strings.NewReader(j2)).Decode(&v2); err != nil {
		panic(err)
	}

	fmt.Printf("%#v %#v\n", v1, v2)
	assert.Equal(v1, v2)
}

func TestZerologr(t *testing.T) {
	assert := assert.New(t)

	{
		buf := &strings.Builder{}
		logger := zerolog.New(buf)
		l := (*Logger)(&logger)
		l.Info("msg", "k", "v")
		jsonEqual(assert, `{"level":"info","k":"v","message":"msg"}`, strings.TrimSpace(buf.String()))
	}

	{
		buf := &strings.Builder{}
		logger := zerolog.New(buf)
		l := (*Logger)(&logger)
		l.Error(fmt.Errorf("err"), "msg", "k", "v")
		jsonEqual(assert, `{"level":"error","error":"err","k":"v","message":"msg"}`, strings.TrimSpace(buf.String()))
	}

	{
		buf := &strings.Builder{}
		logger := zerolog.New(buf)
		l := (*Logger)(&logger)
		l2 := l.WithValues("k", "v")
		l2.Error(fmt.Errorf("err"), "msg")
		jsonEqual(assert, `{"level":"error","error":"err","k":"v","message":"msg"}`, strings.TrimSpace(buf.String()))
	}
}
