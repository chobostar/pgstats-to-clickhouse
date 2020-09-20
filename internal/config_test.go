package internal

import (
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestNewConfig(t *testing.T) {
	var (
		givenInterval      = "60s"
		givenPostgresDsn   = "mock_postgresDsn"
		givenClickhouseDsn = "mock_clickhouseDsn"
	)
	os.Setenv("INTERVAL", givenInterval)
	os.Setenv("POSTGRES_DSN", givenPostgresDsn)
	os.Setenv("CLICKHOUSE_DSN", givenClickhouseDsn)

	actualConfig, err := NewConfig()
	if err != nil {
		t.Error(err.Error())
		return
	}

	parsedGivenInterval, _ := time.ParseDuration(givenInterval)
	assert.Equal(t, actualConfig.Interval, parsedGivenInterval, "Not correct Interval parsed")
	assert.Equal(t, actualConfig.PostgresDsn, givenPostgresDsn, "Not correct Interval parsed")
	assert.Equal(t, actualConfig.ClickhouseDsn, givenClickhouseDsn, "Not correct Interval parsed")
}
