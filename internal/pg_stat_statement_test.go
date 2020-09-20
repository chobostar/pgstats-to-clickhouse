package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPgStatsStatement_isSkippable(t *testing.T) {
	given := &PgStatStatement{}
	assertPanic(t, func() { given.isSkippable(&SomePgMetric{}) }, "Not PgStatStatement. Excepted panic.")
}

func TestPgStatsStatement_delta(t *testing.T) {
	given := &PgStatStatement{}
	assertPanic(t, func() { given.delta(&SomePgMetric{}) }, "Not PgStatStatement. Excepted panic.")
}

func TestPgStatsStatement_Name(t *testing.T) {
	given := &PgStatStatementsFactory{}
	assert.Equal(t, "PgStatStatements", given.Name())
}
