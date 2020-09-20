package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func getMockPgTableSize() PgMetric {
	return &PgTableSize{
		datname:    "postgres",
		schemaname: "public",
		tablename:  "test",
		n_live_tup: 0,
		n_dead_tup: 1,
		size:       2,
		idx_size:   3,
	}
}

func getMockPgTableSizeSlice() []PgMetric {
	mock := make([]PgMetric, 0, 1)
	mock = append(mock, getMockPgTableSize())
	return mock
}

func TestPgTableSize_isSkippable_Panic(t *testing.T) {
	given := &PgTableSize{}
	assertPanic(t, func() { given.isSkippable(&SomePgMetric{}) }, "Not PgTableSize. Excepted panic.")
}

func TestPgTableSize_isSkippable(t *testing.T) {
	given := &PgTableSize{}
	assert.Equal(t, false, given.isSkippable(&PgTableSize{}), "PgTableSize should always return true")
}

func TestPgTableSize_Delta_Panic(t *testing.T) {
	given := &PgTableSize{}
	assertPanic(t, func() { given.isSkippable(&SomePgMetric{}) }, "Not PgTableSize. Excepted panic.")
}

func TestPgTableSize_Delta(t *testing.T) {
	given := getMockPgTableSize()
	expected := getMockPgTableSize()
	assert.Equal(t, expected, given.delta(given), "Delta should be mocked - return the same value")
}

func TestStatsCollector_Push_PgTableSize(t *testing.T) {
	sc, err := NewStatsCollector(&PgTableSizeFactory{}, "hostname", postgresDockerDsn, clickhouseDockerDsn, 60)
	assert.Empty(t, err, "error init collector")

	given := getMockPgTableSizeSlice()

	assert.NoError(t, sc.Push(given), "error during push metrics")
}
