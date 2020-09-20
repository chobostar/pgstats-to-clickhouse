package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPgStatioTable_isSkippable_Panic(t *testing.T) {
	given := &PgStatioTable{}
	assertPanic(t, func() { given.isSkippable(&SomePgMetric{}) }, "Not PgStatioTable. Excepted panic.")
}

func TestPgStatioTable_Delta_Panic(t *testing.T) {
	given := &PgStatioTable{}
	assertPanic(t, func() { given.delta(&SomePgMetric{}) }, "Not PgStatioTable. Excepted panic.")
}

func TestPgStatioTableFactory_Name(t *testing.T) {
	given := &PgStatioTableFactory{}
	assert.Equal(t, "PgStatioTable", given.Name())
}

func getMockPgStatioSlice() []PgMetric {
	mock := make([]PgMetric, 0, 1)
	mock = append(mock, getMockPgStatio())
	return mock
}

func getMockPgStatio() PgMetric {
	return &PgStatioTable{
		datname:           "postgres",
		schemaname:        "public",
		tablename:         "test",
		heap_blks_read:    0,
		heap_blks_hit:     1,
		idx_blks_read:     2,
		idx_blks_hit:      3,
		toast_blks_read:   4,
		toast_blks_hit:    5,
		tidx_blks_read:    6,
		tidx_blks_hit:     7,
		seq_scan:          8,
		seq_tup_read:      9,
		idx_scan:          10,
		idx_tup_fetch:     11,
		n_tup_ins:         12,
		n_tup_upd:         13,
		n_tup_del:         14,
		n_tup_hot_upd:     15,
		vacuum_count:      16,
		autovacuum_count:  17,
		analyze_count:     18,
		autoanalyze_count: 19,
	}
}

func TestPgStatioTable_Delta(t *testing.T) {
	newSnap := &PgStatioTable{
		datname:           "postgres",
		schemaname:        "public",
		tablename:         "test",
		heap_blks_read:    0,
		heap_blks_hit:     2,
		idx_blks_read:     4,
		idx_blks_hit:      6,
		toast_blks_read:   8,
		toast_blks_hit:    10,
		tidx_blks_read:    12,
		tidx_blks_hit:     14,
		seq_scan:          16,
		seq_tup_read:      18,
		idx_scan:          20,
		idx_tup_fetch:     22,
		n_tup_ins:         24,
		n_tup_upd:         26,
		n_tup_del:         28,
		n_tup_hot_upd:     30,
		vacuum_count:      32,
		autovacuum_count:  34,
		analyze_count:     36,
		autoanalyze_count: 38,
	}

	oldSnap := getMockPgStatio()

	delta := newSnap.delta(oldSnap)

	assert.Equal(t, oldSnap, delta, "Delta is wrong")
}

func TestPgStatioTable_Delta_AfterReset(t *testing.T) {
	newSnap := &PgStatioTable{
		datname:           "postgres",
		schemaname:        "public",
		tablename:         "test",
		heap_blks_read:    0,
		heap_blks_hit:     0,
		idx_blks_read:     0,
		idx_blks_hit:      0,
		toast_blks_read:   0,
		toast_blks_hit:    0,
		tidx_blks_read:    0,
		tidx_blks_hit:     0,
		seq_scan:          0,
		seq_tup_read:      0,
		idx_scan:          0,
		idx_tup_fetch:     0,
		n_tup_ins:         0,
		n_tup_upd:         0,
		n_tup_del:         0,
		n_tup_hot_upd:     0,
		vacuum_count:      0,
		autovacuum_count:  0,
		analyze_count:     0,
		autoanalyze_count: 0,
	}

	oldSnap := getMockPgStatio()

	delta := newSnap.delta(oldSnap)

	assert.Equal(t, newSnap, delta, "Delta is wrong")
}

func TestStatsCollector_Push_PgStatioTable(t *testing.T) {
	sc, err := NewStatsCollector(&PgStatioTableFactory{}, "hostname", postgresDockerDsn, clickhouseDockerDsn, 60)
	assert.Empty(t, err, "error init collector")

	given := getMockPgStatioSlice()

	assert.NoError(t, sc.Push(given), "error during push metrics")
}
