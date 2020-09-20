package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	postgresDockerDsn   = "postgres://postgres@localhost:5432/postgres?sslmode=disable"
	clickhouseDockerDsn = "http://127.0.0.1:8123/default"
)

func getDefaultMock() PgMetric {
	return &PgStatStatement{
		queryid:             0,
		datname:             "postgres",
		username:            "postgres",
		query:               "select 1",
		calls:               1,
		total_time:          2,
		rows:                3,
		shared_blks_hit:     4,
		shared_blks_read:    5,
		shared_blks_dirtied: 6,
		shared_blks_written: 7,
		local_blks_hit:      8,
		local_blks_read:     9,
		local_blks_dirtied:  10,
		local_blks_written:  11,
		temp_blks_read:      12,
		temp_blks_written:   13,
		blk_read_time:       14,
		blk_write_time:      15,
	}
}

func getDefaultMockSlice() []PgMetric {
	mock := make([]PgMetric, 0, 1)
	mock = append(mock, getDefaultMock())
	return mock
}

func TestStatsCollector_Collect(t *testing.T) {
	var givenHostname = "hostname"

	sc, err := NewStatsCollector(&PgStatStatementsFactory{}, givenHostname, postgresDockerDsn, clickhouseDockerDsn, 60)
	assert.Empty(t, err, "error init collector")

	assert.Equal(t, givenHostname, sc.hostname, "hostname is not initiated")

	excepted := getDefaultMockSlice()
	assert.Equal(t, excepted, sc.snapshot.rows, "Wrong snapshot is initiated")

	// put mock snapshot
	mock := make([]PgMetric, 0, 1)
	mock = append(mock,
		&PgStatStatement{
			queryid:             0,
			datname:             "postgres",
			username:            "postgres",
			query:               "select 1",
			calls:               0,
			total_time:          0,
			rows:                0,
			shared_blks_hit:     0,
			shared_blks_read:    0,
			shared_blks_dirtied: 0,
			shared_blks_written: 0,
			local_blks_hit:      0,
			local_blks_read:     0,
			local_blks_dirtied:  0,
			local_blks_written:  0,
			temp_blks_read:      0,
			temp_blks_written:   0,
			blk_read_time:       0,
			blk_write_time:      0,
		})
	sc.snapshot.rows = mock

	metrics, err := sc.Collect()
	if err != nil {
		t.Error(err.Error())
		return
	}

	assert.Equal(t, excepted, metrics.rows, "Wrong data collected")
}

func TestStatsCollector_Push(t *testing.T) {
	sc, err := NewStatsCollector(&PgStatStatementsFactory{}, "hostname", postgresDockerDsn, clickhouseDockerDsn, 60)
	assert.Empty(t, err, "error init collector")

	given := getDefaultMockSlice()

	assert.NoError(t, sc.Push(given), "error during push metrics")
}

func TestStatsCollector_Delta(t *testing.T) {
	newSnap := &PgStatStatement{
		queryid:             0,
		datname:             "postgres",
		username:            "postgres",
		query:               "select 1",
		calls:               2,
		total_time:          4,
		rows:                6,
		shared_blks_hit:     8,
		shared_blks_read:    10,
		shared_blks_dirtied: 12,
		shared_blks_written: 14,
		local_blks_hit:      16,
		local_blks_read:     18,
		local_blks_dirtied:  20,
		local_blks_written:  22,
		temp_blks_read:      24,
		temp_blks_written:   26,
		blk_read_time:       28,
		blk_write_time:      30,
	}

	oldSnap := getDefaultMock()

	delta := newSnap.delta(oldSnap)

	assert.Equal(t, oldSnap, delta, "Delta is wrong")
}

func TestStatsCollector_Delta_AfterReset(t *testing.T) {
	newSnap := &PgStatStatement{
		queryid:             0,
		datname:             "postgres",
		username:            "postgres",
		query:               "select 1",
		calls:               0, // calls < oldSnap.calls
		total_time:          0,
		rows:                0,
		shared_blks_hit:     0,
		shared_blks_read:    0,
		shared_blks_dirtied: 0,
		shared_blks_written: 0,
		local_blks_hit:      0,
		local_blks_read:     0,
		local_blks_dirtied:  0,
		local_blks_written:  0,
		temp_blks_read:      0,
		temp_blks_written:   0,
		blk_read_time:       0,
		blk_write_time:      0,
	}

	oldSnap := getDefaultMock()

	delta := newSnap.delta(oldSnap)

	assert.Equal(t, newSnap, delta, "Delta is wrong")
}

func TestStatsCollector_Merge_StaleErr(t *testing.T) {
	var givenTtl = int64(60)
	var tooHighInterval = int64(givenTtl * 2)

	sc, err := NewStatsCollector(&PgStatStatementsFactory{}, "hostname", postgresDockerDsn, clickhouseDockerDsn, givenTtl)
	assert.Empty(t, err, "error init collector")

	mock := make([]PgMetric, 0, 1)
	newSnap := &PgStatStatement{
		queryid:             0,
		datname:             "postgres",
		username:            "postgres",
		query:               "select 1",
		calls:               2,
		total_time:          4,
		rows:                6,
		shared_blks_hit:     8,
		shared_blks_read:    10,
		shared_blks_dirtied: 12,
		shared_blks_written: 14,
		local_blks_hit:      16,
		local_blks_read:     18,
		local_blks_dirtied:  20,
		local_blks_written:  22,
		temp_blks_read:      24,
		temp_blks_written:   26,
		blk_read_time:       28,
		blk_write_time:      30,
	}
	mock = append(mock, newSnap)

	hash := newSnap.getHash()
	newState := &PgStatMetrics{
		rows:    mock,
		version: time.Now().Unix() + tooHighInterval,
		keysHash: map[uint32]int{
			hash: 0,
		},
	}

	_, err = sc.Merge(newState)
	assert.NotEmpty(t, err, "expected snapshot stale error")
	assert.Equal(t, mock, sc.snapshot.rows, "expected new snapshot rows")
}

func TestStatsCollector_Merge_Delta(t *testing.T) {
	sc, err := NewStatsCollector(&PgStatStatementsFactory{}, "hostname", postgresDockerDsn, clickhouseDockerDsn, 60)
	assert.Empty(t, err, "error init collector")

	mock := make([]PgMetric, 0, 1)
	newSnap := &PgStatStatement{
		queryid:             0,
		datname:             "postgres",
		username:            "postgres",
		query:               "select 1",
		calls:               2,
		total_time:          4,
		rows:                6,
		shared_blks_hit:     8,
		shared_blks_read:    10,
		shared_blks_dirtied: 12,
		shared_blks_written: 14,
		local_blks_hit:      16,
		local_blks_read:     18,
		local_blks_dirtied:  20,
		local_blks_written:  22,
		temp_blks_read:      24,
		temp_blks_written:   26,
		blk_read_time:       28,
		blk_write_time:      30,
	}
	mock = append(mock, newSnap)

	hash := newSnap.getHash()
	newState := &PgStatMetrics{
		rows:    mock,
		version: time.Now().Unix(),
		keysHash: map[uint32]int{
			hash: 0,
		},
	}

	excepted := getDefaultMockSlice()
	actual, err := sc.Merge(newState)
	assert.Empty(t, err, "should be ok, because version is set to Now()")
	assert.Equal(t, excepted, actual, "wrong merge")
}

func TestStatsCollector_Merge_Delta_Complicated(t *testing.T) {
	sc, err := NewStatsCollector(&PgStatStatementsFactory{}, "hostname", postgresDockerDsn, clickhouseDockerDsn, 60)
	assert.Empty(t, err, "error init collector")

	//в старый snapshot подсовываем метрику с queryid = 666 которого не будет в новом снапшоте
	metric := getDefaultMock()
	staleMetric := metric.(*PgStatStatement)
	staleMetric.queryid = 666
	hashStale := staleMetric.getHash()
	sc.snapshot.rows = append(sc.snapshot.rows, staleMetric)
	sc.snapshot.keysHash[hashStale] = len(sc.snapshot.rows) - 1

	//в новый snapshot создаем пару для существующей метрики queryid = 0, и новую метрику c queryid = 123
	mock := make([]PgMetric, 0, 1)
	newSnap := &PgStatStatement{
		queryid:             0,
		datname:             "postgres",
		username:            "postgres",
		query:               "select 1",
		calls:               2,
		total_time:          4,
		rows:                6,
		shared_blks_hit:     8,
		shared_blks_read:    10,
		shared_blks_dirtied: 12,
		shared_blks_written: 14,
		local_blks_hit:      16,
		local_blks_read:     18,
		local_blks_dirtied:  20,
		local_blks_written:  22,
		temp_blks_read:      24,
		temp_blks_written:   26,
		blk_read_time:       28,
		blk_write_time:      30,
	}
	snap := getDefaultMock()
	secondSnap := snap.(*PgStatStatement)
	secondSnap.queryid = 123
	mock = append(mock, newSnap, secondSnap)

	hashFirst := newSnap.getHash()
	hashSecond := secondSnap.getHash()
	newState := &PgStatMetrics{
		rows:    mock,
		version: time.Now().Unix(),
		keysHash: map[uint32]int{
			hashFirst:  0,
			hashSecond: 1,
		},
	}

	excepted := getDefaultMockSlice()
	oneMore := getDefaultMock()
	oneMoreExpected := oneMore.(*PgStatStatement)
	oneMoreExpected.queryid = 123
	excepted = append(excepted, oneMoreExpected)

	actual, err := sc.Merge(newState)
	assert.Empty(t, err, "should be ok, because version is set to Now()")
	assert.ElementsMatch(t, excepted, actual, "wrong merge")
}

func TestStatsCollector_Merge_Delta_Skippable(t *testing.T) {
	sc, err := NewStatsCollector(&PgStatStatementsFactory{}, "hostname", postgresDockerDsn, clickhouseDockerDsn, 60)
	assert.Empty(t, err, "error init collector")

	// подсовываем такую метрику как и в init
	metrics := getDefaultMockSlice()
	newState := &PgStatMetrics{
		rows:    getDefaultMockSlice(),
		version: time.Now().Unix(),
		keysHash: map[uint32]int{
			metrics[0].getHash(): 0,
		},
	}
	actual, err := sc.Merge(newState)
	assert.Empty(t, err, "error is not expected here")
	assert.Empty(t, actual, "metric must be skipped")
}
