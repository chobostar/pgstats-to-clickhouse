package internal

import (
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/stdlib"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/mailru/go-clickhouse"
	"time"
)

var (
	pgMaxOpenConns = 1 // use only one connection per collector
	pgMaxIdleConns = 0 // disable pooling
)

// StatsCollector - хранит последний state снапшота метрик и при отправке считает дельты по ней.
//    не считает дельту и не отправляет метрики, снапшот истек по ttl
type StatsCollector struct {
	hostname string
	cf       CollectorFactory
	postgres *sql.DB
	ch       *sql.DB
	snapshot *PgStatMetrics
	ttl      int64
}

// PgMetric метрики postgres-а с которым оперирует StatsCollector
type PgMetric interface {
	isSkippable(old PgMetric) bool
	delta(old PgMetric) PgMetric
	getHash() uint32
	getValue(hostname string) []interface{}
}

// CollectorFactory читает метрики определенной структуры и ответственнен за sql запросы
type CollectorFactory interface {
	Name() string
	CollectQuery() string
	NewMetric(rows *sql.Rows) (PgMetric, error)
	PushQuery() string
}

// PgStatMetrics - хранит метрики и hash map по ключам метрик для подсчета delta между отравками, version - время сбора
type PgStatMetrics struct {
	rows     []PgMetric
	keysHash map[uint32]int
	version  int64
}

func NewStatsCollector(collector CollectorFactory, hostname string, postgresDsn string, clickhouseDsn string, ttl int64) (*StatsCollector, error) {
	connConfig, _ := pgx.ParseConfig(postgresDsn)
	connConfig.PreferSimpleProtocol = true
	connStr := stdlib.RegisterConnConfig(connConfig)
	postgres, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("postgres conn failed with: %w", err)
	}
	if err = postgres.Ping(); err != nil {
		return nil, fmt.Errorf("postgres ping failed with: %w", err)
	}
	postgres.SetMaxOpenConns(pgMaxOpenConns)
	postgres.SetMaxIdleConns(pgMaxIdleConns)

	ch, err := sql.Open("clickhouse", clickhouseDsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse conn failed with: %w", err)
	}
	if err = ch.Ping(); err != nil {
		return nil, fmt.Errorf("clickhouse ping failed with: %w", err)
	}
	sc := &StatsCollector{
		cf:       collector,
		hostname: hostname,
		postgres: postgres,
		ch:       ch,
		ttl:      ttl,
	}
	sc.snapshot, err = sc.Collect()
	if err != nil {
		return nil, fmt.Errorf("can't save initial stats snapshot: %w", err)
	}
	return sc, nil
}

//Tick is main loop
func (sc *StatsCollector) Tick() error {
	newSnap, err := sc.Collect()
	if err != nil {
		return fmt.Errorf("collect failed with: %w", err)
	}
	deltaMetrics, err := sc.Merge(newSnap)
	if err != nil {
		return fmt.Errorf("merge failed with: %w", err)
	}
	err = sc.Push(deltaMetrics)
	if err != nil {
		return fmt.Errorf("push failed: %w", err)
	}
	return nil
}

func (sc *StatsCollector) Collect() (*PgStatMetrics, error) {
	rows, err := sc.postgres.Query(sc.cf.CollectQuery())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	metrics := make([]PgMetric, 0)
	keysHash := make(map[uint32]int)
	i := 0
	for rows.Next() {
		metric, err := sc.cf.NewMetric(rows)
		if err != nil {
			return nil, err
		}
		metrics = append(metrics, metric)
		keysHash[metric.getHash()] = i
		i++
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return &PgStatMetrics{
		rows:     metrics,
		version:  time.Now().Unix(),
		keysHash: keysHash,
	}, nil
}

/*
	Допускается коллизия или data loss при вызовах pg_stat_statements_reset().
	Сравнивается предыдущий снапшот и считается дельта при допустимом snapshot stale по ttl.
*/
func (sc *StatsCollector) Merge(metrics *PgStatMetrics) ([]PgMetric, error) {
	if metrics.version-sc.snapshot.version > sc.ttl {
		sc.snapshot = metrics
		return nil, fmt.Errorf("metrics snapshot ttl is expired")
	}
	mergedRows := make([]PgMetric, 0, len(metrics.rows))

	for k, mIdx := range metrics.keysHash {
		if sIdx, ok := sc.snapshot.keysHash[k]; ok {
			// экономим на метриках, если не было вызовов не отправляем ничего
			if metrics.rows[mIdx].isSkippable(sc.snapshot.rows[sIdx]) {
				continue
			}
			mergedRows = append(mergedRows, metrics.rows[mIdx].delta(sc.snapshot.rows[sIdx]))
		} else {
			mergedRows = append(mergedRows, metrics.rows[mIdx])
		}
	}
	sc.snapshot = metrics
	return mergedRows, nil
}

func (sc *StatsCollector) Push(metrics []PgMetric) error {
	tx, err := sc.ch.Begin()
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			err = tx.Rollback()
			panic(p)
		} else if err != nil {
			_ = tx.Rollback()
		}
	}()

	stmt, err := tx.Prepare(sc.cf.PushQuery())
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, metric := range metrics {
		if _, err := stmt.Exec(
			metric.getValue(sc.hostname)...,
		); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (sc *StatsCollector) Shutdown() error {
	if err := sc.postgres.Close(); err != nil {
		return fmt.Errorf("error closing postgres: %w", err)
	}
	if err := sc.ch.Close(); err != nil {
		return fmt.Errorf("error closing clickhouse: %w", err)
	}
	return nil
}
