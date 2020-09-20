package internal

import (
	"database/sql"
	"fmt"
)

type PgTableSizeFactory struct{}

type PgTableSize struct {
	datname    string
	schemaname string
	tablename  string
	n_live_tup float64
	n_dead_tup float64
	size       float64
	idx_size   float64
}

func (f *PgTableSizeFactory) Name() string {
	return "PgTableSize"
}

func (f *PgTableSizeFactory) CollectQuery() string {
	//main query to get metrics
	return `SELECT
			  current_database() datname,
			  schemaname,
			  relname as tablename,
			  n_live_tup,
			  n_dead_tup,
			  pg_table_size(relid) AS size,
			  pg_indexes_size(relid) AS idx_size
			FROM pg_stat_user_tables 
			WHERE schemaname NOT IN ('pg_catalog', 'pg_toast', 'information_schema')`
}

func (f *PgTableSizeFactory) PushQuery() string {
	//query to store in clickhouse populated data with hostname
	return `INSERT INTO pg.pg_table_size_buffer(
						hostname,
						datname,
						schemaname,
						tablename,
						n_live_tup,
						n_dead_tup,
						size,
						idx_size) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
}

func (f *PgTableSizeFactory) NewMetric(rows *sql.Rows) (PgMetric, error) {
	metric := new(PgTableSize)

	err := rows.Scan(
		&metric.datname,
		&metric.schemaname,
		&metric.tablename,
		&metric.n_live_tup,
		&metric.n_dead_tup,
		&metric.size,
		&metric.idx_size,
	)
	if err != nil {
		return nil, err
	}
	return metric, nil
}

func (p *PgTableSize) isSkippable(old PgMetric) bool {
	_, ok := old.(*PgTableSize)
	if !ok {
		panic(fmt.Sprintf("isSkippable: this is not PgTableSize: %v", old))
	}
	// PgTableSize не пропускаются пишутся всегда, т.к. это GAUGE метрика
	return false
}

func (p *PgTableSize) delta(old PgMetric) PgMetric {
	_, ok := old.(*PgTableSize)
	if !ok {
		panic(fmt.Sprintf("delta: this is not PgTableSize: %v", old))
	}

	return &PgTableSize{
		datname:    p.datname,
		schemaname: p.schemaname,
		tablename:  p.tablename,
		n_live_tup: p.n_live_tup,
		n_dead_tup: p.n_dead_tup,
		size:       p.size,
		idx_size:   p.idx_size,
	}
}

func (p *PgTableSize) getHash() uint32 {
	return getHash(p.datname, p.schemaname, p.tablename)
}

func (p *PgTableSize) getValue(hostname string) []interface{} {
	return []interface{}{
		hostname,
		&p.datname,
		&p.schemaname,
		&p.tablename,
		&p.n_live_tup,
		&p.n_dead_tup,
		&p.size,
		&p.idx_size,
	}
}
