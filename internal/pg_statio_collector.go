package internal

import (
	"database/sql"
	"fmt"
)

type PgStatioTableFactory struct{}

type PgStatioTable struct {
	datname           string
	schemaname        string
	tablename         string
	heap_blks_read    float64
	heap_blks_hit     float64
	idx_blks_read     float64
	idx_blks_hit      float64
	toast_blks_read   float64
	toast_blks_hit    float64
	tidx_blks_read    float64
	tidx_blks_hit     float64
	seq_scan          float64
	seq_tup_read      float64
	idx_scan          float64
	idx_tup_fetch     float64
	n_tup_ins         float64
	n_tup_upd         float64
	n_tup_del         float64
	n_tup_hot_upd     float64
	vacuum_count      float64
	autovacuum_count  float64
	analyze_count     float64
	autoanalyze_count float64
}

func (f *PgStatioTableFactory) Name() string {
	return "PgStatioTable"
}

func (f *PgStatioTableFactory) CollectQuery() string {
	//main query to get metrics
	return `SELECT
				current_database() datname,
				a.schemaname,
				a.relname tablename,
				heap_blks_read,
				heap_blks_hit,
				idx_blks_read,
				idx_blks_hit,
				toast_blks_read,
				toast_blks_hit,
				tidx_blks_read,
				tidx_blks_hit,
				seq_scan,
				seq_tup_read,
				idx_scan,
				idx_tup_fetch,
				n_tup_ins,
				n_tup_upd,
				n_tup_del,
				n_tup_hot_upd,
				vacuum_count,
				autovacuum_count,
				analyze_count,
				autoanalyze_count
			FROM pg_statio_user_tables a 
			JOIN pg_stat_user_tables b USING(relid) 
			WHERE a.schemaname not in ('pg_toast', 'information_schema')`
}

func (f *PgStatioTableFactory) PushQuery() string {
	//query to store in clickhouse populated data with hostname
	return `INSERT INTO pg.pg_statio_tables_buffer(
						hostname,
						datname,
						schemaname,
						tablename,
						heap_blks_read,
						heap_blks_hit,
						idx_blks_read,
						idx_blks_hit,
						toast_blks_read,
						toast_blks_hit,
						tidx_blks_read,
						tidx_blks_hit,
						seq_scan,
						seq_tup_read,
						idx_scan,
						idx_tup_fetch,
						n_tup_ins,
						n_tup_upd,
						n_tup_del,
						n_tup_hot_upd,
						vacuum_count,
						autovacuum_count,
						analyze_count,
						autoanalyze_count) VALUES (
						?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
					    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
						?, ?, ?, ?
					)`
}

func (f *PgStatioTableFactory) NewMetric(rows *sql.Rows) (PgMetric, error) {
	metric := new(PgStatioTable)

	var idx_blks_read,
		idx_blks_hit,
		toast_blks_read,
		toast_blks_hit,
		tidx_blks_read,
		tidx_blks_hit,
		idx_scan,
		idx_tup_fetch sql.NullFloat64

	err := rows.Scan(
		&metric.datname,
		&metric.schemaname,
		&metric.tablename,
		&metric.heap_blks_read,
		&metric.heap_blks_hit,
		&idx_blks_read,
		&idx_blks_hit,
		&toast_blks_read,
		&toast_blks_hit,
		&tidx_blks_read,
		&tidx_blks_hit,
		&metric.seq_scan,
		&metric.seq_tup_read,
		&idx_scan,
		&idx_tup_fetch,
		&metric.n_tup_ins,
		&metric.n_tup_upd,
		&metric.n_tup_del,
		&metric.n_tup_hot_upd,
		&metric.vacuum_count,
		&metric.autovacuum_count,
		&metric.analyze_count,
		&metric.autoanalyze_count,
	)
	if idx_blks_read.Valid {
		metric.toast_blks_read = idx_blks_read.Float64
	}
	if idx_blks_hit.Valid {
		metric.idx_blks_hit = idx_blks_hit.Float64
	}
	if toast_blks_read.Valid {
		metric.toast_blks_read = toast_blks_read.Float64
	}
	if toast_blks_hit.Valid {
		metric.toast_blks_read = toast_blks_hit.Float64
	}
	if tidx_blks_read.Valid {
		metric.toast_blks_read = tidx_blks_read.Float64
	}
	if tidx_blks_hit.Valid {
		metric.toast_blks_read = tidx_blks_hit.Float64
	}
	if idx_scan.Valid {
		metric.idx_scan = idx_scan.Float64
	}
	if idx_tup_fetch.Valid {
		metric.idx_tup_fetch = idx_tup_fetch.Float64
	}
	if err != nil {
		return nil, err
	}
	return metric, nil
}

func (p *PgStatioTable) isSkippable(old PgMetric) bool {
	v, ok := old.(*PgStatioTable)
	if !ok {
		panic(fmt.Sprintf("isSkippable: this is not PgStatioTable: %v", old))
	}
	// если после обнуление счетчика новое значение будет совпадать со старым значением будет data loss
	return int64(p.idx_scan) == int64(v.idx_scan) &&
		int64(p.seq_scan) == int64(v.seq_scan) &&
		int64(p.n_tup_ins) == int64(v.n_tup_ins) &&
		int64(p.n_tup_upd) == int64(v.n_tup_upd) &&
		int64(p.n_tup_del) == int64(v.n_tup_del)
}

func (p *PgStatioTable) delta(old PgMetric) PgMetric {
	v, ok := old.(*PgStatioTable)
	if !ok {
		panic(fmt.Sprintf("delta: this is not PgStatioTable: %v", old))
	}

	// если такое произошло, значит или коллизия хэша или обнулили счетчик
	// здесь обрабатывается только обнуление счетчика
	if v.idx_scan > p.idx_scan ||
		v.seq_scan > p.seq_scan ||
		v.n_tup_ins > p.n_tup_ins ||
		v.n_tup_upd > p.n_tup_upd ||
		v.n_tup_del > p.n_tup_del {
		return &PgStatioTable{
			datname:           p.datname,
			schemaname:        p.schemaname,
			tablename:         p.tablename,
			heap_blks_read:    p.heap_blks_read,
			heap_blks_hit:     p.heap_blks_hit,
			idx_blks_read:     p.idx_blks_read,
			idx_blks_hit:      p.idx_blks_hit,
			toast_blks_read:   p.toast_blks_read,
			toast_blks_hit:    p.toast_blks_hit,
			tidx_blks_read:    p.tidx_blks_read,
			tidx_blks_hit:     p.tidx_blks_hit,
			seq_scan:          p.seq_scan,
			seq_tup_read:      p.seq_tup_read,
			idx_scan:          p.idx_scan,
			idx_tup_fetch:     p.idx_tup_fetch,
			n_tup_ins:         p.n_tup_ins,
			n_tup_upd:         p.n_tup_upd,
			n_tup_del:         p.n_tup_del,
			n_tup_hot_upd:     p.n_tup_hot_upd,
			vacuum_count:      p.vacuum_count,
			autovacuum_count:  p.autovacuum_count,
			analyze_count:     p.analyze_count,
			autoanalyze_count: p.autoanalyze_count,
		}
	} else {
		return &PgStatioTable{
			datname:           p.datname,
			schemaname:        p.schemaname,
			tablename:         p.tablename,
			heap_blks_read:    p.heap_blks_read - v.heap_blks_read,
			heap_blks_hit:     p.heap_blks_hit - v.heap_blks_hit,
			idx_blks_read:     p.idx_blks_read - v.idx_blks_read,
			idx_blks_hit:      p.idx_blks_hit - v.idx_blks_hit,
			toast_blks_read:   p.toast_blks_read - v.toast_blks_read,
			toast_blks_hit:    p.toast_blks_hit - v.toast_blks_hit,
			tidx_blks_read:    p.tidx_blks_read - v.tidx_blks_read,
			tidx_blks_hit:     p.tidx_blks_hit - v.tidx_blks_hit,
			seq_scan:          p.seq_scan - v.seq_scan,
			seq_tup_read:      p.seq_tup_read - v.seq_tup_read,
			idx_scan:          p.idx_scan - v.idx_scan,
			idx_tup_fetch:     p.idx_tup_fetch - v.idx_tup_fetch,
			n_tup_ins:         p.n_tup_ins - v.n_tup_ins,
			n_tup_upd:         p.n_tup_upd - v.n_tup_upd,
			n_tup_del:         p.n_tup_del - v.n_tup_del,
			n_tup_hot_upd:     p.n_tup_hot_upd - v.n_tup_hot_upd,
			vacuum_count:      p.vacuum_count - v.vacuum_count,
			autovacuum_count:  p.autovacuum_count - v.autovacuum_count,
			analyze_count:     p.analyze_count - v.analyze_count,
			autoanalyze_count: p.autoanalyze_count - v.autoanalyze_count,
		}
	}
}

func (p *PgStatioTable) getHash() uint32 {
	return getHash(p.datname, p.schemaname, p.tablename)
}

func (p *PgStatioTable) getValue(hostname string) []interface{} {
	return []interface{}{
		hostname,
		&p.datname,
		&p.schemaname,
		&p.tablename,
		&p.heap_blks_read,
		&p.heap_blks_hit,
		&p.idx_blks_read,
		&p.idx_blks_hit,
		&p.toast_blks_read,
		&p.toast_blks_hit,
		&p.tidx_blks_read,
		&p.tidx_blks_hit,
		&p.seq_scan,
		&p.seq_tup_read,
		&p.idx_scan,
		&p.idx_tup_fetch,
		&p.n_tup_ins,
		&p.n_tup_upd,
		&p.n_tup_del,
		&p.n_tup_hot_upd,
		&p.vacuum_count,
		&p.autovacuum_count,
		&p.analyze_count,
		&p.autoanalyze_count,
	}
}
