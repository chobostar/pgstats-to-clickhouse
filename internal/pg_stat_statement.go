package internal

import (
	"database/sql"
	"fmt"
)

type PgStatStatementsFactory struct{}

type PgStatStatement struct {
	queryid             float64
	datname             string
	username            string
	query               string
	calls               float64
	total_time          float64
	rows                float64
	shared_blks_hit     float64
	shared_blks_read    float64
	shared_blks_dirtied float64
	shared_blks_written float64
	local_blks_hit      float64
	local_blks_read     float64
	local_blks_dirtied  float64
	local_blks_written  float64
	temp_blks_read      float64
	temp_blks_written   float64
	blk_read_time       float64
	blk_write_time      float64
}

func (f *PgStatStatementsFactory) Name() string {
	return "PgStatStatements"
}

func (f *PgStatStatementsFactory) CollectQuery() string {
	//main query to get metrics
	return `SELECT
				queryid,
				datname,
				pg_catalog.pg_get_userbyid(userid) username,
				left(query, 3000) as query, 
				calls as calls, 
				total_time as total_time, 
				rows as rows,
				shared_blks_hit,
				shared_blks_read,
				shared_blks_dirtied,
				shared_blks_written,
				local_blks_hit,
				local_blks_read,
				local_blks_dirtied,
				local_blks_written,
				temp_blks_read,
				temp_blks_written,
				blk_read_time,
				blk_write_time
			FROM pg_stat_statements 
			JOIN pg_database ON pg_stat_statements.dbid = pg_database.oid
			ORDER BY queryid, datname, username, query`
}

func (f *PgStatStatementsFactory) PushQuery() string {
	//query to store in clickhouse populated data with hostname
	return `INSERT INTO pg.pg_stat_statements_buffer(
					hostname,
					datname,
					username,
					query,
					calls,
					total_time,
					rows,
					shared_blks_hit,
					shared_blks_read,
					shared_blks_dirtied,
					shared_blks_written,
					local_blks_hit,
					local_blks_read,
					local_blks_dirtied,
					local_blks_written,
					temp_blks_read,
					temp_blks_written,
					blk_read_time,
					blk_write_time) VALUES (
						?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
					)`
}

func (f *PgStatStatementsFactory) NewMetric(rows *sql.Rows) (PgMetric, error) {
	metric := new(PgStatStatement)
	err := rows.Scan(
		&metric.queryid,
		&metric.datname,
		&metric.username,
		&metric.query,
		&metric.calls,
		&metric.total_time,
		&metric.rows,
		&metric.shared_blks_hit,
		&metric.shared_blks_read,
		&metric.shared_blks_dirtied,
		&metric.shared_blks_written,
		&metric.local_blks_hit,
		&metric.local_blks_read,
		&metric.local_blks_dirtied,
		&metric.local_blks_written,
		&metric.temp_blks_read,
		&metric.temp_blks_written,
		&metric.blk_read_time,
		&metric.blk_write_time,
	)
	if err != nil {
		return nil, err
	}
	return metric, nil
}

func (pss *PgStatStatement) isSkippable(old PgMetric) bool {
	v, ok := old.(*PgStatStatement)
	if !ok {
		panic(fmt.Sprintf("isSkippable: this is not PgStatStatement: %v", old))
	}
	// если после обнуление счетчика новое значение будет совпадать со старым значением будет data loss
	return int64(pss.calls) == int64(v.calls)
}

func (pss *PgStatStatement) delta(old PgMetric) PgMetric {
	v, ok := old.(*PgStatStatement)
	if !ok {
		panic(fmt.Sprintf("delta: this is not PgStatStatement: %v", old))
	}

	// если такое произошло, значит или коллизия хэша или обнулили счетчик
	// здесь обрабатывается только обнуление счетчика
	if v.calls > pss.calls {
		return &PgStatStatement{
			queryid:             pss.queryid,
			datname:             pss.datname,
			username:            pss.username,
			query:               pss.query,
			calls:               pss.calls,
			total_time:          pss.total_time,
			rows:                pss.rows,
			shared_blks_hit:     pss.shared_blks_hit,
			shared_blks_read:    pss.shared_blks_read,
			shared_blks_dirtied: pss.shared_blks_dirtied,
			shared_blks_written: pss.shared_blks_written,
			local_blks_hit:      pss.local_blks_hit,
			local_blks_read:     pss.local_blks_read,
			local_blks_dirtied:  pss.local_blks_dirtied,
			local_blks_written:  pss.local_blks_written,
			temp_blks_read:      pss.temp_blks_read,
			temp_blks_written:   pss.temp_blks_written,
			blk_read_time:       pss.blk_read_time,
			blk_write_time:      pss.blk_write_time,
		}
	} else {
		return &PgStatStatement{
			queryid:             pss.queryid,
			datname:             pss.datname,
			username:            pss.username,
			query:               pss.query,
			calls:               pss.calls - v.calls,
			total_time:          pss.total_time - v.total_time,
			rows:                pss.rows - v.rows,
			shared_blks_hit:     pss.shared_blks_hit - v.shared_blks_hit,
			shared_blks_read:    pss.shared_blks_read - v.shared_blks_read,
			shared_blks_dirtied: pss.shared_blks_dirtied - v.shared_blks_dirtied,
			shared_blks_written: pss.shared_blks_written - v.shared_blks_written,
			local_blks_hit:      pss.local_blks_hit - v.local_blks_hit,
			local_blks_read:     pss.local_blks_read - v.local_blks_read,
			local_blks_dirtied:  pss.local_blks_dirtied - v.local_blks_dirtied,
			local_blks_written:  pss.local_blks_written - v.local_blks_written,
			temp_blks_read:      pss.temp_blks_read - v.temp_blks_read,
			temp_blks_written:   pss.temp_blks_written - v.temp_blks_written,
			blk_read_time:       pss.blk_read_time - v.blk_read_time,
			blk_write_time:      pss.blk_write_time - v.blk_write_time,
		}
	}
}

/*
	как здесь написано, допускается использовать queryid в комбо с dbid, userid для идентификации запросов
	https://www.postgresql.org/docs/current/pgstatstatements.html
*/
func (pss *PgStatStatement) getHash() uint32 {
	return getHash(fmt.Sprintf("%f", pss.queryid), pss.datname, pss.username)
}

func (pss *PgStatStatement) getValue(hostname string) []interface{} {
	return []interface{}{
		hostname,
		pss.datname,
		pss.username,
		pss.query,
		pss.calls,
		pss.total_time,
		pss.rows,
		pss.shared_blks_hit,
		pss.shared_blks_read,
		pss.shared_blks_dirtied,
		pss.shared_blks_written,
		pss.local_blks_hit,
		pss.local_blks_read,
		pss.local_blks_dirtied,
		pss.local_blks_written,
		pss.temp_blks_read,
		pss.temp_blks_written,
		pss.blk_read_time,
		pss.blk_write_time,
	}
}
