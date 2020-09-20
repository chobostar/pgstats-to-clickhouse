CREATE DATABASE IF NOT EXISTS pg;

CREATE TABLE IF NOT EXISTS pg.pg_stat_statements (
     created_date Date DEFAULT today(),
     created_at UInt32 DEFAULT toUInt32(now()) Codec(Delta, ZSTD),
     created_hour UInt32 DEFAULT toUInt32(toStartOfHour(now())) Codec(Delta, ZSTD),
     hostname LowCardinality(String),
     datname LowCardinality(String),
     username LowCardinality(String),
     query String,
     calls Float64,
     total_time Float64,
     rows Float64,
     shared_blks_hit Float64,
     shared_blks_read Float64,
     shared_blks_dirtied Float64,
     shared_blks_written Float64,
     local_blks_hit Float64,
     local_blks_read Float64,
     local_blks_dirtied Float64,
     local_blks_written Float64,
     temp_blks_read Float64,
     temp_blks_written Float64,
     blk_read_time Float64,
     blk_write_time Float64
) ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/pg_stat_statements', '{replica}')
    PARTITION BY created_date
    ORDER BY (created_hour, hostname, created_at, datname, username)
    TTL created_date + toIntervalDay(3)
    SETTINGS index_granularity = 8192;

--https://clickhouse.tech/docs/en/operations/table_engines/buffer/
CREATE TABLE IF NOT EXISTS pg.pg_stat_statements_buffer AS pg.pg_stat_statements ENGINE = Buffer(pg, pg_stat_statements, 16, 10, 30, 1000, 10000, 1000000, 10000000);

CREATE TABLE IF NOT EXISTS pg.pg_statio_tables (
     created_date Date DEFAULT today(),
     created_at UInt32 DEFAULT toUInt32(now()) Codec(Delta, ZSTD),
     created_hour UInt32 DEFAULT toUInt32(toStartOfHour(now())) Codec(Delta, ZSTD),
     hostname LowCardinality(String),
     datname LowCardinality(String),
     schemaname String,
     tablename String,
     heap_blks_read Float64,
     heap_blks_hit Float64,
     idx_blks_read Float64,
     idx_blks_hit Float64,
     toast_blks_read Float64,
     toast_blks_hit Float64,
     tidx_blks_read Float64,
     tidx_blks_hit Float64,
     seq_scan Float64,
     seq_tup_read Float64,
     idx_scan Float64,
     idx_tup_fetch Float64,
     n_tup_ins Float64,
     n_tup_upd Float64,
     n_tup_del Float64,
     n_tup_hot_upd Float64,
     vacuum_count Float64,
     autovacuum_count Float64,
     analyze_count Float64,
     autoanalyze_count Float64
) ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/pg_statio_tables', '{replica}')
    PARTITION BY created_date
    ORDER BY (created_hour, hostname, created_at, datname)
    TTL created_date + toIntervalDay(3)
    SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pg.pg_statio_tables_buffer AS pg.pg_statio_tables ENGINE = Buffer(pg, pg_statio_tables, 16, 10, 30, 1000, 10000, 1000000, 10000000);

CREATE TABLE IF NOT EXISTS pg.pg_table_size (
    created_date Date DEFAULT today(),
    created_at UInt32 DEFAULT toUInt32(now()) Codec(Delta, ZSTD),
    created_hour UInt32 DEFAULT toUInt32(toStartOfHour(now())) Codec(Delta, ZSTD),
    hostname LowCardinality(String),
    datname LowCardinality(String),
    schemaname String,
    tablename String,
    n_live_tup Float64,
    n_dead_tup Float64,
    size Float64,
    idx_size Float64
) ENGINE = ReplicatedMergeTree('/clickhouse/{cluster}/tables/{shard}/pg_table_size', '{replica}')
    PARTITION BY created_date
    ORDER BY (created_hour, hostname, created_at, datname)
    TTL created_date + toIntervalDay(12)
    SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS pg.pg_table_size_buffer AS pg.pg_table_size ENGINE = Buffer(pg, pg_table_size, 16, 10, 30, 1000, 10000, 1000000, 10000000);
