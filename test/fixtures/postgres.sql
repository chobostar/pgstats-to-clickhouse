drop table if exists pg_stat_statements;

create table if not exists pg_stat_statements (
     userid oid,
     dbid  oid,
     queryid  bigint,
     query text,
     calls bigint,
     total_time double precision,
     rows  bigint,
     shared_blks_hit bigint,
     shared_blks_read bigint,
     shared_blks_dirtied bigint,
     shared_blks_written bigint,
     local_blks_hit bigint,
     local_blks_read bigint,
     local_blks_dirtied bigint,
     local_blks_written bigint,
     temp_blks_read bigint,
     temp_blks_written bigint,
     blk_read_time double precision,
     blk_write_time double precision
);

insert into pg_stat_statements
select
    (select usesysid from pg_user where usename = 'postgres' limit 1),
    (select oid from pg_database where datname = 'postgres'),
    0,
    'select 1',
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14,
    15;
