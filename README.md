pgstat-to-clickhouse
====================

Collects pg_stat_statements from postgresql and pushes to remote clickhouse.
Also optionally can collect (if `STATIO_POSTGRES_DSN` is defined):
 - pg_statio_user_tables
 - pg_stat_user_tables

See logic in `StatsCollector.Merge`.

Features:
- counts delta (counter -> gauge)
- skips not changing metrics (not for gauge metrics like n_live_tup, n_dead_tup, relation_size)

#### Example Dashboards
![pg_stat_statements](examples/img/2e640f2055.png)

![pg_stat_tables](examples/img/62add3afdb.png)

Quick demo:
```
make up
make build

env INTERVAL="30s" \
     POSTGRES_DSN="postgres://postgres@localhost:5432/postgres?sslmode=disable" \
     CLICKHOUSE_DSN="http://localhost:8123/default" \
     STATIO_POSTGRES_DSN="postgres://postgres@localhost:5432/postgres?sslmode=disable" \
     ./bin/pgstats-to-clickhouse

docker-compose -f ./test/docker-compose.yml exec -T postgres psql -U postgres -c "drop table pg_stat_statements"
docker-compose -f ./test/docker-compose.yml exec -T postgres psql -U postgres -c "create extension pg_stat_statements"
docker-compose -f ./test/docker-compose.yml exec -T postgres pgbench -U postgres -i

docker-compose -f ./test/docker-compose.yml exec -T postgres pgbench -U postgres
...
# wait 30 sec
...
docker-compose -f ./test/docker-compose.yml exec -T postgres pgbench -U postgres
```
open: http://localhost:3000/d/TzHfPjPnz/pg_top_queries

#### Requirements

tested on
```
Clickhouse 20.1
PostgreSQL 11
Go 1.14
```

- postgres user should have atleast `pg_monitor` role, otherwise it will fail with error about queryid is NULL
- postgres user should have connect grants

#### Usage:
```
env INTERVAL="60s" \
    POSTGRES_DSN="postgres://postgres@localhost:5432/postgres?sslmode=disable" \
    CLICKHOUSE_DSN="http://localhost:8123/default" \
    STATIO_POSTGRES_DSN="postgres://postgres@localhost:5432/postgres?sslmode=disable"
    pgstats-to-clickhouse
```

#### Params:
- `INTERVAL` - collect interval in seconds (default: "30s", valid units are "ns", "us" (or "µs"), "ms", "s", "m", "h")
- `POSTGRES_DSN` - connection to postgres (default: "postgres://postgres@localhost:5432/postgres?sslmode=disable")
- `STATIO_POSTGRES_DSN` - connection to postgres user's database for tables monitores (default: "")
- `CLICKHOUSE_DSN` - connection to clickhouse (default: "http://localhost:8123/default"). As `schema://user:password@host[:port]/database?param1=value1&...&paramN=valueN`
    - example: `http://user:password@host:8123/clicks?timeout=5s&read_timeout=10s&write_timeout=20s`

#### Dev

```
$ make test-full
```

#### Known possible issues
- hash collision is not handled
- possible data loss or not honest metrics after `pg_stat_statements_reset()`
- hangs on during network calls if packets are dropped. Can't be interrapted by SIGINT (solved by connect_timeout)
- data loss if clickhouse is not accessable
- can open up to 3 connection in a once, if 3 collectors are used

#### Caveat
- pg_stat_stamenents file on disk can be too huge and it causes disk swap during reading pg_stat_statements' view. Use small value `pg_stat_statements.max`
- in case of 1000+ amount of schemas and tables `pgstats-to-clickhouse` daemon may use too much memory, because it holds previous metrics' snapshot. 1 table == 1 entry.
