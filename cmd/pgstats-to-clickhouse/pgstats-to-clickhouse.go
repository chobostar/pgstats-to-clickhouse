package main

import (
	"context"
	"gitlab.ozon.ru/infrastructure/ebuilds/pgstats-to-clickhouse/internal"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var usage = `pgstats-to-clickhouse - collects pg_stat_statements, pg_statio_all_tables and pg_stat_tables output and pushes to remote clickhouse

read settings from ENV:
	INTERVAL - collect interval in seconds (default: "30s", valid units are "ns", "us" (or "µs"), "ms", "s", "m", "h")
	POSTGRES_DSN - connection to postgres for pg_stat_statements (default: "postgres://postgres@localhost:5432/postgres?sslmode=disable")
	CLICKHOUSE_DSN - connection to clickhouse for pg_stat_statements (default: "http://localhost:8123/default")
	STATIO_POSTGRES_DSN - connection to postgres for pg_statio and pg_stat_tables (disabled by default: "")
`

func main() {
	cfg, err := internal.NewConfig()
	if err != nil {
		log.Println(usage)
		log.Fatalf(err.Error())
	}

	log.Println("- - - - - - - - - - - - - - -")
	log.Println("daemon started")

	var wg sync.WaitGroup
	if cfg.StatioPostgresDsn != "" {
		wg.Add(1)
		go setupPSTCollector(handleSignals(), cfg.Interval, cfg.StatioPostgresDsn, cfg.ClickhouseDsn, &wg)
		wg.Add(1)
		//use x4 interval because of slowly changing value
		go setupPTSCollector(handleSignals(), cfg.Interval*4, cfg.StatioPostgresDsn, cfg.ClickhouseDsn, &wg)
	}
	wg.Add(1)
	go setupPSSCollector(handleSignals(), cfg.Interval, cfg.PostgresDsn, cfg.ClickhouseDsn, &wg)

	wg.Wait()

	log.Println("daemon terminated")
}

func handleSignals() context.Context {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		oscall := <-c
		log.Printf("got system call:%+v", oscall)
		cancel()
	}()

	return ctx
}

func setupPSSCollector(ctx context.Context, interval time.Duration, postgresDsn string, clickhouseDsn string, wg *sync.WaitGroup) {
	defer wg.Done()
	setupCollector(ctx, &internal.PgStatStatementsFactory{}, interval, postgresDsn, clickhouseDsn)
}

func setupPSTCollector(ctx context.Context, interval time.Duration, postgresDsn string, clickhouseDsn string, wg *sync.WaitGroup) {
	defer wg.Done()
	setupCollector(ctx, &internal.PgStatioTableFactory{}, interval, postgresDsn, clickhouseDsn)
}

func setupPTSCollector(ctx context.Context, interval time.Duration, postgresDsn string, clickhouseDsn string, wg *sync.WaitGroup) {
	defer wg.Done()
	setupCollector(ctx, &internal.PgTableSizeFactory{}, interval, postgresDsn, clickhouseDsn)
}

func setupCollector(ctx context.Context, collector internal.CollectorFactory, interval time.Duration, postgresDsn string, clickhouseDsn string) {
	hostname, _ := os.Hostname()
	ttl := int64(interval/time.Second) * 2
	sc, err := internal.NewStatsCollector(
		collector,
		hostname,
		postgresDsn,
		clickhouseDsn,
		ttl,
	)
	if err != nil {
		log.Fatalf("[%s] Unable to init collector: %v", collector.Name(), err)
	}

	go func() {
		collectTick := time.Tick(interval)
		for {
			<-collectTick
			if err := sc.Tick(); err != nil {
				log.Printf("[%s] Error during tick: %v", collector.Name(), err)
			}
		}
	}()

	log.Printf("[%s] collector started", collector.Name())

	<-ctx.Done()

	if err = sc.Shutdown(); err != nil {
		log.Fatalf("[%s] collector shutdown failed: %v", collector.Name(), err)
	}

	log.Printf("[%s] collector stopped", collector.Name())
}
