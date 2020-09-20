docker-compose-file=./test/docker-compose.yml

test-full: up
	@go test ./...
	@make down

test:
	@go test ./...

up:
	docker-compose -f ${docker-compose-file} up -d --build

	sleep 3

	docker-compose -f ${docker-compose-file} exec -T postgres psql -U postgres  < ./test/fixtures/postgres.sql
	docker-compose -f ${docker-compose-file} exec -T clickhouse clickhouse-client -mn < ./test/fixtures/clickhouse.sql

down:
	docker-compose -f ${docker-compose-file} down --volumes

fmt:
	@go fmt ./...

vet:
	@go vet ./...

lint:
	@golangci-lint run

build:
	mkdir -p ./bin
	@go build -o ./bin/pgstats-to-clickhouse ./cmd/pgstats-to-clickhouse

.PHONY: test-full test up down fmt vet lint build