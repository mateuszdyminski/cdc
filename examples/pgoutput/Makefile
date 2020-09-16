POSTGRES_VERSION := 12.4

MIGRATION_INDEX ?= 1

.DEFAULT_GOAL := run
.PHONY: run run-db run-db-wal stop-db migrate help

run-db: ## Runs postgres
	docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password -e POSTGRES_DB=app_db --name postgres postgres:${POSTGRES_VERSION}-alpine

run-db-wal: ## Runs postgres with wal_level set to logical
	docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password -e POSTGRES_DB=app_db --name postgres postgres:${POSTGRES_VERSION}-alpine postgres -c wal_level=logical

stop-db: ## Stops postgres
	docker stop postgres
	docker rm postgres

run: ## Runs CDC plugin for postgres
	CONN_STRING="postgres://postgres:password@127.0.0.1/app_db?replication=database" go run main.go

psql: ## Runs psql
	psql -d app_db -U postgres -p 5432 -h localhost

migrate: ## Runs migration of Postgres
	migrate -path migrations -database postgres://postgres:password@localhost:5432/app_db?sslmode=disable goto $(MIGRATION_INDEX)

help: ## Displays this help screen
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} /^[a-zA-Z_-]+:.*?##/ \
	{ printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
