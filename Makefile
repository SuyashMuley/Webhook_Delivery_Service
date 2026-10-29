DATABASE_URL ?= postgres://webhooks:webhooks@localhost:5432/webhooks?sslmode=disable
export DATABASE_URL

# Claim-query benchmark (see docs/plan.md, "Claiming").
bench:
	docker compose up -d postgres --wait
	go test ./internal/store -run xxx -bench ClaimParallel -benchtime 2000x -cpu 1,8

.PHONY: bench build test lint vet up down migrate-up migrate-down migrate-status run-api run-worker

build:
	go build ./...

vet:
	go vet ./...

# Integration tests need Postgres (DATABASE_URL); start it if it isn't running.
test:
	docker compose up -d postgres --wait
	go test -race ./...

lint:
	golangci-lint run

up:
	docker compose up --build -d

down:
	docker compose down

migrate-up:
	go run ./cmd/migrate up

migrate-down:
	go run ./cmd/migrate down

migrate-status:
	go run ./cmd/migrate status

run-api:
	go run ./cmd/api

run-worker:
	go run ./cmd/worker
