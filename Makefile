DATABASE_URL ?= postgres://webhooks:webhooks@localhost:5432/webhooks?sslmode=disable
export DATABASE_URL

# Build the service binaries (used by the load/chaos tool, which runs them as real processes).
bin:
	go build -o bin/ ./cmd/api ./cmd/worker ./cmd/loadtest

# Load test: 400 events/s x 5 endpoints = 2000 deliveries/s for 60s. SCENARIO=none|kill-workers|kill-api|
# db-restart|db-partition|receiver-faults|mixed. Needs Postgres (make up or docker compose up -d postgres).
SCENARIO ?= none
loadtest: bin
	docker compose up -d postgres --wait
	./bin/loadtest -scenario $(SCENARIO) -rate 400 -fanout 5 -duration 60s

# Claim-query benchmark (see docs/plan.md, "Claiming").
bench:
	docker compose up -d postgres --wait
	go test ./internal/store -run xxx -bench ClaimParallel -benchtime 2000x -cpu 1,8

.PHONY: bin loadtest bench up-obs test-alerts gen-k8s-rules validate-k8s build test lint vet up down migrate-up migrate-down migrate-status run-api run-worker

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

# Everything plus Prometheus (:9090), Grafana (:3000, dashboard preloaded) and Jaeger (:16686).
up-obs:
	OTEL_EXPORTER_OTLP_ENDPOINT=http://jaeger:4318 docker compose --profile observability up --build -d

# Regenerate the Kubernetes PrometheusRule from deploy/prometheus/alerts.yml.
gen-k8s-rules:
	python3 -c 'import yaml;r=yaml.safe_load(open("deploy/prometheus/alerts.yml"));d={"apiVersion":"monitoring.coreos.com/v1","kind":"PrometheusRule","metadata":{"name":"webhook-delivery","namespace":"webhooks"},"spec":r};f=open("deploy/k8s/optional/prometheusrule.yaml","w");f.write("# GENERATED from deploy/prometheus/alerts.yml by `make gen-k8s-rules`. Do not edit.\n");yaml.safe_dump(d,f,sort_keys=False,width=1000)'

# Render the kustomize base and schema-check it (needs kubectl and docker).
validate-k8s:
	kubectl kustomize deploy/k8s | docker run --rm -i ghcr.io/yannh/kubeconform:latest -strict -summary -kubernetes-version 1.30.0 -
	docker run --rm -v "$(CURDIR)/deploy/k8s/optional:/o" ghcr.io/yannh/kubeconform:latest -strict -summary -kubernetes-version 1.30.0 -ignore-missing-schemas /o

# Unit-test the Prometheus alert rules.
test-alerts:
	docker run --rm -v "$(CURDIR)/deploy/prometheus:/p" -w /p --entrypoint promtool prom/prometheus:latest check rules alerts.yml
	docker run --rm -v "$(CURDIR)/deploy/prometheus:/p" -w /p --entrypoint promtool prom/prometheus:latest test rules alerts_test.yml

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
