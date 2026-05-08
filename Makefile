SHELL := /bin/bash
SERVICES := api scheduler worker incident-engine alert-service migrate

.PHONY: fmt tidy build test vet docker-build up down migrate-local demo

fmt:
	gofmt -w internal services demo

tidy:
	go mod tidy

build:
	@for service in $(SERVICES); do \
		echo "building $$service"; \
		go build ./services/$$service/cmd/$$service; \
	done

test:
	go test ./...

vet:
	go vet ./...

docker-build:
	@for service in $(SERVICES); do \
		echo "docker building $$service"; \
		docker build --build-arg SERVICE=$$service -t distributed-monitoring/$$service:local .; \
	done

up:
	docker compose up --build -d

down:
	docker compose down --remove-orphans

migrate-local:
	MIGRATIONS_DIR=migrations go run ./services/migrate/cmd/migrate

demo:
	go run ./demo/monitor-targets
