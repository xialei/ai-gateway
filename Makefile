.PHONY: build run test vet fmt clean demo

BINARY ?= bin/ai-gateway
CONFIG ?= config.example.yaml

build:
	mkdir -p bin
	go build -o $(BINARY) ./cmd/gateway

run:
	go run ./cmd/gateway -config $(CONFIG)

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt:
	gofmt -s -w .

clean:
	rm -rf bin

# demo runs the gateway against the in-process mock backend (no external
# services required) and prints a sample streaming request. Uses
# config.demo.yaml, which sets scheduler.mock: true so the gateway starts an
# in-process mock and schedules against it.
demo:
	@echo ">> starting gateway on :8080 (mock backend, scheduler.mock=true)"
	@go run ./cmd/gateway -config config.demo.yaml &
	@sleep 1.5
	@echo ">> streaming chat request:"
	@curl -s -N -H "Authorization: Bearer sk-gateway-demo" -H "Content-Type: application/json" \
		-d '{"model":"demo-model","stream":true,"messages":[{"role":"user","content":"hello gateway"}]}' \
		http://127.0.0.1:8080/v1/chat/completions
	@echo
	@pkill -f "ai-gateway -config" 2>/dev/null || true
