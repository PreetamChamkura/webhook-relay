export GOFLAGS := -buildvcs=false

.PHONY: ci build test integration lint proto up down loadtest outage
build:       ; go build ./...
test:        ; go test -race ./...
integration: ; go test -tags integration -timeout 15m -v ./test/integration/...
lint:        ; golangci-lint run
proto:       ; buf generate
up:          ; docker compose up -d --build
down:        ; docker compose down -v
loadtest:    ; docker run --rm -i --network host -e RECEIVER_URL=http://receiver:8090/hook grafana/k6 run -e RATE=$${RATE:-1000} - < loadtest/publish.js
outage:      ; ./loadtest/outage.sh
ci:          ; ./scripts/ci.sh
