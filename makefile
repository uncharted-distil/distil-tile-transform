VERSION=`git describe --tags`
TIMESTAMP=`date +%FT%T%z`

LDFLAGS=-ldflags "-X main.version=${VERSION} -X main.timestamp=${TIMESTAMP}"

.PHONY: all

all:
	@echo "make <cmd>"
	@echo ""
	@echo "commands:"
	@echo "  build         - build the source code"
	@echo "  fmt           - format the source code"
	@echo "  install       - install dependencies"

lint:
	@golangci-lint run

fmt:
	@go fmt ./...

build: lint
	@go build -i ${LDFLAGS}

compile: lint
	@go build ./...

test: build
	@go test ./...

install:
	@go get github.com/golangci/golangci-lint/cmd/golangci-lint@v1.33.0
