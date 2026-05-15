VERSION := $(shell git describe --tags --always --dirty)
LDFLAGS := -X flipgroup.com.au/jellyfish/internal/version.Version=$(VERSION)

build:
	go build -ldflags "$(LDFLAGS)" -o bin/jellyfish .

install:
	go install -ldflags "$(LDFLAGS)" .

test:
	go test ./...

lint:
	golangci-lint run

.PHONY: build install test lint
