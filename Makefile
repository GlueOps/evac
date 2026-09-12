# Local dev has no Go toolchain: `make docker-<target>` runs any target in a
# container. CI has Go natively and calls the plain targets. One definition,
# two entry points, so they cannot drift.

GO      ?= go
PKG     := github.com/GlueOps/evac
BIN     := evac
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w \
	-X main.version=$(VERSION) \
	-X main.commit=$(COMMIT) \
	-X main.date=$(DATE)

# Debian-based, not -alpine: the alpine image ships no `make`, and the wrapper
# re-invokes make inside the container so targets stay defined in one place.
GO_IMAGE ?= golang:1.26

.PHONY: build test lint vet tidy clean help

build:
	CGO_ENABLED=0 $(GO) build -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/evac

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BIN) dist/

# Run any target inside the builder image. -u keeps files in the tree owned by
# the invoking user; without it docker writes root-owned files. Caches live in
# .cache/ (gitignored) so module downloads survive between runs.
docker-%:
	@mkdir -p .cache/gocache .cache/gomodcache
	docker run --rm \
		-u $$(id -u):$$(id -g) \
		-v $(CURDIR):/src -w /src \
		-e GOCACHE=/src/.cache/gocache \
		-e GOMODCACHE=/src/.cache/gomodcache \
		-e GOFLAGS=-buildvcs=false \
		$(GO_IMAGE) make $*

help:
	@echo "make docker-build   build the binary (no local Go needed)"
	@echo "make docker-test    run unit tests"
	@echo "make docker-lint    run linters"
	@echo "make docker-tidy    go mod tidy"
