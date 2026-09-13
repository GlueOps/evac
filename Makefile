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
#
# Pinned by digest so a local build is reproducible and cannot silently move to
# a different toolchain patch than the one that was reviewed. Renovate keeps it
# current; the tag in the comment is what it tracks.
# renovate: datasource=docker depName=golang versioning=docker
GO_IMAGE ?= golang:1.26@sha256:3c3e25a4da13fd0478eed2df1eb35a0e667094a7124d3993a6a1d30f71c17e79

# Linting and releasing need their own tools, so those two wrappers use their
# own images rather than GO_IMAGE. Pinned the same way.
# renovate: datasource=docker depName=golangci/golangci-lint versioning=docker
LINT_IMAGE ?= golangci/golangci-lint:v2.13.2@sha256:ba07dffad130794ae79ebaa0056809d18c0168f3f846480ffd3eb6c04578b83d
# renovate: datasource=docker depName=goreleaser/goreleaser versioning=docker
GORELEASER_IMAGE ?= goreleaser/goreleaser:v2.12.7@sha256:a2a47c0dda85f8d40eaaa5b9765bf76c69addb6060666f8a51441410d9b008e9

.PHONY: build test lint vet tidy clean help integration snapshot \
	docker-lint docker-snapshot

build:
	CGO_ENABLED=0 $(GO) build -ldflags '$(LDFLAGS)' -o $(BIN) ./cmd/evac

test:
	$(GO) test -race -count=1 ./...

vet:
	$(GO) vet ./...
	$(GO) vet -tags integration ./test/...

lint:
	golangci-lint run

# Destructive: see README. Requires EVAC_TEST_CLUSTER plus the sentinel
# namespace on the target cluster.
integration:
	$(GO) test -tags integration -timeout 25m -v ./test/integration/...

tidy:
	$(GO) mod tidy

# Unreleased binaries for every release target, built from the working tree
# with no tag and nothing published. Output lands in dist/.
snapshot:
	goreleaser release --snapshot --clean --skip=publish

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

# Explicit rules beat the pattern rule above, so these two get their own image.
# golangci-lint needs its own cache directory or it re-analyses everything.
docker-lint:
	@mkdir -p .cache/gocache .cache/gomodcache .cache/lintcache
	docker run --rm \
		-u $$(id -u):$$(id -g) \
		-v $(CURDIR):/src -w /src \
		-e GOCACHE=/src/.cache/gocache \
		-e GOMODCACHE=/src/.cache/gomodcache \
		-e GOLANGCI_LINT_CACHE=/src/.cache/lintcache \
		-e GOFLAGS=-buildvcs=false \
		$(LINT_IMAGE) golangci-lint run

# GOTOOLCHAIN=auto is required: the goreleaser image ships an older Go than
# go.mod's floor and defaults to GOTOOLCHAIN=local, which fails outright.
# safe.directory is needed because the mounted tree is owned by the host user.
docker-snapshot:
	@mkdir -p .cache/gocache .cache/gomodcache
	docker run --rm --entrypoint sh \
		-u $$(id -u):$$(id -g) \
		-v $(CURDIR):/src -w /src \
		-e GOCACHE=/src/.cache/gocache \
		-e GOMODCACHE=/src/.cache/gomodcache \
		-e GOFLAGS=-buildvcs=false \
		-e GOTOOLCHAIN=auto \
		-e HOME=/tmp \
		$(GORELEASER_IMAGE) \
		-c 'git config --global --add safe.directory /src && make snapshot'

help:
	@echo "make docker-build     build the binary (no local Go needed)"
	@echo "make docker-test      run unit tests"
	@echo "make docker-vet       go vet, including the integration-tagged tests"
	@echo "make docker-lint      run golangci-lint"
	@echo "make docker-tidy      go mod tidy"
	@echo "make docker-snapshot  unreleased binaries for every target, into dist/"
