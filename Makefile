# 
# © 2026 PLOMID Technologies INDIA
# PLOMID — Platform for Modern Intelligence and Data
# Author: Sainath Sapa
# Email: info.djsai@gmail.com
# GitHub: https://github.com/sainathsapa
# Organization: https://github.com/plomid
# PLOMID — build, test and packaging automation.
#
# `make docker` is the release path: it cross-builds the Linux server binary in a
# throwaway Rust container, then packages that single artifact into a runtime
# image that contains no source code (see Dockerfile and .dockerignore).
#
# The container build mounts the source read-only and keeps all caches under
# `target/`, so nothing outside this directory is touched and the repository is
# never modified by a build.

SHELL := /bin/bash

# ---------- configuration (override on the command line) ----------

APP          := plomid-server
VERSION      ?= $(shell sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)

# Docker arch names, derived from the host when not specified.
ARCH         ?= $(shell uname -m | sed 's/^x86_64$$/amd64/; s/^aarch64$$/arm64/; s/^arm64$$/arm64/')
PLATFORM     ?= linux/$(ARCH)

# The runtime image must use the same libc as the builder image's default target
# (both Debian bookworm here), otherwise the copied binary will not load.
# Use the full image, not `-slim`: aws-lc-sys compiles AWS-LC's C/C++ sources
# and needs the Debian build toolchain (g++), which the slim image omits.
RUST_IMAGE    ?= rust:1.99-bookworm

DOCKER_IMAGE ?= plomid/plomid
DOCKER_TAG   ?= $(VERSION)
IMAGE        := $(DOCKER_IMAGE):$(DOCKER_TAG)

# Runtime knobs for `make docker-run`. PLOMID_PASSWORD has NO default here:
# the server refuses the built-in password on non-loopback binds, and the
# `docker-run` target fails with instructions when it is unset.
PORT              ?= 5432
DATA_VOLUME       ?= plomid-data
PLOMID_USER       ?= plomid
PLOMID_PASSWORD   ?=

# Build/package locations (all inside the project).
DIST             := dist
CARGO_HOME_DIR   := $(CURDIR)/target/docker-cargo-home
LINUX_TARGET_DIR := $(CURDIR)/target/linux-$(ARCH)

.DEFAULT_GOAL := help
.PHONY: help check check-ci test test-unit test-integration test-ci test-local test-scale test-full gauntlet bench-check fmt fmt-check clippy clippy-ci deny release run \
        linux-binary linux-binary-all docker docker-run docker-stop docker-logs \
        docker-push image-info clean distclean package-mac release-beta

# ---------- help ----------

help: ## Show available targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  ARCH=$(ARCH)  PLATFORM=$(PLATFORM)  IMAGE=$(IMAGE)"

# ---------- development ----------

check: ## Type-check the whole workspace including tests
	cargo check --workspace --all-targets

check-ci: ## Type-check for CI/release gates (lib+tests only, benches excluded)
	cargo check --workspace --lib --tests

test: ## Full workspace suite (all functions; #[ignore] scale/bench gates skipped by default).
	cargo test --workspace --jobs 2

# Tier A — fast unit tests: isolated `#[test]`s inside crate sources
# (`src/`). No benchmark or scale workloads by construction. Finishes in
# seconds; run it first and often.
test-unit: ## Fast unit tier: lib tests only, no scale/bench workloads
	cargo test --workspace --lib --jobs 2

# Tier B — bounded integration + regression tests: every `tests/` target
# across the workspace (SQL execution, transactions, storage, indexes,
# WAL/recovery, persistence, protocol, regression cases), with
# `#[ignore]` scale gates off (no --include-ignored) and no scale env
# (SCALE_ROWS/FUZZ/EXTENDED), so fuzz/EXTENDED default to their small fast
# sizes. Heavy scale/soak/fuzz-200 + benches run only via `make test-scale`
# in the nightly workflow.
test-integration: ## Bounded integration tier: all tests/ targets, scale gates off
	cargo test --workspace --tests --jobs 2

# Tier C — required CI verification: Tier A + Tier B in a SINGLE cargo
# invocation (one compilation and one freshness pass; with no inline
# `src/` tests today this equals A+B exactly). Do NOT split this into two
# workspace-wide invocations: each pass re-stats the whole target dir,
# which costs minutes on cold or I/O-loaded machines. This is the routine
# correctness gate for pull requests and releases (CI adds fmt, check,
# clippy, and deny as parallel jobs). `--jobs 2`: the 7 GiB/14 GiB runners
# OOM (ld Bus error) when linking the giant executor test binaries in
# parallel; cap concurrent links. Total routine budget: 30 minutes
# (see .github/workflows/ci.yml); the heavy suites below never run here.
test-ci: ## Required gate: unit + bounded integration in one pass
	cargo test --workspace --jobs 2

bench-check: ## On-demand only (nightly/manual): verify all 22 criterion benches compile
	cargo bench --workspace --no-run

test-local: ## Cool MacBook loop: unit + integration, skips #[ignore] scale tests
	cargo test --workspace --lib
	cargo test --workspace --tests

test-scale: ## Tier D nightly/manual scale rip: full suite + ignored scale at 100K (runs hot; never in routine local/CI/release validation)
	PLOMID_SCALE_ROWS=100000 PLOMID_FUZZ_CASES=200 cargo test --workspace -- --include-ignored --nocapture

test-full: test-scale ## Alias for the nightly scale rip (never in routine local/CI/release validation)

gauntlet: ## Manual HTAP Gauntlet qualification (scale-gated, writes target/gauntlet_report.md; never in CI/release)
	cargo test -p plomid-gauntlet --lib
	cargo test -p plomid-gauntlet --test gauntlet_qualification -- --include-ignored

fmt: ## Format the workspace
	cargo fmt --all

fmt-check: ## Verify formatting (CI)
	cargo fmt --all --check

clippy: ## Lint the workspace; correctness/suspicious/complexity/perf are deny, style stays advisory
	cargo clippy --workspace --all-targets -- -D clippy::correctness -D clippy::suspicious -D clippy::complexity -D clippy::perf

clippy-ci: ## Lint for CI/release gates (lib+tests only, benches excluded)
	cargo clippy --workspace --lib --tests -- -D clippy::correctness -D clippy::suspicious -D clippy::complexity -D clippy::perf

deny: ## Run the dependency/license/advisory policy check
	cargo deny check

sbom: ## Emit a CycloneDX SBOM of the locked tree (needs cargo-cyclonedx: cargo install cargo-cyclonedx). Releases ship SPDX instead (see release.yml); both derive from Cargo.lock
	cargo cyclonedx --format json --output sbom.cdx.json

release: ## Build the server binary for this host into target/release
	cargo build --release -p $(APP)

run: release ## Run the server locally (loopback, ./data)
	./target/release/$(APP) --data ./data --host 127.0.0.1 --port $(PORT) \
		--username $(PLOMID_USER) --password $(PLOMID_PASSWORD)

# ---------- packaging ----------

linux-binary: ## Cross-build the Linux server binary into dist/linux-$(ARCH)/
	@mkdir -p "$(DIST)/linux-$(ARCH)" "$(CARGO_HOME_DIR)" "$(LINUX_TARGET_DIR)"
	@echo "==> building $(APP) for $(PLATFORM) in $(RUST_IMAGE)"
	docker run --rm --platform $(PLATFORM) \
		-v "$(CURDIR)":/src:ro \
		-v "$(CARGO_HOME_DIR)":/cargo-home \
		-v "$(LINUX_TARGET_DIR)":/build-target \
		-e CARGO_HOME=/cargo-home \
		-e CARGO_TARGET_DIR=/build-target \
		-w /src \
		$(RUST_IMAGE) \
		cargo build --release --locked -p $(APP)
	@cp "$(LINUX_TARGET_DIR)/release/$(APP)" "$(DIST)/linux-$(ARCH)/$(APP)"
	@echo "==> artifact: $(DIST)/linux-$(ARCH)/$(APP) ($$(du -h "$(DIST)/linux-$(ARCH)/$(APP)" | cut -f1))"

linux-binary-all: ## Cross-build both linux/arm64 and linux/amd64 artifacts
	$(MAKE) linux-binary ARCH=arm64
	$(MAKE) linux-binary ARCH=amd64

docker: linux-binary ## Build the binary-only runtime image for this host's arch
	docker build --platform $(PLATFORM) \
		--build-arg TARGETARCH=$(ARCH) \
		-t $(IMAGE) \
		.
	@echo "==> image: $(IMAGE) ($$(docker image inspect -f '{{.Size}}' $(IMAGE) | awk '{printf "%.1f MiB", $$1/1048576}'))"

docker-run: docker ## Start the server container on port $(PORT)
	@if [ -z "$(PLOMID_PASSWORD)" ] || [ "$(PLOMID_PASSWORD)" = "plomid" ]; then \
		echo "!! refusing to start: set PLOMID_PASSWORD to a unique secret (the built-in password is rejected on non-loopback binds)"; \
		exit 1; \
	fi
	@echo "!! container binds 0.0.0.0 without TLS: credentials cross the network in the clear; trusted networks only (see docs/v0.1.0/operations/tls.mdx for the TLS setup)"
	@docker rm -f plomid-server >/dev/null 2>&1 || true
	docker run -d --name plomid-server --platform $(PLATFORM) \
		-p $(PORT):5432 \
		-v $(DATA_VOLUME):/var/lib/plomid/data \
		-e PLOMID_USER=$(PLOMID_USER) \
		-e PLOMID_PASSWORD=$(PLOMID_PASSWORD) \
		-e PLOMID_ALLOW_INSECURE_REMOTE=1 \
		--read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m \
		--cap-drop ALL --security-opt no-new-privileges \
		$(IMAGE)
	@echo "==> listening on 127.0.0.1:$(PORT); logs: make docker-logs"

docker-stop: ## Stop and remove the server container (keeps the data volume)
	-docker rm -f plomid-server

docker-logs: ## Follow container logs
	docker logs -f plomid-server

image-info: ## Show image size and the files it ships
	@docker image inspect -f 'size={{.Size}} arch={{.Architecture}} user={{.Config.User}}' $(IMAGE)
	@docker run --rm --entrypoint /bin/bash $(IMAGE) -c \
		'echo "--- shipped files (excluding base OS) ---"; ls -l /usr/local/bin; \
		 echo "--- source files inside the image ---"; \
		 find / -xdev -name "*.rs" -o -xdev -name "Cargo.toml" 2>/dev/null | head'

# docker-push: linux-binary-all ## Build and push a multi-arch manifest (needs REGISTRY + login)
# 	@test -n "$(REGISTRY)" || { echo "set REGISTRY=ghcr.io/<owner>"; exit 1; }
# 	# BuildKit sets TARGETARCH per platform, so each manifest entry picks up
# 	# dist/linux-<arch>/plomid-server produced by linux-binary-all above.
# 	docker buildx build --platform linux/arm64,linux/amd64 \
# 		-t $(REGISTRY)/$(DOCKER_IMAGE):$(DOCKER_TAG) \
# 		--push .

# ---------- cleanup ----------

clean: ## Remove build artifacts (keeps the docker caches under target/)
	cargo clean

distclean: docker-stop ## Remove build artifacts and packaging OUTPUT (never dist/ sources)
	rm -rf "$(DIST)/artifacts" "$(DIST)/stage" "$(DIST)/linux-amd64" "$(DIST)/linux-arm64" "$(CARGO_HOME_DIR)" "$(CURDIR)/target/linux-arm64" "$(CURDIR)/target/linux-amd64"

# ---------- local packaging (macOS host) ----------

package-mac: ## Build macOS tarball + .dmg locally into dist/artifacts (needs Xcode tools)
	@test "$(shell uname)" = "Darwin" || { echo "macOS packaging needs a Mac host (CI builds it otherwise)"; exit 1; }
	cargo build --release --locked -p $(APP)
	bash dist/macos/make-dmg.sh "v$(VERSION)" "$(shell uname -m | sed 's/^arm64$$/arm64/; s/^x86_64$$/x64/')"
	tar -czf "dist/artifacts/plomid-v$(VERSION)-macos-$(shell uname -m | sed 's/^arm64$$/arm64/; s/^x86_64$$/x64/').tar.gz" -C dist/stage/plomid plomid-server README.txt
	@cd dist/artifacts && shasum -a 256 plomid-* > SHA256SUMS.txt && cat SHA256SUMS.txt

# ---------- gated beta release ----------

# Full beta release from this machine. EVERY gate runs in order and make
# stops at the first failure, so no tag — and therefore no GitHub Release,
# no Docker push, no platform artifact — is produced unless everything is
# green: formatting, type-check, lints, dependency policy, the unit +
# integration suite (scale/benchmarks excluded), and a local release build.
# Scale, benches, and the HTAP Gauntlet run only in nightly scale
# (`make test-scale`, `make bench-check`, `make gauntlet`) — never here.
# Usage:
#
#   make release-beta VERSION=0.1.0-beta.1
#
# VERSION must equal Cargo.toml (bump it first). The tag push is the only
# network write; CI takes over from there (matrix builds, checksums,
# GitHub Release, multi-arch Docker).
release-beta: ## Gate everything, then tag v$(VERSION) to trigger the release pipeline
	@test -n "$(VERSION)" || { echo "usage: make release-beta VERSION=0.1.0-beta.1"; exit 1; }
	@test "$(VERSION)" != "0.1.0" || { echo "refusing to release the default version; set VERSION explicitly"; exit 1; }
	@test -z "$$(git status --porcelain -- . ':!dist/artifacts' ':!dist/stage')" || { echo "working tree dirty; commit first"; git status --short | head; exit 1; }
	@test "$$(sed -n 's/^version = "\(.*\)"/\1/p' Cargo.toml | head -1)" = "$(VERSION)" || { echo "Cargo.toml version != $(VERSION); bump it first"; exit 1; }
	$(MAKE) fmt-check
	$(MAKE) check-ci
	$(MAKE) clippy-ci
	$(MAKE) deny
	$(MAKE) test-ci
	cargo build --release --locked -p $(APP)
	./target/release/$(APP) --version
	git tag -a "v$(VERSION)" -m "PLOMID v$(VERSION)"
	git push origin "v$(VERSION)"
	@echo "==> tagged v$(VERSION); watch the Release workflow publish every platform"
