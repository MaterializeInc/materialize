# Build / run helper for the Materialize Antithesis harness.
#
# Usage:
#   make build                 # build every local image
#   make up                    # export compose, build, bring up the stack
#   make test                  # smoke test against the running cluster
#   make push                  # push locally-built images to Antithesis registry
#   make down                  # tear down (preserves volumes)
#   make clean                 # tear down + remove volumes + images
#   make smoke                 # full cycle: build → up → test

SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

PROJECT   := materialize
REPO_ROOT := $(realpath $(dir $(lastword $(MAKEFILE_LIST)))/..)

ifndef RUNTIME
  RUNTIME := $(shell command -v podman >/dev/null 2>&1 && echo podman || (command -v docker >/dev/null 2>&1 && echo docker || echo none))
endif
ifeq ($(RUNTIME),none)
  $(error neither podman nor docker found in PATH; set RUNTIME=docker or install podman)
endif

COMPOSE := $(RUNTIME) compose -p $(PROJECT) -f config/docker-compose.yaml
PSQL    := $(COMPOSE) exec materialized psql -h localhost -p 6875 -U materialize

REGISTRY      ?= us-central1-docker.pkg.dev
REGISTRY_PATH ?= /molten-verve-216720/materialize-repository

# ---------------------------------------------------------------------------
# Export — generate the resolved docker-compose YAML for Antithesis.
# ---------------------------------------------------------------------------
.PHONY: export-compose
export-compose:
	cd $(REPO_ROOT) && $(REPO_ROOT)/bin/pyactivate test/antithesis/export-compose.py > antithesis/config/docker-compose.yaml
	@echo "Wrote config/docker-compose.yaml"

# ---------------------------------------------------------------------------
# Build — build images that don't have public equivalents.
# ---------------------------------------------------------------------------
LOCAL_IMAGES  := workload
BUILD_TARGETS := $(LOCAL_IMAGES:%=build-%)

.PHONY: build $(BUILD_TARGETS)
build: $(BUILD_TARGETS)

$(BUILD_TARGETS): build-%:
	$(RUNTIME) build \
	  --platform linux/amd64 \
	  -t $(PROJECT)-$*:latest \
	  $(REPO_ROOT)/test/antithesis/$*

# ---------------------------------------------------------------------------
# Up / Down
# ---------------------------------------------------------------------------
.PHONY: up
up: export-compose build
	$(COMPOSE) up -d

.PHONY: down
down:
	$(COMPOSE) down

# ---------------------------------------------------------------------------
# Test — quick smoke test against the running cluster
# ---------------------------------------------------------------------------
.PHONY: test
test:
	$(PSQL) -c "CREATE TABLE IF NOT EXISTS smoke_test (k INT, v TEXT)"
	$(PSQL) -c "INSERT INTO smoke_test VALUES (1, 'hello'), (2, 'world')"
	$(PSQL) -c "SELECT * FROM smoke_test ORDER BY k"
	$(PSQL) -c "DROP TABLE smoke_test"

# ---------------------------------------------------------------------------
# Push — tag local images and push to the Antithesis registry
# ---------------------------------------------------------------------------
.PHONY: push
push:
	@$(RUNTIME) images --format '{{.Repository}}:{{.Tag}}' \
	  | grep ':latest$$' | grep '^\(localhost/\)\?$(PROJECT)-' \
	  | while read item; do \
	      nametag="$${item#localhost/}"; \
	      name="$${nametag%:*}"; \
	      remote="$(REGISTRY)$(REGISTRY_PATH)/$${name}:latest"; \
	      echo "Pushing $${item} -> $${remote}"; \
	      $(RUNTIME) tag "$${item}" "$${remote}" || exit 1; \
	      $(RUNTIME) push "$${remote}" || exit 1; \
	  done

# ---------------------------------------------------------------------------
# Clean
# ---------------------------------------------------------------------------
.PHONY: clean
clean: down
	$(COMPOSE) down -v --remove-orphans 2>/dev/null || true
	-$(RUNTIME) rmi $$($(RUNTIME) images --format '{{.Repository}}:{{.Tag}}' | grep '^$(PROJECT)-' || true) 2>/dev/null

# ---------------------------------------------------------------------------
# Smoke — full cycle: build → up → test
# ---------------------------------------------------------------------------
.PHONY: smoke
smoke: up test
	@echo "[smoke] passed"
