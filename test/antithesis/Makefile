# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Local-dev helper for the Materialize Antithesis harness.
#
# Antithesis images ship via the standard mzbuild → GHCR flow; CI publishes
# the same images CI publishes for everything else, fingerprint-tagged with
# `mzbuild-<fp>`. Locally, we just acquire the mzbuild images, regenerate
# the compose YAML, and let `docker compose` find them by their canonical
# spec.
#
# Targets:
#   make build    # regenerate compose YAML, acquire local mzbuild images
#   make up       # build + bring up the stack
#   make down     # tear down (preserves volumes)
#   make smoke    # build + up + smoke test
#   make test     # smoke test against a running stack
#   make clean    # tear down + remove volumes

SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

PROJECT   := materialize-antithesis
REPO_ROOT := $(realpath $(dir $(lastword $(MAKEFILE_LIST)))/../..)

# Pick podman if available, else docker.
ifndef RUNTIME
  RUNTIME := $(shell command -v podman >/dev/null 2>&1 && echo podman || (command -v docker >/dev/null 2>&1 && echo docker || echo none))
endif
ifeq ($(RUNTIME),none)
  $(error neither podman nor docker found in PATH; set RUNTIME=docker or install podman)
endif
ifeq ($(RUNTIME),podman)
  export MZ_DEV_CI_BUILDER_RUNTIME := podman
endif

COMPOSE_FILE := $(REPO_ROOT)/test/antithesis/config/docker-compose.yaml
ENV_FILE     := $(REPO_ROOT)/test/antithesis/config/.env
COMPOSE      := $(RUNTIME) compose -p $(PROJECT) --env-file $(ENV_FILE) -f $(COMPOSE_FILE)
PSQL         := $(COMPOSE) exec materialized psql -h localhost -p 6875 -U materialize

# mzbuild images we need built locally. Third-party images (postgres, minio,
# kafka, …) are pulled by `docker compose` from their upstream registries.
MZBUILD_IMAGES := materialized antithesis-workload

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
.PHONY: build export-compose export-env acquire-images

build: export-compose export-env acquire-images

export-compose:
	cd $(REPO_ROOT) && bin/pyactivate test/antithesis/export-compose.py > $(COMPOSE_FILE)
	@echo "Wrote $(COMPOSE_FILE)"

export-env:
	cd $(REPO_ROOT) && bin/pyactivate test/antithesis/export-env.py > $(ENV_FILE)
	@echo "Wrote $(ENV_FILE)"

acquire-images:
	@for image in $(MZBUILD_IMAGES); do \
	  echo "--- Acquiring $$image (--antithesis)"; \
	  cd $(REPO_ROOT) && bin/mzimage acquire "$$image" --antithesis; \
	done

# ---------------------------------------------------------------------------
# Up / Down
# ---------------------------------------------------------------------------
.PHONY: up down clean

up: build
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

clean: down
	$(COMPOSE) down -v --remove-orphans 2>/dev/null || true

# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------
.PHONY: test smoke

test:
	$(PSQL) -c "CREATE TABLE IF NOT EXISTS smoke_test (k INT, v TEXT)"
	$(PSQL) -c "INSERT INTO smoke_test VALUES (1, 'hello'), (2, 'world')"
	$(PSQL) -c "SELECT * FROM smoke_test ORDER BY k"
	$(PSQL) -c "DROP TABLE smoke_test"

smoke: up test
	@echo "[smoke] passed"
