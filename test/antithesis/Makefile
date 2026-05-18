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
# The harness emits one docker-compose.yaml per workload group (see
# `test/antithesis/groups.yaml`).  Use `GROUP=<name>` to pick one;
# defaults to `combined` (kitchen-sink) for back-compat.  Valid groups:
# kafka, pg-cdc, mysql-cdc, parallel-workload, upsert-stress, combined.
#
# Targets:
#   make build GROUP=kafka           # regen this group's compose YAML, acquire images
#   make up    GROUP=kafka           # build + bring up the stack
#   make down  GROUP=kafka           # tear down (preserves volumes)
#   make smoke GROUP=kafka           # build + up + smoke test
#   make test  GROUP=kafka           # smoke test against a running stack
#   make clean GROUP=kafka           # tear down + remove volumes
#
#   make build-local GROUP=kafka     # build for local dev (no --antithesis flavor)
#   make up-local    GROUP=kafka     # build-local + bring up the stack
#
#   make build-all                   # build every group sequentially
#
# The `-local` targets are for validating the workload + drivers without
# the Antithesis platform.  They build the plain (non-antithesis-flavored)
# images, which (a) don't need libvoidstar.so locally and (b) cover all
# images including new transitive deps (e.g. testdrive) that CI doesn't
# yet publish under the antithesis flavor.

SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

GROUP     ?= combined
PROJECT   := materialize-antithesis-$(GROUP)
REPO_ROOT := $(realpath $(dir $(lastword $(MAKEFILE_LIST)))/../..)
ALL_GROUPS := kafka pg-cdc mysql-cdc parallel-workload upsert-stress combined

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

CONFIG_DIR   := $(REPO_ROOT)/test/antithesis/configs/$(GROUP)
COMPOSE_FILE := $(CONFIG_DIR)/docker-compose.yaml
ENV_FILE     := $(CONFIG_DIR)/.env
COMPOSE      := $(RUNTIME) compose -p $(PROJECT) --env-file $(ENV_FILE) -f $(COMPOSE_FILE)
PSQL         := $(COMPOSE) exec materialized psql -h localhost -p 6875 -U materialize

# mzbuild images we need built locally. Third-party images (postgres, minio,
# kafka, …) are pulled by `docker compose` from their upstream registries.
# Per-group `antithesis-config-<group>` is built by `mzimage acquire`
# transitively, since it's a no-op FROM-scratch image referencing the
# materialized + antithesis-workload fingerprints.
MZBUILD_IMAGES := materialized antithesis-workload-$(GROUP)

# The upsert-stress group's compose adds `upsert-hammer-{i}` services
# backed by a dedicated `antithesis-upsert-hammer` image (kept tiny +
# free of any /opt/antithesis/ directory so Antithesis doesn't pick it
# up as a Test Composer target).  Only acquire it when the active
# group actually references it; other groups would just be paying the
# build cost for no consumer.
ifeq ($(GROUP),upsert-stress)
  MZBUILD_IMAGES += antithesis-upsert-hammer
endif

# ---------------------------------------------------------------------------
# Build
# ---------------------------------------------------------------------------
.PHONY: build export-compose export-env acquire-images build-all

build: export-compose export-env acquire-images

export-compose:
	cd $(REPO_ROOT) && bin/pyactivate test/antithesis/export-compose.py --group=$(GROUP) > $(COMPOSE_FILE)
	@echo "Wrote $(COMPOSE_FILE)"

export-env:
	cd $(REPO_ROOT) && bin/pyactivate test/antithesis/export-env.py --group=$(GROUP) > $(ENV_FILE)
	@echo "Wrote $(ENV_FILE)"

acquire-images:
	@# Force `--arch x86_64` to match what `export-env.py` writes into the
	@# `.env` file. The Antithesis platform itself runs amd64-only — both
	@# `export-env.py` and `export-compose.py` pin `arch=Arch.X86_64` — so
	@# the fingerprints baked into the compose YAML are always for x86_64.
	@# Without this flag, `bin/mzimage acquire` defaults to the host arch
	@# (aarch64 on Apple Silicon), producing a different fingerprint than
	@# the one the compose YAML references; the resulting image doesn't
	@# match the compose's `image:` tag and the local stack fails to pull.
	@# Also: aarch64 cross-compile of `--antithesis` builds needs an aarch64
	@# `libvoidstar.so` which isn't published — x86_64 is the only flavor
	@# Antithesis ships.
	@for image in $(MZBUILD_IMAGES); do \
	  echo "--- Acquiring $$image (--antithesis --arch x86_64)"; \
	  cd $(REPO_ROOT) && bin/mzimage acquire "$$image" --antithesis --arch x86_64; \
	done

build-all:
	@for g in $(ALL_GROUPS); do \
	  echo "==> Building group: $$g"; \
	  $(MAKE) build GROUP=$$g; \
	done

# ---------------------------------------------------------------------------
# Local (non-antithesis) targets
# ---------------------------------------------------------------------------
#
# Build and run the same compose topology without the Antithesis flavor.
# Used for validating the workload + drivers locally before pushing to CI.
# Plain (non-antithesis) mzbuild images:
#   * don't need libvoidstar.so installed in the cross-sysroot
#   * cover all transitive deps (e.g. testdrive), unlike the antithesis
#     flavor which CI only publishes for materialized + antithesis-workload
#     + antithesis-config-<group>.
# The fault-orchestrator service is a no-op outside Antithesis (its
# pause_faults.sh exits cleanly when ANTITHESIS_STOP_FAULTS is unset), so
# the topology behaves like a regular docker-compose stack.

.PHONY: build-local export-compose-local export-env-local acquire-images-local up-local

build-local: export-compose-local export-env-local acquire-images-local

export-compose-local:
	cd $(REPO_ROOT) && bin/pyactivate test/antithesis/export-compose.py \
	    --group=$(GROUP) --no-antithesis > $(COMPOSE_FILE)
	@echo "Wrote $(COMPOSE_FILE) (host arch)"

export-env-local:
	cd $(REPO_ROOT) && bin/pyactivate test/antithesis/export-env.py \
	    --group=$(GROUP) --no-antithesis > $(ENV_FILE)
	@echo "Wrote $(ENV_FILE) (non-antithesis)"

acquire-images-local:
	@# Use the host arch (no `--arch` flag) so the resulting workload image
	@# runs natively. On Apple Silicon, running the x86_64 testdrive binary
	@# under Docker's rosetta/qemu emulation segfaults inside the
	@# foundationdb client init — native aarch64 sidesteps that entirely.
	@# `export-env.py --no-antithesis` mirrors the same logic and emits
	@# host-arch fingerprints to the .env file.
	@for image in $(MZBUILD_IMAGES); do \
	  echo "--- Acquiring $$image (plain, host arch)"; \
	  cd $(REPO_ROOT) && bin/mzimage acquire "$$image"; \
	done

up-local: build-local
	$(COMPOSE) up -d

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
	@echo "[smoke ($(GROUP))] passed"
