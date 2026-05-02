# CONTEXT-MAP — index of all `CONTEXT.md` and `ARCH_REVIEW.md` files

Navigation index across the architecture-review file tree. Every directory
≥5K LOC has a `CONTEXT.md`; some also have an `ARCH_REVIEW.md` flagging
verifiable architectural friction.

Start here for a top-down read:
1. [`CONTEXT.md`](CONTEXT.md) — repo overview, core architecture commitments, workspace-wide friction.
2. [`ARCH_REVIEW.md`](ARCH_REVIEW.md) — round-2 cross-crate patterns (5 candidates + 2 honest skips). Read after the per-crate ARCH_REVIEW.md files have been internalized.
3. [`src/CONTEXT.md`](src/CONTEXT.md) — Rust workspace map, crates by tier, workspace-wide seams.
4. Any individual crate or subtree below.

## src/ — Rust workspace (27 top-level crates)

### Process / coordinator

- [`src/environmentd/CONTEXT.md`](src/environmentd/CONTEXT.md) — process entry point (pgwire, http, adapter, 0dt deploy)
  - [`src/environmentd/src/CONTEXT.md`](src/environmentd/src/CONTEXT.md)
    - [`src/environmentd/src/http/CONTEXT.md`](src/environmentd/src/http/CONTEXT.md) — HTTP module
    - [`src/environmentd/src/http/ARCH_REVIEW.md`](src/environmentd/src/http/ARCH_REVIEW.md) — `execute_request` over-generalization, auth co-located with routing
  - [`src/environmentd/tests/CONTEXT.md`](src/environmentd/tests/CONTEXT.md) — integration tests (1.35:1 test:source ratio)
- [`src/adapter/CONTEXT.md`](src/adapter/CONTEXT.md) — coordinator, sequencer, RBAC
  - [`src/adapter/src/CONTEXT.md`](src/adapter/src/CONTEXT.md)
    - [`src/adapter/src/catalog/CONTEXT.md`](src/adapter/src/catalog/CONTEXT.md) — adapter-side catalog (distinct from mz-catalog)
    - [`src/adapter/src/coord/CONTEXT.md`](src/adapter/src/coord/CONTEXT.md) — Coordinator core
      - [`src/adapter/src/coord/sequencer/CONTEXT.md`](src/adapter/src/coord/sequencer/CONTEXT.md) — Plan dispatch
      - [`src/adapter/src/coord/sequencer/ARCH_REVIEW.md`](src/adapter/src/coord/sequencer/ARCH_REVIEW.md) — 9 `Message::*StageReady` parallel variants
        - [`src/adapter/src/coord/sequencer/inner/CONTEXT.md`](src/adapter/src/coord/sequencer/inner/CONTEXT.md) — per-statement sequencing
        - [`src/adapter/src/coord/sequencer/inner/ARCH_REVIEW.md`](src/adapter/src/coord/sequencer/inner/ARCH_REVIEW.md) — parallel `validity()`/`stage()` match arms

### SQL frontend

- [`src/sql-parser/CONTEXT.md`](src/sql-parser/CONTEXT.md) — text → AST<Raw>
  - [`src/sql-parser/src/CONTEXT.md`](src/sql-parser/src/CONTEXT.md)
    - [`src/sql-parser/src/ast/CONTEXT.md`](src/sql-parser/src/ast/CONTEXT.md)
      - [`src/sql-parser/src/ast/defs/CONTEXT.md`](src/sql-parser/src/ast/defs/CONTEXT.md) — grammar-region split
      - [`src/sql-parser/src/ast/defs/ARCH_REVIEW.md`](src/sql-parser/src/ast/defs/ARCH_REVIEW.md) — `WithOptionName::redact_value` 36 impls / wildcard arms (PII leak risk)
- [`src/sql/CONTEXT.md`](src/sql/CONTEXT.md) — SQL compiler (purify + plan)
  - [`src/sql/src/CONTEXT.md`](src/sql/src/CONTEXT.md)
    - [`src/sql/src/plan/CONTEXT.md`](src/sql/src/plan/CONTEXT.md) — Plan enum, HIR, decorrelation
      - [`src/sql/src/plan/statement/CONTEXT.md`](src/sql/src/plan/statement/CONTEXT.md)
      - [`src/sql/src/plan/statement/ARCH_REVIEW.md`](src/sql/src/plan/statement/ARCH_REVIEW.md) — `ddl.rs` 8K LOC / 110 fns / 30 domains; split by domain
    - [`src/sql/src/session/CONTEXT.md`](src/sql/src/session/CONTEXT.md) — Vars, FeatureFlag

### IR + optimizer

- [`src/expr/CONTEXT.md`](src/expr/CONTEXT.md) — IR hub (MIR)
  - [`src/expr/src/CONTEXT.md`](src/expr/src/CONTEXT.md)
    - [`src/expr/src/relation/CONTEXT.md`](src/expr/src/relation/CONTEXT.md) — `MirRelationExpr`
    - [`src/expr/src/scalar/CONTEXT.md`](src/expr/src/scalar/CONTEXT.md) — `MirScalarExpr`
      - [`src/expr/src/scalar/func/CONTEXT.md`](src/expr/src/scalar/func/CONTEXT.md) — function/operator definitions
      - [`src/expr/src/scalar/func/ARCH_REVIEW.md`](src/expr/src/scalar/func/ARCH_REVIEW.md) — stale `enum_dispatch` comments; container-cast `introduces_nulls=true` conservatism
        - [`src/expr/src/scalar/func/impls/CONTEXT.md`](src/expr/src/scalar/func/impls/CONTEXT.md) — 34 per-type submodules
- [`src/transform/CONTEXT.md`](src/transform/CONTEXT.md) — MIR optimizer
  - [`src/transform/src/CONTEXT.md`](src/transform/src/CONTEXT.md)
  - [`src/transform/src/ARCH_REVIEW.md`](src/transform/src/ARCH_REVIEW.md) — `Transform` trait dead surface; deprecated `logical_optimizer` still called; `ordering.rs` empty stub; `Fixpoint` O(n²); pipeline ordering convention
- [`src/compute-types/CONTEXT.md`](src/compute-types/CONTEXT.md) — LIR plan
  - [`src/compute-types/src/CONTEXT.md`](src/compute-types/src/CONTEXT.md)
    - [`src/compute-types/src/plan/CONTEXT.md`](src/compute-types/src/plan/CONTEXT.md) — Plan, RenderPlan, Interpreter

### Foundational data layer

- [`src/repr/CONTEXT.md`](src/repr/CONTEXT.md) — Datum, Row, RelationDesc
  - [`src/repr/src/CONTEXT.md`](src/repr/src/CONTEXT.md)
  - [`src/repr/src/ARCH_REVIEW.md`](src/repr/src/ARCH_REVIEW.md) — dual-type Sql*/Repr* `backport_nullability` warning; `OptimizerFeatures` layer leak; `NamedPlan` requires `mz-sql-parser` build-heavy dep
    - [`src/repr/src/adt/CONTEXT.md`](src/repr/src/adt/CONTEXT.md) — abstract data types (13 modules)

### Catalog

- [`src/catalog/CONTEXT.md`](src/catalog/CONTEXT.md) — durable + in-memory
  - [`src/catalog/src/CONTEXT.md`](src/catalog/src/CONTEXT.md)
    - [`src/catalog/src/durable/CONTEXT.md`](src/catalog/src/durable/CONTEXT.md) — durable layer
    - [`src/catalog/src/durable/ARCH_REVIEW.md`](src/catalog/src/durable/ARCH_REVIEW.md) — `Transaction` god-struct (19 fields, 103 methods, 4K LOC); upgrade-step outlier `v78_to_v79`
- [`src/catalog-protos/CONTEXT.md`](src/catalog-protos/CONTEXT.md) — frozen protos + upgrade chain
  - [`src/catalog-protos/src/CONTEXT.md`](src/catalog-protos/src/CONTEXT.md) — thin pointer

### Compute

- [`src/compute/CONTEXT.md`](src/compute/CONTEXT.md) — Timely execution engine
  - [`src/compute/src/CONTEXT.md`](src/compute/src/CONTEXT.md) — `Correction` V1/V2 in-flight migration noted
    - [`src/compute/src/render/CONTEXT.md`](src/compute/src/render/CONTEXT.md) — RenderPlan → Timely operators
    - [`src/compute/src/render/ARCH_REVIEW.md`](src/compute/src/render/ARCH_REVIEW.md) — per-source collection seam friction
- [`src/compute-client/CONTEXT.md`](src/compute-client/CONTEXT.md) — client API
  - [`src/compute-client/src/CONTEXT.md`](src/compute-client/src/CONTEXT.md)
  - [`src/compute-client/src/ARCH_REVIEW.md`](src/compute-client/src/ARCH_REVIEW.md) — `as_of_selection` misplacement; `read_capabilities` comment-only contract; `SequentialHydration` single-export assumption

### Storage

- [`src/storage/CONTEXT.md`](src/storage/CONTEXT.md) — timely cluster crate
  - [`src/storage/src/CONTEXT.md`](src/storage/src/CONTEXT.md)
  - [`src/storage/src/ARCH_REVIEW.md`](src/storage/src/ARCH_REVIEW.md) — Upsert v1/v2/dispatch triplication (~3,350 LOC, ENABLE_UPSERT_V2 dyncfg); deletion checklist
    - [`src/storage/src/source/CONTEXT.md`](src/storage/src/source/CONTEXT.md) — connector implementations
      - [`src/storage/src/source/generator/CONTEXT.md`](src/storage/src/source/generator/CONTEXT.md) — load-generator sources
- [`src/storage-controller/CONTEXT.md`](src/storage-controller/CONTEXT.md) — `lib.rs` "Leviathan" 4K LOC
  - [`src/storage-controller/src/CONTEXT.md`](src/storage-controller/src/CONTEXT.md) — thin pointer
- [`src/storage-client/CONTEXT.md`](src/storage-client/CONTEXT.md) — `StorageCollectionsImpl` inversion
  - [`src/storage-client/src/CONTEXT.md`](src/storage-client/src/CONTEXT.md) — thin pointer
- [`src/storage-types/CONTEXT.md`](src/storage-types/CONTEXT.md) — shared types
  - [`src/storage-types/src/CONTEXT.md`](src/storage-types/src/CONTEXT.md)
    - [`src/storage-types/src/connections/CONTEXT.md`](src/storage-types/src/connections/CONTEXT.md)
    - [`src/storage-types/src/sinks/CONTEXT.md`](src/storage-types/src/sinks/CONTEXT.md)
    - [`src/storage-types/src/sources/CONTEXT.md`](src/storage-types/src/sources/CONTEXT.md)

### Persistence

- [`src/persist/CONTEXT.md`](src/persist/CONTEXT.md) — Blob/Consensus seam
  - [`src/persist/src/CONTEXT.md`](src/persist/src/CONTEXT.md)
    - [`src/persist/src/indexed/CONTEXT.md`](src/persist/src/indexed/CONTEXT.md)
- [`src/persist-client/CONTEXT.md`](src/persist-client/CONTEXT.md) — high-level client + `PersistClientCache`
  - [`src/persist-client/src/CONTEXT.md`](src/persist-client/src/CONTEXT.md)
    - [`src/persist-client/src/internal/CONTEXT.md`](src/persist-client/src/internal/CONTEXT.md)
    - [`src/persist-client/src/internal/ARCH_REVIEW.md`](src/persist-client/src/internal/ARCH_REVIEW.md) — `service.rs` placement; `BlobMemCache`/`MetricsBlob` decorator chain
- [`src/persist-types/CONTEXT.md`](src/persist-types/CONTEXT.md) — Codec
  - [`src/persist-types/src/CONTEXT.md`](src/persist-types/src/CONTEXT.md)
- [`src/txn-wal/CONTEXT.md`](src/txn-wal/CONTEXT.md) — atomic multi-shard txns
  - [`src/txn-wal/src/CONTEXT.md`](src/txn-wal/src/CONTEXT.md) — thin pointer

### Operations / infra

- [`src/orchestratord/CONTEXT.md`](src/orchestratord/CONTEXT.md) — K8s operator
  - [`src/orchestratord/src/CONTEXT.md`](src/orchestratord/src/CONTEXT.md)
- [`src/cloud-resources/CONTEXT.md`](src/cloud-resources/CONTEXT.md) — K8s CRDs + `vpc-endpoints` flag
  - [`src/cloud-resources/src/CONTEXT.md`](src/cloud-resources/src/CONTEXT.md)
    - [`src/cloud-resources/src/crd/CONTEXT.md`](src/cloud-resources/src/crd/CONTEXT.md) — Materialize/Balancer/Console/VpcEndpoint
- [`src/frontegg-mock/CONTEXT.md`](src/frontegg-mock/CONTEXT.md) — auth mock (no trait seam to real)
  - [`src/frontegg-mock/src/CONTEXT.md`](src/frontegg-mock/src/CONTEXT.md)
- [`src/testdrive/CONTEXT.md`](src/testdrive/CONTEXT.md) — SQL test runner
  - [`src/testdrive/src/CONTEXT.md`](src/testdrive/src/CONTEXT.md)
  - [`src/testdrive/src/ARCH_REVIEW.md`](src/testdrive/src/ARCH_REVIEW.md) — adapter+catalog direct prod deps; `Config` ~100-field monolith; built-in command string-match dispatch
    - [`src/testdrive/src/action/CONTEXT.md`](src/testdrive/src/action/CONTEXT.md)
- [`src/avro/CONTEXT.md`](src/avro/CONTEXT.md) — Avro fork
  - [`src/avro/src/CONTEXT.md`](src/avro/src/CONTEXT.md)

### Standard library

- [`src/ore/CONTEXT.md`](src/ore/CONTEXT.md) — workspace stdlib (~50 modules)
  - [`src/ore/src/CONTEXT.md`](src/ore/src/CONTEXT.md)
- [`src/timely-util/CONTEXT.md`](src/timely-util/CONTEXT.md) — Timely helpers
  - [`src/timely-util/src/CONTEXT.md`](src/timely-util/src/CONTEXT.md)

## misc/ — Python tooling

- [`misc/python/CONTEXT.md`](misc/python/CONTEXT.md) — Python tooling root
  - [`misc/python/materialize/CONTEXT.md`](misc/python/materialize/CONTEXT.md) — `materialize` package
    - [`misc/python/materialize/checks/CONTEXT.md`](misc/python/materialize/checks/CONTEXT.md) — platform-checks framework
      - [`misc/python/materialize/checks/all_checks/CONTEXT.md`](misc/python/materialize/checks/all_checks/CONTEXT.md) — individual checks
    - [`misc/python/materialize/cli/CONTEXT.md`](misc/python/materialize/cli/CONTEXT.md) — operational CLIs
    - [`misc/python/materialize/mzcompose/CONTEXT.md`](misc/python/materialize/mzcompose/CONTEXT.md) — docker-compose harness; `composition.py` 1.9K LOC monolith
    - [`misc/python/materialize/output_consistency/CONTEXT.md`](misc/python/materialize/output_consistency/CONTEXT.md) — differential SQL testing
      - [`misc/python/materialize/output_consistency/input_data/CONTEXT.md`](misc/python/materialize/output_consistency/input_data/CONTEXT.md) — test corpora
    - [`misc/python/materialize/parallel_workload/CONTEXT.md`](misc/python/materialize/parallel_workload/CONTEXT.md) — stress testing; ~70 action classes parallel-list dispatch
- [`misc/dbt-materialize/CONTEXT.md`](misc/dbt-materialize/CONTEXT.md) — dbt adapter (PostgresAdapter + blue-green deploy)

## console/ — TypeScript web console

- [`console/CONTEXT.md`](console/CONTEXT.md) — SPA, three deployment modes
  - [`console/src/CONTEXT.md`](console/src/CONTEXT.md)
    - [`console/src/api/CONTEXT.md`](console/src/api/CONTEXT.md) — auth adapters, REST clients
    - [`console/src/api/ARCH_REVIEW.md`](console/src/api/ARCH_REVIEW.md) — dual SQL path (executeSql v1/v2 migration); WebSocket singleton coupling; per-call region client construction; query_key as URL param
      - [`console/src/api/materialize/CONTEXT.md`](console/src/api/materialize/CONTEXT.md) — SQL execution layer
    - [`console/src/platform/CONTEXT.md`](console/src/platform/CONTEXT.md) — feature routes, React Query

## test/ — Integration tests

- [`test/CONTEXT.md`](test/CONTEXT.md) — four-framework taxonomy (testdrive 1,185 .td; sqllogictest 481 .slt; mzcompose ~98; pytest)
  - [`test/cluster/CONTEXT.md`](test/cluster/CONTEXT.md) — external-clusterd suite (Toxiproxy fault injection)

## Summary

- 98 `CONTEXT.md` files
- 14 `ARCH_REVIEW.md` files
- 96 directories ≥5K LOC reviewed
- ~38 commits in `git log --oneline | grep arch-review` documenting the DFS
  evolution
