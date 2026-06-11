# mzcompose

Docker Compose harness for Materialize integration tests. Provides the
`Composition` runtime, service descriptors, and the workflow execution engine
that all `test/*/mzcompose.py` files depend on.

## Subtree (≈ 6,650 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `composition.py` | 1,876 | `Composition` — core runtime: workflow dispatch, Docker Compose subprocess management, testdrive invocation, SQL connections |
| `services/` | ~3,373 | 30+ `Service` subclasses (one per container: Materialized, Cockroach, Kafka, Postgres, etc.) |
| `helpers/` | ~50 | `iceberg.py` — Iceberg-specific helpers |
| `service.py` | 198 | `Service` base class — YAML descriptor |
| `loader.py` | 13 | Module loader for `mzcompose.py` files |
| `__init__.py` | ~140 | Package-level utilities (`cluster_replica_size_map`, `get_default_system_parameters`) |

## Purpose

Wraps `docker compose` with Materialize-aware workflow orchestration. Test
files define named `workflow_*` functions; `composition.py` discovers and
dispatches them. `Service` subclasses in `services/` emit docker-compose YAML
fragments. `Composition` owns connections, testdrive subprocess, and parallel
execution via selectors.

## Key interfaces

- `Composition` — `run()`, `invoke()`, `sql()`, `testdrive()`, `up()`, `down()`; workflow dispatch via `workflow_<name>` function naming convention
- `Service` (base) — `__init__(name, config)` → YAML descriptor; subclasses extend with typed kwargs
- `WorkflowArgumentParser` — `argparse.ArgumentParser` subclass injected into each workflow
- `TestResult` / `FailedTestExecutionError` — structured test result reporting

## Naming convention

`workflow_default` is the entry point when no workflow name is specified.
`workflow_*` functions are discovered by name inspection; no decorator needed.

## Services directory

30+ service modules follow a consistent pattern: a single class inheriting
`MzComposeService` with typed constructor kwargs that map to docker-compose
config keys. `materialized.py` (412 LOC) is the most complex, encoding
deployment status, health checks, and leader-status logic.

## Bubbled findings for materialize/CONTEXT.md

- **`composition.py` is 1,876 LOC**: monolithic; owns subprocess management,
  SQL connections, testdrive orchestration, parallel selectors, and workflow
  dispatch all in one class. A seam between "compose subprocess adapter" and
  "test orchestration runtime" would reduce coupling.
- **Workflow discovery by name convention** (`workflow_*`): implicit, not
  type-checked; typos silently result in an undiscoverable workflow.
- **`services/` is a well-factored adapter layer**: each external service is
  an isolated module; adding a new container requires only a new file.
