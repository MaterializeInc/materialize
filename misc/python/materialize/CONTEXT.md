# materialize (Python package)

Top-level Python package for all Materialize test infrastructure, CI tooling,
and developer utilities. Located at `misc/python/materialize/`.

## Subtree (≈ 94,690 LOC total)

### Major sub-packages reviewed with their own CONTEXT.md

| Path | LOC | Purpose |
|---|---|---|
| `checks/` | 15,819 | Platform-checks: upgrade/restart survival tests (see `checks/CONTEXT.md`) |
| `output_consistency/` | 15,248 | Differential SQL consistency testing (see `output_consistency/CONTEXT.md`) |
| `cli/` | 8,452 | Standalone CLI tools: mzcompose, run, ci_annotate_errors, etc. (see `cli/CONTEXT.md`) |
| `mzcompose/` | 6,650 | Docker Compose harness and service descriptors (see `mzcompose/CONTEXT.md`) |
| `parallel_workload/` | 5,995 | Concurrent SQL stress tester (see `parallel_workload/CONTEXT.md`) |

### Other sub-packages (not leaf-reviewed)

| Path | LOC | Purpose |
|---|---|---|
| `feature_benchmark/` | 4,681 | Micro-benchmark scenarios for feature performance |
| `zippy/` | 4,443 | Stateful random-action framework (similar to parallel_workload but scenario-based) |
| `workload_replay/` | 4,274 | Workload capture/replay harness |
| `scalability/` | 3,062 | Scalability test framework |
| `buildkite_insights/` | 3,030 | Buildkite analytics and flake detection |
| `cloudtest/` | 2,852 | Kubernetes-based integration test runtime (mirrors mzcompose for cloud) |
| `data_ingest/` | 2,692 | Data ingestion primitives for test data generation |
| `test_analytics/` | 2,176 | Test result analytics DB (pushes results to staging DB) |
| `postgres_consistency/` | 2,025 | Postgres cross-validation variant of output_consistency |

### Key top-level modules

| Module | LOC | Purpose |
|---|---|---|
| `mzbuild.py` | 1,705 | mzbuild image build system |
| `version_list.py` | 665 | Release version list and ancestry |
| `scratch.py` | 591 | EC2 scratch environment core |
| `git.py` | 436 | Git utilities |
| `buildkite.py` | 320 | Buildkite API helpers |
| `mz_version.py` | 143 | `MzVersion` semver type |
| `util.py` | 239 | `all_subclasses()`, `filter_cmd()`, misc helpers |

## Architectural patterns

**Runtime class discovery**: `all_subclasses(Check)` and `all_subclasses(Scenario)`
(from `util.py`) are used by the platform-checks test driver to discover all
registered test classes without an explicit registry. Requires that modules
be imported first — handled by `all_checks/__init__.py`'s glob import.

**Executor/Strategy adapters**: Both `checks/` (via `Executor`) and
`output_consistency/` (via `EvaluationStrategy`) use the same pattern: an
abstract interface with mzcompose and cloudtest/pg-wire implementations,
allowing the same test logic to run in different environments.

**Weighted action lists**: `parallel_workload/` and `zippy/` both express
test actions as weighted tuples in manually maintained lists at module level.

## Cross-cutting findings for root CONTEXT.md

- **Staleness accumulation pattern**: `@disabled(reason)` in `checks/`,
  `is_enabled=False` in `output_consistency/`, and `# TODO: Reenable` in
  `parallel_workload/` all track known-broken tests by string annotation with
  no automated staleness detection. `cli/ci_closed_issues_detect.py` solves
  this for GitHub issues but is not wired to the other mechanisms.
- **Parallel-list dispatch friction**: `parallel_workload/action.py` (3,272 LOC,
  ~70 classes) and `output_consistency/input_data/operations/all_operations_provider.py`
  both require editing two locations to register a new item (class + list entry).
  Auto-discovery via `all_subclasses()` (already used for checks/scenarios) would
  eliminate the split.
- **`mzcompose/composition.py` is 1,876 LOC**: the monolith owns subprocess
  management, SQL connections, testdrive orchestration, and workflow dispatch.
  The largest single source of coupling in the test infra.
- **`checks/checks.py` `manipulate()` len==2 invariant**: enforced at runtime
  only; a structural type (e.g., a 2-tuple) would surface violations earlier.
- **Two parallel random-action frameworks**: `parallel_workload/` and `zippy/`
  serve overlapping purposes (concurrent random SQL actions). The distinction
  (scenario-based vs. worker-pool-based) is not documented at the package level.
