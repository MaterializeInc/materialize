# misc/python

Root of the Materialize Python tooling tree. Contains the `materialize` package
(94,690 LOC) plus dependency manifests and type stubs.

## Subtree (≈ 94,690 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `materialize/` | 94,690 | Entire Python test infra and CI tooling (see `materialize/CONTEXT.md`) |
| `stubs/` | — | Type stubs for third-party deps lacking PEP 561 markers |
| `requirements.txt` | — | Full pinned dependency set |
| `requirements-core.txt` | — | Minimal subset for lightweight environments |

## Purpose

All Python code in the repo lives here: integration test harnesses (mzcompose,
platform-checks, output-consistency, parallel-workload), CI utilities
(ci_annotate_errors, ci_closed_issues_detect), and developer tools (run,
mzexplore, scratch).

## Package entry points

Scripts are invoked as `python -m materialize.cli.<name>` or via `bin/`
wrappers. The `mzcompose` CLI (`materialize/cli/mzcompose.py`) is the primary
integration-test entry point.

## Key findings bubbled from materialize/CONTEXT.md

- **Staleness accumulation**: `@disabled`, `is_enabled=False`, and
  `# TODO: Reenable` annotations in checks, output_consistency, and
  parallel_workload accumulate without automated detection. A single
  CI-enforced staleness check (e.g., cross-referencing closed GitHub issues)
  would cover all three mechanisms.
- **Parallel-list dispatch friction** in `parallel_workload/action.py` and
  `output_consistency/all_operations_provider.py`: new items require edits in
  two places. `all_subclasses()` (already used for checks/scenarios) is the
  natural fix.
- **`mzcompose/composition.py` (1,876 LOC)** is the largest single-file
  coupling point in the test infrastructure.
- **Two parallel random-action frameworks** (`parallel_workload/` and `zippy/`)
  with overlapping scope and no documented boundary.
- **`manipulate()` len==2 protocol invariant** in platform-checks is enforced
  only at runtime; a structural type would surface violations at definition time.
