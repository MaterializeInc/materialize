# cli

Collection of standalone CLI tools for CI, developer workflow, and environment
management. Each script is independently invocable; no shared sub-command
dispatcher.

## Subtree (≈ 8,452 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `ci_annotate_errors.py` | 1,116 | Annotate Buildkite builds with error summaries |
| `mzcompose.py` | 1,069 | `mzcompose` CLI driver (Docker Compose wrapper) |
| `mz_workload_capture.py` | 644 | Capture/replay workload traces |
| `run.py` | 615 | Build + run core services (environmentd, sqllogictest) locally |
| `orchestratord.py` | 501 | orchestratord lifecycle management |
| `mzexplore.py` | 425 | SQL explain/explore tool |
| `ci_closed_issues_detect.py` | 346 | Detect `@disabled` with closed GitHub issues |
| `mz_workload_anonymize.py` | 327 | Anonymize captured workload traces |
| `gen-lints.py` | 325 | Generate lint rules |
| `scratch/` | 824 | EC2 scratch environment management (create/destroy/ssh/sftp) |
| remaining | ~1,455 | lint, fmt, xcompile, mzimage, cloudbench, helm tools, etc. |

## Purpose

Heterogeneous bag of developer and CI tools. `mzcompose.py` is the primary
entry point for integration test execution. `run.py` drives local dev. The
`scratch/` sub-package manages ephemeral EC2 environments for CI and manual
testing.

## Key interfaces

Each script has its own `main()` / `argparse` setup. There is no shared
dispatcher; scripts are invoked directly via `python -m materialize.cli.<name>`
or via `bin/` wrappers.

## Notable patterns

- `ci_annotate_errors.py` parses Buildkite logs and annotates failures; tightly
  coupled to Buildkite API.
- `mzcompose.py` is the user-facing mzcompose entrypoint; it delegates heavily
  to `mzcompose/composition.py`.
- `scratch/` uses boto3 for EC2; has its own subcommand dispatch via `__main__.py`.

## Bubbled findings for materialize/CONTEXT.md

- **No shared CLI framework**: each script reinvents `argparse` setup; no
  common entry-point dispatcher makes it hard to discover available tools.
- **`ci_closed_issues_detect.py` provides staleness detection for disabled
  tests** but is not hooked to the `@disabled` decorator in `checks/`; these
  two mechanisms are only loosely coupled.
