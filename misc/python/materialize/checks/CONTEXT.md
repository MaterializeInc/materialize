# checks

Platform-checks framework. Drives upgrade/restart/zero-downtime survival tests
by composing ~147 `Check` subclasses with ~32 `Scenario` subclasses.

## Subtree (≈ 15,819 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `all_checks/` | 13,050 | All concrete Check subclasses (see its CONTEXT.md) |
| `scenarios_upgrade.py` | 891 | 14 upgrade scenarios (rolling, multi-version, preflight) |
| `mzcompose_actions.py` | 423 | Docker-compose-level lifecycle actions (StartMz, KillMz, etc.) |
| `scenarios_zero_downtime.py` | 353 | 7 ZDT scenarios |
| `scenarios.py` | 346 | Base `Scenario` class + 11 basic scenarios |
| `executors.py` | 180 | `Executor` interface + `MzcomposeExecutor` / `CloudtestExecutor` |
| `actions.py` | 164 | `Action` base + `Testdrive` / `PyAction` |
| `checks.py` | 128 | `Check` base + `@disabled` / `@externally_idempotent` / `@supports_forced_migrations` decorators |

## Purpose

Verifies that individual SQL features survive upgrades and restarts. A `Scenario`
sequences `Initialize → Manipulate × 2 → [lifecycle event] → Validate` across
`Check` instances. Execution is delegated to an `Executor` (mzcompose or
cloudtest). The test binary at `test/platform-checks/mzcompose.py` stitches
these together via `all_subclasses()` runtime discovery.

## Key interfaces

- `Check` — `initialize()`, `manipulate()` (len==2 invariant), `validate()`
- `Scenario` — `actions() → list[Action]`; `checks() → list[type[Check]]`
- `Executor` — `testdrive()`, `run_pyaction()`; two implementations: mzcompose and cloudtest
- `Action` — `execute(e: Executor)` / `join(e: Executor)`

## Registration / discovery

`all_checks/__init__.py` glob-imports every module; the test driver calls
`all_subclasses(Check)` and `all_subclasses(Scenario)` for runtime discovery.
No explicit registry; relies on Python's `__subclasses__()` chain.

## Bubbled findings for materialize/CONTEXT.md

- **`manipulate()` len==2 is a hidden protocol invariant**: type-checked only at
  runtime via `assert`, not via the signature. A `list[T]` return with a literal
  length constraint is a seam that wants either a fixed-tuple type or explicit
  method split.
- **Import-side-effect discovery**: auto-import in `__init__.py` works but makes
  test-binary startup sensitive to module-level errors in any check file.
- **`@disabled` checks accumulate without staleness tracking**: 8 disabled checks
  across the codebase; no mechanism to alert when the referenced issue is closed.
- **Scenario count vs. check count imbalance**: 32 scenarios × 147 checks =
  ~4,700 (scenario, check) pairs run in full; CI parallelizes by scenario.
