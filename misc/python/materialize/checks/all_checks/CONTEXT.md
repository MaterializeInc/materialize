# checks/all_checks

Platform-check leaf package. 95 modules, ~147 `Check` subclasses covering
individual SQL features (aggregation, cast, sink, CDC, owners, window functions,
etc.).

## Subtree (≈ 13,050 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `*.py` | 13,050 | One or more `Check` subclasses per file |

## Purpose

Each file implements one or more `Check` subclasses with three lifecycle
methods: `initialize()`, `manipulate()` (exactly 2 elements required by
contract), and `validate()`. Checks survive restart/upgrade scenarios defined
in `checks/scenarios*.py`.

## Key interfaces

- `Check.initialize()` → `Testdrive | PyAction`
- `Check.manipulate()` → `list[Testdrive] | list[PyAction]` (invariant: len == 2)
- `Check.validate()` → `Testdrive | PyAction`
- Class-level decorators: `@disabled(reason)`, `@externally_idempotent(bool)`,
  `@supports_forced_migrations(bool)` in `checks/checks.py`

## Discovery

`__init__.py` uses `glob` + `import_module` to auto-import every `.py` file in
this directory. Consumers call `all_subclasses(Check)` (from `util.py`) after
doing `from materialize.checks.all_checks import *`.

## Bubbled findings for checks/CONTEXT.md

- **Hard-wired `manipulate()` length == 2**: enforced by `assert` in `checks.py`,
  not the type system; a 1- or 3-phase check cannot be expressed without changing
  the protocol.
- **8 `@disabled` checks**: tracked via `reason` string (usually a GitHub issue
  URL), no automated staleness detection.
- **`sink.py` is 1,500 LOC**: the largest file; contains 14+ Check subclasses.
  Consider splitting by sink type.
