# parallel_workload

Concurrent SQL stress tester. Runs multiple worker threads simultaneously
issuing random DML/DDL actions against a live Materialize instance to expose
race conditions, concurrency bugs, and crash scenarios.

## Subtree (≈ 5,995 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `action.py` | 3,272 | ~70 `Action` subclasses + `ActionList` registry + 5 named action lists |
| `database.py` | 1,201 | In-memory database state model (`DB`, `Table`, `View`, `KafkaSource`, etc.) |
| `parallel_workload.py` | 565 | Top-level harness: thread spawning, signal handling, summary |
| `expression.py` | 303 | SQL expression builders |
| `executor.py` | 266 | `Executor` — connection management and query dispatch |
| `column.py` | 173 | Column type descriptors |
| `worker.py` | 146 | `Worker` — per-thread run loop |
| `settings.py` | 47 | `Settings` dataclass (thread counts, timeouts) |

## Purpose

Each `Worker` picks a random `ActionList` and samples an `Action` by weight,
then executes it against a live connection. The in-memory `DB` state tracks
created objects so actions can reference valid targets. Five named lists
partition action space: `read_action_list`, `fetch_action_list`,
`write_action_list`, `dml_nontrans_action_list`, `ddl_action_list`.

## Key interfaces

- `Action.run(executor: Executor, db: DB, random: Random) → None` — per-action contract
- `ActionList(action_classes_weights, autocommit)` — weighted sampler
- `action_lists: list[ActionList]` — global registry at module bottom of `action.py`
- `Worker.run()` — main thread loop
- `DB` — shared mutable state; access serialized by per-object locks

## Registration pattern

`action_lists` at the bottom of `action.py` is a manually maintained
parallel list of `(ActionClass, weight)` tuples. New actions must be added
both as a class and as an entry in the appropriate `ActionList`. Several
actions are commented out with `# TODO: Reenable when <issue>` notes.

## Bubbled findings for materialize/CONTEXT.md

- **`action.py` is 3,272 LOC / ~70 classes in one file**: the file is a
  parallel-list dispatch pattern (class definition + weight in separate
  locations); splitting into sub-modules per action category would reduce
  merge friction.
- **Commented-out actions with TODO+issue refs** (3 in `ddl_action_list`):
  no automated signal when issues are resolved, same staleness problem as
  `@disabled` in checks.
- **`DB` shared state + per-object locks**: threading model is correct but
  subtle; the state model in `database.py` mirrors the DB schema in Python —
  any schema divergence is a silent failure mode.
