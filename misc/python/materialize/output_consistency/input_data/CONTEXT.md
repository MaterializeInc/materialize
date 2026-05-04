# output_consistency/input_data

Input catalogue for the differential SQL consistency test. Declares all SQL
types and operations that the test engine can combine into random queries.

## Subtree (≈ 6,916 LOC)

| Path | LOC | What it owns |
|---|---|---|
| `operations/` | ~3,800 | 21 per-type operation provider modules + `all_operations_provider.py` |
| `types/` | ~1,200 | 21 per-type type provider modules + `all_types_provider.py` |
| `test_input_data.py` | 60 | `ConsistencyTestInputData` — top-level container |
| `test_input_operations.py` | 55 | `ConsistencyTestOperationsInput` — filters disabled ops |
| `test_input_types.py` | ~30 | `ConsistencyTestTypesInput` — assembles type set |
| `constants/`, `params/`, `return_specs/`, `values/`, `validators/` | remainder | Supporting descriptors |

## Purpose

Provides the full SQL function/operator catalogue (strings, numbers, date/time,
arrays, JSONB, aggregates, etc.) as Python `DbOperationOrFunction` objects.
Operations are assembled in `all_operations_provider.py` via `itertools.chain`
over type-specific lists. `ConsistencyTestInputData` unifies types + operations
for the test runner.

## Key interfaces

- `ConsistencyTestInputData` — `.types_input`, `.operations_input`,
  `.predefined_queries`; `assign_columns_to_tables()`, `get_stats()`
- `ConsistencyTestOperationsInput` — `all_operation_types: list[DbOperationOrFunction]`;
  `remove_functions(predicate)`
- `ALL_OPERATION_TYPES` — flat list in `all_operations_provider.py`; manually
  extended when new SQL functions are added

## Bubbled findings for output_consistency/CONTEXT.md

- **Manual registration list**: `all_operations_provider.py` concatenates
  type-specific lists explicitly; adding a new type requires editing this file.
  No auto-discovery.
- **3 `is_enabled=False` operations**: disabled inline in provider modules;
  no staleness signaling.
- **Provider naming convention**: `*_operations_provider.py` / `*_type_provider.py`
  is consistent and searchable.
