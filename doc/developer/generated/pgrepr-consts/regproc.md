---
source: src/pgrepr-consts/src/regproc.rs
revision: c317ceee3c
---

# `pgrepr_consts::regproc`

Text renderings of `regproc` values for Materialize's builtin functions.

## Overview

PostgreSQL renders a `regproc` in text format through `regprocout`, which resolves the OID to the function's name. Because Materialize has no `CREATE FUNCTION`, every function is a builtin and the OID-to-name mapping is fixed at build time. This makes a static table viable: the text encoder has no catalog access and cannot gain any, because the catalog crates depend on `mz-pgrepr` rather than the reverse, and some encode paths run in `clusterd`, which has no catalog at all.

## `NAMES`

A sorted slice of `(u32, &str)` pairs mapping every builtin function OID to the text rendering a `regproc` cast produces. The name is schema-qualified whenever the bare name would not resolve back to that OID under the default search path.

`NAMES` is generated from Materialize's builtin function registry via `mz_catalog::builtin::BUILTINS::funcs()`. The `mz_catalog::builtin` test `test_regproc_names_match_builtin_functions` recomputes the whole table from that registry and fails when the two have drifted. Do not hand-edit entries; instead add the function to the registry and regenerate:

```shell
REWRITE=1 cargo test -p mz-catalog test_regproc_names_match_builtin_functions
```

OIDs that PostgreSQL assigns are taken from the PostgreSQL 13 `pg_proc` catalog. Unpinned PostgreSQL functions draw from `mz_pgrepr_consts::oid`'s `FIRST_UNPINNED_OID` range; Materialize-only functions draw from `FIRST_MATERIALIZE_OID`.

## Functions

- `name(oid: u32) -> Option<&'static str>` — binary-searches `NAMES` for the text rendering of `oid`; returns `None` for unrecognized OIDs (including 0)
- `oid(name: &str) -> Result<u32, NameLookupError>` — linear-scans `NAMES` for the OID whose rendering is `name`; returns `Err(Ambiguous)` when multiple overloads share the same rendering

## `NameLookupError`

- `NotFound` — no builtin function renders as the given name
- `Ambiguous` — multiple builtin functions render as the given name, matching PostgreSQL's `regprocin` behavior
