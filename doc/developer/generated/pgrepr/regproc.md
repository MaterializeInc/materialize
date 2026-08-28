---
source: src/pgrepr/src/regproc.rs
revision: c317ceee3c
---

# `pgrepr::regproc`

Re-exports `mz_pgrepr_consts::regproc::*` and adds conformance tests.

## Overview

This module is a thin shim that makes the `regproc` name-lookup API available under `mz_pgrepr`. The substantive implementation lives in `mz_pgrepr_consts::regproc`; see that module for documentation of `NAMES`, `name()`, `oid()`, and `NameLookupError`.

## Tests

- `names_is_sorted_by_oid` — verifies that `NAMES` is sorted ascending by OID and has no duplicate OIDs, which is required for binary search to work correctly
- `lookups_round_trip` — verifies that `name(oid)` and `oid(name)` are inverse for every entry in `NAMES`, handling overloaded names that legitimately resolve as `Ambiguous`
- `type_io_functions_resolve` — spot-checks that the `pg_type` I/O function OIDs present in `NAMES` have the expected renderings, catching a regeneration that dropped them
