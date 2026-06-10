---
source: src/repr-test-util/src/lib.rs
revision: db271c31b1
---

# mz-repr-test-util

Provides helper functions for constructing `mz-repr` objects (`Row`, `Datum`, `SqlScalarType`) from text literals in test specifications.
The three main entry points are `test_spec_to_row` (builds a `Row` from `(litval, littyp)` pairs), `get_scalar_type_or_default` (parses or infers a `SqlScalarType` from a token stream), and `extract_literal_string` / `parse_vec_of_literals` (tokenizer helpers that consume proc-macro2 token trees).
Used by other crates (e.g. `mz-expr-test-util`) that need to express `repr` values in data-driven test files without depending on the full SQL parser.
