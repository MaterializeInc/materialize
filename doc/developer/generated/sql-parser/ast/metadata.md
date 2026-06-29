---
source: src/sql-parser/src/ast/metadata.rs
revision: 72277f8ac9
---

# mz-sql-parser::ast::metadata

Defines `AstInfo`, the trait that parameterizes the AST over its semantic stage, and the `Raw` struct that implements it for the initial unresolved parse output.
`AstInfo` declares associated types for names, column references, schema/database/cluster names, data types, and nested statements; these types change as the AST moves through planning (e.g., from `Raw` to `Aug`).
Also defines the `StatementContext`, `Version`, and several name wrapper types (`DeferredItemName`, `ResolvedItemName`, etc.) used during planning.

`RawItemName::Id`'s `AstDisplay` impl quotes the id component when `Ident::can_be_printed_bare` returns false, and otherwise prints it bare. This keeps normal global ids (e.g. `u1`) unquoted in all modes (including stable mode as used by `pg_get_viewdef`) while safely quoting crafted ids that contain spaces or keyword characters.

`RawDataType::Other`'s `AstDisplay` impl forces the always-quoted stable form when the first component of the type name is a keyword that `parse_data_type` would re-dispatch into a special grammar or canonicalize to a different spelling. The private `data_type_keyword_needs_quoting` function enumerates those keywords: `MAP`, `STRING`, `BIGINT`, `SMALLINT`, `DEC`, `DECIMAL`, `DOUBLE`, `FLOAT`, `INT`, `INTEGER`, `REAL`, `BOOLEAN`, `BYTES`, `JSON`, `CHAR`, `CHARACTER`. Keywords whose canonicalized name matches the keyword text itself (e.g. `bpchar`, `varchar`, `time`, `timestamp`, `timestamptz`) round-trip unquoted and are intentionally excluded.
