---
source: src/catalog/src/builtin/mz_object_dependencies.rs
revision: 7053f0b019
---

# catalog::builtin::mz_object_dependencies

Generates the `mz_internal.mz_object_dependencies_raw` builtin view and defines the `mz_internal.mz_object_dependencies` builtin materialized view.

`make_mz_object_dependencies_raw` builds a `BuiltinView` whose SQL body is generated at startup from the full set of builtins. It inlines every builtin-to-builtin dependency edge as `VALUES` rows derived from parsing each builtin's SQL, then unions those rows with edges derived at query time from `mz_internal.mz_catalog_raw`. The generated view is inserted into `BUILTINS_STATIC` immediately before `MZ_OBJECT_DEPENDENCIES` so that the materialized view can read from it.

## Edge sources

The generated view unions five edge sources:

- **User id edges** — references by catalog ID from stored `create_sql`, extracted via `mz_internal.parse_catalog_item_references`. These cover the common case where name resolution has already replaced names with bracketed IDs.
- **User function/type/relation edges** — named references that did not carry an ID, common in builtin SQL that is name-based. Each named reference is joined to `GidMapping` rows to recover the catalog ID.
- **Builtin edges** — the `VALUES` rows inlined at startup by `BuiltinEdgeCollector`. One row per (object, reference) pair, classified by kind (`rel`, `func`, `type`).
- **Introspection source index edges** — `ClusterIntrospectionSourceIndex` catalog entries link each introspection source index to the source it indexes.

`UNION` rather than `UNION ALL` is used because a reference can appear both by ID and by name in the same statement.

## BuiltinEdgeCollector

`BuiltinEdgeCollector` parses each builtin's SQL via `mz_sql_parser::ast::item_refs::collect_item_references` and records one `BuiltinEdgeRow` per referenced catalog item. Named references are resolved to `(schema, name)` pairs using the `schema_by_name` map built from the full builtin list. Array type references (`T[]`) are mapped to the paired `_T` builtin type via `array_type_by_elem`. `assert_safe_builtin_name` is called on every name before it is inlined into the SQL fragment, rejecting names containing quotes or backslashes.

`make_mz_object_dependencies_raw` runs collection over all builtins, then runs a second pass to collect `mz_object_dependencies_raw`'s own outgoing edges (since the view itself references builtins it joins against) and folds those rows in before generating the final SQL.

## MZ_OBJECT_DEPENDENCIES

`MZ_OBJECT_DEPENDENCIES` is a `BuiltinMaterializedView` in `mz_catalog_server` that selects `object_id` and `referenced_object_id` from `mz_object_dependencies_raw` with `ASSERT NOT NULL` on both columns. Its `ontology` field describes two `DependsOn` links connecting `object_id` and `referenced_object_id` to the `object` entity in the catalog ontology, both with `SemanticType::CatalogItemId`.
