---
source: src/sql-parser/src/ast/item_refs.rs
revision: 7053f0b019
---

# mz-sql-parser::ast::item_refs

Extracts catalog item references from a raw SQL AST statement without access to the catalog.

`ItemReferences` collects all references a statement makes to catalog items, split into buckets by syntactic position:
- `ids` — bracketed `[<id> AS <name>]` references; these already carry catalog IDs.
- `named_relations` — unbracketed names in relation position, with CTE bindings excluded.
- `named_funcs` — names in function-call position.
- `named_types` — names in data-type position (excluding array element references).
- `named_array_elements` — names appearing as the element type of `T[]`; these map to the named array type `_T` rather than to `T` itself.

`collect_item_references` is the entry point. It constructs a `ReferenceCollector`, runs `Visit` over the statement, and returns the collected `ItemReferences`.

## CTE scoping

`ReferenceCollector` maintains a `cte_names` stack that tracks CTE bindings in scope. An unqualified name in relation position that matches a CTE binding is not recorded in `named_relations`, since it refers to the query-local binding rather than any catalog object. Qualified names and names in other positions are never filtered. The stack is managed by overriding `visit_query`: names are pushed as each CTE is entered and the stack is truncated back to its prior depth after the query body is visited, giving correct lexical scoping.

For `WITH MUTUALLY RECURSIVE` blocks, all CTE names are pushed before visiting any body, matching the mutual-recursion semantics where each CTE can see all others.

## Array type handling

`visit_data_type` overrides the default visitor to separate `T[]` from plain type references. A `RawDataType::Array` whose element is a `RawItemName::Name` is reported on `named_array_elements` rather than `named_types`; the caller maps the element name to the array type using catalog information the parser does not have. A `RawDataType::Array` whose element carries an ID (`RawItemName::Id`) reports nothing, since mapping an element ID to the array type requires the catalog. Compound types (`list`, `map`, nested arrays) recurse so that no inner reference is dropped.
