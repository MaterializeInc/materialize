---
source: src/lowertest-derive/src/lib.rs
revision: 5680493e7d
---

# mz-lowertest-derive

Provides the `#[derive(MzReflect)]` procedural macro used by `mz_lowertest` to build runtime type information for structs and enums used in lower-stack unit tests.

## Module structure

The crate consists of a single `lib.rs` with no submodules:

* `mzreflect_derive` — the public `proc_macro_derive` entry point; generates an `impl MzReflect` that populates `ReflectedTypeInfo` with field names and type strings for each annotated type.
* Helper functions (`get_fields_names_types`, `get_field_name_type`, `get_type_as_string`, `extract_reflected_type`) — parse `syn` AST nodes to extract field metadata and recursively identify Materialize-defined types that also need reflection, while ignoring primitives, external types (`String`, `Tz`, etc.), and fields marked `#[mzreflect(ignore)]`.

## Key dependencies

* `syn` — parses the input token stream into a typed AST.
* `quote` — assembles the generated `impl` block.
* `proc-macro2` — used to convert `syn::Type` back to a string representation.

## Downstream consumers

Consumed exclusively by `mz-lowertest`, which uses the generated `MzReflect` implementations to drive its test-object construction logic.
