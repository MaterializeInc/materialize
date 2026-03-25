---
source: src/walkabout/src/ir.rs
revision: e757b4d11b
---

# mz-walkabout::ir

Defines the intermediate representation (IR) used between parsing and code generation.
`Ir` holds a `BTreeMap<String, Item>` of all struct/enum types and a `BTreeMap<String, BTreeSet<String>>` of accumulated generic parameter bounds.
`Item`, `Struct`, `Enum`, `Variant`, `Field`, and `Type` model the AST being analyzed; `Type` distinguishes primitives, abstract generics, `Option`/`Vec`/`Box` containers, local AST types, and `BTreeMap`.
The `analyze` function performs lightweight semantic analysis on `syn::DeriveInput` slices to classify each field's type and validate that all referenced local types are present.
