---
source: src/mz-deploy/src/project.rs
revision: 8ee3def844
---

# mz-deploy::project

Project compiler: loads `.sql` files from disk, validates and type-checks them, resolves dependencies, and produces a deployment graph.

Submodules:
* `analysis` — changeset computation, deployment snapshot diffing, dependency graph analysis, topology ordering.
* `ast` — AST helpers.
* `clusters` — cluster configuration in the project IR.
* `compiler` — core compilation pipeline: parsing, name resolution, type-checking, object validation, cache management.
* `error` — project error types: `LoadError`, `ParseError`, `ValidationError`, `DependencyError`.
* `ir` — intermediate representation: `CompiledObject`, `ObjectGraph`, `ObjectId`, infrastructure types, unit test IR.
* `network_policies` — network policy definitions in the project.
* `resolve` — name resolution and SQL normalization (CTE scoping, overlay transformers, mod rewriters).
* `roles` — role definitions in the project.
* `syntax` — SQL file loading, input abstraction, variable interpolation, psql-variable handling.
