---
source: src/mz-deploy/src/project/resolve/normalize/overlay_transformer.rs
revision: 5cc5e835e7
---

# mz-deploy::project::resolve::normalize::overlay_transformer

Name transformation for `mz-deploy dev` overlay compilation.
`OverlayTransformer` applies a two-step rule: external references (database not in `in_project_databases`) are left verbatim; in-project references are rewritten to the overlay database (`<database>__<profile_name>`) only when the fully-qualified object is in `overlay_objects: &BTreeSet<ObjectId>`. Routing by object rather than by schema means a non-overlaid object that shares a schema with an overlaid one continues to resolve to its production address.
