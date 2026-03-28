# Materialize Project

This is a Materialize project managed by the `mz-deploy` CLI tool.
Run `mz-deploy help <command>` for agent-optimized command documentation.

## Data mesh pattern

Build reusable **data products** by separating core entities from
domain-specific applications:

1. **Ontology database** — defines canonical business entities (e.g.
   customers, orders, products). Uses an `internal` schema for views with
   indexes and unit tests, and a `public` stable API schema (`SET api =
   stable`) with thin materialized views that expose the internal views.
   Runs on a dedicated cluster. See mz-deploy skill.
2. **Application databases** — domain-specific databases (e.g. fulfillment,
   storefront) that build on top of ontology entities by referencing
   `ontology.public.*`. Each application runs on its own cluster.

This gives you strong data product reuse, a stable non-breaking API surface
for core entities, and independent scaling per application.
