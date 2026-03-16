---
source: src/cloud-resources/src/bin/crd_writer.rs
revision: bb2be56038
---

# cloud-resources::bin::crd_writer

A CLI binary that generates JSON documentation for the `MaterializeSpec` Kubernetes CRD by introspecting its JSON schema, then prints a structured field list (name, type, description, required, deprecated, default) for each type in the schema hierarchy.
Used to produce the reference documentation for the Materialize operator's CRD fields.
