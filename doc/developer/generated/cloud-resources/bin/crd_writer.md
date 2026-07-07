---
source: src/cloud-resources/src/bin/crd_writer.rs
revision: 8a4a8121df
---

# cloud-resources::bin::crd_writer

A CLI binary that generates JSON documentation for the `MaterializeSpec` Kubernetes CRD by introspecting its JSON schema, then prints a structured field list (name, type, description, required, deprecated, default) for each type in the schema hierarchy.
The binary accepts a required positional argument `<v1alpha1|v1>` to select which CRD API version's schema to document.
Used to produce the reference documentation for the Materialize operator's CRD fields.
