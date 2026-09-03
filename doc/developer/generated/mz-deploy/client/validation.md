---
source: src/mz-deploy/src/client/validation.rs
revision: d50441fcbe
---

# mz-deploy::client::validation

Database validation operations.
System-catalog dependencies (those with no database component, such as `mz_catalog` schema objects) are excluded from external-dependency existence checks because they are always present and their 2-part name never matches the 3-part FQN the existence query builds.

`validate_source_references` (exposed on `ValidationClient`) checks that every `CREATE TABLE ... FROM SOURCE` in the set of tables to create names an upstream object its source can read. It first issues `ALTER SOURCE ... REFRESH REFERENCES` for each source so the check reflects the upstream system's current state rather than a stale snapshot. References in MySQL system schemas (`mysql`, `sys`, `performance_schema`, `information_schema`) are skipped because those schemas are excluded from `mz_source_references` by design. Sources created in the same apply run are skipped because they do not yet exist in the catalog. A source with no recorded references is also skipped; an empty record is not authoritative. Fuzzy suggestions (Damerau-Levenshtein ranked) are included in `MissingSourceReference.suggestions` when a close match exists among the recorded references.
