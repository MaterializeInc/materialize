---
source: src/sql/src/pure/references.rs
revision: bfaab7aa5f
---

# mz-sql::pure::references

Defines `SourceReferenceClient` (an enum over Postgres, MySQL, SQL Server, Kafka, and load-generator upstream clients) and `RetrievedSourceReferences` (a unified container for all available table/topic references from an upstream).
`SourceReferenceClient::fetch_references` performs the async upstream query; `RetrievedSourceReferences::resolve` matches user-specified `ExternalReferences` against the retrieved set and returns `RequestedSourceExport`s.
The `SourceReferenceResolver` built during reference retrieval uses a database name that matches what each source type embeds in its `ReferenceMetadata::external_reference()`: Postgres and SQL Server use the real upstream database name, load generators use `LOAD_GENERATOR_DATABASE_NAME` (the synthetic `mz_load_generators` database), and MySQL and Kafka—which store no database component in their external references—use the `DATABASE_FAKE_NAME` placeholder. `DATABASE_FAKE_NAME` is documented as applying only to MySQL and Kafka, not to all non-Postgres sources.
