---
source: src/sql/src/pure/references.rs
revision: 10ba6116a5
---

# mz-sql::pure::references

Defines `SourceReferenceClient` (an enum over Postgres, MySQL, SQL Server, Kafka, and load-generator upstream clients) and `RetrievedSourceReferences` (a unified container for all available table/topic references from an upstream).
`SourceReferenceClient::fetch_references` performs the async upstream query; `RetrievedSourceReferences::resolve` matches user-specified `ExternalReferences` against the retrieved set and returns `RequestedSourceExport`s.
