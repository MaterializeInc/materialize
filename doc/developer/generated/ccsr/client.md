---
source: src/ccsr/src/client.rs
revision: 036d7729d1
---

# mz-ccsr::client

Implements `Client`, an async HTTP client for the Confluent-compatible schema registry API.
Key methods include `get_schema_by_id`, `get_schema_by_subject`, `list_subjects`, `publish_schema`, and `delete_subject`, all communicating over `reqwest` with optional HTTP basic auth.
Schema compatibility checking (`get_subject_config`, `set_subject_config`) and soft-delete / permanent-delete semantics are also covered.
`PublishError::IncompatibleSchema` is returned for both API error code `409` and `40901`; the latter is the more specific subcode used by Confluent Schema Registry 8.0+.
