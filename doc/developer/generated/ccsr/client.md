---
source: src/ccsr/src/client.rs
revision: f23bdd4c1d
---

# mz-ccsr::client

Implements `Client`, an async HTTP client for the Confluent-compatible schema registry API.
Key methods include `get_schema_by_id`, `get_schema_by_subject`, `list_subjects`, `publish_schema`, and `delete_subject`, all communicating over `reqwest` with optional HTTP basic auth.
Schema compatibility checking (`get_subject_config`, `set_subject_config`) and soft-delete / permanent-delete semantics are also covered.
