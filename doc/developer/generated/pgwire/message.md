---
source: src/pgwire/src/message.rs
revision: 2c17d81232
---

# pgwire::message

Defines `BackendMessage`, the internal enum of all PostgreSQL backend messages that the server can send to a client, including authentication, query result, copy, and error variants.
Provides supporting types `FieldDescription`, `SASLServerFirstMessage`, and `SASLServerFinalMessage` used when encoding SASL and row-description messages.
The `encode_row_description` helper converts a `RelationDesc` and format slice into a `Vec<FieldDescription>` ready for encoding.
