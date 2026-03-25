---
source: src/fivetran-destination/src/logging.rs
revision: 82d92a7fad
---

# mz-fivetran-destination::logging

Implements `FivetranLoggingFormat`, a `tracing_subscriber` event formatter that emits JSON log lines in the format required by the Fivetran SDK (fields: `level`, `message`, `message-origin`).
Tracing levels `TRACE`/`DEBUG`/`INFO` map to Fivetran `INFO`; `WARN` maps to `WARNING`; `ERROR` maps to `SEVERE`.
