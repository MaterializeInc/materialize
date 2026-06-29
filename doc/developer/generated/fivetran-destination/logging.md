---
source: src/fivetran-destination/src/logging.rs
revision: 849327076c
---

# mz-fivetran-destination::logging

`FivetranLoggingFormat` implements `tracing_subscriber::fmt::FormatEvent` to format log events according to the [Fivetran SDK logging format](https://github.com/fivetran/fivetran_sdk/blob/main/development-guide.md#logging).

`FivetranLoggingFormat::destination()` constructs an instance configured for the `SdkDestination` origin. The formatted output includes a JSON-structured event with level and message fields suitable for ingestion by the Fivetran SDK infrastructure.
