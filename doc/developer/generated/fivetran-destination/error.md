---
source: src/fivetran-destination/src/error.rs
revision: 849327076c
---

# mz-fivetran-destination::error

Error types for the Fivetran destination connector.

`OpError` wraps an `OpErrorKind` and a context stack (`Vec<Cow<'static, str>>`). The `Context` trait (implemented for `OpErrorKind`, `Result<T, OpError>`, and `Result<T, E: Into<OpErrorKind>>`) provides `.context("msg")` and `.with_context(|| msg)` to push context strings onto an existing error.

`OpErrorKind` covers: `FieldMissing`, `TemporaryResource` (postgres error on temp resources), `InvariantViolated`, `MaterializeError` (postgres), `PgTypeError`, `MissingPrivilege`, `Filesystem`, `Crypto` (openssl), `CsvReader`, `CsvMapping`, `Unsupported`, `IdentError`, `UnknownTable`, `UnknownRequest`.

`OpErrorKind::can_retry()` returns `true` for connection-related postgres error codes (`CONNECTION_EXCEPTION`, `CONNECTION_FAILURE`, `CONNECTION_DOES_NOT_EXIST`, `SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION`, `TRANSACTION_RESOLUTION_UNKNOWN`) and for `DUPLICATE_OBJECT`/`DUPLICATE_TABLE` on temporary resources, plus select I/O errors (`NotFound`, `ConnectionReset`, `ConnectionAborted`, `BrokenPipe`, `TimedOut`, `Interrupted`, `UnexpectedEof`).
