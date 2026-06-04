---
source: src/sql/src/session/vars/value.rs
revision: 2deb792cb0
---

# mz-sql::session::vars::value

Defines the `Value` trait (parse/format/clone for variable values) and implements it for all supported variable types: `bool`, integers, `String`, `Duration`, `Numeric`, `IsolationLevel`, `TimeZone`, `IntervalStyle`, `ClientEncoding`, `ClientSeverity`, `CloneableEnvFilter`, `ByteSize`, and many others.
Also defines `AsAny` and the helper enums `IsolationLevel`, `TimeZone`, `IntervalStyle`, `ClientSeverity`, `ClientEncoding`, `Failpoints`.
`Duration` parsing splits the input at the end of the leading run of ASCII digits (not all Unicode numeric characters) to avoid mis-indexing multi-byte Unicode numerals such as `²` or `½`.
