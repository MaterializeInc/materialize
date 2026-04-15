---
source: src/sql/src/session/vars/value.rs
revision: d2a5974dbd
---

# mz-sql::session::vars::value

Defines the `Value` trait (parse/format/clone for variable values) and implements it for all supported variable types: `bool`, integers, `String`, `Duration`, `Numeric`, `IsolationLevel`, `TimeZone`, `IntervalStyle`, `ClientEncoding`, `ClientSeverity`, `CloneableEnvFilter`, `ByteSize`, and many others.
Also defines `AsAny` and the helper enums `IsolationLevel`, `TimeZone`, `IntervalStyle`, `ClientSeverity`, `ClientEncoding`, `Failpoints`.
