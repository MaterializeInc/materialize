---
source: src/adapter/src/command.rs
revision: 07e0546b22
---

# adapter::command

Defines the `Command` enum — the complete set of messages that external clients (pgwire, webhooks, internal tools) send to the coordinator — and the `ExecuteResponse` / `ExecuteResponseKind` enums that the coordinator returns.
`Command` covers session lifecycle (`Startup`, `Terminate`), SQL execution (`Execute`, `Commit`), authentication (`AuthenticatePassword`, SASL variants), streaming data ingestion (`StartCopyFromStdin`), and several fast-path peek variants introduced to reduce coordinator contention.
`ExecuteResponse` enumerates every possible result of a SQL statement execution, from `CreatedTable` to `SendingRowsStreaming`; `generated_from` maps each `PlanKind` to its permitted response kinds.
`CopyFromStdinWriter` carries the channels that pgwire uses to stream raw CSV/text bytes to parallel batch-builder tasks during `COPY FROM STDIN`.
