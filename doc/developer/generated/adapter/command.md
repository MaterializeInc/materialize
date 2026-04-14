---
source: src/adapter/src/command.rs
revision: fab33b2689
---

# adapter::command

Defines the `Command` enum — the complete set of messages that external clients (pgwire, webhooks, internal tools) send to the coordinator — and the `ExecuteResponse` / `ExecuteResponseKind` enums that the coordinator returns.
`Command` covers session lifecycle (`Startup`, `Terminate`), SQL execution (`Execute`, `Commit`), authentication (`AuthenticatePassword`, SASL variants, `CheckRoleCanLogin`), streaming data ingestion (`StartCopyFromStdin`), audit event injection (`InjectAuditEvents`), and several variants that offload work from the coordinator's main loop: `ExecuteSlowPathPeek`, `ExecuteSubscribe`, `ExecuteCopyTo`, `CopyToPreflight`, `ExecuteSideEffectingFunc`, `RegisterFrontendPeek`, `UnregisterFrontendPeek`, `ExplainTimestamp`, and `FrontendStatementLogging`.
`ExecuteResponse` enumerates every possible result of a SQL statement execution, from `CreatedTable` to `SendingRowsStreaming`; `generated_from` maps each `PlanKind` to its permitted response kinds.
`CopyFromStdinWriter` carries parallel `mpsc` batch sender channels and a `oneshot` completion receiver that pgwire uses to stream raw byte chunks to background batch-builder tasks and then commit the resulting `ProtoBatch` values during `COPY FROM STDIN`.
