---
source: src/pgwire-common/src/message.rs
revision: 2c17d81232
---

# mz-pgwire-common::message

Defines the pgwire message types and protocol version constants.
Version constants (`VERSION_1`/`2`/`3`) encode major versions as the high 16 bits; pseudo-versions `VERSION_CANCEL`, `VERSION_SSL`, and `VERSION_GSSENC` use values outside the normal range to avoid collisions.
`FrontendStartupMessage` covers the pre-authentication messages: `Startup`, `SslRequest`, `GssEncRequest`, and `CancelRequest`.
`FrontendMessage` covers all authenticated frontend messages: `Query`, `Parse`, `DescribeStatement`, `DescribePortal`, `Bind`, `Execute`, `Flush`, `Sync`, `CloseStatement`, `ClosePortal`, `Terminate`, `CopyData`, `CopyDone`, `CopyFail`, `Password`, `SASLInitialResponse`, and `SASLResponse`.
`ErrorResponse` carries a `Severity`, a `SqlState` code, a message string, and optional detail, hint, and position fields; constructor methods `fatal`, `error`, and `notice` cover the common severity levels.
SCRAM authentication helpers `GS2Header`, `ChannelBinding`, `SASLInitialResponse`, and `SASLClientFinalResponse` carry the fields required by SCRAM-SHA-256 channel binding negotiation.
