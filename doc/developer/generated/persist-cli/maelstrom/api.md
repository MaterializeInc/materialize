---
source: src/persist-cli/src/maelstrom/api.rs
revision: 82d92a7fad
---

# persistcli::maelstrom::api

Defines the Rust types that model the Jepsen Maelstrom wire protocol: `Msg`, `Body` (an enum covering init, txn, and lin-kv request/response variants), `MsgId`, `NodeId`, `ErrorCode`, `ReqTxnOp`, and `ResTxnOp`.
Provides serde implementations that match the Maelstrom JSON encoding and `From`/`TryFrom` conversions from persist error types (`ExternalError`, `Indeterminate`, `InvalidUsage`) into `MaelstromError`.
This module is the shared wire layer used by all other modules in `maelstrom`.
