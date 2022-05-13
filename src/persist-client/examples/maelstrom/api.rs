// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types used in the Maelstrom API
//!
//! These are documented in Maelstrom's [protocol], [workloads], and [services]
//! docs.
//!
//! [protocol]: https://github.com/jepsen-io/maelstrom/blob/v0.2.1/doc/protocol.md
//! [workloads]: https://github.com/jepsen-io/maelstrom/blob/v0.2.1/doc/workloads.md
//! [services]: https://github.com/jepsen-io/maelstrom/blob/v0.2.1/doc/services.md

use std::str::FromStr;

use num_enum::{IntoPrimitive, TryFromPrimitive};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MsgId(pub usize);

impl MsgId {
    pub fn next(&self) -> Self {
        MsgId(self.0 + 1)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeId(pub String);

// A struct that exactly matches the structure of the Maelstrom json. Used as an
// intermediary so we can map it into real rust enums (as opposed to the value
// of the first field determining the structure of the other two).
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TxnOpHelper(pub String, pub u64, pub Value);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "TxnOpHelper", into = "TxnOpHelper")]
pub enum ReqTxnOp {
    Append { key: u64, val: u64 },
    Read { key: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "TxnOpHelper")]
pub enum ResTxnOp {
    Append { key: u64, val: u64 },
    Read { key: u64, val: Vec<u64> },
}

/// <https://github.com/jepsen-io/maelstrom/blob/v0.2.1/doc/protocol.md#message-bodies>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Body {
    #[serde(rename = "error")]
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        msg_id: Option<MsgId>,
        in_reply_to: MsgId,
        code: ErrorCode,
        text: String,
    },

    #[serde(rename = "init")]
    ReqInit {
        msg_id: MsgId,
        node_id: NodeId,
        node_ids: Vec<NodeId>,
    },
    #[serde(rename = "init_ok")]
    ResInit { msg_id: MsgId, in_reply_to: MsgId },

    #[serde(rename = "txn")]
    ReqTxn { msg_id: MsgId, txn: Vec<ReqTxnOp> },
    #[serde(rename = "txn_ok")]
    ResTxn {
        msg_id: MsgId,
        in_reply_to: MsgId,
        txn: Vec<ResTxnOp>,
    },

    #[serde(rename = "read")]
    ReqLinKvRead { msg_id: MsgId, key: Value },
    #[serde(rename = "read_ok")]
    ResLinKvRead {
        #[serde(skip_serializing_if = "Option::is_none")]
        msg_id: Option<MsgId>,
        in_reply_to: MsgId,
        value: Value,
    },

    #[serde(rename = "write")]
    ReqLinKvWrite {
        msg_id: MsgId,
        key: Value,
        value: Value,
    },
    #[serde(rename = "write_ok")]
    ResLinKvWrite {
        #[serde(skip_serializing_if = "Option::is_none")]
        msg_id: Option<MsgId>,
        in_reply_to: MsgId,
    },

    #[serde(rename = "cas")]
    ReqLinKvCaS {
        msg_id: MsgId,
        key: Value,
        from: Value,
        to: Value,
        #[serde(skip_serializing_if = "Option::is_none")]
        create_if_not_exists: Option<bool>,
    },
    #[serde(rename = "cas_ok")]
    ResLinKvCaS {
        #[serde(skip_serializing_if = "Option::is_none")]
        msg_id: Option<MsgId>,
        in_reply_to: MsgId,
    },
}

impl Body {
    pub fn in_reply_to(&self) -> Option<MsgId> {
        match self {
            Body::Error { in_reply_to, .. } => Some(*in_reply_to),
            Body::ResLinKvRead { in_reply_to, .. } => Some(*in_reply_to),
            Body::ResLinKvWrite { in_reply_to, .. } => Some(*in_reply_to),
            Body::ResLinKvCaS { in_reply_to, .. } => Some(*in_reply_to),
            _ => None,
        }
    }
}

/// <https://github.com/jepsen-io/maelstrom/blob/v0.2.1/doc/protocol.md#messages>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Msg {
    pub src: NodeId,
    pub dest: NodeId,
    pub body: Body,
}

impl FromStr for Msg {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(s).map_err(|err| err.to_string())
    }
}

impl std::fmt::Display for Msg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&serde_json::to_string(self).expect("msg wasn't json-able"))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaelstromError {
    pub code: ErrorCode,
    pub text: String,
}

impl std::fmt::Display for MaelstromError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.text)
    }
}

impl std::error::Error for MaelstromError {}

/// <https://github.com/jepsen-io/maelstrom/blob/v0.2.1/doc/protocol.md#errors>
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, IntoPrimitive, TryFromPrimitive)]
#[serde(try_from = "usize", into = "usize")]
#[repr(usize)]
pub enum ErrorCode {
    Timeout = 0,
    NodeNotFound = 1,
    NotSupported = 10,
    TemporarilyUnavailable = 11,
    MalformedRequest = 12,
    Crash = 13,
    Abort = 14,
    KeyDoesNotExist = 20,
    KeyAlreadyExists = 21,
    PreconditionFailed = 22,
    TxnConflict = 30,
}

mod from_impls {
    use std::fmt::Debug;

    use mz_persist::location::ExternalError;
    use mz_persist_client::error::InvalidUsage;
    use serde_json::Value;
    use tracing::debug;

    use crate::maelstrom::api::{ErrorCode, MaelstromError, ReqTxnOp, ResTxnOp, TxnOpHelper};

    impl TryFrom<TxnOpHelper> for ReqTxnOp {
        type Error = String;

        fn try_from(value: TxnOpHelper) -> Result<Self, Self::Error> {
            let TxnOpHelper(f, key, val) = value;
            match f.as_str() {
                "r" => match val {
                    Value::Null => Ok(ReqTxnOp::Read { key }),
                    x => Err(format!("unexpected read value: {}", x)),
                },
                "append" => match val {
                    Value::Number(x) => match x.as_u64() {
                        Some(val) => Ok(ReqTxnOp::Append { key, val }),
                        None => Err(format!("unexpected append value: {}", x)),
                    },
                    x => Err(format!("unexpected append value: {}", x)),
                },
                x => Err(format!("format txn type: {}", x)),
            }
        }
    }

    impl From<ReqTxnOp> for TxnOpHelper {
        fn from(value: ReqTxnOp) -> Self {
            match value {
                ReqTxnOp::Read { key } => TxnOpHelper("r".into(), key, Value::Null),
                ReqTxnOp::Append { key, val } => {
                    TxnOpHelper("append".into(), key, Value::from(val))
                }
            }
        }
    }

    impl From<ResTxnOp> for TxnOpHelper {
        fn from(value: ResTxnOp) -> Self {
            match value {
                ResTxnOp::Read { key, val } => TxnOpHelper("r".into(), key, Value::from(val)),
                ResTxnOp::Append { key, val } => {
                    TxnOpHelper("append".into(), key, Value::from(val))
                }
            }
        }
    }

    impl From<ExternalError> for MaelstromError {
        fn from(x: ExternalError) -> Self {
            if x.is_timeout() {
                // Toss a debug log in here so that, with RUST_BACKTRACE=1, we
                // can more easily see where timeouts are coming from. Perhaps
                // better to to attach the backtrace to the MaelstromError.
                debug!("creating timeout: {:?}", x);
                MaelstromError {
                    code: ErrorCode::Timeout,
                    text: x.to_string(),
                }
            } else {
                MaelstromError {
                    code: ErrorCode::Crash,
                    text: x.to_string(),
                }
            }
        }
    }

    impl<T: Debug> From<InvalidUsage<T>> for MaelstromError {
        fn from(x: InvalidUsage<T>) -> Self {
            // ErrorCode::Abort means this definitely didn't happen. Maelstrom
            // will call us out on it if this is a lie.
            MaelstromError {
                code: ErrorCode::Abort,
                text: x.to_string(),
            }
        }
    }
}
