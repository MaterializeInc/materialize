// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Common PostgreSQL network ("wire") protocol logic.

#![warn(clippy::as_conversions)]
#![warn(unused_extern_crates)]

mod codec;
mod conn;
mod format;
mod message;
mod severity;

pub use codec::{
    ACCEPT_SSL_ENCRYPTION, CodecError, Cursor, DecodeState, MAX_REQUEST_SIZE, Pgbuf,
    REJECT_ENCRYPTION, decode_startup, input_err, parse_frame_len,
};
pub use conn::{
    CONN_UUID_KEY, Conn, ConnectionCounter, ConnectionError, ConnectionHandle,
    MZ_FORWARDED_FOR_KEY, UserMetadata,
};
pub use format::Format;
pub use message::{
    ChannelBinding, ErrorResponse, FrontendMessage, FrontendStartupMessage, GS2Header,
    SASLClientFinalResponse, SASLInitialResponse, VERSION_3, VERSION_CANCEL, VERSION_GSSENC,
    VERSION_SSL, VERSIONS,
};
pub use severity::Severity;
