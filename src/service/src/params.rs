// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_proto::{ProtoType, RustType, TryFromProtoError};
use serde::{Deserialize, Serialize};
use std::time::Duration;

include!(concat!(env!("OUT_DIR"), "/mz_service.params.rs"));

/// gRPC client parameters.
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct GrpcClientParameters {
    /// Timeout to apply to initial connection establishment.
    pub connect_timeout: Option<Duration>,
    /// Time waited after the last received HTTP/2 frame before sending
    /// a keep-alive PING to the server. Note that this is an HTTP/2
    /// keep-alive, and is separate from TCP keep-alives.
    pub http2_keep_alive_interval: Option<Duration>,
    /// Time waited without response after a keep-alive PING before
    /// terminating the connection.
    pub http2_keep_alive_timeout: Option<Duration>,
}

impl GrpcClientParameters {
    pub fn update(&mut self, other: Self) {
        let Self {
            connect_timeout,
            http2_keep_alive_interval,
            http2_keep_alive_timeout,
        } = self;
        let Self {
            connect_timeout: other_connect_timeout,
            http2_keep_alive_interval: other_http2_keep_alive_interval,
            http2_keep_alive_timeout: other_http2_keep_alive_timeout,
        } = other;

        if let Some(v) = other_connect_timeout {
            *connect_timeout = Some(v);
        }
        if let Some(v) = other_http2_keep_alive_interval {
            *http2_keep_alive_interval = Some(v);
        }
        if let Some(v) = other_http2_keep_alive_timeout {
            *http2_keep_alive_timeout = Some(v);
        }
    }
}

impl RustType<ProtoGrpcClientParameters> for GrpcClientParameters {
    fn into_proto(&self) -> ProtoGrpcClientParameters {
        ProtoGrpcClientParameters {
            connect_timeout: self.connect_timeout.into_proto(),
            http2_keep_alive_interval: self.http2_keep_alive_interval.into_proto(),
            http2_keep_alive_timeout: self.http2_keep_alive_timeout.into_proto(),
        }
    }

    fn from_proto(proto: ProtoGrpcClientParameters) -> Result<Self, TryFromProtoError> {
        Ok(Self {
            connect_timeout: proto.connect_timeout.into_rust()?,
            http2_keep_alive_interval: proto.http2_keep_alive_interval.into_rust()?,
            http2_keep_alive_timeout: proto.http2_keep_alive_timeout.into_rust()?,
        })
    }
}
