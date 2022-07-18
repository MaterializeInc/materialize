// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use proptest::strategy::Strategy;
use tokio_postgres::config::SslMode;

use crate::{RustType, TryFromProtoError};

include!(concat!(env!("OUT_DIR"), "/mz_proto.tokio_postgres.rs"));

pub fn any_ssl_mode() -> impl Strategy<Value = SslMode> {
    proptest::sample::select(vec![
        SslMode::Disable,
        SslMode::Prefer,
        SslMode::Require,
        SslMode::VerifyCa,
        SslMode::VerifyFull,
    ])
}

impl RustType<ProtoSslMode> for SslMode {
    fn into_proto(&self) -> ProtoSslMode {
        use proto_ssl_mode::Kind::*;
        ProtoSslMode {
            kind: Some(match self {
                SslMode::Disable => Disable(()),
                SslMode::Prefer => Prefer(()),
                SslMode::Require => Require(()),
                SslMode::VerifyCa => VerifyCa(()),
                SslMode::VerifyFull => VerifyFull(()),
                _ => Unknown(()),
            }),
        }
    }

    fn from_proto(proto: ProtoSslMode) -> Result<Self, TryFromProtoError> {
        use proto_ssl_mode::Kind::*;
        match proto.kind {
            Some(Disable(())) => Ok(SslMode::Disable),
            Some(Prefer(())) => Ok(SslMode::Prefer),
            Some(Require(())) => Ok(SslMode::Require),
            Some(VerifyCa(())) => Ok(SslMode::VerifyCa),
            Some(VerifyFull(())) => Ok(SslMode::VerifyFull),
            Some(Unknown(())) => Err(TryFromProtoError::unknown_enum_variant(
                "ProtoSslMode::kind",
            )),
            None => Err(TryFromProtoError::missing_field("ProtoSslMode::kind")),
        }
    }
}
