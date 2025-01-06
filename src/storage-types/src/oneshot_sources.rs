// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for oneshot sources.

use mz_proto::{IntoRustIfSome, RustType};

use serde::{Deserialize, Serialize};
use url::Url;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.oneshot_sources.rs"
));


#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ContentSource {
    Http { url: Url },
}

impl RustType<proto_oneshot_ingestion_request::Source> for ContentSource {
    fn into_proto(&self) -> proto_oneshot_ingestion_request::Source {
        match self {
            ContentSource::Http { url } => {
                proto_oneshot_ingestion_request::Source::Http(ProtoHttpContentSource {
                    url: url.to_string(),
                })
            }
        }
    }

    fn from_proto(
        proto: proto_oneshot_ingestion_request::Source,
    ) -> Result<Self, mz_proto::TryFromProtoError> {
        match proto {
            proto_oneshot_ingestion_request::Source::Http(source) => {
                let url = Url::parse(&source.url).expect("failed to roundtrip Url");
                Ok(ContentSource::Http { url })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ContentFormat {
    Csv,
}

impl RustType<proto_oneshot_ingestion_request::Format> for ContentFormat {
    fn into_proto(&self) -> proto_oneshot_ingestion_request::Format {
        match self {
            ContentFormat::Csv => {
                proto_oneshot_ingestion_request::Format::Csv(ProtoCsvContentFormat::default())
            }
        }
    }

    fn from_proto(
        proto: proto_oneshot_ingestion_request::Format,
    ) -> Result<Self, mz_proto::TryFromProtoError> {
        match proto {
            proto_oneshot_ingestion_request::Format::Csv(ProtoCsvContentFormat {}) => {
                Ok(ContentFormat::Csv)
            }
        }
    }
}
