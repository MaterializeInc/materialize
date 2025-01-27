// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for oneshot sources.

use mz_pgcopy::CopyCsvFormatParams;
use mz_proto::{IntoRustIfSome, RustType};
use mz_repr::CatalogItemId;
use mz_timely_util::builder_async::PressOnDropButton;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;
use url::Url;

use crate::connections::aws::AwsConnection;

include!(concat!(
    env!("OUT_DIR"),
    "/mz_storage_types.oneshot_sources.rs"
));

/// Callback type used to send the result of a oneshot source.
pub type OneshotResultCallback<Batch> =
    Box<dyn FnOnce(Vec<Result<Batch, String>>) -> () + Send + 'static>;

pub struct OneshotIngestionDescription<Batch> {
    /// Tokens for the running dataflows.
    pub tokens: Vec<PressOnDropButton>,
    /// Receiving end of the channel the dataflow uses to report results.
    pub results: UnboundedReceiver<Result<Option<Batch>, String>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct OneshotIngestionRequest {
    pub source: ContentSource,
    pub format: ContentFormat,
    pub filter: ContentFilter,
}

impl RustType<ProtoOneshotIngestionRequest> for OneshotIngestionRequest {
    fn into_proto(&self) -> ProtoOneshotIngestionRequest {
        ProtoOneshotIngestionRequest {
            source: Some(self.source.into_proto()),
            format: Some(self.format.into_proto()),
            filter: Some(self.filter.into_proto()),
        }
    }

    fn from_proto(
        proto: ProtoOneshotIngestionRequest,
    ) -> Result<Self, mz_proto::TryFromProtoError> {
        let source = proto
            .source
            .into_rust_if_some("ProtoOneshotIngestionRequest::source")?;
        let format = proto
            .format
            .into_rust_if_some("ProtoOneshotIngestionRequest::format")?;
        let filter = proto
            .filter
            .into_rust_if_some("ProtoOneshotIngestionRequest::filter")?;

        Ok(OneshotIngestionRequest {
            source,
            format,
            filter,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ContentSource {
    Http {
        url: Url,
    },
    AwsS3 {
        connection: AwsConnection,
        connection_id: CatalogItemId,
        uri: String,
    },
}

impl RustType<proto_oneshot_ingestion_request::Source> for ContentSource {
    fn into_proto(&self) -> proto_oneshot_ingestion_request::Source {
        match self {
            ContentSource::Http { url } => {
                proto_oneshot_ingestion_request::Source::Http(ProtoHttpContentSource {
                    url: url.to_string(),
                })
            }
            ContentSource::AwsS3 {
                connection,
                connection_id,
                uri,
            } => proto_oneshot_ingestion_request::Source::AwsS3(ProtoAwsS3Source {
                connection: Some(connection.into_proto()),
                connection_id: Some(connection_id.into_proto()),
                uri: uri.to_string(),
            }),
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
            proto_oneshot_ingestion_request::Source::AwsS3(source) => {
                let connection = source.connection.into_rust_if_some("AwsS3::connection")?;
                let connection_id = source
                    .connection_id
                    .into_rust_if_some("AwsS3::connection_id")?;
                Ok(ContentSource::AwsS3 {
                    connection,
                    connection_id,
                    uri: source.uri,
                })
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ContentFormat {
    Csv(CopyCsvFormatParams<'static>),
    Parquet,
}

impl RustType<proto_oneshot_ingestion_request::Format> for ContentFormat {
    fn into_proto(&self) -> proto_oneshot_ingestion_request::Format {
        match self {
            ContentFormat::Csv(params) => {
                proto_oneshot_ingestion_request::Format::Csv(ProtoCsvContentFormat {
                    params: Some(params.into_proto()),
                })
            }
            ContentFormat::Parquet => proto_oneshot_ingestion_request::Format::Parquet(
                ProtoParquetContentFormat::default(),
            ),
        }
    }

    fn from_proto(
        proto: proto_oneshot_ingestion_request::Format,
    ) -> Result<Self, mz_proto::TryFromProtoError> {
        match proto {
            proto_oneshot_ingestion_request::Format::Csv(ProtoCsvContentFormat { params }) => {
                let params = params.into_rust_if_some("ProtoCsvContentFormat::params")?;
                Ok(ContentFormat::Csv(params))
            }
            proto_oneshot_ingestion_request::Format::Parquet(ProtoParquetContentFormat {}) => {
                Ok(ContentFormat::Parquet)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ContentFilter {
    /// No filtering, fetch everything at a remote resource.
    None,
    /// Filter to only the files specified in this list.
    Files(Vec<String>),
    /// Regex pattern to filter the files with.
    Pattern(String),
}

impl RustType<proto_oneshot_ingestion_request::Filter> for ContentFilter {
    fn into_proto(&self) -> proto_oneshot_ingestion_request::Filter {
        match self {
            ContentFilter::None => {
                proto_oneshot_ingestion_request::Filter::None(Default::default())
            }
            ContentFilter::Files(files) => {
                proto_oneshot_ingestion_request::Filter::Files(ProtoFilterFiles {
                    files: files.clone(),
                })
            }
            ContentFilter::Pattern(pattern) => {
                proto_oneshot_ingestion_request::Filter::Pattern(ProtoFilterPattern {
                    pattern: pattern.clone(),
                })
            }
        }
    }

    fn from_proto(
        proto: proto_oneshot_ingestion_request::Filter,
    ) -> Result<Self, mz_proto::TryFromProtoError> {
        match proto {
            proto_oneshot_ingestion_request::Filter::None(()) => Ok(ContentFilter::None),
            proto_oneshot_ingestion_request::Filter::Files(files) => {
                Ok(ContentFilter::Files(files.files))
            }
            proto_oneshot_ingestion_request::Filter::Pattern(pattern) => {
                Ok(ContentFilter::Pattern(pattern.pattern))
            }
        }
    }
}
