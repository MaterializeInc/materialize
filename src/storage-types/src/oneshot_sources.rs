// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types for oneshot sources.

use mz_expr::SafeMfpPlan;
use mz_pgcopy::CopyCsvFormatParams;
use mz_repr::{CatalogItemId, RelationDesc};
use mz_timely_util::builder_async::PressOnDropButton;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedReceiver;
use url::Url;

use crate::connections::aws::AwsConnection;

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
    pub shape: ContentShape,
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

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub enum ContentFormat {
    Csv(CopyCsvFormatParams<'static>),
    Parquet,
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

/// Shape of the data we are copying into Materialize.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ContentShape {
    /// Describes the shape of the data we are copying from.
    pub source_desc: RelationDesc,
    /// An MFP to transform the data to match the destination table.
    pub source_mfp: SafeMfpPlan,
}
