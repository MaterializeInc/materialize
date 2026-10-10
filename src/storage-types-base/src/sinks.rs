// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Types to describe sinks.

use mz_pgcopy::CopyFormatParams;
use mz_repr::RelationDesc;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub enum S3SinkFormat {
    /// Encoded using the PG `COPY` protocol, with one of its supported formats.
    PgCopy(CopyFormatParams<'static>),
    /// Encoded as Parquet.
    Parquet,
}

/// Info required to copy the data to s3.
#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub struct S3UploadInfo {
    /// The s3 uri path to write the data to.
    pub uri: String,
    /// The max file size of each file uploaded to S3.
    pub max_file_size: u64,
    /// The relation desc of the data to be uploaded to S3.
    pub desc: RelationDesc,
    /// The selected sink format.
    pub format: S3SinkFormat,
}
