// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{}", s)]
    Message { s: Cow<'static, str> },

    #[error("Unexpected error running postgres command")]
    PgError(#[from] tokio_postgres::Error),

    #[error("Interacting with kafka: {0}")]
    KafkaError(#[from] rdkafka::error::KafkaError),

    #[error("Sending kafka message")]
    FuturesCancelled(#[from] futures_channel::oneshot::Canceled),

    #[error("Unable to encode message")]
    ProtobufError(#[from] protobuf::error::ProtobufError),

    #[error("Waiting for futures to complete")]
    JoinError(#[from] tokio::task::JoinError),

    #[error("Unable to find csv file")]
    CsvReadError(#[from] csv::Error),

    #[error("Unable to flush csv file")]
    CsvFlushError(#[from] std::io::Error),
}

impl From<String> for Error {
    fn from(s: String) -> Error {
        Error::Message { s: Cow::from(s) }
    }
}

impl From<&'static str> for Error {
    fn from(s: &'static str) -> Error {
        Error::Message { s: Cow::from(s) }
    }
}
