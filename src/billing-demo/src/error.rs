// Copyright Copyright 2019-2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

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
