// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;

use futures::Future;
use mz_ore::retry::{Retry, RetryResult};
use postgres_protocol::escape;
use tonic::{Request, Response, Status};

use crate::error::{Context, OpError};
use crate::fivetran_sdk::destination_server::Destination;
use crate::fivetran_sdk::{
    self, AlterTableRequest, AlterTableResponse, ConfigurationFormRequest,
    ConfigurationFormResponse, CreateTableRequest, CreateTableResponse, DescribeTableRequest,
    DescribeTableResponse, TestRequest, TestResponse, TruncateRequest, TruncateResponse,
    WriteBatchRequest, WriteBatchResponse,
};
use crate::utils;

mod config;
mod ddl;
mod dml;

/// Tracks if a row has been "soft deleted" if this column to true.
const FIVETRAN_SYSTEM_COLUMN_DELETE: &str = "_fivetran_deleted";
/// Tracks the last time this Row was modified by Fivetran.
const FIVETRAN_SYSTEM_COLUMN_SYNCED: &str = "_fivetran_synced";
/// Fivetran will synthesize a primary key column when one doesn't exist.
const FIVETRAN_SYSTEM_COLUMN_ID: &str = "_fivetran_id";

pub struct MaterializeDestination;

#[tonic::async_trait]
impl Destination for MaterializeDestination {
    async fn configuration_form(
        &self,
        _: Request<ConfigurationFormRequest>,
    ) -> Result<Response<ConfigurationFormResponse>, Status> {
        to_grpc(Ok(config::handle_configuration_form_request()))
    }

    async fn test(&self, request: Request<TestRequest>) -> Result<Response<TestResponse>, Status> {
        let request = request.into_inner();
        let result = with_retry_and_logging(|| async {
            config::handle_test_request(request.clone())
                .await
                .context("handle_test_request")
        })
        .await;

        let response = match result {
            Ok(()) => fivetran_sdk::test_response::Response::Success(true),
            Err(e) => fivetran_sdk::test_response::Response::Failure(e.to_string()),
        };
        to_grpc(Ok(TestResponse {
            response: Some(response),
        }))
    }

    async fn describe_table(
        &self,
        request: Request<DescribeTableRequest>,
    ) -> Result<Response<DescribeTableResponse>, Status> {
        let request = request.into_inner();
        let result = with_retry_and_logging(|| async {
            ddl::handle_describe_table(request.clone())
                .await
                .context("describe_table")
        })
        .await;

        let response = match result {
            Ok(None) => fivetran_sdk::describe_table_response::Response::NotFound(true),
            Ok(Some(table)) => fivetran_sdk::describe_table_response::Response::Table(table),
            Err(e) => fivetran_sdk::describe_table_response::Response::Failure(e.to_string()),
        };
        to_grpc(Ok(DescribeTableResponse {
            response: Some(response),
        }))
    }

    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        let request = request.into_inner();
        let result = with_retry_and_logging(|| async {
            ddl::handle_create_table(request.clone())
                .await
                .context("create table")
        })
        .await;

        let response = match result {
            Ok(()) => fivetran_sdk::create_table_response::Response::Success(true),
            Err(e) => fivetran_sdk::create_table_response::Response::Failure(e.to_string()),
        };
        to_grpc(Ok(CreateTableResponse {
            response: Some(response),
        }))
    }

    async fn alter_table(
        &self,
        request: Request<AlterTableRequest>,
    ) -> Result<Response<AlterTableResponse>, Status> {
        let request = request.into_inner();
        let result = with_retry_and_logging(|| async {
            ddl::handle_alter_table(request.clone())
                .await
                .context("alter_table")
        })
        .await;

        let response = match result {
            Ok(()) => fivetran_sdk::alter_table_response::Response::Success(true),
            Err(e) => fivetran_sdk::alter_table_response::Response::Failure(e.to_string()),
        };
        to_grpc(Ok(AlterTableResponse {
            response: Some(response),
        }))
    }

    async fn truncate(
        &self,
        request: Request<TruncateRequest>,
    ) -> Result<Response<TruncateResponse>, Status> {
        let request = request.into_inner();
        let result = with_retry_and_logging(|| async {
            dml::handle_truncate_table(request.clone())
                .await
                .context("truncate_table")
        })
        .await;

        let response = match result {
            Ok(()) => fivetran_sdk::truncate_response::Response::Success(true),
            Err(e) => fivetran_sdk::truncate_response::Response::Failure(e.to_string()),
        };
        to_grpc(Ok(TruncateResponse {
            response: Some(response),
        }))
    }

    async fn write_batch(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> Result<Response<WriteBatchResponse>, Status> {
        let request = request.into_inner();
        let result = with_retry_and_logging(|| async {
            dml::handle_write_batch(request.clone())
                .await
                .context("write_batch")
        })
        .await;

        let response = match result {
            Ok(()) => fivetran_sdk::write_batch_response::Response::Success(true),
            Err(e) => fivetran_sdk::write_batch_response::Response::Failure(e.to_string()),
        };
        to_grpc(Ok(WriteBatchResponse {
            response: Some(response),
        }))
    }
}

/// Automatically retries the provided closure, if the error is retry-able, and traces failures
/// appropriately.
async fn with_retry_and_logging<C, F, T>(closure: C) -> Result<T, OpError>
where
    F: Future<Output = Result<T, OpError>>,
    C: FnMut() -> F,
{
    let (_c, result) = Retry::default()
        .max_tries(3)
        // Sort of awkward, but we need to pass the `closure` around so each iteration can call it.
        .retry_async_with_state(closure, |retry_state, mut closure| async move {
            let result = match closure().await {
                Ok(t) => RetryResult::Ok(t),
                Err(err) if err.kind().can_retry() => {
                    tracing::warn!(%err, attempt = retry_state.i, "retry-able operation failed");
                    RetryResult::RetryableErr(err)
                }
                Err(e) => RetryResult::FatalErr(e),
            };

            (closure, result)
        })
        .await;

    if let Err(err) = &result {
        tracing::error!(%err, "request failed!")
    }
    result
}

/// Convert the result of an operation to a gRPC response.
///
/// Note: We're expected to __never__ return a gRPC error and instead we should return a 200 with
/// the error code embedded.
fn to_grpc<T>(response: Result<T, OpError>) -> Result<Response<T>, Status> {
    match response {
        Ok(t) => Ok(Response::new(t)),
        Err(e) => Err(Status::unknown(e.to_string())),
    }
}

/// Metadata about a column that is relevant to operations peformed by the destination.
#[derive(Debug)]
struct ColumnMetadata {
    /// Name of the column in the destination table, with necessary characters escaped.
    escaped_name: String,
    /// Type of the column in the destination table.
    ty: Cow<'static, str>,
    /// Is this column a primary key.
    is_primary: bool,
}

impl ColumnMetadata {
    /// Returns a [`String`] that is suitable for use when creating a table.
    fn to_column_def(&self) -> String {
        let mut def = format!("{} {}", self.escaped_name, self.ty);

        if self.is_primary {
            def.push_str(" NOT NULL");
        }

        def
    }
}

impl TryFrom<&crate::fivetran_sdk::Column> for ColumnMetadata {
    type Error = OpError;

    fn try_from(value: &crate::fivetran_sdk::Column) -> Result<Self, Self::Error> {
        let escaped_name = escape::escape_identifier(&value.name);
        let mut ty: Cow<'static, str> = utils::to_materialize_type(value.r#type())?.into();
        if let Some(d) = &value.decimal {
            ty.to_mut()
                .push_str(&format!("({}, {})", d.precision, d.scale));
        }
        let is_primary =
            value.primary_key || value.name.to_lowercase() == FIVETRAN_SYSTEM_COLUMN_ID;

        Ok(ColumnMetadata {
            escaped_name,
            ty,
            is_primary,
        })
    }
}
