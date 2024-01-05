// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::error::ErrorExt;
use tonic::{Request, Response, Status};

use crate::fivetran_sdk::destination_server::Destination;
use crate::fivetran_sdk::{
    AlterTableRequest, AlterTableResponse, ConfigurationFormRequest, ConfigurationFormResponse,
    CreateTableRequest, CreateTableResponse, DescribeTableRequest, DescribeTableResponse,
    TestRequest, TestResponse, TruncateRequest, TruncateResponse, WriteBatchRequest,
    WriteBatchResponse,
};

mod config;
mod ddl;
mod dml;

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
        to_grpc(config::handle_test_request(request.into_inner()).await)
    }

    async fn describe_table(
        &self,
        request: Request<DescribeTableRequest>,
    ) -> Result<Response<DescribeTableResponse>, Status> {
        to_grpc(ddl::handle_describe_table_request(request.into_inner()).await)
    }

    async fn create_table(
        &self,
        request: Request<CreateTableRequest>,
    ) -> Result<Response<CreateTableResponse>, Status> {
        to_grpc(ddl::handle_create_table_request(request.into_inner()).await)
    }

    async fn alter_table(
        &self,
        request: Request<AlterTableRequest>,
    ) -> Result<Response<AlterTableResponse>, Status> {
        to_grpc(ddl::handle_alter_table_request(request.into_inner()).await)
    }

    async fn truncate(
        &self,
        request: Request<TruncateRequest>,
    ) -> Result<Response<TruncateResponse>, Status> {
        to_grpc(dml::handle_truncate_request(request.into_inner()).await)
    }

    async fn write_batch(
        &self,
        request: Request<WriteBatchRequest>,
    ) -> Result<Response<WriteBatchResponse>, Status> {
        to_grpc(dml::handle_write_batch_request(request.into_inner()).await)
    }
}

fn to_grpc<T>(response: Result<T, anyhow::Error>) -> Result<Response<T>, Status> {
    match response {
        Ok(t) => Ok(Response::new(t)),
        Err(e) => Err(Status::unknown(e.display_with_causes().to_string())),
    }
}
