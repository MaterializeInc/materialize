// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Typed failures of catalog reconstruction and state-owned transactions.

use mz_controller_types::ClusterId;
use mz_repr::Timestamp;
use timely::progress::Antichain;

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error(transparent)]
    Catalog(#[from] crate::memory::error::Error),
    #[error(transparent)]
    Item(#[from] crate::memory::error::ItemError),
    #[error(transparent)]
    PlanError(#[from] mz_sql::plan::PlanError),
    #[error(transparent)]
    Storage(#[from] mz_storage_types::controller::StorageError),
    #[error("internal error: {0}")]
    Internal(String),
    #[error(transparent)]
    Unstructured(#[from] anyhow::Error),
    #[error("{0} are not supported")]
    Unsupported(&'static str),
    #[error("cannot write in read-only mode")]
    ReadOnly,
    #[error("catalog changed during DDL transaction")]
    DDLTransactionRace,
    #[error("cluster {cluster_id} changed during transaction")]
    ClusterStateChanged { cluster_id: ClusterId },
    #[error("input not readable at refresh timestamp {0}: {1:?}")]
    InputNotReadableAtRefreshAtTime(Timestamp, Antichain<Timestamp>),
}

impl CatalogError {
    pub fn internal(context: &str, error: impl std::fmt::Display) -> Self {
        Self::Internal(format!("{context}: {error}"))
    }
}

impl From<crate::durable::CatalogError> for CatalogError {
    fn from(error: crate::durable::CatalogError) -> Self {
        Self::Catalog(error.into())
    }
}

impl From<crate::durable::DurableCatalogError> for CatalogError {
    fn from(error: crate::durable::DurableCatalogError) -> Self {
        Self::from(crate::durable::CatalogError::from(error))
    }
}

impl From<mz_sql::catalog::CatalogError> for CatalogError {
    fn from(error: mz_sql::catalog::CatalogError) -> Self {
        Self::Catalog(error.into())
    }
}

impl From<mz_sql::session::vars::VarError> for CatalogError {
    fn from(error: mz_sql::session::vars::VarError) -> Self {
        Self::Catalog(error.into())
    }
}

impl From<mz_sql_parser::parser::ParserStatementError> for CatalogError {
    fn from(error: mz_sql_parser::parser::ParserStatementError) -> Self {
        Self::Item(error.into())
    }
}
