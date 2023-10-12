// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::fmt::{Debug, Formatter};

use mz_proto::TryFromProtoError;
use mz_sql::catalog::CatalogError as SqlCatalogError;
use mz_stash_types::{InternalStashError, StashError, MIN_STASH_VERSION, STASH_VERSION};

#[derive(Debug)]
pub enum CatalogError {
    Catalog(SqlCatalogError),
    Durable(DurableCatalogError),
}

impl std::error::Error for CatalogError {}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            CatalogError::Catalog(e) => write!(f, "{e}"),
            CatalogError::Durable(e) => write!(f, "{e}"),
        }
    }
}

impl From<SqlCatalogError> for CatalogError {
    fn from(e: SqlCatalogError) -> Self {
        Self::Catalog(e)
    }
}

impl From<DurableCatalogError> for CatalogError {
    fn from(e: DurableCatalogError) -> Self {
        Self::Durable(e)
    }
}

impl From<StashError> for CatalogError {
    fn from(e: StashError) -> Self {
        Self::Durable(e.into())
    }
}

impl From<TryFromProtoError> for CatalogError {
    fn from(e: TryFromProtoError) -> Self {
        Self::Durable(e.into())
    }
}

/// An error that can occur while interacting with a durable catalog.
#[derive(Debug)]
pub enum DurableCatalogError {
    /// Catalog has been fenced by another writer.
    Fence(String),
    /// The persisted catalog's version is too old for the current catalog to migrate.
    IncompatibleVersion(u64),
    /// Catalog is uninitialized.
    Uninitialized,
    /// Catalog is not in a writable state.
    NotWritable(String),
    /// Unable to serialize/deserialize Protobuf message.
    Proto(TryFromProtoError),
    /// Misc errors from the Stash implementation.
    ///
    /// Once the Stash implementation is removed we can remove this variant.
    MiscStash(StashError),
}

impl DurableCatalogError {
    /// Reports whether the error is unrecoverable (retrying will never succeed,
    /// or a retry is not safe due to an indeterminate state).
    pub fn is_unrecoverable(&self) -> bool {
        match self {
            DurableCatalogError::Fence(_) | DurableCatalogError::NotWritable(_) => true,
            DurableCatalogError::MiscStash(e) => e.is_unrecoverable(),
            _ => false,
        }
    }

    /// Reports whether the error can be recovered if we opened the catalog in a writeable mode.
    pub fn can_recover_with_write_mode(&self) -> bool {
        match self {
            DurableCatalogError::NotWritable(_) => true,
            DurableCatalogError::MiscStash(e) => e.can_recover_with_write_mode(),
            _ => false,
        }
    }

    /// The underlying operation failed in a way that must be resolved by retrying.
    pub fn should_retry(&self) -> bool {
        match self {
            DurableCatalogError::MiscStash(e) => e.should_retry(),
            _ => false,
        }
    }
}

impl std::error::Error for DurableCatalogError {}

impl fmt::Display for DurableCatalogError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DurableCatalogError::Fence(e) => f.write_str(e),
            DurableCatalogError::IncompatibleVersion(v) => write!(f, "incompatible Catalog version {v}, minimum: {MIN_STASH_VERSION}, current: {STASH_VERSION}"),
            DurableCatalogError::Uninitialized => write!(f, "uninitialized"),
            DurableCatalogError::NotWritable(e) => f.write_str(e),
            DurableCatalogError::Proto(e) => write!(f, "proto: {e}"),
            DurableCatalogError::MiscStash(e) => write!(f, "{e}"),
        }
    }
}

impl From<StashError> for DurableCatalogError {
    fn from(e: StashError) -> Self {
        // We're not really supposed to look at the inner error, but we'll make an exception here.
        match e.inner {
            InternalStashError::Fence(msg) => DurableCatalogError::Fence(msg),
            InternalStashError::IncompatibleVersion(version) => {
                DurableCatalogError::IncompatibleVersion(version)
            }
            InternalStashError::Uninitialized => DurableCatalogError::Uninitialized,
            InternalStashError::StashNotWritable(msg) => DurableCatalogError::NotWritable(msg),
            InternalStashError::Proto(e) => DurableCatalogError::Proto(e),
            InternalStashError::Postgres(_)
            | InternalStashError::PeekSinceUpper(_)
            | InternalStashError::Decoding(_)
            | InternalStashError::Other(_) => DurableCatalogError::MiscStash(e),
        }
    }
}

impl From<TryFromProtoError> for DurableCatalogError {
    fn from(e: TryFromProtoError) -> Self {
        DurableCatalogError::Proto(e)
    }
}
