// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;

use mz_proto::TryFromProtoError;
use mz_sql::catalog::CatalogError as SqlCatalogError;
use mz_stash_types::{InternalStashError, StashError};
use mz_storage_types::controller::StorageError;

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error(transparent)]
    Catalog(#[from] SqlCatalogError),
    #[error(transparent)]
    Durable(#[from] DurableCatalogError),
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
#[derive(Debug, thiserror::Error)]
pub enum DurableCatalogError {
    /// Catalog has been fenced by another writer.
    #[error("{0}")]
    Fence(String),
    /// The persisted catalog's version is too old for the current catalog to migrate.
    #[error(
        "incompatible Catalog version {found_version}, minimum: {min_catalog_version}, current: {catalog_version}"
    )]
    IncompatibleDataVersion {
        found_version: u64,
        min_catalog_version: u64,
        catalog_version: u64,
    },
    /// The applier version in persist is too old for the current catalog. Reading from persist
    /// would cause other readers to be fenced out.
    #[error("incompatible persist version {found_version}, current: {catalog_version}, make sure to upgrade the catalog one version at a time")]
    IncompatiblePersistVersion {
        found_version: semver::Version,
        catalog_version: semver::Version,
    },
    /// Catalog is uninitialized.
    #[error("uninitialized")]
    Uninitialized,
    /// Catalog is not in a writable state.
    #[error("{0}")]
    NotWritable(String),
    /// Unable to serialize/deserialize Protobuf message.
    #[error("proto: {0}")]
    Proto(TryFromProtoError),
    /// Duplicate key inserted into some catalog collection.
    #[error("duplicate key")]
    DuplicateKey,
    /// Uniqueness violation occurred in some catalog collection.
    #[error("uniqueness violation")]
    UniquenessViolation,
    /// Misc errors from the Stash implementation.
    ///
    /// Once the Stash implementation is removed we can remove this variant.
    #[error(transparent)]
    MiscStash(StashError),
    /// A programming error occurred during a [`mz_storage_client::controller::StorageTxn`].
    #[error(transparent)]
    Storage(StorageError),
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

impl From<StashError> for DurableCatalogError {
    fn from(e: StashError) -> Self {
        // We're not really supposed to look at the inner error, but we'll make an exception here.
        match e.inner {
            InternalStashError::Fence(msg) => DurableCatalogError::Fence(msg),
            InternalStashError::IncompatibleVersion {
                found_version,
                min_stash_version,
                stash_version,
            } => DurableCatalogError::IncompatibleDataVersion {
                found_version,
                min_catalog_version: min_stash_version,
                catalog_version: stash_version,
            },
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

impl From<StorageError> for DurableCatalogError {
    fn from(e: StorageError) -> Self {
        DurableCatalogError::Storage(e)
    }
}

impl From<TryFromProtoError> for DurableCatalogError {
    fn from(e: TryFromProtoError) -> Self {
        DurableCatalogError::Proto(e)
    }
}
