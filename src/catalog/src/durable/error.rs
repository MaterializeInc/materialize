// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;

use mz_persist_client::error::UpperMismatch;
use mz_proto::TryFromProtoError;
use mz_repr::Timestamp;
use mz_sql::catalog::CatalogError as SqlCatalogError;
use mz_storage_types::controller::StorageError;

use crate::durable::persist::antichain_to_timestamp;
use crate::durable::Epoch;

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error(transparent)]
    Catalog(#[from] SqlCatalogError),
    #[error(transparent)]
    Durable(#[from] DurableCatalogError),
}

impl From<TryFromProtoError> for CatalogError {
    fn from(e: TryFromProtoError) -> Self {
        Self::Durable(e.into())
    }
}

impl From<FenceError> for CatalogError {
    fn from(err: FenceError) -> Self {
        let err: DurableCatalogError = err.into();
        err.into()
    }
}

/// An error that can occur while interacting with a durable catalog.
#[derive(Debug, thiserror::Error)]
pub enum DurableCatalogError {
    /// Catalog has been fenced by another writer.
    #[error(transparent)]
    Fence(#[from] FenceError),
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
    #[error("incompatible persist version {found_version}, current: {catalog_version}, make sure to upgrade the catalog one version forward at a time")]
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
    /// A programming error occurred during a [`mz_storage_client::controller::StorageTxn`].
    #[error(transparent)]
    Storage(StorageError<Timestamp>),
    /// An internal programming error.
    #[error("Internal catalog error: {0}")]
    Internal(String),
}

impl DurableCatalogError {
    /// Reports whether the error can be recovered if we opened the catalog in a writeable mode.
    pub fn can_recover_with_write_mode(&self) -> bool {
        match self {
            DurableCatalogError::NotWritable(_) => true,
            _ => false,
        }
    }
}

impl From<StorageError<Timestamp>> for DurableCatalogError {
    fn from(e: StorageError<Timestamp>) -> Self {
        DurableCatalogError::Storage(e)
    }
}

impl From<TryFromProtoError> for DurableCatalogError {
    fn from(e: TryFromProtoError) -> Self {
        DurableCatalogError::Proto(e)
    }
}

/// An error that indicates the durable catalog has been fenced.
///
/// The order of this enum indicates the most information to the least information.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, thiserror::Error)]
pub enum FenceError {
    /// This instance was fenced by another instance with a higher deployment generation. This
    /// necessarily means that the other instance also had a higher epoch. The instance that fenced
    /// us believes that they are from a later generation than us.
    #[error("current catalog deployment generation {current_generation} fenced by new catalog epoch {fence_generation}")]
    DeployGeneration {
        current_generation: u64,
        fence_generation: u64,
    },
    /// This instance was fenced by another instance with a higher epoch. The instance that fenced
    /// us may or may not be from a later generation than us, we were unable to determine.
    #[error("current catalog epoch {current_epoch} fenced by new catalog epoch {fence_epoch}")]
    Epoch {
        current_epoch: Epoch,
        fence_epoch: Epoch,
    },
    /// This instance was fenced while writing to the migration shard during 0dt builtin table
    /// migrations.
    #[error(
        "builtin table migration shard upper {expected_upper:?} fenced by new builtin table migration shard upper {actual_upper:?}"
    )]
    MigrationUpper {
        expected_upper: Timestamp,
        actual_upper: Timestamp,
    },
}

impl FenceError {
    pub fn migration(err: UpperMismatch<Timestamp>) -> Self {
        Self::MigrationUpper {
            expected_upper: antichain_to_timestamp(err.expected),
            actual_upper: antichain_to_timestamp(err.current),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::durable::{Epoch, FenceError};

    #[mz_ore::test]
    fn test_fence_err_ord() {
        let deploy_generation = FenceError::DeployGeneration {
            current_generation: 90,
            fence_generation: 91,
        };
        let epoch = FenceError::Epoch {
            current_epoch: Epoch::new(80).expect("non zero"),
            fence_epoch: Epoch::new(81).expect("non zero"),
        };
        assert!(deploy_generation < epoch);

        let migration = FenceError::MigrationUpper {
            expected_upper: 60.into(),
            actual_upper: 61.into(),
        };
        assert!(epoch < migration);
    }
}
