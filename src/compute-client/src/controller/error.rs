// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Errors returned by the compute controller.
//!
//! The guiding principle for error handling in the compute controller is that each public method
//! should return an error type that defines exactly the error variants the method can return, and
//! no additional ones. This precludes the usage of a single `ComputeControllerError` enum that
//! simply includes the union of all error variants the compute controller can ever return (and
//! possibly some internal ones that are never returned to external callers). Instead, compute
//! controller methods return bespoke error types that serve as documentation for the failure modes
//! of each method and make it easy for callers to ensure that all possible errors are handled.

use thiserror::Error;

use mz_repr::GlobalId;
use mz_storage_client::controller::StorageError;

use crate::command::ReplicaId;

use super::{instance, ComputeInstanceId};

/// Error returned in response to a reference to an unknown compute instance.
#[derive(Error, Debug)]
#[error("instance does not exist: {0}")]
pub struct InstanceMissing(pub ComputeInstanceId);

/// Error returned in response to a request to create a compute instance with an ID of an existing
/// compute instance.
#[derive(Error, Debug)]
#[error("instance exists already: {0}")]
pub struct InstanceExists(pub ComputeInstanceId);

/// Error returned in response to a reference to an unknown compute collection.
#[derive(Error, Debug)]
#[error("collection does not exist: {0}")]
pub struct CollectionMissing(pub GlobalId);

/// Errors arising during compute collection lookup.
#[derive(Error, Debug)]
pub enum CollectionLookupError {
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
}

impl From<InstanceMissing> for CollectionLookupError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<CollectionMissing> for CollectionLookupError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

/// Errors arising during compute replica creation.
#[derive(Error, Debug)]
pub enum ReplicaCreationError {
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    #[error("replica exists already: {0}")]
    ReplicaExists(ReplicaId),
    #[error("storage interaction error: {0}")]
    Storage(#[from] StorageError),
}

impl From<InstanceMissing> for ReplicaCreationError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<instance::ReplicaCreationError> for ReplicaCreationError {
    fn from(error: instance::ReplicaCreationError) -> Self {
        use instance::ReplicaCreationError::*;
        match error {
            ReplicaExists(id) => Self::ReplicaExists(id),
            Storage(error) => error.into(),
        }
    }
}

/// Errors arising during compute replica removal.
#[derive(Error, Debug)]
pub enum ReplicaDropError {
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
    #[error("storage interaction error: {0}")]
    Storage(#[from] StorageError),
}

impl From<InstanceMissing> for ReplicaDropError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<instance::ReplicaDropError> for ReplicaDropError {
    fn from(error: instance::ReplicaDropError) -> Self {
        use instance::ReplicaDropError::*;
        match error {
            ReplicaMissing(id) => Self::ReplicaMissing(id),
            Storage(error) => error.into(),
        }
    }
}

/// Errors arising during dataflow creation.
#[derive(Error, Debug)]
pub enum DataflowCreationError {
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    #[error("dataflow definition lacks an as_of value")]
    MissingAsOf,
    #[error("dataflow has an as_of not beyond the since of collection: {0}")]
    SinceViolation(GlobalId),
    #[error("storage interaction error: {0}")]
    Storage(#[from] StorageError),
}

impl From<InstanceMissing> for DataflowCreationError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<instance::DataflowCreationError> for DataflowCreationError {
    fn from(error: instance::DataflowCreationError) -> Self {
        use instance::DataflowCreationError::*;
        match error {
            CollectionMissing(id) => Self::CollectionMissing(id),
            MissingAsOf => Self::MissingAsOf,
            SinceViolation(id) => Self::SinceViolation(id),
            Storage(error) => error.into(),
        }
    }
}

/// Errors arising during peek processing.
#[derive(Error, Debug)]
pub enum PeekError {
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    #[error("peek timestamp is not beyond the since of collection: {0}")]
    SinceViolation(GlobalId),
    #[error("storage interaction error: {0}")]
    Storage(#[from] StorageError),
}

impl From<InstanceMissing> for PeekError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<instance::PeekError> for PeekError {
    fn from(error: instance::PeekError) -> Self {
        use instance::PeekError::*;
        match error {
            CollectionMissing(id) => Self::CollectionMissing(id),
            SinceViolation(id) => Self::CollectionMissing(id),
            Storage(error) => error.into(),
        }
    }
}

/// Errors arising during collection updates.
#[derive(Error, Debug)]
pub enum CollectionUpdateError {
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    #[error("storage interaction error: {0}")]
    Storage(#[from] StorageError),
}

impl From<InstanceMissing> for CollectionUpdateError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<instance::CollectionUpdateError> for CollectionUpdateError {
    fn from(error: instance::CollectionUpdateError) -> Self {
        use instance::CollectionUpdateError::*;
        match error {
            CollectionMissing(id) => Self::CollectionMissing(id),
            Storage(error) => error.into(),
        }
    }
}
