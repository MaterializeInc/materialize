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

use mz_repr::GlobalId;
use mz_storage_types::read_holds::ReadHoldError;
use thiserror::Error;

use crate::controller::{ComputeInstanceId, ReplicaId};

/// The error returned by replica-targeted peeks and subscribes when the target replica
/// disconnects.
pub const ERROR_TARGET_REPLICA_FAILED: &str = "target replica failed or was dropped";

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

/// Error returned in response to a reference to an unknown compute collection.
#[derive(Error, Debug)]
#[error("No replicas found in cluster for target list.")]
pub struct HydrationCheckBadTarget(pub Vec<ReplicaId>);

/// Errors arising during compute collection lookup.
#[derive(Error, Debug)]
pub enum CollectionLookupError {
    /// TODO(database-issues#7533): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(database-issues#7533): Add documentation.
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
    /// TODO(database-issues#7533): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(database-issues#7533): Add documentation.
    #[error("replica exists already: {0}")]
    ReplicaExists(ReplicaId),
}

impl From<InstanceMissing> for ReplicaCreationError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

/// Errors arising during compute replica removal.
#[derive(Error, Debug)]
pub enum ReplicaDropError {
    /// TODO(database-issues#7533): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(database-issues#7533): Add documentation.
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
}

impl From<InstanceMissing> for ReplicaDropError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

/// Errors arising during dataflow creation.
#[derive(Error, Debug)]
pub enum DataflowCreationError {
    /// The given instance does not exist.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// One of the imported collections does not exist.
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    /// The targeted replica does not exist.
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
    /// The dataflow definition has doesn't have an `as_of` set.
    #[error("dataflow definition lacks an as_of value")]
    MissingAsOf,
    /// One of the imported collections has a read frontier greater than the dataflow `as_of`.
    #[error("dataflow has an as_of not beyond the since of collection: {0}")]
    SinceViolation(GlobalId),
    /// We skip dataflow creation for empty `as_of`s, which would be a problem for a SUBSCRIBE,
    /// because an initial response is expected.
    #[error("subscribe dataflow has an empty as_of")]
    EmptyAsOfForSubscribe,
    /// We skip dataflow creation for empty `as_of`s, which would be a problem for a COPY TO,
    /// because it should always have an external side effect.
    #[error("copy to dataflow has an empty as_of")]
    EmptyAsOfForCopyTo,
}

impl From<InstanceMissing> for DataflowCreationError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<CollectionMissing> for DataflowCreationError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

impl From<ReadHoldError> for DataflowCreationError {
    fn from(error: ReadHoldError) -> Self {
        match error {
            ReadHoldError::CollectionMissing(id) => Self::CollectionMissing(id),
        }
    }
}

/// Errors arising during peek processing.
#[derive(Error, Debug)]
pub enum PeekError {
    /// TODO(database-issues#7533): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(database-issues#7533): Add documentation.
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    /// TODO(database-issues#7533): Add documentation.
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
    /// TODO(database-issues#7533): Add documentation.
    #[error("peek timestamp is not beyond the since of collection: {0}")]
    SinceViolation(GlobalId),
    /// TODO(database-issues#7533): Add documentation.
    #[error("read hold ID does not match peeked collection: {0}")]
    ReadHoldIdMismatch(GlobalId),
    /// TODO(database-issues#7533): Add documentation.
    #[error("insufficient read hold provided: {0}")]
    ReadHoldInsufficient(GlobalId),
}

impl From<InstanceMissing> for PeekError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<CollectionMissing> for PeekError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

impl From<ReadHoldError> for PeekError {
    fn from(error: ReadHoldError) -> Self {
        match error {
            ReadHoldError::CollectionMissing(id) => Self::CollectionMissing(id),
        }
    }
}

impl From<crate::controller::instance::PeekError> for PeekError {
    fn from(error: crate::controller::instance::PeekError) -> Self {
        use crate::controller::instance::PeekError::*;
        match error {
            ReplicaMissing(id) => PeekError::ReplicaMissing(id),
            ReadHoldIdMismatch(id) => PeekError::ReadHoldIdMismatch(id),
            ReadHoldInsufficient(id) => PeekError::ReadHoldInsufficient(id),
        }
    }
}

/// Errors arising during collection updates.
#[derive(Error, Debug)]
pub enum CollectionUpdateError {
    /// TODO(database-issues#7533): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(database-issues#7533): Add documentation.
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
}

impl From<InstanceMissing> for CollectionUpdateError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<CollectionMissing> for CollectionUpdateError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

/// Errors arising during collection read policy assignment.
#[derive(Error, Debug)]
pub enum ReadPolicyError {
    /// TODO(database-issues#7533): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(database-issues#7533): Add documentation.
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    /// TODO(database-issues#7533): Add documentation.
    #[error("collection is write-only: {0}")]
    WriteOnlyCollection(GlobalId),
}

impl From<InstanceMissing> for ReadPolicyError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<CollectionMissing> for ReadPolicyError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

/// Errors arising during orphan removal.
#[derive(Error, Debug)]
pub enum RemoveOrphansError {
    /// TODO(database-issues#7533): Add documentation.
    #[error("orchestrator error: {0}")]
    OrchestratorError(anyhow::Error),
}

impl From<anyhow::Error> for RemoveOrphansError {
    fn from(error: anyhow::Error) -> Self {
        Self::OrchestratorError(error)
    }
}
