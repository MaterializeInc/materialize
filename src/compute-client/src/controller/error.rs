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
use thiserror::Error;

use crate::controller::{instance, ComputeInstanceId, ReplicaId};

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
    /// TODO(#25239): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(#25239): Add documentation.
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
    /// TODO(#25239): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(#25239): Add documentation.
    #[error("replica exists already: {0}")]
    ReplicaExists(ReplicaId),
    /// TODO(#25239): Add documentation.
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
}

impl From<InstanceMissing> for ReplicaCreationError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<instance::ReplicaExists> for ReplicaCreationError {
    fn from(error: instance::ReplicaExists) -> Self {
        Self::ReplicaExists(error.0)
    }
}

impl From<CollectionMissing> for ReplicaCreationError {
    fn from(error: CollectionMissing) -> Self {
        Self::CollectionMissing(error.0)
    }
}

/// Errors arising during compute replica removal.
#[derive(Error, Debug)]
pub enum ReplicaDropError {
    /// TODO(#25239): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(#25239): Add documentation.
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
}

impl From<InstanceMissing> for ReplicaDropError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<instance::ReplicaMissing> for ReplicaDropError {
    fn from(error: instance::ReplicaMissing) -> Self {
        Self::ReplicaMissing(error.0)
    }
}

/// Errors arising during dataflow creation.
#[derive(Error, Debug)]
pub enum DataflowCreationError {
    /// TODO(#25239): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(#25239): Add documentation.
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    /// TODO(#25239): Add documentation.
    #[error("dataflow definition lacks an as_of value")]
    MissingAsOf,
    /// TODO(#25239): Add documentation.
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

impl From<instance::DataflowCreationError> for DataflowCreationError {
    fn from(error: instance::DataflowCreationError) -> Self {
        use instance::DataflowCreationError::*;
        match error {
            CollectionMissing(id) => Self::CollectionMissing(id),
            MissingAsOf => Self::MissingAsOf,
            SinceViolation(id) => Self::SinceViolation(id),
            EmptyAsOfForSubscribe => Self::EmptyAsOfForSubscribe,
            EmptyAsOfForCopyTo => Self::EmptyAsOfForCopyTo,
        }
    }
}

/// Errors arising during peek processing.
#[derive(Error, Debug)]
pub enum PeekError {
    /// TODO(#25239): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(#25239): Add documentation.
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    /// TODO(#25239): Add documentation.
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
    /// TODO(#25239): Add documentation.
    #[error("peek timestamp is not beyond the since of collection: {0}")]
    SinceViolation(GlobalId),
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
            ReplicaMissing(id) => Self::ReplicaMissing(id),
            SinceViolation(id) => Self::SinceViolation(id),
        }
    }
}

/// Errors arising during collection updates.
#[derive(Error, Debug)]
pub enum CollectionUpdateError {
    /// TODO(#25239): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(#25239): Add documentation.
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
    /// TODO(#25239): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(#25239): Add documentation.
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    /// TODO(#25239): Add documentation.
    #[error("collection is write-only: {0}")]
    WriteOnlyCollection(GlobalId),
}

impl From<InstanceMissing> for ReadPolicyError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<instance::ReadPolicyError> for ReadPolicyError {
    fn from(error: instance::ReadPolicyError) -> Self {
        use instance::ReadPolicyError::*;
        match error {
            CollectionMissing(id) => Self::CollectionMissing(id),
            WriteOnlyCollection(id) => Self::WriteOnlyCollection(id),
        }
    }
}

/// Errors arising during subscribe target assignment.
#[derive(Error, Debug)]
pub enum SubscribeTargetError {
    /// TODO(#25239): Add documentation.
    #[error("instance does not exist: {0}")]
    InstanceMissing(ComputeInstanceId),
    /// TODO(#25239): Add documentation.
    #[error("subscribe does not exist: {0}")]
    SubscribeMissing(GlobalId),
    /// TODO(#25239): Add documentation.
    #[error("replica does not exist: {0}")]
    ReplicaMissing(ReplicaId),
    /// TODO(#25239): Add documentation.
    #[error("subscribe has already produced output")]
    SubscribeAlreadyStarted,
}

impl From<InstanceMissing> for SubscribeTargetError {
    fn from(error: InstanceMissing) -> Self {
        Self::InstanceMissing(error.0)
    }
}

impl From<instance::SubscribeTargetError> for SubscribeTargetError {
    fn from(error: instance::SubscribeTargetError) -> Self {
        use instance::SubscribeTargetError::*;
        match error {
            SubscribeMissing(id) => Self::SubscribeMissing(id),
            ReplicaMissing(id) => Self::ReplicaMissing(id),
            SubscribeAlreadyStarted => Self::SubscribeAlreadyStarted,
        }
    }
}

/// Errors arising during orphan removal.
#[derive(Error, Debug)]
pub enum RemoveOrphansError {
    /// TODO(#25239): Add documentation.
    #[error("orchestrator error: {0}")]
    OrchestratorError(anyhow::Error),
}

impl From<anyhow::Error> for RemoveOrphansError {
    fn from(error: anyhow::Error) -> Self {
        Self::OrchestratorError(error)
    }
}
