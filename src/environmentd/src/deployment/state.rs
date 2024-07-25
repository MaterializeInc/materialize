// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deployment state handling.

use std::future::Future;
use std::sync::{Arc, Mutex};

use mz_ore::channel::trigger::{self, Trigger};
use serde::{Deserialize, Serialize};

enum DeploymentStateInner {
    Initializing,
    ReadyToPromote { _promote_trigger: Trigger },
    Promoting,
    IsLeader,
}

/// The state of an environment deployment.
///
/// This object should be held by the `environmentd` server. It provides methods
/// to handle state transitions that should be driven by the server itself.
///
/// A deployment begins in the `Initializing` state.
///
/// If, during initialization, the server realizes that it is taking over from a
/// failed `environmentd` process of a generation that is already the leader,
/// the server may proceed directly to the `IsLeader` state, via
/// [`DeploymentState::set_is_leader`].
///
/// Otherwise, the server should leave the deployment state in `Initializing`
/// while performing initialization activities. Once the environment is ready to
/// take over from the prior generation, the server should call
/// [`DeploymentState::set_ready_to_promote`]. After this, the server should
/// *not* call [`DeploymentState::set_is_leader`], as an external orchestrator
/// will determine when promotion occurs. The future returned by
/// `set_ready_to_promote` will resolve when promotion has occurred and the
/// deployment should take over from the prior generation and begin serving
/// queries.
#[derive(Clone)]
pub struct DeploymentState {
    inner: Arc<Mutex<DeploymentStateInner>>,
}

impl DeploymentState {
    /// Creates a new `LeaderState` for a deployment.
    ///
    /// Returns the state and a handle to the state.
    pub fn new() -> (DeploymentState, DeploymentStateHandle) {
        let inner = Arc::new(Mutex::new(DeploymentStateInner::Initializing));
        let state = DeploymentState {
            inner: Arc::clone(&inner),
        };
        let handle = DeploymentStateHandle { inner };
        (state, handle)
    }

    /// Marks the deployment as ready to be promoted to leader.
    ///
    /// Returns a future that resolves when the leadership promotion occurs.
    /// When the function returns, the state will be `ReadyToPromote`. When the
    /// returned future resolves, the state will be `Promoting`.
    ///
    /// Panics if the leader state is not `Initializing`.
    pub fn set_ready_to_promote(&self) -> impl Future<Output = ()> {
        let (promote_trigger, promote_trigger_rx) = trigger::channel();
        {
            let mut inner = self.inner.lock().expect("lock poisoned");
            assert!(
                matches!(*inner, DeploymentStateInner::Initializing),
                "LeaderState::set_ready_to_promote called on non-initializing state",
            );
            *inner = DeploymentStateInner::ReadyToPromote {
                _promote_trigger: promote_trigger,
            };
        }
        promote_trigger_rx
    }

    /// Marks the deployment as the leader.
    ///
    /// Panics if the leader state is not `Initializing` or `Promoting`.
    pub fn set_is_leader(&self) {
        let mut inner = self.inner.lock().expect("lock poisoned");
        assert!(
            matches!(
                *inner,
                DeploymentStateInner::Initializing | DeploymentStateInner::Promoting
            ),
            "LeaderState::set_is_leader called on non-initializing state",
        );
        *inner = DeploymentStateInner::IsLeader;
    }
}

/// A cloneable handle to a [`DeploymentState`].
///
/// This should be held by modules providing external interfaces to
/// `environmentd` (e.g., the HTTP server). It provides methods to inspect the
/// current leadership state, and to promote the deployment to the leader if it
/// is ready to do so.
#[derive(Clone)]
pub struct DeploymentStateHandle {
    inner: Arc<Mutex<DeploymentStateInner>>,
}

impl DeploymentStateHandle {
    /// Returns the current deployment status.
    pub fn status(&self) -> DeploymentStatus {
        let inner = self.inner.lock().expect("lock poisoned");
        match *inner {
            DeploymentStateInner::Initializing => DeploymentStatus::Initializing,
            DeploymentStateInner::ReadyToPromote { .. } => DeploymentStatus::ReadyToPromote,
            DeploymentStateInner::Promoting => DeploymentStatus::Promoting,
            DeploymentStateInner::IsLeader => DeploymentStatus::IsLeader,
        }
    }

    /// Attempts to promote this deployment to the leader.
    ///
    /// Deployments in the `Initializing` state cannot be promoted. Deployments
    /// in the `ReadyToPromote`, `Promoting`, and `IsLeader` states can be
    /// promoted (with the latter two cases being no-ops).
    ///
    /// If the leader was successfully promoted, returns `Ok`. Otherwise,
    /// returns `Err`.
    pub fn try_promote(&self) -> Result<(), ()> {
        let mut inner = self.inner.lock().expect("lock poisoned");
        match *inner {
            DeploymentStateInner::Initializing => Err(()),
            DeploymentStateInner::ReadyToPromote { .. } => {
                *inner = DeploymentStateInner::Promoting;
                Ok(())
            }
            DeploymentStateInner::Promoting => Ok(()),
            DeploymentStateInner::IsLeader => Ok(()),
        }
    }
}

/// Describes the status of a deployment.
///
/// This is a simplified representation of [`DeploymentState`], suitable for
/// announcement to the external orchestrator.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DeploymentStatus {
    /// This deployment is not the leader. It is initializing and is not yet
    /// ready to become the leader.
    Initializing,
    /// This deployment is not the leader, but it is ready to become the leader.
    ReadyToPromote,
    /// This deployment is in the process of becoming the leader.
    Promoting,
    /// This deployment is the leader.
    IsLeader,
}
