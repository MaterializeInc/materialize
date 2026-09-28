// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Environment-scoped observation for the prepared-rewrite regression.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, OnceLock};

use mz_repr::{GlobalId, Timestamp};
use mz_sql::catalog::EnvironmentId;
use uuid::Uuid;

/// Copies of prepared selections and local token minima, not read capabilities.
/// Active minima alone do not certify that the incarnation is still live.
#[derive(Clone, Debug)]
pub struct PreparedRewriteObservation {
    pub selections: BTreeMap<GlobalId, Uuid>,
    pub incarnation: Option<u64>,
    pub active_holds_before_preparation: BTreeMap<GlobalId, Timestamp>,
    pub active_holds: BTreeMap<GlobalId, Timestamp>,
}

pub(crate) type Observer = Arc<dyn Fn(&PreparedRewriteObservation) + Send + Sync>;
static OBSERVERS: OnceLock<Mutex<BTreeMap<String, Observer>>> = OnceLock::new();

/// Removes this environment's observation hook when dropped.
#[derive(Debug)]
#[must_use = "keep the guard alive while observing prepared rewrites"]
pub struct PreparedRewriteObserver {
    environment: String,
}

/// Observe nonempty prepared rewrites before their commit attempts.
///
/// Only one observer may be configured for an environment. The callback runs on
/// the coordinator and may provide a test rendezvous. It receives no holds and
/// cannot select a retry outcome. No observation is gathered unless configured.
pub fn observe_prepared_rewrites(
    environment: &EnvironmentId,
    observer: impl Fn(&PreparedRewriteObservation) + Send + Sync + 'static,
) -> PreparedRewriteObserver {
    let environment = environment.to_string();
    let mut observers = OBSERVERS
        .get_or_init(Default::default)
        .lock()
        .expect("prepared rewrite observer mutex poisoned");
    assert!(
        !observers.contains_key(&environment),
        "observer already configured"
    );
    observers.insert(environment.clone(), Arc::new(observer));
    PreparedRewriteObserver { environment }
}

impl Drop for PreparedRewriteObserver {
    fn drop(&mut self) {
        OBSERVERS
            .get()
            .expect("observer was configured")
            .lock()
            .expect("prepared rewrite observer mutex poisoned")
            .remove(&self.environment);
    }
}

pub(crate) fn prepared_rewrite_observer(environment: &EnvironmentId) -> Option<Observer> {
    OBSERVERS
        .get()?
        .lock()
        .expect("prepared rewrite observer mutex poisoned")
        .get(&environment.to_string())
        .cloned()
}
