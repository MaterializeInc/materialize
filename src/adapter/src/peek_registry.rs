// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Connection-partitioned tracking for frontend-owned peeks.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use mz_adapter_types::connection::ConnectionId;
use mz_compute_client::controller::peek_lifecycle::PeekLifecycle;
use mz_compute_client::protocol::response::{PeekError, PeekResponse};
use mz_controller_types::ClusterId;
use mz_repr::GlobalId;
use uuid::Uuid;

use crate::coord::peek::DroppedDependency;

/// The global index is used only for connection management and catalog teardown.
/// Ordinary registration and completion touch only their connection partition.
#[derive(Debug, Default)]
pub(crate) struct PeekRegistry {
    connections: Mutex<BTreeMap<ConnectionId, Weak<ConnectionPeeks>>>,
}

#[derive(Debug, Default)]
pub(crate) struct ConnectionPeeks {
    state: Mutex<ConnectionState>,
}

#[derive(Debug, Default)]
struct ConnectionState {
    closed: bool,
    cancellation_epoch: u64,
    peeks: BTreeMap<Uuid, RegisteredPeek>,
}

#[derive(Debug)]
pub(crate) struct RegisteredPeek {
    pub cluster_id: ClusterId,
    pub depends_on: BTreeSet<GlobalId>,
    pub lifecycle: Weak<PeekLifecycle>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RegisterError {
    CatalogChanged,
    Canceled,
}

impl PeekRegistry {
    pub(crate) fn connection(&self, id: ConnectionId) -> Arc<ConnectionPeeks> {
        let mut connections = self
            .connections
            .lock()
            .expect("peek registry lock poisoned");
        if let Some(connection) = connections.get(&id).and_then(Weak::upgrade) {
            return connection;
        }
        let connection = Arc::new(ConnectionPeeks::default());
        connections.insert(id, Arc::downgrade(&connection));
        connection
    }

    pub(crate) fn close_connection(&self, id: &ConnectionId) {
        let connection = self
            .connections
            .lock()
            .expect("peek registry lock poisoned")
            .remove(id)
            .and_then(|connection| connection.upgrade());
        if let Some(connection) = connection {
            connection.close();
        }
    }

    pub(crate) fn cancel_connection(&self, id: &ConnectionId) {
        let connection = self
            .connections
            .lock()
            .expect("peek registry lock poisoned")
            .get(id)
            .and_then(Weak::upgrade);
        if let Some(connection) = connection {
            connection.cancel();
        }
    }

    /// Catalog invalidation must be published before taking this snapshot.
    /// Each partition then rejects stale registration or includes it in the
    /// invalidation scan. No catalog or lifecycle callback runs under the index lock.
    pub(crate) fn invalidate(
        &self,
        relations: &BTreeMap<GlobalId, String>,
        clusters: &BTreeMap<ClusterId, String>,
    ) {
        let connections: Vec<_> = self
            .connections
            .lock()
            .expect("peek registry lock poisoned")
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        for connection in connections {
            connection.invalidate(relations, clusters);
        }
    }
}

impl ConnectionPeeks {
    pub(crate) fn cancellation_epoch(&self) -> u64 {
        self.state
            .lock()
            .expect("connection peek lock poisoned")
            .cancellation_epoch
    }

    /// Checks catalog freshness under the invalidation scan's lock.
    /// The caller must refresh and validate dependencies after CatalogChanged,
    /// not turn an unrelated catalog edit into a SQL error.
    pub(crate) fn register(
        &self,
        uuid: Uuid,
        peek: RegisteredPeek,
        cancellation_epoch: u64,
        catalog_is_current: impl FnOnce() -> bool,
    ) -> Result<(), RegisterError> {
        let mut state = self.state.lock().expect("connection peek lock poisoned");
        if state.closed || state.cancellation_epoch != cancellation_epoch {
            return Err(RegisterError::Canceled);
        }
        if !catalog_is_current() {
            return Err(RegisterError::CatalogChanged);
        }
        let previous = state.peeks.insert(uuid, peek);
        assert!(previous.is_none(), "peek registered twice");
        Ok(())
    }

    pub(crate) fn remove(&self, uuid: &Uuid) {
        self.state
            .lock()
            .expect("connection peek lock poisoned")
            .peeks
            .remove(uuid);
    }

    pub(crate) fn cancel(&self) {
        self.cancel_all(false);
    }

    fn close(&self) {
        self.cancel_all(true);
    }

    fn cancel_all(&self, close: bool) {
        let peeks = {
            let mut state = self.state.lock().expect("connection peek lock poisoned");
            state.closed |= close;
            state.cancellation_epoch = state
                .cancellation_epoch
                .checked_add(1)
                .expect("peek cancellation epoch overflow");
            std::mem::take(&mut state.peeks)
        };
        for peek in peeks.into_values() {
            if let Some(lifecycle) = peek.lifecycle.upgrade() {
                lifecycle.cancel(PeekResponse::Canceled);
            }
        }
    }

    fn invalidate(
        &self,
        relations: &BTreeMap<GlobalId, String>,
        clusters: &BTreeMap<ClusterId, String>,
    ) {
        let canceled = {
            let mut state = self.state.lock().expect("connection peek lock poisoned");
            let affected: Vec<_> = state
                .peeks
                .iter()
                .filter_map(|(uuid, peek)| {
                    let dependency = peek
                        .depends_on
                        .iter()
                        .find_map(|id| relations.get(id))
                        .map(|name| DroppedDependency::Relation { name: name.clone() })
                        .or_else(|| {
                            clusters
                                .get(&peek.cluster_id)
                                .map(|name| DroppedDependency::Cluster { name: name.clone() })
                        });
                    dependency.map(|dependency| (*uuid, dependency))
                })
                .collect();
            affected
                .into_iter()
                .map(|(uuid, dependency)| {
                    (
                        state.peeks.remove(&uuid).expect("affected peek exists"),
                        dependency,
                    )
                })
                .collect::<Vec<_>>()
        };
        for (peek, dependency) in canceled {
            if let Some(lifecycle) = peek.lifecycle.upgrade() {
                lifecycle.cancel(PeekResponse::Error(PeekError::unstructured(
                    dependency.query_terminated_error(),
                )));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier, Mutex};

    use mz_compute_client::controller::PeekNotification;
    use mz_compute_client::controller::peek_lifecycle::PeekLifecycle;
    use mz_controller_types::ClusterId;
    use mz_repr::GlobalId;
    use uuid::Uuid;

    use super::{ConnectionPeeks, PeekRegistry, RegisterError, RegisteredPeek};

    fn peek(lifecycle: &Arc<PeekLifecycle>) -> RegisteredPeek {
        RegisteredPeek {
            cluster_id: ClusterId::User(1),
            depends_on: BTreeSet::from([GlobalId::User(1)]),
            lifecycle: Arc::downgrade(lifecycle),
        }
    }

    #[mz_ore::test]
    fn stale_catalog_does_not_publish_peek() {
        let connection = ConnectionPeeks::default();
        let lifecycle = Arc::new(PeekLifecycle::new(|_| panic!("unregistered peek retired")));
        let uuid = Uuid::new_v4();
        assert_eq!(
            connection.register(uuid, peek(&lifecycle), 0, || false),
            Err(RegisterError::CatalogChanged)
        );
        assert!(
            connection
                .state
                .lock()
                .expect("test lock poisoned")
                .peeks
                .is_empty()
        );
        assert_eq!(
            connection.register(uuid, peek(&lifecycle), 0, || true),
            Ok(())
        );
        connection.remove(&uuid);
    }

    #[mz_ore::test]
    fn cancel_before_registration_rejects_old_execution_epoch() {
        let connection = ConnectionPeeks::default();
        let lifecycle = Arc::new(PeekLifecycle::new(|_| panic!("unregistered peek retired")));
        let epoch = connection.cancellation_epoch();
        connection.cancel();
        let uuid = Uuid::new_v4();
        assert_eq!(
            connection.register(uuid, peek(&lifecycle), epoch, || true),
            Err(RegisterError::Canceled)
        );
        assert_eq!(
            connection.register(
                uuid,
                peek(&lifecycle),
                connection.cancellation_epoch(),
                || true
            ),
            Ok(())
        );
        connection.remove(&uuid);
    }

    #[mz_ore::test]
    fn closed_connection_never_registers_again() {
        let connection = ConnectionPeeks::default();
        connection.close();
        let lifecycle = Arc::new(PeekLifecycle::new(|_| panic!("unregistered peek retired")));
        assert_eq!(
            connection.register(
                Uuid::new_v4(),
                peek(&lifecycle),
                connection.cancellation_epoch(),
                || true
            ),
            Err(RegisterError::Canceled)
        );
    }

    #[mz_ore::test]
    fn dependency_invalidation_retires_only_matching_peeks() {
        let registry = PeekRegistry::default();
        let connection = registry.connection(mz_ore::id_gen::IdHandle::Static(1));
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let retired = Arc::clone(&outcomes);
        let lifecycle = Arc::new(PeekLifecycle::new(move |outcome| {
            retired.lock().expect("test lock poisoned").push(outcome)
        }));
        connection
            .register(Uuid::new_v4(), peek(&lifecycle), 0, || true)
            .expect("test operation failed");
        registry.invalidate(
            &BTreeMap::from([(GlobalId::User(2), "other".into())]),
            &BTreeMap::new(),
        );
        assert!(outcomes.lock().expect("test lock poisoned").is_empty());
        registry.invalidate(
            &BTreeMap::from([(GlobalId::User(1), "t".into())]),
            &BTreeMap::new(),
        );
        registry.invalidate(
            &BTreeMap::new(),
            &BTreeMap::from([(ClusterId::User(1), "c".into())]),
        );
        assert_eq!(
            *outcomes.lock().expect("test lock poisoned"),
            vec![PeekNotification::Canceled]
        );
        assert!(
            connection
                .state
                .lock()
                .expect("test lock poisoned")
                .peeks
                .is_empty()
        );
    }

    #[mz_ore::test]
    fn retirement_callback_can_remove_registration() {
        let connection = Arc::new(ConnectionPeeks::default());
        let retired = Arc::clone(&connection);
        let uuid = Uuid::new_v4();
        let lifecycle = Arc::new(PeekLifecycle::new(move |_| retired.remove(&uuid)));
        connection
            .register(uuid, peek(&lifecycle), 0, || true)
            .expect("test operation failed");
        connection.cancel();
        assert!(
            connection
                .state
                .lock()
                .expect("test lock poisoned")
                .peeks
                .is_empty()
        );
    }

    #[mz_ore::test]
    fn drop_racing_catalog_check_sees_published_registration() {
        let registry = PeekRegistry::default();
        let connection = registry.connection(mz_ore::id_gen::IdHandle::Static(1));
        let outcomes = Arc::new(Mutex::new(Vec::new()));
        let retired = Arc::clone(&outcomes);
        let lifecycle = Arc::new(PeekLifecycle::new(move |outcome| {
            retired.lock().expect("test lock poisoned").push(outcome)
        }));
        let checked = Barrier::new(2);
        let published = Barrier::new(2);
        let current = AtomicBool::new(true);
        std::thread::scope(|scope| {
            let registering = scope.spawn(|| {
                connection.register(Uuid::new_v4(), peek(&lifecycle), 0, || {
                    let result = current.load(Ordering::SeqCst);
                    checked.wait();
                    published.wait();
                    result
                })
            });
            // Publish the catalog revision after the freshness check, while
            // registration still holds the partition lock. Invalidation must
            // wait for publication into that partition, then find the peek.
            checked.wait();
            current.store(false, Ordering::SeqCst);
            published.wait();
            registry.invalidate(
                &BTreeMap::from([(GlobalId::User(1), "t".into())]),
                &BTreeMap::new(),
            );
            assert_eq!(registering.join().expect("test thread panicked"), Ok(()));
        });
        assert_eq!(
            *outcomes.lock().expect("test lock poisoned"),
            vec![PeekNotification::Canceled]
        );
        assert!(
            connection
                .state
                .lock()
                .expect("test lock poisoned")
                .peeks
                .is_empty()
        );
    }
}
