use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use mz_adapter_types::connection::ConnectionId;
use mz_compute_client::controller::PeekNotification;
use mz_compute_client::protocol::response::PeekResponse;
use mz_controller_types::ClusterId;
use mz_ore::cast::CastFrom;
use mz_ore::tracing::OpenTelemetryContext;
use mz_repr::GlobalId;
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::coord::ExecuteContextExtra;
use crate::statement_logging::WatchSetCreation;
use crate::statement_logging::{StatementEndedExecutionReason, StatementExecutionStrategy};

#[derive(Debug)]
pub enum QueryTrackerCmd {
    TrackPeek(TrackedPeek),
    UntrackPeek {
        uuid: Uuid,
    },
    CancelConn {
        conn_id: ConnectionId,
    },
    CancelByDrop(CancelByDrop),
    ObservePeekNotification {
        uuid: Uuid,
        notification: PeekNotification,
        otel_ctx: OpenTelemetryContext,
    },
    Dump {
        tx: oneshot::Sender<QueryTrackerDump>,
    },
}

#[derive(Debug)]
pub struct TrackedPeek {
    pub uuid: Uuid,
    pub conn_id: ConnectionId,
    pub cluster_id: ClusterId,
    pub depends_on: BTreeSet<GlobalId>,
    pub ctx_extra: ExecuteContextExtra,
    pub execution_strategy: StatementExecutionStrategy,
    pub watch_set: Option<WatchSetCreation>,
}

#[derive(Debug)]
pub struct CancelByDrop {
    pub dropped_collections: BTreeSet<GlobalId>,
    pub dropped_clusters: BTreeSet<ClusterId>,
    /// Pre-formatted “relation …” names keyed by GlobalId.
    pub dropped_collection_names: BTreeMap<GlobalId, String>,
    /// Pre-formatted “cluster …” names keyed by ClusterId.
    pub dropped_cluster_names: BTreeMap<ClusterId, String>,
}

#[derive(Debug)]
pub struct QueryTrackerDump {
    pub pending_peeks: BTreeMap<String, String>,
    pub client_pending_peeks: BTreeMap<String, BTreeMap<String, ClusterId>>,
}

#[allow(dead_code)]
pub trait QueryTrackerHandle: Clone + Send + Sync + 'static {
    fn send(&self, cmd: QueryTrackerCmd);
}

#[derive(Clone, Debug)]
pub struct Handle {
    tx: mpsc::UnboundedSender<QueryTrackerCmd>,
}

impl Handle {
    pub fn new(tx: mpsc::UnboundedSender<QueryTrackerCmd>) -> Self {
        Self { tx }
    }

    pub fn send(&self, cmd: QueryTrackerCmd) {
        let _ = self.tx.send(cmd);
    }
}

impl QueryTrackerHandle for Handle {
    fn send(&self, cmd: QueryTrackerCmd) {
        self.send(cmd);
    }
}

pub trait QueryTrackerEffects: Send + Sync + 'static {
    fn cancel_compute_peek(&self, cluster_id: ClusterId, uuid: Uuid, response: PeekResponse);
    fn inc_canceled_peeks(&self, by: u64);
    fn install_peek_watch_sets(&self, conn_id: ConnectionId, watch_set: WatchSetCreation);
    fn retire_execute(
        &self,
        otel_ctx: OpenTelemetryContext,
        reason: StatementEndedExecutionReason,
        ctx_extra: ExecuteContextExtra,
    );
}

#[derive(Debug)]
pub struct QueryTracker<E> {
    effects: E,
    peeks_by_uuid: BTreeMap<Uuid, TrackedPeek>,
    peeks_by_conn: BTreeMap<ConnectionId, BTreeSet<Uuid>>,
    dropped_collections: BTreeMap<GlobalId, DroppedObject>,
    dropped_clusters: BTreeMap<ClusterId, DroppedObject>,
}

#[derive(Debug, Clone)]
struct DroppedObject {
    dropped_at: Instant,
    name: Option<String>,
}

impl<E: QueryTrackerEffects> QueryTracker<E> {
    const DROP_TOMBSTONE_TTL: Duration = Duration::from_secs(60);

    pub fn new(effects: E) -> Self {
        Self {
            effects,
            peeks_by_uuid: BTreeMap::new(),
            peeks_by_conn: BTreeMap::new(),
            dropped_collections: BTreeMap::new(),
            dropped_clusters: BTreeMap::new(),
        }
    }

    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<QueryTrackerCmd>) {
        while let Some(cmd) = rx.recv().await {
            match cmd {
                QueryTrackerCmd::TrackPeek(peek) => self.track_peek(peek),
                QueryTrackerCmd::UntrackPeek { uuid } => self.untrack_peek(uuid),
                QueryTrackerCmd::CancelConn { conn_id } => self.cancel_conn(conn_id),
                QueryTrackerCmd::CancelByDrop(drop) => self.cancel_by_drop(drop),
                QueryTrackerCmd::ObservePeekNotification {
                    uuid,
                    notification,
                    otel_ctx,
                } => self.observe_peek_notification(uuid, notification, otel_ctx),
                QueryTrackerCmd::Dump { tx } => {
                    let _ = tx.send(self.dump());
                }
            }
        }
    }

    fn track_peek(&mut self, mut peek: TrackedPeek) {
        self.expire_dropped_objects();

        if let Some(error) = self.peek_error_for_tombstones(&peek) {
            self.effects.inc_canceled_peeks(1);
            self.effects.cancel_compute_peek(
                peek.cluster_id,
                peek.uuid,
                PeekResponse::Error(error),
            );
            self.effects.retire_execute(
                OpenTelemetryContext::obtain(),
                StatementEndedExecutionReason::Canceled,
                peek.ctx_extra,
            );
            return;
        }

        if let Some(watch_set) = peek.watch_set.take() {
            self.effects
                .install_peek_watch_sets(peek.conn_id.clone(), watch_set);
        }
        self.peeks_by_conn
            .entry(peek.conn_id.clone())
            .or_default()
            .insert(peek.uuid);
        self.peeks_by_uuid.insert(peek.uuid, peek);
    }

    fn untrack_peek(&mut self, uuid: Uuid) {
        let Some(peek) = self.remove_peek(uuid) else {
            return;
        };
        // The frontend will log/report the issuance failure. We must ensure the non-trivial
        // ExecuteContextExtra does not get dropped without being retired.
        let _ = peek.ctx_extra.retire();
    }

    fn cancel_conn(&mut self, conn_id: ConnectionId) {
        let Some(uuids) = self.peeks_by_conn.remove(&conn_id) else {
            return;
        };

        self.effects.inc_canceled_peeks(u64::cast_from(uuids.len()));

        for uuid in uuids {
            let Some(peek) = self.peeks_by_uuid.remove(&uuid) else {
                continue;
            };
            self.effects
                .cancel_compute_peek(peek.cluster_id, uuid, PeekResponse::Canceled);
            self.effects.retire_execute(
                OpenTelemetryContext::obtain(),
                StatementEndedExecutionReason::Canceled,
                peek.ctx_extra,
            );
        }
    }

    fn cancel_by_drop(&mut self, drop: CancelByDrop) {
        self.expire_dropped_objects();
        self.record_dropped_objects(&drop);

        let mut to_cancel = Vec::new();

        for (uuid, peek) in &self.peeks_by_uuid {
            if let Some(id) = peek
                .depends_on
                .iter()
                .find(|id| drop.dropped_collections.contains(id))
            {
                if let Some(name) = drop.dropped_collection_names.get(id) {
                    to_cancel.push((
                        *uuid,
                        peek.cluster_id,
                        PeekResponse::Error(format!(
                            "query could not complete because {name} was dropped"
                        )),
                    ));
                } else {
                    to_cancel.push((
                        *uuid,
                        peek.cluster_id,
                        PeekResponse::Error(
                            "query could not complete because a dependency was dropped".into(),
                        ),
                    ));
                }
            } else if drop.dropped_clusters.contains(&peek.cluster_id) {
                if let Some(name) = drop.dropped_cluster_names.get(&peek.cluster_id) {
                    to_cancel.push((
                        *uuid,
                        peek.cluster_id,
                        PeekResponse::Error(format!(
                            "query could not complete because {name} was dropped"
                        )),
                    ));
                } else {
                    to_cancel.push((
                        *uuid,
                        peek.cluster_id,
                        PeekResponse::Error(
                            "query could not complete because a cluster was dropped".into(),
                        ),
                    ));
                }
            }
        }

        self.effects
            .inc_canceled_peeks(u64::cast_from(to_cancel.len()));

        for (uuid, cluster_id, response) in to_cancel {
            if let Some(peek) = self.remove_peek(uuid) {
                self.effects.cancel_compute_peek(cluster_id, uuid, response);
                self.effects.retire_execute(
                    OpenTelemetryContext::obtain(),
                    StatementEndedExecutionReason::Canceled,
                    peek.ctx_extra,
                );
            }
        }
    }

    fn observe_peek_notification(
        &mut self,
        uuid: Uuid,
        notification: PeekNotification,
        otel_ctx: OpenTelemetryContext,
    ) {
        self.expire_dropped_objects();

        let Some(peek) = self.remove_peek(uuid) else {
            return;
        };

        let reason = match notification {
            PeekNotification::Success {
                rows: num_rows,
                result_size,
            } => StatementEndedExecutionReason::Success {
                result_size: Some(result_size),
                rows_returned: Some(num_rows),
                execution_strategy: Some(peek.execution_strategy),
            },
            PeekNotification::Error(error) => StatementEndedExecutionReason::Errored { error },
            PeekNotification::Canceled => StatementEndedExecutionReason::Canceled,
        };

        self.effects
            .retire_execute(otel_ctx, reason, peek.ctx_extra);
    }

    fn expire_dropped_objects(&mut self) {
        let now = Instant::now();
        self.dropped_collections
            .retain(|_, d| now.duration_since(d.dropped_at) <= Self::DROP_TOMBSTONE_TTL);
        self.dropped_clusters
            .retain(|_, d| now.duration_since(d.dropped_at) <= Self::DROP_TOMBSTONE_TTL);
    }

    fn record_dropped_objects(&mut self, drop: &CancelByDrop) {
        let now = Instant::now();
        for id in &drop.dropped_collections {
            let name = drop.dropped_collection_names.get(id).cloned();
            self.dropped_collections.insert(
                *id,
                DroppedObject {
                    dropped_at: now,
                    name,
                },
            );
        }
        for cluster_id in &drop.dropped_clusters {
            let name = drop.dropped_cluster_names.get(cluster_id).cloned();
            self.dropped_clusters.insert(
                *cluster_id,
                DroppedObject {
                    dropped_at: now,
                    name,
                },
            );
        }
    }

    fn peek_error_for_tombstones(&self, peek: &TrackedPeek) -> Option<String> {
        if let Some(id) = peek
            .depends_on
            .iter()
            .find(|id| self.dropped_collections.contains_key(id))
        {
            let dropped = self.dropped_collections.get(id).expect("checked above");
            let msg = if let Some(name) = &dropped.name {
                format!("query could not complete because {name} was dropped")
            } else {
                "query could not complete because a dependency was dropped".to_string()
            };
            return Some(msg);
        }

        if let Some(dropped) = self.dropped_clusters.get(&peek.cluster_id) {
            let msg = if let Some(name) = &dropped.name {
                format!("query could not complete because {name} was dropped")
            } else {
                "query could not complete because a cluster was dropped".to_string()
            };
            return Some(msg);
        }

        None
    }

    fn remove_peek(&mut self, uuid: Uuid) -> Option<TrackedPeek> {
        let peek = self.peeks_by_uuid.remove(&uuid)?;
        if let Some(uuids) = self.peeks_by_conn.get_mut(&peek.conn_id) {
            uuids.remove(&uuid);
            if uuids.is_empty() {
                self.peeks_by_conn.remove(&peek.conn_id);
            }
        }
        Some(peek)
    }

    fn dump(&self) -> QueryTrackerDump {
        let pending_peeks = self
            .peeks_by_uuid
            .iter()
            .map(|(uuid, peek)| {
                (
                    uuid.to_string(),
                    format!(
                        "PendingPeek {{ conn_id: {:?}, cluster_id: {:?}, depends_on: {:?}, statement_logging_id: {:?}, execution_strategy: {:?} }}",
                        peek.conn_id,
                        peek.cluster_id,
                        peek.depends_on,
                        peek.ctx_extra.contents(),
                        peek.execution_strategy,
                    ),
                )
            })
            .collect();

        let client_pending_peeks = self
            .peeks_by_conn
            .iter()
            .map(|(conn_id, uuids)| {
                let entries = uuids
                    .iter()
                    .filter_map(|uuid| {
                        let peek = self.peeks_by_uuid.get(uuid)?;
                        Some((uuid.to_string(), peek.cluster_id))
                    })
                    .collect::<BTreeMap<_, _>>();
                (conn_id.unhandled().to_string(), entries)
            })
            .collect();

        QueryTrackerDump {
            pending_peeks,
            client_pending_peeks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Default)]
    struct RecordingEffects {
        canceled: Arc<Mutex<Vec<(ClusterId, Uuid, PeekResponse)>>>,
        retired: Arc<Mutex<Vec<StatementEndedExecutionReason>>>,
        canceled_metric: Arc<Mutex<u64>>,
    }

    impl QueryTrackerEffects for RecordingEffects {
        fn cancel_compute_peek(&self, cluster_id: ClusterId, uuid: Uuid, response: PeekResponse) {
            self.canceled
                .lock()
                .unwrap()
                .push((cluster_id, uuid, response));
        }

        fn inc_canceled_peeks(&self, by: u64) {
            *self.canceled_metric.lock().unwrap() += by;
        }

        fn install_peek_watch_sets(&self, _conn_id: ConnectionId, _watch_set: WatchSetCreation) {}

        fn retire_execute(
            &self,
            _otel_ctx: OpenTelemetryContext,
            reason: StatementEndedExecutionReason,
            ctx_extra: ExecuteContextExtra,
        ) {
            let _ = ctx_extra.retire();
            self.retired.lock().unwrap().push(reason);
        }
    }

    #[mz_ore::test]
    fn cancel_conn_is_idempotent() {
        let effects = RecordingEffects::default();
        let mut tracker = QueryTracker::new(effects);

        let conn_id = ConnectionId::Static(1);
        let uuid = Uuid::new_v4();
        tracker.track_peek(TrackedPeek {
            uuid,
            conn_id: conn_id.clone(),
            cluster_id: ClusterId::User(1),
            depends_on: BTreeSet::new(),
            ctx_extra: ExecuteContextExtra::new(None),
            execution_strategy: StatementExecutionStrategy::FastPath,
            watch_set: None,
        });

        tracker.cancel_conn(conn_id.clone());
        tracker.cancel_conn(conn_id);
    }

    #[mz_ore::test]
    fn track_peek_is_canceled_if_cluster_was_dropped() {
        let effects = RecordingEffects::default();
        let mut tracker = QueryTracker::new(RecordingEffects {
            canceled: Arc::clone(&effects.canceled),
            retired: Arc::clone(&effects.retired),
            canceled_metric: Arc::clone(&effects.canceled_metric),
        });

        let cluster_id = ClusterId::User(42);
        tracker.cancel_by_drop(CancelByDrop {
            dropped_collections: BTreeSet::new(),
            dropped_clusters: BTreeSet::from([cluster_id]),
            dropped_collection_names: BTreeMap::new(),
            dropped_cluster_names: BTreeMap::from([(cluster_id, "cluster \"c\"".to_string())]),
        });

        let uuid = Uuid::new_v4();
        tracker.track_peek(TrackedPeek {
            uuid,
            conn_id: ConnectionId::Static(1),
            cluster_id,
            depends_on: BTreeSet::new(),
            ctx_extra: ExecuteContextExtra::new(None),
            execution_strategy: StatementExecutionStrategy::Standard,
            watch_set: None,
        });

        let canceled = effects.canceled.lock().unwrap().clone();
        assert_eq!(canceled.len(), 1);
        assert_eq!(canceled[0].0, cluster_id);
        assert_eq!(canceled[0].1, uuid);
        assert_eq!(
            canceled[0].2,
            PeekResponse::Error(
                "query could not complete because cluster \"c\" was dropped".to_string()
            )
        );
    }
}
