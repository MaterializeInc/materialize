// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Keeps sources and sinks in a restart loop from writing status rows on
//! every restart.

use std::collections::BTreeMap;

use mz_cluster_client::ReplicaId;
use mz_repr::GlobalId;
use mz_storage_client::client::{Status, StatusUpdate};

/// Drops the statuses of a restart that ends in the stall it started from, so
/// a source or sink in a restart loop writes one row per distinct error.
///
/// An ingestion and its exports restart together and form a group. The
/// ingestion is the group's primary, the object whose halt restarts the group.
/// A sink is the primary of a group of its own. While the primary's last
/// written status is `stalled`, `starting` and `running` statuses that repeat
/// their object's last written status are held. They are dropped if the
/// primary stalls again with the same error, and written with their original
/// timestamps when a collection in the group advances its write frontier or
/// the group reports any other status. A source can report `running` before it
/// connects, so only a frontier advance shows that a restart worked.
///
/// NOTE: A restart that is up but cannot advance, such as an Iceberg sink
/// before its first commit or a sink whose input is stalled, keeps showing its
/// previous stall until it advances.
#[derive(Debug, Default)]
pub(crate) struct RestartLoops {
    /// The last status and error written for each object, per replica.
    written: BTreeMap<(GlobalId, Option<ReplicaId>), (Status, Option<String>)>,
    /// The statuses held since each group's restart began.
    held: BTreeMap<(GlobalId, Option<ReplicaId>), Vec<StatusUpdate>>,
}

impl RestartLoops {
    /// The statuses to write for `update`, which belongs to the group whose
    /// primary is `group`, in order.
    pub(crate) fn observe(&mut self, group: GlobalId, update: StatusUpdate) -> Vec<StatusUpdate> {
        let key = (group, update.replica_id);
        let is_primary = update.id == group;
        let primary_stalled = matches!(self.written.get(&key), Some((Status::Stalled, _)));
        match update.status {
            // The primary's restart, or an export that was already up.
            Status::Starting | Status::Running
                if primary_stalled
                    && (is_primary
                        || matches!(
                            self.written.get(&(update.id, update.replica_id)),
                            Some((Status::Starting | Status::Running, _))
                        )) =>
            {
                self.held.entry(key).or_default().push(update);
                Vec::new()
            }
            Status::Stalled
                if is_primary
                    && self.held.contains_key(&key)
                    && matches!(
                        self.written.get(&key),
                        Some((Status::Stalled, error)) if *error == update.error
                    ) =>
            {
                // The restart ended in the stall it started from.
                self.held.remove(&key);
                Vec::new()
            }
            _ => {
                let held = self.held.remove(&key).unwrap_or_default();
                self.write(held.into_iter().chain([update]).collect())
            }
        }
    }

    /// The held statuses of `group`'s restart on every replica, which is up
    /// because a collection in the group advanced its write frontier.
    ///
    /// The write frontier is shared by all replicas, and only the first to
    /// report an advance moves it. Releasing just that replica would leave the
    /// others showing their stall indefinitely.
    pub(crate) fn progressed(&mut self, group: GlobalId) -> Vec<StatusUpdate> {
        let mut released = Vec::new();
        self.held.retain(|(held_group, _), held| {
            let keep = *held_group != group;
            if !keep {
                released.append(held);
            }
            keep
        });
        self.write(released)
    }

    /// Forgets the objects that no longer satisfy `exists`.
    ///
    /// The controller writes `Dropped` directly to the status history, so a
    /// dropped object's state never sees it.
    pub(crate) fn retain_groups(&mut self, exists: impl Fn(GlobalId) -> bool) {
        self.written.retain(|(id, _), _| exists(*id));
        self.held.retain(|(group, _), _| exists(*group));
    }

    /// Forgets `replica_id`, which was dropped. Its held statuses are dropped
    /// too, since releasing them would write `running` after the `Paused` the
    /// controller writes for the dropped replica.
    pub(crate) fn forget_replica(&mut self, replica_id: ReplicaId) {
        self.written
            .retain(|(_, replica), _| *replica != Some(replica_id));
        self.held
            .retain(|(_, replica), _| *replica != Some(replica_id));
    }

    /// Forgets all state and returns the held statuses, for when the filter is
    /// turned off.
    pub(crate) fn clear(&mut self) -> Vec<StatusUpdate> {
        self.written.clear();
        std::mem::take(&mut self.held)
            .into_values()
            .flatten()
            .collect()
    }

    /// Records `updates` as written and returns them.
    fn write(&mut self, updates: Vec<StatusUpdate>) -> Vec<StatusUpdate> {
        for update in &updates {
            self.written.insert(
                (update.id, update.replica_id),
                (update.status, update.error.clone()),
            );
        }
        updates
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;

    const SOURCE: GlobalId = GlobalId::User(1);
    const TABLE: GlobalId = GlobalId::User(2);

    fn status(id: GlobalId, status: Status, error: Option<&str>) -> StatusUpdate {
        let mut update = StatusUpdate::new(
            id,
            DateTime::from_timestamp(0, 0).expect("valid timestamp"),
            status,
        );
        update.error = error.map(str::to_string);
        update
    }

    /// Feeds one restart of `SOURCE` and its export `TABLE` to `loops`: both
    /// report `starting` and `running`, then `SOURCE` reports `last`. Returns
    /// what is written, as (id, status, error).
    fn restart(
        loops: &mut RestartLoops,
        last: Option<(Status, Option<&str>)>,
    ) -> Vec<(GlobalId, Status, Option<String>)> {
        let mut updates = vec![
            status(SOURCE, Status::Starting, None),
            status(TABLE, Status::Starting, None),
            status(SOURCE, Status::Running, None),
            status(TABLE, Status::Running, None),
        ];
        updates.extend(last.map(|(s, error)| status(SOURCE, s, error)));
        updates
            .into_iter()
            .flat_map(|update| loops.observe(SOURCE, update))
            .map(|update| (update.id, update.status, update.error))
            .collect()
    }

    /// Has `SOURCE` stall and then start a restart on each of `replicas`.
    fn hold_restarts(loops: &mut RestartLoops, replicas: &[ReplicaId]) {
        for replica_id in replicas {
            for update in [
                status(SOURCE, Status::Stalled, Some("dns failure")),
                status(SOURCE, Status::Starting, None),
            ] {
                let update = StatusUpdate {
                    replica_id: Some(*replica_id),
                    ..update
                };
                loops.observe(SOURCE, update);
            }
        }
    }

    fn full_restart(last: (Status, Option<&str>)) -> Vec<(GlobalId, Status, Option<String>)> {
        vec![
            (SOURCE, Status::Starting, None),
            (TABLE, Status::Starting, None),
            (SOURCE, Status::Running, None),
            (TABLE, Status::Running, None),
            (SOURCE, last.0, last.1.map(str::to_string)),
        ]
    }

    #[mz_ore::test]
    fn test_restarts_that_stall_again_write_nothing() {
        let mut loops = RestartLoops::default();
        let stall = (Status::Stalled, Some("dns failure"));

        assert_eq!(
            restart(&mut loops, Some(stall)),
            full_restart(stall),
            "the first run is written in full"
        );
        for cycle in 1..=10 {
            assert_eq!(restart(&mut loops, Some(stall)), vec![], "restart {cycle}");
        }
    }

    #[mz_ore::test]
    fn test_a_restart_that_stalls_differently_is_written() {
        let mut loops = RestartLoops::default();
        restart(&mut loops, Some((Status::Stalled, Some("dns failure"))));

        let other = (Status::Stalled, Some("connection refused"));
        assert_eq!(restart(&mut loops, Some(other)), full_restart(other));
    }

    #[mz_ore::test]
    fn test_a_restart_whose_frontier_advances_is_released() {
        let mut loops = RestartLoops::default();
        let stall = (Status::Stalled, Some("dns failure"));
        restart(&mut loops, Some(stall));
        assert_eq!(restart(&mut loops, None), vec![]);

        let released: Vec<_> = loops
            .progressed(SOURCE)
            .into_iter()
            .map(|update| (update.id, update.status, update.error))
            .collect();
        let mut expected = full_restart(stall);
        expected.pop();
        assert_eq!(released, expected);
        assert_eq!(loops.progressed(SOURCE), vec![]);

        // The source recovered, so a later stall is a new one and is written.
        assert_eq!(restart(&mut loops, Some(stall)), full_restart(stall));
    }

    #[mz_ore::test]
    fn test_a_frontier_advance_releases_every_replica() {
        let replicas = [ReplicaId::User(1), ReplicaId::User(2)];
        let mut loops = RestartLoops::default();
        hold_restarts(&mut loops, &replicas);

        let released: Vec<_> = loops
            .progressed(SOURCE)
            .into_iter()
            .map(|update| update.replica_id)
            .collect();
        assert_eq!(released, replicas.map(Some));
    }

    #[mz_ore::test]
    fn test_clearing_returns_held_statuses_and_forgets_stalls() {
        let mut loops = RestartLoops::default();
        let stall = (Status::Stalled, Some("dns failure"));
        restart(&mut loops, Some(stall));
        restart(&mut loops, None);

        let cleared: Vec<_> = loops
            .clear()
            .into_iter()
            .map(|update| (update.id, update.status, update.error))
            .collect();
        let mut held = full_restart(stall);
        held.pop();
        assert_eq!(cleared, held);
        assert_eq!(
            restart(&mut loops, Some(stall)),
            full_restart(stall),
            "a cleared filter writes the next restart in full"
        );
    }

    #[mz_ore::test]
    fn test_held_statuses_are_written_before_other_statuses() {
        let mut loops = RestartLoops::default();
        restart(&mut loops, Some((Status::Stalled, Some("dns failure"))));

        let paused = (Status::Paused, None);
        assert_eq!(restart(&mut loops, Some(paused)), full_restart(paused));
    }

    #[mz_ore::test]
    fn test_dropped_groups_are_forgotten() {
        let mut loops = RestartLoops::default();
        restart(&mut loops, Some((Status::Stalled, Some("dns failure"))));
        restart(&mut loops, None);

        loops.retain_groups(|id| id != SOURCE && id != TABLE);
        assert!(loops.written.is_empty() && loops.held.is_empty());
    }

    #[mz_ore::test]
    fn test_a_dropped_replica_is_forgotten() {
        let (dropped, kept) = (ReplicaId::User(1), ReplicaId::User(2));
        let mut loops = RestartLoops::default();
        hold_restarts(&mut loops, &[dropped, kept]);

        loops.forget_replica(dropped);
        let released: Vec<_> = loops
            .progressed(SOURCE)
            .into_iter()
            .map(|update| update.replica_id)
            .collect();
        assert_eq!(released, vec![Some(kept)]);
        assert!(loops.written.keys().all(|(_, r)| *r == Some(kept)));
    }

    #[mz_ore::test]
    fn test_exports_racing_the_source_to_stall_lose_nothing() {
        let stall = (Status::Stalled, Some("dns failure"));
        let racing_restart = |first: GlobalId, second: GlobalId| {
            let mut loops = RestartLoops::default();
            restart(&mut loops, Some(stall));
            assert_eq!(restart(&mut loops, None), vec![]);
            [first, second]
                .into_iter()
                .flat_map(|id| loops.observe(SOURCE, status(id, stall.0, stall.1)))
                .map(|update| (update.id, update.status, update.error))
                .collect::<Vec<_>>()
        };
        let stalled = |id| (id, Status::Stalled, Some("dns failure".to_string()));

        // The export's stall ends the hold, so the restart is written in full.
        let mut expected = full_restart(stall);
        expected.insert(4, stalled(TABLE));
        assert_eq!(racing_restart(TABLE, SOURCE), expected);

        // The source's stall drops the restart, and the export's is still
        // written.
        assert_eq!(racing_restart(SOURCE, TABLE), vec![stalled(TABLE)]);
    }

    #[mz_ore::test]
    fn test_an_export_stall_is_written() {
        let mut loops = RestartLoops::default();
        restart(&mut loops, Some((Status::Stalled, Some("dns failure"))));
        assert_eq!(restart(&mut loops, None), vec![]);

        // An export's own stall does not end the source's restart.
        let written: Vec<_> = loops
            .observe(
                SOURCE,
                status(TABLE, Status::Stalled, Some("schema change")),
            )
            .into_iter()
            .map(|update| (update.id, update.status, update.error))
            .collect();
        assert_eq!(
            written,
            vec![
                (SOURCE, Status::Starting, None),
                (TABLE, Status::Starting, None),
                (SOURCE, Status::Running, None),
                (TABLE, Status::Running, None),
                (TABLE, Status::Stalled, Some("schema change".to_string())),
            ]
        );

        // The source is up again, so its next identical stall is a new one.
        let stall = (Status::Stalled, Some("dns failure"));
        assert_eq!(restart(&mut loops, Some(stall)), full_restart(stall));
    }

    #[mz_ore::test]
    fn test_a_new_export_writes_its_first_status() {
        const NEW_TABLE: GlobalId = GlobalId::User(3);
        let mut loops = RestartLoops::default();
        restart(&mut loops, Some((Status::Stalled, Some("dns failure"))));

        let written: Vec<_> = [
            status(SOURCE, Status::Starting, None),
            status(NEW_TABLE, Status::Starting, None),
            status(SOURCE, Status::Stalled, Some("dns failure")),
        ]
        .into_iter()
        .flat_map(|update| loops.observe(SOURCE, update))
        .map(|update| (update.id, update.status, update.error))
        .collect();
        assert_eq!(
            written,
            vec![
                (SOURCE, Status::Starting, None),
                (NEW_TABLE, Status::Starting, None),
                (SOURCE, Status::Stalled, Some("dns failure".to_string())),
            ]
        );
    }

    #[mz_ore::test]
    fn test_an_export_recovering_from_its_own_stall_is_written() {
        let mut loops = RestartLoops::default();
        let stall = (Status::Stalled, Some("dns failure"));
        restart(&mut loops, None);
        loops.observe(
            SOURCE,
            status(TABLE, Status::Stalled, Some("schema change")),
        );
        loops.observe(SOURCE, status(SOURCE, Status::Stalled, stall.1));

        assert_eq!(restart(&mut loops, Some(stall)), full_restart(stall));
    }
}
