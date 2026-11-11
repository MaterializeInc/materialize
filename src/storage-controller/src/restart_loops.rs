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
use std::time::Duration;

use mz_cluster_client::ReplicaId;
use mz_ore::now::EpochMillis;
use mz_repr::GlobalId;
use mz_storage_client::client::{Status, StatusUpdate};

/// How long [`RestartLoops`] holds a restart's statuses. Restarts that fail on
/// connect stalled within 0.3 s in production, and a recovering object shows
/// `running` this much later.
const RESTART_LOOP_HOLD: Duration = Duration::from_secs(5);

/// Drops the statuses of a restart that ends in the stall it started from, so
/// a source or sink in a restart loop writes one row per distinct error.
///
/// An ingestion and its exports restart together and form a group, headed by
/// the ingestion. A sink is a group of its own. While the head's last written
/// status is `stalled`, `starting` and `running` statuses that repeat their
/// object's last written status are held. They are dropped if the head stalls
/// again with the same error. Any other status writes them first, with their
/// original timestamps.
#[derive(Debug, Default)]
pub(crate) struct RestartLoops {
    /// The last status and error written for each object, per replica.
    written: BTreeMap<(GlobalId, Option<ReplicaId>), (Status, Option<String>)>,
    /// The statuses held since each group's restart began, with the time the
    /// first one arrived.
    held: BTreeMap<(GlobalId, Option<ReplicaId>), (EpochMillis, Vec<StatusUpdate>)>,
}

impl RestartLoops {
    /// The statuses to write for `update`, which belongs to the group headed by
    /// `group` and arrived at `now`, in order.
    pub(crate) fn observe(
        &mut self,
        group: GlobalId,
        update: StatusUpdate,
        now: EpochMillis,
    ) -> Vec<StatusUpdate> {
        let key = (group, update.replica_id);
        let is_head = update.id == group;
        let head_stalled = matches!(self.written.get(&key), Some((Status::Stalled, _)));
        match update.status {
            // The head's restart, or an export that was already up.
            Status::Starting | Status::Running
                if head_stalled
                    && (is_head
                        || matches!(
                            self.written.get(&(update.id, update.replica_id)),
                            Some((Status::Starting | Status::Running, _))
                        )) =>
            {
                self.held
                    .entry(key)
                    .or_insert_with(|| (now, Vec::new()))
                    .1
                    .push(update);
                Vec::new()
            }
            Status::Stalled
                if is_head
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
                let held = self.held.remove(&key).map(|(_, held)| held);
                self.write(held.into_iter().flatten().chain([update]).collect())
            }
        }
    }

    /// The held statuses of restarts that have been up for at least
    /// [`RESTART_LOOP_HOLD`] at `now`.
    pub(crate) fn release_expired(&mut self, now: EpochMillis) -> Vec<StatusUpdate> {
        let hold = u64::try_from(RESTART_LOOP_HOLD.as_millis()).expect("hold fits in u64");
        let expired: Vec<_> = self
            .held
            .iter()
            .filter(|(_, (since, _))| now.saturating_sub(*since) >= hold)
            .map(|(key, _)| *key)
            .collect();
        let mut released = Vec::new();
        for key in expired {
            let (_, held) = self.held.remove(&key).expect("key was just listed");
            released.extend(held);
        }
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

    /// Feeds one restart of `SOURCE` and its export `TABLE` to `loops` at
    /// `now`: both report `starting` and `running`, then `SOURCE` reports
    /// `last`. Returns what is written, as (id, status, error).
    fn restart(
        loops: &mut RestartLoops,
        now: EpochMillis,
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
            .flat_map(|update| loops.observe(SOURCE, update, now))
            .map(|update| (update.id, update.status, update.error))
            .collect()
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
            restart(&mut loops, 0, Some(stall)),
            full_restart(stall),
            "the first run is written in full"
        );
        for cycle in 1..=10 {
            let now = cycle * 5_000;
            assert_eq!(
                restart(&mut loops, now, Some(stall)),
                vec![],
                "restart {cycle}"
            );
            assert_eq!(loops.release_expired(now + 300), vec![]);
        }
    }

    #[mz_ore::test]
    fn test_a_restart_that_stalls_differently_is_written() {
        let mut loops = RestartLoops::default();
        restart(&mut loops, 0, Some((Status::Stalled, Some("dns failure"))));

        let other = (Status::Stalled, Some("connection refused"));
        assert_eq!(restart(&mut loops, 5_000, Some(other)), full_restart(other));
    }

    #[mz_ore::test]
    fn test_a_restart_that_stays_up_is_released_after_the_hold() {
        let mut loops = RestartLoops::default();
        let stall = (Status::Stalled, Some("dns failure"));
        restart(&mut loops, 0, Some(stall));
        assert_eq!(restart(&mut loops, 5_000, None), vec![]);

        let hold = u64::try_from(RESTART_LOOP_HOLD.as_millis()).expect("fits");
        assert_eq!(loops.release_expired(5_000 + hold - 1), vec![]);
        let released: Vec<_> = loops
            .release_expired(5_000 + hold)
            .into_iter()
            .map(|update| (update.id, update.status, update.error))
            .collect();
        let mut expected = full_restart(stall);
        expected.pop();
        assert_eq!(released, expected);

        // The source recovered, so a later stall is a new one and is written.
        assert_eq!(
            restart(&mut loops, 60_000, Some(stall)),
            full_restart(stall)
        );
    }

    #[mz_ore::test]
    fn test_held_statuses_are_written_before_other_statuses() {
        let mut loops = RestartLoops::default();
        restart(&mut loops, 0, Some((Status::Stalled, Some("dns failure"))));

        let paused = (Status::Paused, None);
        assert_eq!(
            restart(&mut loops, 5_000, Some(paused)),
            full_restart(paused)
        );
    }

    #[mz_ore::test]
    fn test_dropped_groups_are_forgotten() {
        let mut loops = RestartLoops::default();
        restart(&mut loops, 0, Some((Status::Stalled, Some("dns failure"))));
        restart(&mut loops, 5_000, None);

        loops.retain_groups(|id| id != SOURCE && id != TABLE);
        assert!(loops.written.is_empty() && loops.held.is_empty());
    }

    #[mz_ore::test]
    fn test_an_export_stall_is_written() {
        let mut loops = RestartLoops::default();
        restart(&mut loops, 0, Some((Status::Stalled, Some("dns failure"))));
        assert_eq!(restart(&mut loops, 5_000, None), vec![]);

        // An export's own stall does not end the source's restart.
        let written: Vec<_> = loops
            .observe(
                SOURCE,
                status(TABLE, Status::Stalled, Some("schema change")),
                5_100,
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
        assert_eq!(
            restart(&mut loops, 60_000, Some(stall)),
            full_restart(stall)
        );
    }

    #[mz_ore::test]
    fn test_a_new_export_writes_its_first_status() {
        const NEW_TABLE: GlobalId = GlobalId::User(3);
        let mut loops = RestartLoops::default();
        restart(&mut loops, 0, Some((Status::Stalled, Some("dns failure"))));

        let written: Vec<_> = [
            status(SOURCE, Status::Starting, None),
            status(NEW_TABLE, Status::Starting, None),
            status(SOURCE, Status::Stalled, Some("dns failure")),
        ]
        .into_iter()
        .flat_map(|update| loops.observe(SOURCE, update, 5_000))
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
        restart(&mut loops, 0, None);
        loops.observe(
            SOURCE,
            status(TABLE, Status::Stalled, Some("schema change")),
            100,
        );
        loops.observe(SOURCE, status(SOURCE, Status::Stalled, stall.1), 200);

        assert_eq!(restart(&mut loops, 5_000, Some(stall)), full_restart(stall));
    }
}
