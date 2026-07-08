// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Multi-view consistency: subscribe to several views at once and observe them
//! at one shared, consistent moment.
//!
//! Materialize gives every object a place on one logical timeline, so an
//! `mz_timestamp` means the same instant across views. A cohort exploits that:
//! it runs one `SUBSCRIBE` per view (each on its own connection) and releases
//! every view only up to the *minimum* closed frontier across all of them. That
//! minimum is the latest instant that is final for every view at once, so the
//! changes it hands back form a genuine cross-view snapshot, never a mix of a
//! newer view with a staler one.
//!
//! This is the same engine as the single-view [`crate::batch::Batcher`],
//! generalized. A single view is the cohort of one: its minimum frontier is its
//! only frontier. The cost of the guarantee is that a laggard member holds the
//! joint moment back, and the leading members' changes buffer in memory until
//! it catches up.
//!
//! Dynamic membership (adding, dropping, merging, or splitting members of a live
//! cohort) is out of scope here. A cohort's membership is fixed for its life.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::client::{connect_cursor, run_drain, DEFAULT_BUFFER_CAPACITY};
use crate::engine::ReleaseBuffer;
use crate::envelope::{Change, Envelope, StreamMessage};
use crate::error::SubscribeError;
use crate::statement::Subscribe;
use crate::token::CohortToken;

/// The changes one view contributes to a [`CohortMoment`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ViewChanges {
    /// The view's name, as given when the cohort was created.
    pub name: String,
    /// The view's changes for this moment, consolidated to their net effect.
    pub updates: Vec<Change>,
}

/// A consistent moment across every view in a cohort.
///
/// Every view's `updates` are closed at the same `frontier`, so applying the
/// whole moment moves a downstream store from one cross-view-consistent state to
/// the next. Persist `resume_token` atomically with the effects for
/// exactly-once state across the cohort.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CohortMoment {
    /// Per-view changes, in the order the cohort's members were given.
    pub views: Vec<ViewChanges>,
    /// The joint closed frontier shared by every view.
    pub frontier: u64,
    /// The checkpoint for this joint position.
    pub resume_token: CohortToken,
    /// Whether this moment completes the cohort's initial snapshot. True once,
    /// for the moment that closes the last member's snapshot.
    pub is_snapshot: bool,
}

impl CohortMoment {
    /// Whether no view carries any change. Empty moments still advance the
    /// frontier and carry a fresh token.
    pub fn is_empty(&self) -> bool {
        self.views.iter().all(|v| v.updates.is_empty())
    }
}

/// One member's buffered state within a cohort.
#[derive(Debug)]
struct CohortMember {
    name: String,
    fingerprint: String,
    buffer: ReleaseBuffer,
    /// The member's own first frontier, its effective `AS OF`.
    first_frontier: Option<u64>,
}

/// The pure state machine behind a cohort: N per-member buffers released at
/// their shared minimum frontier. Kept separate from the transport so it can be
/// exhaustively unit-tested without a server.
#[derive(Debug)]
pub(crate) struct CohortEngine {
    members: Vec<CohortMember>,
    with_snapshot: bool,
    /// The last joint frontier released, or `None` before the first release.
    last_release: Option<u64>,
    /// Whether the joint snapshot has been completed yet.
    snapshot_complete: bool,
}

impl CohortEngine {
    /// Builds an engine from each member's `(name, fingerprint, envelope)`, in
    /// member order. `with_snapshot` is `true` for a fresh cohort, `false` for a
    /// resume.
    pub(crate) fn new(members: Vec<(String, String, Envelope)>, with_snapshot: bool) -> Self {
        let members = members
            .into_iter()
            .map(|(name, fingerprint, envelope)| CohortMember {
                name,
                fingerprint,
                buffer: ReleaseBuffer::new(envelope),
                first_frontier: None,
            })
            .collect();
        CohortEngine {
            members,
            with_snapshot,
            last_release: None,
            snapshot_complete: false,
        }
    }

    /// Buffers a change from member `idx`.
    pub(crate) fn push_data(
        &mut self,
        idx: usize,
        timestamp: u64,
        change: Change,
    ) -> Result<(), SubscribeError> {
        self.members[idx].buffer.push_data(timestamp, change)
    }

    /// Records progress from member `idx`, and emits a joint moment if the
    /// minimum frontier across all members advanced.
    pub(crate) fn push_progress(
        &mut self,
        idx: usize,
        frontier: u64,
    ) -> Result<Option<CohortMoment>, SubscribeError> {
        let member = &mut self.members[idx];
        member.buffer.observe_progress(frontier)?;
        if member.first_frontier.is_none() {
            member.first_frontier = Some(frontier);
        }

        // The joint frontier is the minimum across members, and is defined only
        // once every member has reported at least one progress. Until then a
        // silent member could still emit at an earlier timestamp, so nothing is
        // safe to release.
        let release = match self
            .members
            .iter()
            .map(|m| m.buffer.frontier())
            .collect::<Option<Vec<u64>>>()
        {
            Some(frontiers) => frontiers.into_iter().min().expect("cohort is non-empty"),
            None => return Ok(None),
        };
        // Emit only when the joint frontier actually advances.
        if let Some(last) = self.last_release {
            if release <= last {
                return Ok(None);
            }
        }

        // Every member has reported, so every `first_frontier` is set. The joint
        // snapshot is complete once the release frontier passes the last
        // member's `AS OF`.
        let max_first = self
            .members
            .iter()
            .map(|m| m.first_frontier.expect("every member reported"))
            .max()
            .expect("cohort is non-empty");

        let views = self
            .members
            .iter_mut()
            .map(|m| ViewChanges {
                name: m.name.clone(),
                updates: m.buffer.release_below(release),
            })
            .collect();

        let was_complete = self.snapshot_complete;
        if !was_complete && release > max_first {
            self.snapshot_complete = true;
        }
        let is_snapshot = self.with_snapshot && !was_complete && self.snapshot_complete;

        self.last_release = Some(release);
        let fingerprints = self.members.iter().map(|m| m.fingerprint.clone()).collect();

        Ok(Some(CohortMoment {
            views,
            frontier: release,
            resume_token: CohortToken::new(release, fingerprints),
            is_snapshot,
        }))
    }
}

/// A live cohort of subscriptions, yielding consistent cross-view moments.
///
/// Each member runs its own `SUBSCRIBE` on its own connection, so a cohort of N
/// views holds N connections. All members drain into one shared bounded buffer,
/// so if the consumer falls behind, the cohort fails loud with
/// [`SubscribeError::BufferOverflow`] rather than letting the server buffer.
#[derive(Debug)]
pub struct Cohort {
    rx: mpsc::Receiver<(usize, StreamMessage)>,
    status: Arc<Mutex<Option<SubscribeError>>>,
    engine: CohortEngine,
}

impl Cohort {
    /// Starts a fresh cohort over the given `members`, each a `(name,
    /// subscription)` pair. All members connect with `conninfo` and take their
    /// initial snapshot. Names are how a member's changes are identified in each
    /// [`CohortMoment`], and need not match the object name.
    pub async fn connect(
        conninfo: &str,
        members: Vec<(String, Subscribe)>,
    ) -> Result<Cohort, SubscribeError> {
        Self::start(conninfo, members, None).await
    }

    /// Resumes a cohort from `token`, skipping snapshots and reconstructing the
    /// exact joint cut. Fails with [`SubscribeError::SchemaMismatch`] if the
    /// members no longer match the token (different shapes or order).
    pub async fn resume(
        conninfo: &str,
        members: Vec<(String, Subscribe)>,
        token: &CohortToken,
    ) -> Result<Cohort, SubscribeError> {
        Self::start(conninfo, members, Some(token)).await
    }

    async fn start(
        conninfo: &str,
        members: Vec<(String, Subscribe)>,
        token: Option<&CohortToken>,
    ) -> Result<Cohort, SubscribeError> {
        if members.is_empty() {
            return Err(SubscribeError::Protocol(
                "a cohort needs at least one member".to_string(),
            ));
        }
        if let Some(token) = token {
            let fingerprints: Vec<String> = members.iter().map(|(_, s)| s.fingerprint()).collect();
            if fingerprints != token.members() {
                return Err(SubscribeError::SchemaMismatch {
                    expected: token.members().join(","),
                    actual: fingerprints.join(","),
                });
            }
        }

        let (tx, rx) = mpsc::channel(DEFAULT_BUFFER_CAPACITY);
        let status = Arc::new(Mutex::new(None));
        let cancel = Arc::new(AtomicBool::new(false));

        let mut specs = Vec::with_capacity(members.len());
        for (idx, (name, subscribe)) in members.iter().enumerate() {
            let sql = match token {
                None => subscribe.to_sql_initial(),
                Some(token) => subscribe.to_sql_resume_at(token.as_of()),
            };
            let client = match connect_cursor(conninfo, &sql).await {
                Ok(client) => client,
                Err(error) => {
                    // Tear down members already started before propagating.
                    cancel.store(true, Ordering::SeqCst);
                    return Err(error);
                }
            };
            let index = idx;
            tokio::spawn(run_drain(
                client,
                subscribe.envelope().clone(),
                subscribe.is_bounded(),
                tx.clone(),
                move |message| (index, message),
                Arc::clone(&cancel),
                Arc::clone(&status),
            ));
            specs.push((
                name.clone(),
                subscribe.fingerprint(),
                subscribe.envelope().clone(),
            ));
        }
        // Drop the original sender so the channel closes once every member's
        // drain has exited.
        drop(tx);

        Ok(Cohort {
            rx,
            status,
            engine: CohortEngine::new(specs, token.is_none()),
        })
    }

    /// Returns the next consistent cross-view moment, or `None` when every
    /// member has terminated (only bounded members do).
    pub async fn next(&mut self) -> Result<Option<CohortMoment>, SubscribeError> {
        loop {
            match self.rx.recv().await {
                Some((idx, StreamMessage::Data { timestamp, change })) => {
                    self.engine.push_data(idx, timestamp, change)?;
                }
                Some((idx, StreamMessage::Progress { frontier })) => {
                    if let Some(moment) = self.engine.push_progress(idx, frontier)? {
                        return Ok(Some(moment));
                    }
                }
                None => {
                    return match self.status.lock().expect("status mutex poisoned").take() {
                        Some(error) => Err(error),
                        None => Ok(None),
                    };
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn diff(id: &str, diff: i64) -> Change {
        Change::Diff {
            row: vec![Some(id.to_string())],
            diff,
        }
    }

    fn engine(members: &[&str]) -> CohortEngine {
        let specs = members
            .iter()
            .map(|name| (name.to_string(), format!("fp-{name}"), Envelope::Diff))
            .collect();
        CohortEngine::new(specs, true)
    }

    #[test]
    fn no_release_until_every_member_reports() {
        let mut e = engine(&["a", "b"]);
        // Only member 0 has reported progress; the joint frontier is undefined.
        assert_eq!(e.push_progress(0, 10).unwrap(), None);
        // Once member 1 reports, the minimum (5) is known and a moment emits.
        let moment = e.push_progress(1, 5).unwrap().expect("moment");
        assert_eq!(moment.frontier, 5);
    }

    #[test]
    fn releases_at_the_minimum_frontier() {
        let mut e = engine(&["a", "b"]);
        // Member `a` has data at t=3 and t=7; member `b` at t=4.
        e.push_data(0, 3, diff("a3", 1)).unwrap();
        e.push_data(0, 7, diff("a7", 1)).unwrap();
        e.push_data(1, 4, diff("b4", 1)).unwrap();
        // `a` closes to 10, `b` only to 6. The joint frontier is min(10, 6) = 6.
        e.push_progress(0, 10).unwrap();
        let moment = e.push_progress(1, 6).unwrap().expect("moment");
        assert_eq!(moment.frontier, 6);
        // Below 6: a3 and b4 released; a7 held back for the laggard.
        assert_eq!(moment.views[0].updates, vec![diff("a3", 1)]);
        assert_eq!(moment.views[1].updates, vec![diff("b4", 1)]);
    }

    #[test]
    fn laggard_holds_the_joint_moment() {
        let mut e = engine(&["fast", "slow"]);
        e.push_data(0, 5, diff("x", 1)).unwrap();
        // The fast member races ahead, but the slow member has not reported, so
        // nothing releases.
        assert_eq!(e.push_progress(0, 100).unwrap(), None);
        // The slow member finally reports a low frontier; the joint frontier
        // follows it, not the fast member.
        let moment = e.push_progress(1, 6).unwrap().expect("moment");
        assert_eq!(moment.frontier, 6);
        assert_eq!(moment.views[0].updates, vec![diff("x", 1)]);
        assert!(moment.views[1].updates.is_empty());
    }

    #[test]
    fn moment_carries_a_token_at_the_joint_frontier() {
        let mut e = engine(&["a", "b"]);
        e.push_progress(0, 8).unwrap();
        let moment = e.push_progress(1, 5).unwrap().expect("moment");
        assert_eq!(moment.resume_token.frontier(), 5);
        assert_eq!(moment.resume_token.members(), ["fp-a", "fp-b"]);
    }

    #[test]
    fn per_view_changes_are_consolidated() {
        let mut e = engine(&["a", "b"]);
        // Member `a` inserts then retracts the same row within the window.
        e.push_data(0, 3, diff("x", 1)).unwrap();
        e.push_data(0, 4, diff("x", -1)).unwrap();
        e.push_progress(0, 10).unwrap();
        let moment = e.push_progress(1, 6).unwrap().expect("moment");
        // `x` nets to zero and disappears.
        assert!(moment.views[0].updates.is_empty());
    }

    #[test]
    fn snapshot_completes_once_the_last_member_passes_its_as_of() {
        // Members reveal different as-ofs: `a` at 5, `b` at 8.
        let mut e = engine(&["a", "b"]);
        // Leading moment at min(5, 8) = 5 is not yet the completed snapshot.
        let leading = e.push_progress(0, 5).unwrap();
        assert!(leading.is_none()); // b has not reported yet
        let leading = e.push_progress(1, 8).unwrap().expect("moment");
        assert_eq!(leading.frontier, 5);
        assert!(!leading.is_snapshot);
        // Advance the joint frontier past the last as_of (8). That moment
        // completes the snapshot.
        e.push_progress(0, 9).unwrap(); // a -> 9, min still 8
        let done = e.push_progress(1, 9).unwrap().expect("moment");
        assert_eq!(done.frontier, 9);
        assert!(done.is_snapshot);
        // Subsequent moments are not snapshots.
        e.push_progress(0, 11).unwrap();
        let live = e.push_progress(1, 11).unwrap().expect("moment");
        assert!(!live.is_snapshot);
    }

    #[test]
    fn single_member_cohort_matches_a_batcher() {
        // A cohort of one is the single-view case: the joint frontier is that
        // member's own frontier, and the snapshot completes on the first advance
        // past its as_of.
        let mut e = engine(&["only"]);
        let leading = e.push_progress(0, 100).unwrap().expect("moment");
        assert_eq!(leading.frontier, 100);
        assert!(!leading.is_snapshot);
        e.push_data(0, 100, diff("snap", 1)).unwrap();
        let snap = e.push_progress(0, 101).unwrap().expect("moment");
        assert!(snap.is_snapshot);
        assert_eq!(snap.views[0].updates, vec![diff("snap", 1)]);
    }

    #[test]
    fn protocol_errors_surface_from_members() {
        let mut e = engine(&["a", "b"]);
        e.push_progress(0, 10).unwrap();
        // Data below a member's own frontier is a protocol violation.
        assert!(e.push_data(0, 5, diff("late", 1)).is_err());
    }
}
