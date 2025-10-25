// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;
use std::sync::Arc;

use mz_repr::GlobalId;
use thiserror::Error;
use timely::PartialOrder;
use timely::progress::{Antichain, ChangeBatch, Timestamp as TimelyTimestamp};
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::error::SendError;

pub type ChangeTx<T> = Arc<
    dyn Fn(GlobalId, ChangeBatch<T>) -> Result<(), SendError<(GlobalId, ChangeBatch<T>)>>
        + Send
        + Sync,
>;

/// Token that represents a hold on a collection. This prevents the since of the
/// collection from progressing beyond the hold. In other words, it cannot
/// become true that our hold is `less_than` the since.
///
/// This [ReadHold] is safe to drop. The installed read hold will be returned to
/// the issuer behind the scenes.
pub struct ReadHold<T: TimelyTimestamp> {
    /// Identifies that collection that we have a hold on.
    id: GlobalId,

    /// The times at which we hold.
    since: Antichain<T>,

    /// For communicating changes to this read hold back to whoever issued it.
    change_tx: ChangeTx<T>,
}

impl<T: TimelyTimestamp> Debug for ReadHold<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadHold")
            .field("id", &self.id)
            .field("since", &self.since)
            .finish_non_exhaustive()
    }
}

/// Errors for manipulating read holds.
#[derive(Error, Debug)]
pub enum ReadHoldDowngradeError<T> {
    /// The new frontier is not beyond the current since.
    #[error("since violation: new frontier {frontier:?} is not beyond current since {since:?}")]
    SinceViolation {
        /// The frontier to downgrade to.
        frontier: Antichain<T>,
        /// The since of the collection.
        since: Antichain<T>,
    },
}

impl<T: TimelyTimestamp> ReadHold<T> {
    pub fn new(id: GlobalId, since: Antichain<T>, change_tx: ChangeTx<T>) -> Self {
        Self {
            id,
            since,
            change_tx,
        }
    }

    pub fn with_channel(
        id: GlobalId,
        since: Antichain<T>,
        channel_tx: UnboundedSender<(GlobalId, ChangeBatch<T>)>,
    ) -> Self {
        let tx = Arc::new(move |id, changes| channel_tx.send((id, changes)));
        Self::new(id, since, tx)
    }

    /// Returns the [GlobalId] of the collection that this [ReadHold] is for.
    pub fn id(&self) -> GlobalId {
        self.id
    }

    /// Returns the frontier at which this [ReadHold] is holding back the since
    /// of the collection identified by `id`. This does not mean that the
    /// overall since of the collection is what we report here. Only that it is
    /// _at least_ held back to the reported frontier by this read hold.
    pub fn since(&self) -> &Antichain<T> {
        &self.since
    }

    /// Merges `other` into `self`, keeping the overall read hold.
    ///
    /// # Panics
    ///
    /// Panics when trying to merge a [ReadHold] for a different collection
    /// (different [GlobalId]).
    pub fn merge_assign(&mut self, mut other: ReadHold<T>) {
        assert_eq!(
            self.id, other.id,
            "can only merge ReadHolds for the same ID"
        );

        let mut changes = ChangeBatch::new();
        changes.extend(self.since.iter().map(|t| (t.clone(), -1)));
        changes.extend(other.since.iter().map(|t| (t.clone(), -1)));

        // It's very important that we clear the since of other. Otherwise, it's
        // Drop impl would try and drop it again, by sending another ChangeBatch
        // on drop.
        let other_since = std::mem::take(&mut other.since);

        self.since.extend(other_since);

        // Record the new requirements, which we're guaranteed to be possible
        // because we're only retracing the two merged sinces together with this
        // in one go.
        changes.extend(self.since.iter().map(|t| (t.clone(), 1)));

        match (self.change_tx)(self.id, changes) {
            Ok(_) => (),
            Err(e) => {
                panic!("cannot merge ReadHold: {}", e);
            }
        }
    }

    /// Downgrades `self` to the given `frontier`. Returns `Err` when the new
    /// frontier is `less_than` the frontier at which this [ReadHold] is
    /// holding.
    pub fn try_downgrade(
        &mut self,
        frontier: Antichain<T>,
    ) -> Result<(), ReadHoldDowngradeError<T>> {
        if PartialOrder::less_than(&frontier, &self.since) {
            return Err(ReadHoldDowngradeError::SinceViolation {
                frontier,
                since: self.since.clone(),
            });
        }

        let mut changes = ChangeBatch::new();

        changes.extend(self.since.iter().map(|t| (t.clone(), -1)));
        changes.extend(frontier.iter().map(|t| (t.clone(), 1)));
        self.since = frontier;

        if !changes.is_empty() {
            // If the other side already hung up, that's ok.
            let _ = (self.change_tx)(self.id, changes);
        }

        Ok(())
    }

    /// Release this read hold.
    pub fn release(&mut self) {
        self.try_downgrade(Antichain::new())
            .expect("known to succeed");
    }
}

impl<T: TimelyTimestamp> Clone for ReadHold<T> {
    fn clone(&self) -> Self {
        if self.id.is_user() {
            tracing::trace!("cloning ReadHold on {}: {:?}", self.id, self.since);
        }

        // Let the other end know.
        let mut changes = ChangeBatch::new();

        changes.extend(self.since.iter().map(|t| (t.clone(), 1)));

        if !changes.is_empty() {
            // We do care about sending here. If the other end hung up we don't
            // really have a read hold anymore.
            match (self.change_tx)(self.id.clone(), changes) {
                Ok(_) => (),
                Err(e) => {
                    panic!("cannot clone ReadHold: {}", e);
                }
            }
        }

        Self {
            id: self.id.clone(),
            since: self.since.clone(),
            change_tx: Arc::clone(&self.change_tx),
        }
    }
}

impl<T: TimelyTimestamp> Drop for ReadHold<T> {
    fn drop(&mut self) {
        if self.id.is_user() {
            tracing::trace!("dropping ReadHold on {}: {:?}", self.id, self.since);
        }

        self.release();
    }
}
