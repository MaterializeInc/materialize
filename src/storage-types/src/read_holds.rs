// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Debug;

use mz_repr::GlobalId;
use thiserror::Error;
use timely::progress::{Antichain, ChangeBatch, Timestamp as TimelyTimestamp};
use timely::PartialOrder;
use tokio::sync::mpsc::UnboundedSender;

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
    holds_tx: UnboundedSender<(GlobalId, ChangeBatch<T>)>,
}

impl<T: TimelyTimestamp> Debug for ReadHold<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadHold")
            .field("id", &self.id)
            .field("since", &self.since)
            .finish_non_exhaustive()
    }
}

impl<T: TimelyTimestamp> ReadHold<T> {
    pub fn new(
        id: GlobalId,
        since: Antichain<T>,
        holds_tx: UnboundedSender<(GlobalId, ChangeBatch<T>)>,
    ) -> Self {
        Self {
            id,
            since,
            holds_tx,
        }
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
    /// Panics when trying to merge a [ReadHold] for a different collection
    /// (different [GlobalId]) or when trying to merge a [ReadHold] from a
    /// different issuer.
    pub fn merge_assign(&mut self, mut other: ReadHold<T>) {
        assert_eq!(
            self.id, other.id,
            "can only merge ReadHolds for the same ID"
        );
        assert!(
            self.holds_tx.same_channel(&other.holds_tx),
            "can only merge ReadHolds that come from the same issuer"
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

        match self.holds_tx.send((self.id.clone(), changes)) {
            Ok(_) => (),
            Err(e) => {
                panic!("cannot merge ReadHold: {}", e);
            }
        }
    }

    /// Downgrades `self` to the given `frontier`. Returns `Err` when the new
    /// frontier is `less_than` the frontier at which this [ReadHold] is
    /// holding.
    pub fn try_downgrade(&mut self, frontier: Antichain<T>) -> Result<(), anyhow::Error> {
        if PartialOrder::less_than(&frontier, &self.since) {
            return Err(anyhow::anyhow!(
                "new frontier {:?} is not beyond current since {:?}",
                frontier,
                self.since
            ));
        }

        let mut changes = ChangeBatch::new();

        changes.extend(self.since.iter().map(|t| (t.clone(), -1)));
        changes.extend(frontier.iter().map(|t| (t.clone(), 1)));
        self.since = frontier;

        if !changes.is_empty() {
            // If the other side already hung up, that's ok.
            let _ = self.holds_tx.send((self.id.clone(), changes));
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
            match self.holds_tx.send((self.id.clone(), changes)) {
                Ok(_) => (),
                Err(e) => {
                    panic!("cannot clone ReadHold: {}", e);
                }
            }
        }

        Self {
            id: self.id.clone(),
            since: self.since.clone(),
            holds_tx: self.holds_tx.clone(),
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

#[derive(Error, Debug)]
pub enum ReadHoldError {
    #[error("collection does not exist: {0}")]
    CollectionMissing(GlobalId),
    #[error("desired read hold frontier is not beyond the since of collection: {0}")]
    SinceViolation(GlobalId),
}
