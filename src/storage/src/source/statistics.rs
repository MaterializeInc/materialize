// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Helpers for managing storage statistics.

use std::cell::RefCell;
use std::rc::Rc;

use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;

use mz_repr::GlobalId;
use mz_storage_client::client::SourceStatisticsUpdate;

/// A helper struct designed to make it easy for operators to update metrics
#[derive(Clone)]
pub struct SourceStatistics {
    // We just use `SourceStatisticsUpdate` for convenience here!
    // The boolean its an initialization flag, to ensure we
    // don't report a false `snapshot_committed` value before
    // the `persist_sink` reports the current shard upper.
    // TODO(guswynn): this boolean is kindof gross, it should
    // probably be cleaned up.
    stats: Rc<RefCell<(bool, SourceStatisticsUpdate)>>,
}

impl SourceStatistics {
    pub fn new(id: GlobalId, index: usize) -> Self {
        Self {
            stats: Rc::new(RefCell::new((
                false,
                SourceStatisticsUpdate {
                    id,
                    worker_id: index,
                    snapshot_committed: false,
                    messages_received: 0,
                    updates_staged: 0,
                    updates_committed: 0,
                    bytes_received: 0,
                },
            ))),
        }
    }

    /// Return a snapshot of the stats data, if its been initialized.
    pub fn snapshot(&self) -> Option<SourceStatisticsUpdate> {
        let inner = self.stats.borrow();
        inner.0.then(|| inner.1.clone())
    }

    /// Set the `snapshot_committed` stat based on the reported upper, and
    /// mark the stats as initialized.
    // TODO(guswynn): Actually test that this initialization logic works.
    pub fn initialize_snapshot_committed<T: Timestamp>(&self, upper: &Antichain<T>) {
        self.update_snapshot_committed(upper);
        self.stats.borrow_mut().0 = true;
    }

    /// Set the `snapshot_committed` stat based on the reported upper.
    pub fn update_snapshot_committed<T: Timestamp>(&self, upper: &Antichain<T>) {
        let value = *upper != Antichain::from_elem(T::minimum());
        self.stats.borrow_mut().1.snapshot_committed = value
    }

    /// Increment the `messages_received` stat.
    pub fn inc_messages_received_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.1.messages_received = cur.1.messages_received + value;
    }

    /// Increment the `updates` stat.
    pub fn inc_updates_staged_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.1.updates_staged = cur.1.updates_staged + value;
    }

    /// Increment the `messages_committed` stat.
    pub fn inc_updates_committed_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.1.updates_committed = cur.1.updates_committed + value;
    }

    /// Increment the `bytes_received` stat.
    pub fn inc_bytes_received_by(&self, value: u64) {
        let mut cur = self.stats.borrow_mut();
        cur.1.bytes_received = cur.1.bytes_received + value;
    }
}
