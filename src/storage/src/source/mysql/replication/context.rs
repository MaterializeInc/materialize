// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::pin::Pin;

use mysql_async::BinlogStream;
use timely::dataflow::operators::CapabilitySet;
use timely::progress::Antichain;
use tracing::trace;

use mz_mysql_util::Config;
use mz_storage_types::sources::mysql::{GtidPartition, GtidState};

use crate::metrics::source::mysql::MySqlSourceMetrics;
use crate::source::RawSourceCreationConfig;
use crate::source::mysql::{DataOutputs, MySqlTableName, RewindRequest, SourceOutputInfo};

/// A container to hold various context information for the replication process, used when
/// processing events from the binlog stream.
pub(super) struct ReplContext<'a> {
    pub(super) config: &'a RawSourceCreationConfig,
    pub(super) connection_config: &'a Config,
    pub(super) stream: Pin<&'a mut futures::stream::Peekable<BinlogStream>>,
    pub(super) table_info: &'a BTreeMap<MySqlTableName, Vec<SourceOutputInfo>>,
    pub(super) metrics: &'a MySqlSourceMetrics,
    pub(super) data_outputs: &'a mut DataOutputs,
    pub(super) data_cap_sets: &'a mut [CapabilitySet<GtidPartition>],
    // Owned values:
    pub(super) rewinds: BTreeMap<usize, RewindRequest>,
    pub(super) errored_outputs: BTreeSet<usize>,
}

impl<'a> ReplContext<'a> {
    pub(super) fn new(
        config: &'a RawSourceCreationConfig,
        connection_config: &'a Config,
        stream: Pin<&'a mut futures::stream::Peekable<BinlogStream>>,
        table_info: &'a BTreeMap<MySqlTableName, Vec<SourceOutputInfo>>,
        metrics: &'a MySqlSourceMetrics,
        data_outputs: &'a mut DataOutputs,
        data_cap_sets: &'a mut [CapabilitySet<GtidPartition>],
        rewinds: BTreeMap<usize, RewindRequest>,
    ) -> Self {
        Self {
            config,
            connection_config,
            stream,
            table_info,
            metrics,
            data_outputs,
            data_cap_sets,
            rewinds,
            errored_outputs: BTreeSet::new(),
        }
    }

    /// Drops the rewind requests that `new_upper` has passed, then advances the frontier of the
    /// data capability sets to `new_upper` once no rewind request is pending.
    pub(super) fn downgrade_data_cap_sets(
        &mut self,
        reason: &str,
        new_upper: Antichain<GtidPartition>,
    ) {
        let (id, worker_id) = (self.config.id, self.config.worker_id);

        trace!(%id, "timely-{worker_id} [{reason}] advancing data frontier to {new_upper:?}");

        self.metrics.gtid_txids.set(
            new_upper
                .iter()
                .filter_map(|part| match part.timestamp() {
                    GtidState::Absent => None,
                    GtidState::Active(tx_id) => Some(tx_id.get()),
                })
                .sum(),
        );

        self.rewinds.retain(|_, req| {
            // We need to retain the rewind requests whose snapshot_upper
            // has at least one timestamp such that new_upper is less than
            // that timestamp
            let res = req.snapshot_upper.iter().any(|ts| new_upper.less_than(ts));
            if !res {
                trace!(%id, "timely-{worker_id} removing rewind request {req:?}");
            }
            res
        });

        // A pending rewind emits negated snapshot data at the minimum timestamp. Its port has to
        // keep the minimum capability until the rewind resolves. The other ports wait as well and
        // every export observes the same frontier.
        if self.rewinds.is_empty() {
            for cap_set in self.data_cap_sets.iter_mut() {
                cap_set.downgrade(&*new_upper);
            }
        }
    }
}
