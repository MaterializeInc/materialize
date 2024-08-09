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
use mz_storage_types::errors::DataflowError;
use timely::dataflow::operators::{Capability, CapabilitySet};
use timely::progress::Antichain;
use tracing::trace;

use mz_mysql_util::Config;
use mz_storage_types::sources::mysql::{GtidPartition, GtidState};

use crate::metrics::source::mysql::MySqlSourceMetrics;
use crate::source::mysql::{
    MySqlTableName, RewindRequest, SourceOutputInfo, StackedAsyncOutputHandle,
};
use crate::source::types::SourceMessage;
use crate::source::RawSourceCreationConfig;

/// A container to hold various context information for the replication process, used when
/// processing events from the binlog stream.
pub(super) struct ReplContext<'a> {
    pub(super) config: &'a RawSourceCreationConfig,
    pub(super) connection_config: &'a Config,
    pub(super) stream: Pin<&'a mut futures::stream::Peekable<BinlogStream>>,
    pub(super) table_info: &'a BTreeMap<MySqlTableName, Vec<SourceOutputInfo>>,
    pub(super) metrics: &'a MySqlSourceMetrics,
    pub(super) data_output: &'a mut StackedAsyncOutputHandle<
        GtidPartition,
        (usize, Result<SourceMessage, DataflowError>),
    >,
    pub(super) data_cap_set: &'a mut CapabilitySet<GtidPartition>,
    pub(super) upper_cap_set: &'a mut CapabilitySet<GtidPartition>,
    // Owned values:
    pub(super) rewinds: BTreeMap<usize, (Capability<GtidPartition>, RewindRequest)>,
    pub(super) errored_outputs: BTreeSet<usize>,
}

impl<'a> ReplContext<'a> {
    pub(super) fn new(
        config: &'a RawSourceCreationConfig,
        connection_config: &'a Config,
        stream: Pin<&'a mut futures::stream::Peekable<BinlogStream>>,
        table_info: &'a BTreeMap<MySqlTableName, Vec<SourceOutputInfo>>,
        metrics: &'a MySqlSourceMetrics,
        data_output: &'a mut StackedAsyncOutputHandle<
            GtidPartition,
            (usize, Result<SourceMessage, DataflowError>),
        >,
        data_cap_set: &'a mut CapabilitySet<GtidPartition>,
        upper_cap_set: &'a mut CapabilitySet<GtidPartition>,
        rewinds: BTreeMap<usize, (Capability<GtidPartition>, RewindRequest)>,
    ) -> Self {
        Self {
            config,
            connection_config,
            stream,
            table_info,
            metrics,
            data_output,
            data_cap_set,
            upper_cap_set,
            rewinds,
            errored_outputs: BTreeSet::new(),
        }
    }

    /// Advances the frontier of the data capability set to `new_upper`,
    /// and drops any existing rewind requests that are no longer applicable.
    pub(super) fn downgrade_data_cap_set(
        &mut self,
        reason: &str,
        new_upper: Antichain<GtidPartition>,
    ) {
        let (id, worker_id) = (self.config.id, self.config.worker_id);

        trace!(%id, "timely-{worker_id} [{reason}] advancing data frontier to {new_upper:?}");

        self.data_cap_set.downgrade(&*new_upper);

        self.metrics.gtid_txids.set(
            new_upper
                .iter()
                .filter_map(|part| match part.timestamp() {
                    GtidState::Absent => None,
                    GtidState::Active(tx_id) => Some(tx_id.get()),
                })
                .sum(),
        );

        self.rewinds.retain(|_, (_, req)| {
            // We need to retain the rewind requests whose snapshot_upper
            // has at least one timestamp such that new_upper is less than
            // that timestamp
            let res = req.snapshot_upper.iter().any(|ts| new_upper.less_than(ts));
            if !res {
                trace!(%id, "timely-{worker_id} removing rewind request {req:?}");
            }
            res
        });
    }

    /// Advances the frontier of the upper capability set to `new_upper`,
    pub(super) fn downgrade_progress_cap_set(
        &mut self,
        reason: &str,
        new_upper: Antichain<GtidPartition>,
    ) {
        let (id, worker_id) = (self.config.id, self.config.worker_id);
        trace!(%id, "timely-{worker_id} [{reason}] advancing progress frontier to {new_upper:?}");
        self.upper_cap_set.downgrade(&*new_upper);
    }
}
