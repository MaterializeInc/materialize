// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Passive wallclock lag history and histogram sampling.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, DurationRound, TimeDelta, Utc};
use mz_controller_types::ReplicaId;
use mz_controller_types::dyncfgs::WALLCLOCK_LAG_RECORDING_INTERVAL;
use mz_dyncfg::ConfigSet;
use mz_ore::soft_panic_or_log;
use mz_repr::adt::timestamp::CheckedTimestamp;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_storage_client::controller::{IntrospectionType, WallclockLag, WallclockLagHistogramPeriod};

type Labels = BTreeMap<&'static str, String>;
type HistogramKey = (WallclockLagHistogramPeriod, WallclockLag, Labels);

#[derive(Debug, Default)]
pub(super) struct NativeWallclockLag {
    last_recorded: Option<DateTime<Utc>>,
    maxima: BTreeMap<(GlobalId, ReplicaId), WallclockLag>,
    histograms: BTreeMap<GlobalId, BTreeMap<HistogramKey, Diff>>,
}

impl NativeWallclockLag {
    /// Sample the supplied current collections once per invocation. The caller
    /// owns readability, lag calculation, labels, and the sampling cadence.
    pub(super) fn update(
        &mut self,
        now_ms: u64,
        dyncfg: &ConfigSet,
        read_only: bool,
        replicas: impl IntoIterator<Item = ((GlobalId, ReplicaId), WallclockLag)>,
        collections: impl IntoIterator<Item = (GlobalId, WallclockLag, Labels)>,
        retained_collections: &BTreeSet<GlobalId>,
    ) -> Vec<(IntrospectionType, Vec<(Row, Diff)>)> {
        let now = mz_ore::now::to_datetime(now_ms);
        let last_recorded = self.last_recorded.get_or_insert(now);
        let period = WallclockLagHistogramPeriod::from_epoch_millis(now_ms, dyncfg);

        let mut current_replicas = BTreeSet::new();
        for (key, lag) in replicas {
            current_replicas.insert(key);
            let max = self.maxima.entry(key).or_insert(WallclockLag::MIN);
            *max = (*max).max(lag);
        }
        for (id, lag, labels) in collections {
            let bucket = lag.map_seconds(u64::next_power_of_two);
            let stash = self.histograms.entry(id).or_default();
            *stash.entry((period, bucket, labels)).or_default() += Diff::ONE;
        }
        // Replica state retires on disconnection, but collection histograms
        // retain measured samples until the writer itself retires.
        self.maxima.retain(|key, _| current_replicas.contains(key));
        self.histograms
            .retain(|id, _| retained_collections.contains(id));

        if read_only {
            return Vec::new();
        }

        // Flush after sampling when crossing an interval boundary, aligned with
        // storage. The first sample anchors the cadence at now, not the epoch.
        let duration_trunc = |datetime: DateTime<Utc>, interval| {
            let td = TimeDelta::from_std(interval).ok()?;
            datetime.duration_trunc(td).ok()
        };
        let interval = WALLCLOCK_LAG_RECORDING_INTERVAL.get(dyncfg);
        let now_trunc = duration_trunc(now, interval).unwrap_or_else(|| {
            soft_panic_or_log!("excessive wallclock lag recording interval: {interval:?}");
            let default = WALLCLOCK_LAG_RECORDING_INTERVAL.default();
            duration_trunc(now, *default).expect("default lag recording interval is valid")
        });
        if now_trunc <= *last_recorded {
            return Vec::new();
        }
        let now_ts: CheckedTimestamp<_> = now_trunc.try_into().expect("must fit");

        let mut updates = Vec::new();
        let mut history = Vec::new();
        for ((id, replica), max) in &mut self.maxima {
            let lag = std::mem::replace(max, WallclockLag::MIN);
            let row = Row::pack_slice(&[
                Datum::String(&id.to_string()),
                Datum::String(&replica.to_string()),
                lag.into_interval_datum(),
                Datum::TimestampTz(now_ts),
            ]);
            history.push((row, Diff::ONE));
        }
        if !history.is_empty() {
            updates.push((IntrospectionType::WallclockLagHistory, history));
        }

        let mut histogram = Vec::new();
        let mut row = Row::default();
        for (id, stash) in &mut self.histograms {
            for ((period, lag, labels), count) in std::mem::take(stash) {
                let mut packer = row.packer();
                packer.extend([
                    Datum::TimestampTz(period.start),
                    Datum::TimestampTz(period.end),
                    Datum::String(&id.to_string()),
                    lag.into_uint64_datum(),
                ]);
                packer.push_dict(labels.iter().map(|(k, v)| (*k, Datum::String(v))));
                histogram.push((row.clone(), count));
            }
        }
        if !histogram.is_empty() {
            updates.push((IntrospectionType::WallclockLagHistogram, histogram));
        }
        *last_recorded = now_trunc;
        updates
    }
}
