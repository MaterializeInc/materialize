// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use itertools::Itertools;

use mz_ore::cast::CastLossy;
use mz_ore::collections::HashMap;

use super::{UpsertKey, UpsertValue};
use crate::source::metrics::UpsertSharedMetrics;
use crate::statistics::{SourceStatisticsMetrics, StorageStatistics};
use mz_storage_client::client::SourceStatisticsUpdate;

/// A trait that defines the fundamental primitives required by a state-backing of
/// the `upsert` operator.
#[async_trait::async_trait(?Send)]
pub trait UpsertState {
    /// Insert or delete for all `puts` keys, prioritizing the last value for
    /// repeated keys.
    ///
    /// Returns the size of the `puts`.
    async fn multi_put<P>(&mut self, puts: P) -> Result<u64, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, Option<UpsertValue>)>;

    /// Get the `gets` keys, which must be unique, placing the results in `results_out`.
    ///
    /// Returns the size of the `gets`.
    ///
    /// Panics if `gets` and `results_out` are not the same length.
    async fn multi_get<'r, G, R>(&mut self, gets: G, results_out: R) -> Result<u64, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut Option<UpsertValue>>;
}

/// A `HashMap` with an extra scratch vector used to
pub struct InMemoryHashMap {
    state: HashMap<UpsertKey, UpsertValue>,
}

impl Default for InMemoryHashMap {
    fn default() -> Self {
        Self {
            state: HashMap::new(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl UpsertState for InMemoryHashMap {
    async fn multi_put<P>(&mut self, puts: P) -> Result<u64, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, Option<UpsertValue>)>,
    {
        let mut size = 0;
        for (key, value) in puts {
            match value {
                Some(value) => {
                    self.state.insert(key, value);
                }
                None => {
                    self.state.remove(&key);
                }
            }
            size += 1;
        }
        Ok(size)
    }

    async fn multi_get<'r, G, R>(&mut self, gets: G, results_out: R) -> Result<u64, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut Option<UpsertValue>>,
    {
        let mut size = 0;
        for (key, result_out) in gets.into_iter().zip_eq(results_out) {
            *result_out = self.state.get(&key).cloned();
            size += 1;
        }
        Ok(size)
    }
}
#[derive(Clone, Debug, Hash, PartialEq, Eq)]
pub struct Stats {
    count: u64,
    diff_records: i64,
    diff_bytes: i64,
}

/// An `UpsertState` wrapper that reports basic metrics about the usage of the `UpsertState`.
pub struct StatsState<S> {
    inner: S,
    metrics: Arc<UpsertSharedMetrics>,
    source_metrics: StorageStatistics<SourceStatisticsUpdate, SourceStatisticsMetrics>,
}

impl<S> StatsState<S> {
    pub(crate) fn new(
        inner: S,
        metrics: Arc<UpsertSharedMetrics>,
        source_metrics: StorageStatistics<SourceStatisticsUpdate, SourceStatisticsMetrics>,
    ) -> Self {
        Self {
            inner,
            metrics,
            source_metrics,
        }
    }
}

impl<S> StatsState<S>
where
    S: UpsertState,
{
    pub(crate) async fn multi_put<'r, P>(&mut self, puts: P) -> Result<u64, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, Option<UpsertValue>)>,
    {
        let now = Instant::now();

        let size = self.inner.multi_put(puts).await?;

        self.metrics
            .multi_put_latency
            .observe(now.elapsed().as_secs_f64());
        self.metrics.multi_put_size.observe(f64::cast_lossy(size));

        Ok(size)
    }

    pub(crate) async fn multi_get<'r, G, R>(
        &mut self,
        gets: G,
        results_out: R,
    ) -> Result<u64, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut Option<UpsertValue>>,
    {
        let now = Instant::now();
        let size = self.inner.multi_get(gets, results_out).await?;

        self.metrics
            .multi_get_latency
            .observe(now.elapsed().as_secs_f64());
        self.metrics.multi_get_size.observe(f64::cast_lossy(size));

        Ok(size)
    }
}
