// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A timestamp oracle backed by FoundationDB for persistence/durability and where
//! all oracle operations are self-sufficiently linearized, without requiring
//! any external precautions/machinery.
//!
//! We store the timestamp data in a subspace at the configured path. Each timeline
//! maps to a subspace with the following structure:
//! * `./read_ts/<timeline> -> <timestamp>`
//! * `./write_ts/<timeline> -> <timestamp>`

use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use foundationdb::directory::{Directory, DirectoryLayer, DirectoryOutput, DirectorySubspace};
use foundationdb::tuple::{
    PackError, PackResult, TupleDepth, TuplePack, TupleUnpack, VersionstampOffset, pack, unpack,
};
use foundationdb::{Database, FdbBindingError, FdbError, TransactError, TransactOption};
use futures_util::future::FutureExt;
use mz_foundationdb::{FdbConfig, init_network};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::url::SensitiveUrl;
use mz_repr::Timestamp;
use tracing::{debug, info};

use crate::metrics::Metrics;
use crate::{GenericNowFn, TimestampOracle, WriteTimestamp};

/// A [`TimestampOracle`] backed by FoundationDB.
pub struct FdbTimestampOracle<N>
where
    N: GenericNowFn<Timestamp>,
{
    timeline: String,
    next: N,
    read_ts_subspace: DirectorySubspace,
    write_ts_subspace: DirectorySubspace,
    db: Database,
    /// A read-only timestamp oracle is NOT allowed to do operations that change
    /// the backing FoundationDB state.
    read_only: bool,
}

impl<N> std::fmt::Debug for FdbTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FdbTimestampOracle")
            .field("timeline", &self.timeline)
            .field("next", &self.next)
            .field("read_ts_subspace", &self.read_ts_subspace)
            .field("write_ts_subspace", &self.write_ts_subspace)
            .field("read_only", &self.read_only)
            .finish_non_exhaustive()
    }
}

/// Configuration to connect to a FoundationDB-backed implementation of
/// [`TimestampOracle`].
#[derive(Clone, Debug)]
pub struct FdbTimestampOracleConfig {
    url: SensitiveUrl,
    metrics: Arc<Metrics>,
}

impl FdbTimestampOracleConfig {
    /// Returns a new instance of [`FdbTimestampOracleConfig`].
    pub fn new(url: SensitiveUrl, metrics_registry: &MetricsRegistry) -> Self {
        let metrics = Arc::new(Metrics::new(metrics_registry));
        Self { url, metrics }
    }

    /// Returns a new [`FdbTimestampOracleConfig`] for use in unit tests.
    ///
    /// By default, fdb oracle tests are no-ops so that `cargo test` works
    /// on new environments without any configuration. To activate the tests for
    /// [`FdbTimestampOracle`] set the `FDB_TIMESTAMP_ORACLE_URL` environment variable
    /// with a valid connection URL.
    pub fn new_for_test() -> Self {
        Self {
            url: FromStr::from_str("foundationdb:?prefix=test/tsoracle").unwrap(),
            metrics: Arc::new(Metrics::new(&MetricsRegistry::new())),
        }
    }

    /// Returns the metrics associated with this config.
    pub(crate) fn metrics(&self) -> &Arc<Metrics> {
        &self.metrics
    }
}

/// An error that can occur during a FoundationDB transaction.
/// This is either a FoundationDB error or an external error.
enum FdbTransactError {
    FdbError(FdbError),
    ExternalError(anyhow::Error),
}

impl std::fmt::Debug for FdbTransactError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FdbTransactError::FdbError(e) => write!(f, "FdbError({})", e),
            FdbTransactError::ExternalError(e) => write!(f, "ExternalError({:?})", e),
        }
    }
}

impl From<FdbError> for FdbTransactError {
    fn from(value: FdbError) -> Self {
        Self::FdbError(value)
    }
}

impl From<anyhow::Error> for FdbTransactError {
    fn from(value: anyhow::Error) -> Self {
        Self::ExternalError(value)
    }
}

impl From<PackError> for FdbTransactError {
    fn from(value: PackError) -> Self {
        Self::ExternalError(anyhow::Error::new(value))
    }
}

impl From<FdbBindingError> for FdbTransactError {
    fn from(value: FdbBindingError) -> Self {
        Self::ExternalError(anyhow::Error::new(value))
    }
}

impl From<FdbTransactError> for anyhow::Error {
    fn from(value: FdbTransactError) -> Self {
        match value {
            FdbTransactError::FdbError(e) => anyhow::Error::new(e),
            FdbTransactError::ExternalError(e) => e,
        }
    }
}

impl TransactError for FdbTransactError {
    fn try_into_fdb_error(self) -> Result<FdbError, Self> {
        match self {
            Self::FdbError(e) => Ok(e),
            other => Err(other),
        }
    }
}

/// Wrapper to implement TuplePack/TupleUnpack for Timestamp.
struct PackableTimestamp(Timestamp);

impl TuplePack for PackableTimestamp {
    fn pack<W: Write>(
        &self,
        w: &mut W,
        tuple_depth: TupleDepth,
    ) -> std::io::Result<VersionstampOffset> {
        u64::from(self.0).pack(w, tuple_depth)
    }
}

impl<'de> TupleUnpack<'de> for PackableTimestamp {
    fn unpack(input: &'de [u8], tuple_depth: TupleDepth) -> PackResult<(&'de [u8], Self)> {
        u64::unpack(input, tuple_depth).map(|(rem, v)| (rem, PackableTimestamp(Timestamp::from(v))))
    }
}

impl<N> FdbTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug + 'static,
{
    /// Open a FoundationDB [`TimestampOracle`] instance with `config`, for the
    /// timeline named `timeline`. `next` generates new timestamps when invoked.
    /// Timestamps that are returned are made durable and will never retract.
    pub async fn open(
        config: FdbTimestampOracleConfig,
        timeline: String,
        initially: Timestamp,
        next: N,
        read_only: bool,
    ) -> Result<Self, anyhow::Error> {
        info!(config = ?config, "opening FdbTimestampOracle");

        let fdb_config = FdbConfig::parse(&config.url)?;

        let _ = init_network();

        let db = Database::new(None)?;
        let prefix = fdb_config.prefix;
        let directory = DirectoryLayer::default();

        // Create subspaces for read_ts and write_ts
        let read_ts_path: Vec<_> = prefix
            .iter()
            .cloned()
            .chain(std::iter::once("read_ts".to_owned()))
            .collect();

        let read_ts_subspace = db
            .run(async |trx, _maybe_committed| {
                Ok(directory
                    .create_or_open(&trx, &read_ts_path, None, None)
                    .await)
            })
            .await?
            .map_err(|e| anyhow!("directory error: {e:?}"))?;

        let read_ts_subspace = match read_ts_subspace {
            DirectoryOutput::DirectorySubspace(subspace) => subspace,
            DirectoryOutput::DirectoryPartition(_partition) => {
                return Err(anyhow!("timestamp oracle read_ts cannot be a partition"));
            }
        };

        let write_ts_path: Vec<_> = prefix
            .into_iter()
            .chain(std::iter::once("write_ts".to_owned()))
            .collect();

        let write_ts_subspace = db
            .run(async |trx, _maybe_committed| {
                Ok(directory
                    .create_or_open(&trx, &write_ts_path, None, None)
                    .await)
            })
            .await?
            .map_err(|e| anyhow!("directory error: {e:?}"))?;

        let write_ts_subspace = match write_ts_subspace {
            DirectoryOutput::DirectorySubspace(subspace) => subspace,
            DirectoryOutput::DirectoryPartition(_partition) => {
                return Err(anyhow!("timestamp oracle write_ts cannot be a partition"));
            }
        };

        let oracle = FdbTimestampOracle {
            timeline: timeline.clone(),
            next: next.clone(),
            read_ts_subspace,
            write_ts_subspace,
            db,
            read_only,
        };

        // Initialize the timestamps for this timeline if they don't exist.
        let read_ts_key = oracle.read_ts_subspace.subspace(&timeline);
        let write_ts_key = oracle.write_ts_subspace.subspace(&timeline);
        let initially_packed = pack(&PackableTimestamp(initially));

        oracle
            .db
            .run(async |trx, _maybe_committed| {
                // Check if read_ts exists, if not initialize it
                let existing_read = trx.get(read_ts_key.bytes(), false).await?;
                if existing_read.is_none() {
                    trx.set(read_ts_key.bytes(), &initially_packed);
                }

                // Check if write_ts exists, if not initialize it
                let existing_write = trx.get(write_ts_key.bytes(), false).await?;
                if existing_write.is_none() {
                    trx.set(write_ts_key.bytes(), &initially_packed);
                }

                Ok(())
            })
            .await?;

        // Forward timestamps to what we're given from outside.
        if !read_only {
            TimestampOracle::apply_write(&oracle, initially).await;
        }

        Ok(oracle)
    }

    /// Returns a `Vec` of all known timelines along with their current greatest
    /// timestamp (max of read_ts and write_ts).
    ///
    /// For use when initializing another [`TimestampOracle`] implementation
    /// from another oracle's state.
    pub async fn get_all_timelines(
        config: FdbTimestampOracleConfig,
    ) -> Result<Vec<(String, Timestamp)>, anyhow::Error> {
        let fdb_config = FdbConfig::parse(&config.url)?;

        let _ = init_network();

        let db = Database::new(None)?;
        let prefix = fdb_config.prefix;
        let directory = DirectoryLayer::default();

        // Try to open the subspaces - if they don't exist, return empty
        let read_ts_path: Vec<_> = prefix
            .iter()
            .cloned()
            .chain(std::iter::once("read_ts".to_owned()))
            .collect();

        let read_ts_subspace = match db
            .run(async |trx, _maybe_committed| Ok(directory.open(&trx, &read_ts_path, None).await))
            .await?
        {
            Ok(DirectoryOutput::DirectorySubspace(subspace)) => subspace,
            _ => return Ok(Vec::new()),
        };

        let write_ts_path: Vec<_> = prefix
            .into_iter()
            .chain(std::iter::once("write_ts".to_owned()))
            .collect();

        let write_ts_subspace = match db
            .run(async |trx, _maybe_committed| Ok(directory.open(&trx, &write_ts_path, None).await))
            .await?
        {
            Ok(DirectoryOutput::DirectorySubspace(subspace)) => subspace,
            _ => return Ok(Vec::new()),
        };

        // Scan all timelines and get max(read_ts, write_ts)
        let result: Vec<(String, Timestamp)> = db
            .transact_boxed(
                (&read_ts_subspace, &write_ts_subspace),
                |trx, (read_ts_subspace, write_ts_subspace)| {
                    async move {
                        let mut timelines = std::collections::BTreeMap::new();

                        // Scan read_ts
                        let read_range = foundationdb::RangeOption::from(read_ts_subspace.range());
                        let read_values = trx.get_range(&read_range, 1, false).await?;
                        for kv in read_values {
                            let timeline: String = read_ts_subspace
                                .unpack(kv.key())
                                .map_err(FdbBindingError::PackError)?;
                            let ts: PackableTimestamp = unpack(kv.value())?;
                            timelines.insert(timeline, ts.0);
                        }

                        // Scan write_ts and take max
                        let write_range =
                            foundationdb::RangeOption::from(write_ts_subspace.range());
                        let write_values = trx.get_range(&write_range, 1, false).await?;
                        for kv in write_values {
                            let timeline: String = write_ts_subspace
                                .unpack(kv.key())
                                .map_err(FdbBindingError::PackError)?;
                            let ts: PackableTimestamp = unpack(kv.value())?;
                            let existing = timelines
                                .get_mut(&timeline)
                                .expect("timeline write_ts missing");
                            if ts.0 > *existing {
                                *existing = ts.0;
                            }
                        }

                        Ok::<_, FdbTransactError>(timelines.into_iter().collect::<Vec<_>>())
                    }
                    .boxed()
                },
                TransactOption::default(),
            )
            .await?;

        Ok(result)
    }
}

#[async_trait]
impl<N> TimestampOracle<Timestamp> for FdbTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug + 'static,
{
    async fn write_ts(&self) -> WriteTimestamp<Timestamp> {
        if self.read_only {
            panic!("attempting write_ts in read-only mode");
        }

        let proposed_next_ts = self.next.now();
        let write_ts_key = self.write_ts_subspace.subspace(&self.timeline);

        let write_ts: Timestamp = self
            .db
            .transact_boxed(
                (&write_ts_key, proposed_next_ts),
                |trx, (write_ts_key, proposed_next_ts)| {
                    async move {
                        // Get current write_ts
                        let current = trx.get(write_ts_key.bytes(), false).await?;
                        let current_ts: Timestamp = match current {
                            Some(data) => {
                                let ts: PackableTimestamp = unpack(&data)?;
                                ts.0
                            }
                            None => {
                                return Err(FdbTransactError::ExternalError(anyhow!(
                                    "timeline not initialized"
                                )));
                            }
                        };

                        // Calculate new timestamp: GREATEST(write_ts+1, proposed_next_ts)
                        let incremented = current_ts.step_forward();
                        let new_ts = std::cmp::max(incremented, *proposed_next_ts);

                        // Update write_ts
                        let new_ts_packed = pack(&PackableTimestamp(new_ts));
                        trx.set(write_ts_key.bytes(), &new_ts_packed);

                        Ok::<_, FdbTransactError>(new_ts)
                    }
                    .boxed()
                },
                TransactOption::default(),
            )
            .await
            .expect("write_ts transaction failed");

        debug!(
            timeline = ?self.timeline,
            write_ts = ?write_ts,
            proposed_next_ts = ?proposed_next_ts,
            "returning from write_ts()"
        );

        let advance_to = write_ts.step_forward();

        WriteTimestamp {
            timestamp: write_ts,
            advance_to,
        }
    }

    async fn peek_write_ts(&self) -> Timestamp {
        let write_ts_key = self.write_ts_subspace.subspace(&self.timeline);

        let write_ts: Timestamp = self
            .db
            .transact_boxed(
                &write_ts_key,
                |trx, write_ts_key| {
                    async move {
                        let data = trx.get(write_ts_key.bytes(), false).await?;
                        match data {
                            Some(data) => {
                                let ts: PackableTimestamp = unpack(&data)?;
                                Ok::<_, FdbTransactError>(ts.0)
                            }
                            None => Err(FdbTransactError::ExternalError(anyhow!(
                                "timeline not initialized"
                            ))),
                        }
                    }
                    .boxed()
                },
                TransactOption::default(),
            )
            .await
            .expect("peek_write_ts transaction failed");

        debug!(
            timeline = ?self.timeline,
            write_ts = ?write_ts,
            "returning from peek_write_ts()"
        );

        write_ts
    }

    async fn read_ts(&self) -> Timestamp {
        let read_ts_key = self.read_ts_subspace.subspace(&self.timeline);

        let read_ts: Timestamp = self
            .db
            .transact_boxed(
                &read_ts_key,
                |trx, read_ts_key| {
                    async move {
                        let data = trx.get(read_ts_key.bytes(), false).await?;
                        match data {
                            Some(data) => {
                                let ts: PackableTimestamp = unpack(&data)?;
                                Ok::<_, FdbTransactError>(ts.0)
                            }
                            None => Err(FdbTransactError::ExternalError(anyhow!(
                                "timeline not initialized"
                            ))),
                        }
                    }
                    .boxed()
                },
                TransactOption::default(),
            )
            .await
            .expect("read_ts transaction failed");

        debug!(
            timeline = ?self.timeline,
            read_ts = ?read_ts,
            "returning from read_ts()"
        );

        read_ts
    }

    async fn apply_write(&self, write_ts: Timestamp) {
        if self.read_only {
            panic!("attempting apply_write in read-only mode");
        }

        let read_ts_key = self.read_ts_subspace.subspace(&self.timeline);
        let write_ts_key = self.write_ts_subspace.subspace(&self.timeline);

        self.db
            .transact_boxed(
                (&read_ts_key, &write_ts_key, write_ts),
                |trx, (read_ts_key, write_ts_key, write_ts)| {
                    async move {
                        // Update read_ts = GREATEST(read_ts, write_ts)
                        let current_read = trx.get(read_ts_key.bytes(), false).await?;
                        let current_read_ts: Timestamp = match current_read {
                            Some(data) => {
                                let ts: PackableTimestamp = unpack(&data)?;
                                ts.0
                            }
                            None => {
                                return Err(FdbTransactError::ExternalError(anyhow!(
                                    "timeline not initialized"
                                )));
                            }
                        };

                        if *write_ts > current_read_ts {
                            let new_ts_packed = pack(&PackableTimestamp(*write_ts));
                            trx.set(read_ts_key.bytes(), &new_ts_packed);
                        }

                        // Update write_ts = GREATEST(write_ts, write_ts_param)
                        let current_write = trx.get(write_ts_key.bytes(), false).await?;
                        let current_write_ts: Timestamp = match current_write {
                            Some(data) => {
                                let ts: PackableTimestamp = unpack(&data)?;
                                ts.0
                            }
                            None => {
                                return Err(FdbTransactError::ExternalError(anyhow!(
                                    "timeline not initialized"
                                )));
                            }
                        };

                        if *write_ts > current_write_ts {
                            let new_ts_packed = pack(&PackableTimestamp(*write_ts));
                            trx.set(write_ts_key.bytes(), &new_ts_packed);
                        }

                        Ok::<_, FdbTransactError>(())
                    }
                    .boxed()
                },
                TransactOption::default(),
            )
            .await
            .expect("apply_write transaction failed");

        debug!(
            timeline = ?self.timeline,
            write_ts = ?write_ts,
            "returning from apply_write()"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::TimestampOracle;
    use mz_ore::now::NowFn;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function
    async fn test_fdb_timestamp_oracle() -> Result<(), anyhow::Error> {
        let config = FdbTimestampOracleConfig::new_for_test();

        crate::tests::timestamp_oracle_impl_test(|timeline, now_fn: NowFn, initial_ts| {
            let config = config.clone();
            async move {
                let oracle = FdbTimestampOracle::open(config, timeline, initial_ts, now_fn, false)
                    .await
                    .expect("failed to open FdbTimestampOracle");

                let arced_oracle: Arc<dyn TimestampOracle<Timestamp> + Send + Sync> =
                    Arc::new(oracle);

                arced_oracle
            }
        })
        .await?;

        Ok(())
    }
}
