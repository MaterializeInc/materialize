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
//! * `./<timeline>/read_ts -> <timestamp>`
//! * `./<timeline>/write_ts -> <timestamp>`

use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::anyhow;
use async_trait::async_trait;
use foundationdb::directory::{Directory, DirectoryLayer, DirectoryOutput};
use foundationdb::tuple::{
    PackError, PackResult, TupleDepth, TuplePack, TupleUnpack, VersionstampOffset, pack, unpack,
};
use foundationdb::{
    Database, FdbBindingError, FdbError, TransactError, TransactOption, Transaction,
};
use futures_util::future::FutureExt;
use mz_foundationdb::FdbConfig;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::url::SensitiveUrl;
use mz_repr::Timestamp;
use tracing::{debug, info};

use crate::metrics::Metrics;
use crate::{GenericNowFn, TimestampOracle, WriteTimestamp};

/// A [`TimestampOracle`] backed by FoundationDB.
pub struct FdbTimestampOracle<N> {
    timeline: String,
    next: N,
    db: Arc<Database>,
    /// A read-only timestamp oracle is NOT allowed to do operations that change
    /// the backing FoundationDB state.
    read_only: bool,
    /// read_ts key for this timeline
    read_ts_key: Vec<u8>,
    /// write_ts key for this timeline
    write_ts_key: Vec<u8>,
}

impl<N> std::fmt::Debug for FdbTimestampOracle<N>
where
    N: GenericNowFn<Timestamp> + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FdbTimestampOracle")
            .field("timeline", &self.timeline)
            .field("next", &self.next)
            .field("read_only", &self.read_only)
            .field("read_ts_key", &self.read_ts_key)
            .field("write_ts_key", &self.write_ts_key)
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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

impl<N: Sync> FdbTimestampOracle<N> {
    async fn open_inner(
        timeline: String,
        next: N,
        read_only: bool,
        db: Arc<Database>,
        prefix: Vec<String>,
        directory: DirectoryLayer,
    ) -> Result<FdbTimestampOracle<N>, anyhow::Error> {
        // Create a subspace for this timeline at <prefix>/<timeline>
        let timeline_path: Vec<_> = prefix
            .into_iter()
            .chain(std::iter::once(timeline.clone()))
            .collect();

        let timeline_subspace = db
            .run(async |trx, _maybe_committed| {
                Ok(directory
                    .create_or_open(&trx, &timeline_path, None, None)
                    .await)
            })
            .await?
            .map_err(|e| anyhow!("directory error: {e:?}"))?;

        let timeline_subspace = match timeline_subspace {
            DirectoryOutput::DirectorySubspace(subspace) => subspace,
            DirectoryOutput::DirectoryPartition(_partition) => {
                return Err(anyhow!("timestamp oracle timeline cannot be a partition"));
            }
        };

        let read_ts_key = timeline_subspace.pack(&"read_ts");
        let write_ts_key = timeline_subspace.pack(&"write_ts");

        Ok(Self {
            timeline,
            next,
            read_ts_key,
            write_ts_key,
            db,
            read_only,
        })
    }

    async fn max_ts(&self) -> Result<Option<PackableTimestamp>, anyhow::Error> {
        let max_ts = self
            .db
            .transact_boxed(
                &(),
                |trx, ()| self.max_rs_trx(trx).boxed(),
                TransactOption::default(),
            )
            .await?;
        Ok(max_ts)
    }

    async fn max_rs_trx(
        &self,
        trx: &Transaction,
    ) -> Result<Option<PackableTimestamp>, FdbTransactError> {
        let read_data = trx.get(&self.read_ts_key, false).await?;
        let write_data = trx.get(&self.write_ts_key, false).await?;

        let read_ts: Option<PackableTimestamp> =
            read_data.map(|data| unpack(&data).expect("must unpack"));

        let write_ts: Option<PackableTimestamp> =
            write_data.map(|data| unpack(&data).expect("must unpack"));

        let max_ts = std::cmp::max(read_ts, write_ts);

        Ok::<_, FdbTransactError>(max_ts)
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

        mz_foundationdb::init_network();

        let db = Arc::new(Database::new(None)?);
        let prefix = fdb_config.prefix;
        let directory = DirectoryLayer::default();

        let oracle = Self::open_inner(timeline, next, read_only, db, prefix, directory).await?;

        // Initialize the timestamps for this timeline if they don't exist.
        oracle.initialize(initially).await?;

        Ok(oracle)
    }

    /// Initialize the timestamps for this timeline if they don't exist.
    ///
    /// Both `read_ts` and `write_ts` are set to `initially` if they do not already exist.
    async fn initialize(&self, initially: Timestamp) -> Result<(), FdbBindingError> {
        // Initialize the timestamps for this timeline if they don't exist.
        // Keys are stored as <timeline_subspace>/read_ts and <timeline_subspace>/write_ts
        let initially_packed = pack(&PackableTimestamp(initially));

        self.db
            .run(async |trx, _maybe_committed| {
                // Check if read_ts exists, if not initialize it
                let existing_read = trx.get(&self.read_ts_key, false).await?;
                if existing_read.is_none() {
                    trx.set(&self.read_ts_key, &initially_packed);
                }

                // Check if write_ts exists, if not initialize it
                let existing_write = trx.get(&self.write_ts_key, false).await?;
                if existing_write.is_none() {
                    trx.set(&self.write_ts_key, &initially_packed);
                }

                Ok(())
            })
            .await?;

        // Forward timestamps to what we're given from outside.
        if !self.read_only {
            self.apply_write(initially).await;
        }

        Ok(())
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

        mz_foundationdb::init_network();

        let db = Arc::new(Database::new(None)?);
        let prefix = fdb_config.prefix;
        let directory = DirectoryLayer::default();

        // List all timeline directories under the prefix
        let timeline_names: Vec<String> = db
            .run(async |trx, _maybe_committed| Ok(directory.list(&trx, &prefix).await))
            .await?
            .map_err(|e| anyhow!("directory error: {e:?}"))?;

        let mut result = Vec::new();

        // For each timeline, read the max of read_ts and write_ts
        for timeline_name in timeline_names {
            let oracle = FdbTimestampOracle::<()>::open_inner(
                timeline_name.clone(),
                (),
                true,
                Arc::clone(&db),
                prefix.clone(),
                directory.clone(),
            )
            .await?;

            if let Some(ts) = oracle.max_ts().await? {
                result.push((timeline_name, ts.0));
            }
        }

        Ok(result)
    }

    async fn write_ts_trx(
        &self,
        trx: &Transaction,
        proposed_next_ts: Timestamp,
    ) -> Result<Timestamp, FdbTransactError> {
        // Get current write_ts
        let current = trx.get(&self.write_ts_key, false).await?;
        let current_ts: PackableTimestamp = match current {
            Some(data) => unpack(&data)?,
            None => {
                return Err(FdbTransactError::ExternalError(anyhow!(
                    "timeline not initialized"
                )));
            }
        };

        // Calculate new timestamp: GREATEST(write_ts+1, proposed_next_ts)
        let incremented = current_ts.0.step_forward();
        let new_ts = std::cmp::max(incremented, proposed_next_ts);

        // Update write_ts
        let new_ts_packed = pack(&PackableTimestamp(new_ts));
        trx.set(&self.write_ts_key, &new_ts_packed);

        Ok(new_ts)
    }

    async fn peek_write_ts_trx(
        &self,
        trx: &Transaction,
    ) -> Result<Option<PackableTimestamp>, FdbTransactError> {
        let data = trx.get(&self.write_ts_key, false).await?;
        Ok(data.map(|data| unpack(&data)).transpose()?)
    }

    async fn read_ts_trx(
        &self,
        trx: &Transaction,
    ) -> Result<Option<PackableTimestamp>, FdbTransactError> {
        let data = trx.get(&self.read_ts_key, false).await?;
        Ok(data.map(|data| unpack(&data)).transpose()?)
    }

    async fn apply_write_trx(
        &self,
        trx: &Transaction,
        write_ts: Timestamp,
    ) -> Result<(), FdbTransactError> {
        // Update read_ts = GREATEST(read_ts, write_ts)
        let current_read = trx.get(&self.read_ts_key, false).await?;
        let current_read_ts: PackableTimestamp = match current_read {
            Some(data) => unpack(&data)?,
            None => {
                return Err(FdbTransactError::ExternalError(anyhow!(
                    "timeline not initialized"
                )));
            }
        };

        if write_ts > current_read_ts.0 {
            let new_ts_packed = pack(&PackableTimestamp(write_ts));
            trx.set(&self.read_ts_key, &new_ts_packed);
        }

        // Update write_ts = GREATEST(write_ts, write_ts_param)
        let current_write = trx.get(&self.write_ts_key, false).await?;
        let current_write_ts: PackableTimestamp = match current_write {
            Some(data) => unpack(&data)?,
            None => {
                return Err(FdbTransactError::ExternalError(anyhow!(
                    "timeline not initialized"
                )));
            }
        };

        if write_ts > current_write_ts.0 {
            let new_ts_packed = pack(&PackableTimestamp(write_ts));
            trx.set(&self.write_ts_key, &new_ts_packed);
        }

        Ok::<_, FdbTransactError>(())
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

        let write_ts: Timestamp = self
            .db
            .transact_boxed(
                &proposed_next_ts,
                |trx, proposed_next_ts| self.write_ts_trx(trx, **proposed_next_ts).boxed(),
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
        let write_ts = self
            .db
            .transact_boxed(
                &(),
                |trx, ()| self.peek_write_ts_trx(trx).boxed(),
                TransactOption::default(),
            )
            .await
            .expect("peek_write_ts transaction failed")
            .expect("timeline not initialized")
            .0;

        debug!(
            timeline = ?self.timeline,
            write_ts = ?write_ts,
            "returning from peek_write_ts()"
        );

        write_ts
    }

    async fn read_ts(&self) -> Timestamp {
        let read_ts = self
            .db
            .transact_boxed(
                &(),
                |trx, ()| self.read_ts_trx(trx).boxed(),
                TransactOption::default(),
            )
            .await
            .expect("read_ts transaction failed")
            .expect("timeline not initialized")
            .0;

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

        self.db
            .transact_boxed(
                &write_ts,
                |trx, write_ts| self.apply_write_trx(trx, **write_ts).boxed(),
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
