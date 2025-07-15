// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Replicate a table from SQL Server using their Change-Data-Capture (CDC) primitives.
//!
//! This module provides a [`CdcStream`] type that provides the following API for
//! replicating a table:
//!
//! 1. [`CdcStream::snapshot`] returns an initial snapshot of a table and the [`Lsn`] at
//!    which the snapshot was taken.
//! 2. [`CdcStream::into_stream`] returns a [`futures::Stream`] of [`CdcEvent`]s
//!    optionally from the [`Lsn`] returned in step 1.
//!
//! The snapshot process is responsible for identifying an [`Lsn`] that corresponds to
//! a point-in-time view of the data for the table(s) being copied. Similarly to
//! MySQL, Microsoft SQL server, as far as we know, does not provide an API to
//! achieve this.
//!
//! SQL Server `SNAPSHOT` isolation provides guarantees that a reader will only
//! see writes committed before the transaction began.  More specficially, this
//! snapshot is implemented using versions that are visibile based on the
//! transaction sequence number (`XSN`). The `XSN` is set at the first
//! read or write, not at `BEGIN TRANSACTION`, see [here](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-locking-and-row-versioning-guide?view=sql-server-ver17).
//! This provides us a suitable starting point for capturing the table data.
//! To force an `XSN` to be assigned, experiments have shown that a table must
//! be read. We choose a well-known table that we should already have access to,
//! [cdc.change_tables](https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/cdc-change-tables-transact-sql?view=sql-server-ver17),
//! and read a single value from it.
//!
//! Due to the asynchronous nature of CDC, we can assume that the [`Lsn`]
//! returned from any CDC tables or CDC functions will always be stale,
//! in relation to the source table that CDC is tracking. The system table
//! [sys.dm_tran_database_transactions](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-tran-database-transactions-transact-sql?view=sql-server-ver17)
//! will contain an [`Lsn`] for any transaction that performs a write operation.
//! Creating a savepoint using [SAVE TRANSACTION](https://learn.microsoft.com/en-us/sql/t-sql/language-elements/save-transaction-transact-sql?view=sql-server-ver17)
//! is sufficient to generate an [`Lsn`] in this case.
//!
//! To ensure that the the point-in-time view is established atomically with
//! collection of the [`Lsn`], we lock the tables to prevent writes from being
//! interleaved between the 2 commands (read to establish `XSN` and creation of
//! the savepoint).
//!
//! SQL server supports table locks, but those will only be released
//! once the outermost transaction completes. For this reason, this module
//! uses two connections for the snapshot process. The first connection is used
//! to initiate a transaction and lock the upstream tables under
//! [`TransactionIsolationLevel::ReadCommitted`] isolation. While the first
//! connection maintains the locks, the second connection starts a
//! transaction with [`TransactionIsolationLevel::Snapshot`] isolation and
//! creates a savepoint. Once the savepoint is created, SQL server has assigned
//! an [`Lsn`] and the the first connection rolls back the transaction.
//! The [`Lsn`] and snapshot are captured by the second connection within the
//! existing transaction.
//!
//! After completing the snapshot we use [`crate::inspect::get_changes_asc`] which will return
//! all changes between a `[lower, upper)` bound of [`Lsn`]s.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use futures::{Stream, StreamExt};
use mz_repr::GlobalId;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};
use tiberius::numeric::Numeric;

use crate::{Client, SqlServerError, TransactionIsolationLevel};

/// A stream of changes from a table in SQL Server that has CDC enabled.
///
/// SQL Server does not have an API to push or notify consumers of changes, so we periodically
/// poll the upstream source.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/change-data-capture-tables-transact-sql?view=sql-server-ver16>
pub struct CdcStream<'a> {
    /// Client we use for querying SQL Server.
    client: &'a mut Client,
    /// Upstream capture instances we'll list changes from.
    capture_instances: BTreeMap<Arc<str>, Option<Lsn>>,
    /// How often we poll the upstream for changes.
    poll_interval: Duration,
    /// How long we'll wait for SQL Server to return a max LSN before taking a snapshot.
    ///
    /// Note: When CDC is first enabled in an instance of SQL Server it can take a moment
    /// for it to "completely" startup. Before starting a `TRANSACTION` for our snapshot
    /// we'll wait this duration for SQL Server to report an [`Lsn`] and thus indicate CDC is
    /// ready to go.
    max_lsn_wait: Duration,
}

impl<'a> CdcStream<'a> {
    pub(crate) fn new(
        client: &'a mut Client,
        capture_instances: BTreeMap<Arc<str>, Option<Lsn>>,
    ) -> Self {
        CdcStream {
            client,
            capture_instances,
            poll_interval: Duration::from_secs(1),
            max_lsn_wait: Duration::from_secs(10),
        }
    }

    /// Set the [`Lsn`] that we should start streaming changes from.
    ///
    /// If the provided [`Lsn`] is not available, the stream will return an error
    /// when first polled.
    pub fn start_lsn(mut self, capture_instance: &str, lsn: Lsn) -> Self {
        let start_lsn = self
            .capture_instances
            .get_mut(capture_instance)
            .expect("capture instance does not exist");
        *start_lsn = Some(lsn);
        self
    }

    /// The cadence at which we'll poll the upstream SQL Server database for changes.
    ///
    /// Default is 1 second.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// The max duration we'll wait for SQL Server to return an [`Lsn`] before taking a
    /// snapshot.
    ///
    /// When CDC is first enabled in SQL Server it can take a moment before it is fully
    /// setup and starts reporting LSNs.
    ///
    /// Default is 10 seconds.
    pub fn max_lsn_wait(mut self, wait: Duration) -> Self {
        self.max_lsn_wait = wait;
        self
    }

    /// Takes a snapshot of the upstream table that the specified `capture_instance` is
    /// replicating changes from.
    ///
    /// An optional `instances` parameter can be provided to only snapshot the specified instances.
    pub async fn snapshot<'b>(
        &'b mut self,
        instances: Option<BTreeSet<Arc<str>>>,
        worker_id: usize,
        source_id: GlobalId,
    ) -> Result<
        (
            Lsn,
            BTreeMap<Arc<str>, usize>,
            impl Stream<Item = (Arc<str>, Result<tiberius::Row, SqlServerError>)> + use<'b, 'a>,
        ),
        SqlServerError,
    > {
        static SAVEPOINT_NAME: &str = "_mz_snap_";

        // Determine what table we need to snapshot.
        let instances = self
            .capture_instances
            .keys()
            .filter(|i| match instances.as_ref() {
                // Only snapshot the instance if the filter includes it.
                Some(filter) => filter.contains(i.as_ref()),
                None => true,
            })
            .map(|i| i.as_ref());
        let tables =
            crate::inspect::get_tables_for_capture_instance(self.client, instances).await?;
        tracing::info!(%source_id, ?tables, "timely-{worker_id} got table for capture instance");

        // Before starting a transaction where the LSN will not advance, ensure
        // the upstream DB is ready for CDC.
        self.wait_for_ready().await?;

        // Intentionally logging this at info for debugging. This section won't get entered
        // often, but if there are problems here, it will be much easier to troubleshoot
        // knowing where stall/hang might be happening.
        tracing::info!(%source_id, "timely-{worker_id} upstream is ready");

        // The client that will be used for fencing does not need any special isolation level
        // as it will be just be locking the table(s).
        let mut fencing_client = self.client.new_connection().await?;
        let mut fence_txn = fencing_client.transaction().await?;

        // TODO improve table locking: https://github.com/MaterializeInc/database-issues/issues/9512
        for (_capture_instance, schema, table) in &tables {
            tracing::trace!(%source_id, %schema, %table, "timely-{worker_id} locking table");
            fence_txn.lock_table_shared(&*schema, &*table).await?;
        }

        // So we know that we locked that tables and roughly how long that took based on the time diff
        // from the last message.
        tracing::info!(%source_id, "timely-{worker_id} locked tables");

        self.client
            .set_transaction_isolation(TransactionIsolationLevel::Snapshot)
            .await?;
        let mut txn = self.client.transaction().await?;

        // Creating a savepoint forces a write to the transaction log, which will
        // assign an LSN, but it does not force a transaction sequence number to be
        // assigned as far as I can tell.  I have not observed any entries added to
        // `sys.dm_tran_active_snapshot_database_transactions` when creating a savepoint
        // or when reading system views to retrieve the LSN.
        //
        // We choose cdc.change_tables because it is a system table that will exist
        // when CDC is enabled, it has a well known schema, and as a CDC client,
        // we should be able to read from it already.
        let res = txn
            .simple_query("SELECT TOP 1 object_id FROM cdc.change_tables")
            .await?;
        if res.len() != 1 {
            Err(SqlServerError::InvariantViolated(
                "No objects found in cdc.change_tables".into(),
            ))?
        }

        // Because the tables are locked, any write operation has either
        // completed, or is blocked. The LSN and XSN acquired now will represent a
        // consistent point-in-time view, such that any committed write will be
        // visible to this snapshot and the LSN of such a write will be less than
        // or equal to the LSN captured here. Creating the savepoint sets the LSN,
        // we can read it after rolling back the locks.
        txn.create_savepoint(SAVEPOINT_NAME).await?;
        tracing::info!(%source_id, %SAVEPOINT_NAME, "timely-{worker_id} created savepoint");

        // Once the XSN is esablished and the LSN captured, the tables no longer
        // need to be locked.  Any writes that happen to the upstream tables
        // will have an LSN higher than our captured LSN, and will be read from CDC.
        fence_txn.rollback().await?;

        let lsn = txn.get_lsn().await?;

        tracing::info!(%source_id, ?lsn, "timely-{worker_id} starting snapshot");

        // Get the size of each table we're about to snapshot.
        //
        // TODO(sql_server3): To expose a more "generic" interface it would be nice to
        // make it configurable about whether or not we take a count first.
        let mut snapshot_stats = BTreeMap::default();
        for (capture_instance, schema, table) in &tables {
            tracing::trace!(%capture_instance, %schema, %table, "snapshot stats start");
            let size = crate::inspect::snapshot_size(txn.client, &*schema, &*table).await?;
            snapshot_stats.insert(Arc::clone(capture_instance), size);
            tracing::trace!(%source_id, %capture_instance, %schema, %table, "timely-{worker_id} snapshot stats end");
        }

        // Run a `SELECT` query to snapshot the entire table.
        let stream = async_stream::stream! {
            // TODO(sql_server3): A stream of streams would be better here than
            // returning the name with each result, but the lifetimes are tricky.
            for (capture_instance, schema_name, table_name) in tables {
                tracing::trace!(%source_id, %capture_instance, %schema_name, %table_name, "timely-{worker_id} snapshot start");

                let snapshot = crate::inspect::snapshot(txn.client, &*schema_name, &*table_name);
                let mut snapshot = std::pin::pin!(snapshot);
                while let Some(result) = snapshot.next().await {
                    yield (Arc::clone(&capture_instance), result);
                }

                tracing::trace!(%source_id, %capture_instance, %schema_name, %table_name, "timely-{worker_id} snapshot end");
            }

            // Slightly awkward, but if the rollback fails we need to conform to
            // type of the stream.
            if let Err(e) = txn.rollback().await {
                yield ("rollback".into(), Err(e));
            }
        };

        Ok((lsn, snapshot_stats, stream))
    }

    /// Consume `self` returning a [`Stream`] of [`CdcEvent`]s.
    pub fn into_stream(mut self) -> impl Stream<Item = Result<CdcEvent, SqlServerError>> + use<'a> {
        async_stream::try_stream! {
            // Initialize all of our start LSNs.
            self.initialize_start_lsns().await?;

            // When starting the stream we'll emit one progress event if we've already observed
            // everything the DB currently has.
            if let Some(starting_lsn) = self.capture_instances.values().filter_map(|x| *x).min() {
                let db_curr_lsn = crate::inspect::get_max_lsn(self.client).await?;
                let next_lsn = db_curr_lsn.increment();
                if starting_lsn >= db_curr_lsn {
                    tracing::debug!(
                        %starting_lsn,
                        %db_curr_lsn,
                        %next_lsn,
                        "yielding initial progress",
                    );
                    yield CdcEvent::Progress { next_lsn };
                }
            }

            loop {
                // Measure the tick before we do any operation so the time it takes
                // to query SQL Server is included in the time that we wait.
                let next_tick = tokio::time::Instant::now()
                    .checked_add(self.poll_interval)
                    .expect("tick interval overflowed!");

                // We always check for changes based on the "global" minimum LSN of any
                // one capture instance.
                let maybe_curr_lsn = self.capture_instances.values().filter_map(|x| *x).min();
                let Some(curr_lsn) = maybe_curr_lsn else {
                    tracing::warn!("shutting down CDC stream because nothing to replicate");
                    break;
                };

                // Get the max LSN for the DB.
                let db_max_lsn = crate::inspect::get_max_lsn(self.client).await?;
                tracing::debug!(?db_max_lsn, ?curr_lsn, "got max LSN");

                // If the LSN of the DB has increased then get all of our changes.
                if db_max_lsn > curr_lsn {
                    for (instance, instance_lsn) in &self.capture_instances {
                        let Some(instance_lsn) = instance_lsn.as_ref() else {
                            tracing::error!(?instance, "found uninitialized LSN!");
                            continue;
                        };

                        // We've already replicated everything up-to db_max_lsn, so
                        // nothing to do.
                        if db_max_lsn < *instance_lsn {
                            continue;
                        }

                        // Get a stream of all the changes for the current instance.
                        let changes = crate::inspect::get_changes_asc(
                            self.client,
                            &*instance,
                            *instance_lsn,
                            db_max_lsn,
                            RowFilterOption::AllUpdateOld,
                        )
                        // TODO(sql_server3): Make this chunk size configurable.
                        .ready_chunks(64);
                        let mut changes = std::pin::pin!(changes);

                        // Map and stream all the rows to our listener.
                        while let Some(chunk) = changes.next().await {
                            // Group events by LSN.
                            //
                            // TODO(sql_server3): Can we maybe re-use this BTreeMap or these Vec
                            // allocations? Something to be careful of is shrinking the allocations
                            // if/when they grow to large, e.g. from a large spike of changes.
                            // Alternatively we could also use a single Vec here since we know the
                            // changes are ordered by LSN.
                            let mut events: BTreeMap<Lsn, Vec<Operation>> = BTreeMap::default();
                            for change in chunk {
                                let (lsn, operation) = change.and_then(Operation::try_parse)?;
                                events.entry(lsn).or_default().push(operation);
                            }

                            // Emit the groups of events.
                            for (lsn, changes) in events {
                                yield CdcEvent::Data {
                                    capture_instance: Arc::clone(instance),
                                    lsn,
                                    changes,
                                };
                            }
                        }
                    }

                    // Increment our LSN (`get_changes` is inclusive).
                    //
                    // TODO(sql_server2): We should occassionally check to see how close the LSN we
                    // generate is to the LSN returned from incrementing via SQL Server itself.
                    let next_lsn = db_max_lsn.increment();
                    tracing::debug!(?curr_lsn, ?next_lsn, "incrementing LSN");

                    // Notify our listener that we've emitted all changes __less than__ this LSN.
                    //
                    // Note: This aligns well with timely's semantics of progress tracking.
                    yield CdcEvent::Progress { next_lsn };

                    // We just listed everything upto next_lsn.
                    for instance_lsn in self.capture_instances.values_mut() {
                        let instance_lsn = instance_lsn.as_mut().expect("should be initialized");
                        // Ensure LSNs don't go backwards.
                        *instance_lsn = std::cmp::max(*instance_lsn, next_lsn);
                    }
                }

                tokio::time::sleep_until(next_tick).await;
            }
        }
    }

    /// Determine the [`Lsn`] to start streaming changes from.
    async fn initialize_start_lsns(&mut self) -> Result<(), SqlServerError> {
        // First, initialize all start LSNs. If a capture instance didn't have
        // one specified then we'll start from the current max.
        let max_lsn = crate::inspect::get_max_lsn(self.client).await?;
        for (_instance, requsted_lsn) in self.capture_instances.iter_mut() {
            if requsted_lsn.is_none() {
                requsted_lsn.replace(max_lsn);
            }
        }

        // For each instance, ensure their requested LSN is available.
        for (instance, requested_lsn) in self.capture_instances.iter() {
            let requested_lsn = requested_lsn
                .as_ref()
                .expect("initialized all values above");

            // Get the minimum Lsn available for this instance.
            let available_lsn = crate::inspect::get_min_lsn(self.client, &*instance).await?;

            // If we cannot start at our desired LSN, we must return an error!.
            if *requested_lsn < available_lsn {
                return Err(CdcError::LsnNotAvailable {
                    capture_instance: Arc::clone(instance),
                    requested: *requested_lsn,
                    minimum: available_lsn,
                }
                .into());
            }
        }

        Ok(())
    }

    /// If CDC was recently enabled on an instance of SQL Server then it will report
    /// `NULL` for the minimum LSN of a capture instance and/or the maximum LSN of the
    /// entire database.
    ///
    /// This method runs a retry loop that waits for the upstream DB to report good
    /// values. It should be called before taking the initial [`CdcStream::snapshot`]
    /// to ensure the system is ready to proceed with CDC.
    async fn wait_for_ready(&mut self) -> Result<(), SqlServerError> {
        // Ensure all of the capture instances are reporting an LSN.
        for instance in self.capture_instances.keys() {
            crate::inspect::get_min_lsn_retry(self.client, instance, self.max_lsn_wait).await?;
        }

        // Ensure the database is reporting a max LSN.
        crate::inspect::get_max_lsn_retry(self.client, self.max_lsn_wait).await?;

        Ok(())
    }
}

/// A change event from a [`CdcStream`].
#[derive(Derivative)]
#[derivative(Debug)]
pub enum CdcEvent {
    /// Changes have occurred upstream.
    Data {
        /// The capture instance these changes are for.
        capture_instance: Arc<str>,
        /// The LSN that this change occurred at.
        lsn: Lsn,
        /// The change itself.
        changes: Vec<Operation>,
    },
    /// We've made progress and observed all the changes less than `next_lsn`.
    Progress {
        /// We've received all of the data for [`Lsn`]s __less than__ this one.
        next_lsn: Lsn,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum CdcError {
    #[error(
        "the requested LSN '{requested:?}' is less then the minimum '{minimum:?}' for `{capture_instance}'"
    )]
    LsnNotAvailable {
        capture_instance: Arc<str>,
        requested: Lsn,
        minimum: Lsn,
    },
    #[error("failed to get the required column '{column_name}': {error}")]
    RequiredColumn {
        column_name: &'static str,
        error: String,
    },
    #[error("failed to cleanup values for '{capture_instance}' at {low_water_mark}")]
    CleanupFailed {
        capture_instance: String,
        low_water_mark: Lsn,
    },
}

/// This type is used to represent the progress of each SQL Server instance in
/// the ingestion dataflow.
///
/// A SQL Server LSN is a three part "number" that provides a __total order__
/// to all transations within a database. Interally we don't really care what
/// these parts mean, but they are:
///
/// 1. A Virtual Log File (VLF) sequence number, bytes [0, 4)
/// 2. Log block number, bytes [4, 8)
/// 3. Log record number, bytes [8, 10)
///
/// For more info on log sequence numbers in SQL Server see:
/// <https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-log-architecture-and-management-guide?view=sql-server-ver16#Logical_Arch>
///
/// Note: The derived impl of [`PartialOrd`] and [`Ord`] relies on the field
/// ordering so do not change it.
#[derive(
    Default,
    Copy,
    Clone,
    Debug,
    Eq,
    PartialEq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Arbitrary,
)]
pub struct Lsn {
    /// Virtual Log File sequence number.
    vlf_id: u32,
    /// Log block number.
    block_id: u32,
    /// Log record number.
    record_id: u16,
}

impl Lsn {
    const SIZE: usize = 10;

    /// Interpret the provided bytes as an [`Lsn`].
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != Self::SIZE {
            return Err(format!("incorrect length, expected 10 got {}", bytes.len()));
        }

        let vlf_id: [u8; 4] = bytes[0..4].try_into().expect("known good length");
        let block_id: [u8; 4] = bytes[4..8].try_into().expect("known good length");
        let record_id: [u8; 2] = bytes[8..].try_into().expect("known good length");

        Ok(Lsn {
            vlf_id: u32::from_be_bytes(vlf_id),
            block_id: u32::from_be_bytes(block_id),
            record_id: u16::from_be_bytes(record_id),
        })
    }

    /// Return the underlying byte slice for this [`Lsn`].
    pub fn as_bytes(&self) -> [u8; 10] {
        let mut raw: [u8; Self::SIZE] = [0; 10];

        raw[0..4].copy_from_slice(&self.vlf_id.to_be_bytes());
        raw[4..8].copy_from_slice(&self.block_id.to_be_bytes());
        raw[8..].copy_from_slice(&self.record_id.to_be_bytes());

        raw
    }

    /// Increment this [`Lsn`].
    ///
    /// The returned [`Lsn`] may not exist upstream yet, but it's guaranteed to
    /// sort greater than `self`.
    pub fn increment(self) -> Lsn {
        let (record_id, carry) = self.record_id.overflowing_add(1);
        let (block_id, carry) = self.block_id.overflowing_add(carry.into());
        let (vlf_id, overflow) = self.vlf_id.overflowing_add(carry.into());
        assert!(!overflow, "overflowed Lsn, {self:?}");

        Lsn {
            vlf_id,
            block_id,
            record_id,
        }
    }

    /// Drops the `record_id` portion of the [`Lsn`] so we can fit an "abbreviation"
    /// of this [`Lsn`] into a [`u64`], without losing the total order.
    pub fn abbreviate(&self) -> u64 {
        let mut abbreviated: u64 = 0;

        #[allow(clippy::as_conversions)]
        {
            abbreviated += (self.vlf_id as u64) << 32;
            abbreviated += self.block_id as u64;
        }

        abbreviated
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}:{}", self.vlf_id, self.block_id, self.record_id)
    }
}

impl TryFrom<&[u8]> for Lsn {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Lsn::try_from_bytes(value)
    }
}

impl TryFrom<Numeric> for Lsn {
    type Error = String;

    fn try_from(value: Numeric) -> Result<Self, Self::Error> {
        if value.dec_part() != 0 {
            return Err(format!(
                "LSN expect Numeric(25,0), but found decimal portion {}",
                value.dec_part()
            ));
        }
        let mut decimal_lsn = value.int_part();
        // LSN is composed of 4 bytes : 4 bytes : 2 bytes
        // and MS provided the method to decode that here
        // https://github.com/microsoft/sql-server-samples/blob/master/samples/features/ssms-templates/Sql/Change%20Data%20Capture/Enumeration/Create%20Function%20fn_convertnumericlsntobinary.sql

        let vlf_id = u32::try_from(decimal_lsn / 10_i128.pow(15))
            .map_err(|e| format!("Failed to decode vlf_id for lsn {decimal_lsn}: {e:?}"))?;
        decimal_lsn -= i128::from(vlf_id) * 10_i128.pow(15);

        let block_id = u32::try_from(decimal_lsn / 10_i128.pow(5))
            .map_err(|e| format!("Failed to decode block_id for lsn {decimal_lsn}: {e:?}"))?;
        decimal_lsn -= i128::from(block_id) * 10_i128.pow(5);

        let record_id = u16::try_from(decimal_lsn)
            .map_err(|e| format!("Failed to decode record_id for lsn {decimal_lsn}: {e:?}"))?;

        Ok(Lsn {
            vlf_id,
            block_id,
            record_id,
        })
    }
}

impl columnation::Columnation for Lsn {
    type InnerRegion = columnation::CopyRegion<Lsn>;
}

impl timely::progress::Timestamp for Lsn {
    // No need to describe complex summaries.
    type Summary = ();

    fn minimum() -> Self {
        Lsn::default()
    }
}

impl timely::progress::PathSummary<Lsn> for () {
    fn results_in(&self, src: &Lsn) -> Option<Lsn> {
        Some(*src)
    }

    fn followed_by(&self, _other: &Self) -> Option<Self> {
        Some(())
    }
}

impl timely::progress::timestamp::Refines<()> for Lsn {
    fn to_inner(_other: ()) -> Self {
        use timely::progress::Timestamp;
        Self::minimum()
    }
    fn to_outer(self) -> () {}

    fn summarize(_path: <Self as timely::progress::Timestamp>::Summary) -> () {}
}

impl timely::order::PartialOrder for Lsn {
    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }

    fn less_than(&self, other: &Self) -> bool {
        self < other
    }
}
impl timely::order::TotalOrder for Lsn {}

/// Structured format of an [`Lsn`].
///
/// Note: The derived impl of [`PartialOrd`] and [`Ord`] relies on the field
/// ordering so do not change it.
#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct StructuredLsn {
    vlf_id: u32,
    block_id: u32,
    record_id: u16,
}

/// When querying CDC functions like `cdc.fn_cdc_get_all_changes_<capture_instance>` this governs
/// what content is returned.
///
/// Note: There exists another option `All` that exclude the _before_ value from an `UPDATE`. We
/// don't support this for SQL Server sources yet, so it's not included in this enum.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql?view=sql-server-ver16#row_filter_option>
#[derive(Debug, Copy, Clone)]
pub enum RowFilterOption {
    /// Includes both the before and after values of a row when changed because of an `UPDATE`.
    AllUpdateOld,
}

impl RowFilterOption {
    /// Returns this option formatted in a way that can be used in a query.
    pub fn to_sql_string(&self) -> &'static str {
        match self {
            RowFilterOption::AllUpdateOld => "all update old",
        }
    }
}

impl fmt::Display for RowFilterOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_sql_string())
    }
}

/// Identifies what change was made to the SQL Server table tracked by CDC.
#[derive(Debug)]
pub enum Operation {
    /// Row was `INSERT`-ed.
    Insert(tiberius::Row),
    /// Row was `DELETE`-ed.
    Delete(tiberius::Row),
    /// Original value of the row when `UPDATE`-ed.
    UpdateOld(tiberius::Row),
    /// New value of the row when `UPDATE`-ed.
    UpdateNew(tiberius::Row),
}

impl Operation {
    /// Parse the provided [`tiberius::Row`] to determine what [`Operation`] occurred.
    ///
    /// See <https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/cdc-fn-cdc-get-all-changes-capture-instance-transact-sql?view=sql-server-ver16#table-returned>.
    fn try_parse(data: tiberius::Row) -> Result<(Lsn, Self), SqlServerError> {
        static START_LSN_COLUMN: &str = "__$start_lsn";
        static OPERATION_COLUMN: &str = "__$operation";

        let lsn: &[u8] = data
            .try_get(START_LSN_COLUMN)
            .map_err(|e| CdcError::RequiredColumn {
                column_name: START_LSN_COLUMN,
                error: e.to_string(),
            })?
            .ok_or_else(|| CdcError::RequiredColumn {
                column_name: START_LSN_COLUMN,
                error: "got null value".to_string(),
            })?;
        let operation: i32 = data
            .try_get(OPERATION_COLUMN)
            .map_err(|e| CdcError::RequiredColumn {
                column_name: OPERATION_COLUMN,
                error: e.to_string(),
            })?
            .ok_or_else(|| CdcError::RequiredColumn {
                column_name: OPERATION_COLUMN,
                error: "got null value".to_string(),
            })?;

        let lsn = Lsn::try_from(lsn).map_err(|msg| SqlServerError::InvalidData {
            column_name: START_LSN_COLUMN.to_string(),
            error: msg,
        })?;
        let operation = match operation {
            1 => Operation::Delete(data),
            2 => Operation::Insert(data),
            3 => Operation::UpdateOld(data),
            4 => Operation::UpdateNew(data),
            other => {
                return Err(SqlServerError::InvalidData {
                    column_name: OPERATION_COLUMN.to_string(),
                    error: format!("unrecognized operation {other}"),
                });
            }
        };

        Ok((lsn, operation))
    }
}

#[cfg(test)]
mod tests {
    use super::Lsn;
    use proptest::prelude::*;
    use tiberius::numeric::Numeric;

    #[mz_ore::test]
    fn smoketest_lsn_ordering() {
        let a = hex::decode("0000003D000019B80004").unwrap();
        let a = Lsn::try_from(&a[..]).unwrap();

        let b = hex::decode("0000003D000019F00011").unwrap();
        let b = Lsn::try_from(&b[..]).unwrap();

        let c = hex::decode("0000003D00001A500003").unwrap();
        let c = Lsn::try_from(&c[..]).unwrap();

        assert!(a < b);
        assert!(b < c);
        assert!(a < c);

        assert_eq!(a, a);
        assert_eq!(b, b);
        assert_eq!(c, c);
    }

    #[mz_ore::test]
    fn smoketest_lsn_roundtrips() {
        #[track_caller]
        fn test_case(hex: &str) {
            let og = hex::decode(hex).unwrap();
            let lsn = Lsn::try_from(&og[..]).unwrap();
            let rnd = lsn.as_bytes();
            assert_eq!(og[..], rnd[..]);
        }

        test_case("0000003D000019B80004");
        test_case("0000003D000019F00011");
        test_case("0000003D00001A500003");
    }

    #[mz_ore::test]
    fn proptest_lsn_roundtrips() {
        #[track_caller]
        fn test_case(bytes: [u8; 10]) {
            let lsn = Lsn::try_from_bytes(&bytes[..]).unwrap();
            let rnd = lsn.as_bytes();
            assert_eq!(&bytes[..], &rnd[..]);
        }
        proptest!(|(random_bytes in any::<[u8; 10]>())| {
            test_case(random_bytes)
        })
    }

    #[mz_ore::test]
    fn proptest_lsn_increment() {
        #[track_caller]
        fn test_case(bytes: [u8; 10]) {
            let lsn = Lsn::try_from_bytes(&bytes[..]).unwrap();
            let new = lsn.increment();
            assert!(lsn < new);
        }
        proptest!(|(random_bytes in any::<[u8; 10]>())| {
            test_case(random_bytes)
        })
    }

    #[mz_ore::test]
    fn proptest_lsn_abbreviate_total_order() {
        #[track_caller]
        fn test_case(bytes: [u8; 10], num_increment: u8) {
            let lsn = Lsn::try_from_bytes(&bytes[..]).unwrap();
            let mut new = lsn;
            for _ in 0..num_increment {
                new = new.increment();
            }

            let a = lsn.abbreviate();
            let b = new.abbreviate();

            assert!(a <= b);
        }
        proptest!(|(random_bytes in any::<[u8; 10]>(), num_increment in any::<u8>())| {
            test_case(random_bytes, num_increment)
        })
    }

    #[mz_ore::test]
    fn test_numeric_lsn_ordering() {
        let a = Lsn::try_from(Numeric::new_with_scale(45_0000008784_00001_i128, 0)).unwrap();
        let b = Lsn::try_from(Numeric::new_with_scale(45_0000008784_00002_i128, 0)).unwrap();
        let c = Lsn::try_from(Numeric::new_with_scale(45_0000008785_00002_i128, 0)).unwrap();
        let d = Lsn::try_from(Numeric::new_with_scale(49_0000008784_00002_i128, 0)).unwrap();
        assert!(a < b);
        assert!(b < c);
        assert!(c < d);
        assert!(a < d);

        assert_eq!(a, a);
        assert_eq!(b, b);
        assert_eq!(c, c);
        assert_eq!(d, d);
    }

    #[mz_ore::test]
    fn test_numeric_lsn_invalid() {
        let with_decimal = Numeric::new_with_scale(1, 20);
        assert!(Lsn::try_from(with_decimal).is_err());

        for v in [
            4294967296_0000000000_00000_i128, // vlf_id is too large
            1_4294967296_00000_i128,          // block_id is too large
            1_0000000001_65536_i128,          // record_id is too large
            -49_0000008784_00002_i128,        // negative is invalid
        ] {
            let invalid_lsn = Numeric::new_with_scale(v, 0);
            assert!(Lsn::try_from(invalid_lsn).is_err());
        }
    }
}
