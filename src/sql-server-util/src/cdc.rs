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
//! Internally we get a snapshot by setting our transaction isolation level to
//! [`TransactionIsolationLevel::Snapshot`], getting the current maximum LSN with
//! [`crate::inspect::get_max_lsn`] and then running a `SELECT *`. We've observed that by
//! using [`TransactionIsolationLevel::Snapshot`] the LSN remains stable for the entire
//! transaction.
//!
//! After completing the snapshot we use [`crate::inspect::get_changes_asc`] which will return
//! all changes between a `[lower, upper)` bound of [`Lsn`]s.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use futures::{Stream, StreamExt};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

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

    /// Takes a snapshot of the upstream table that the specified `capture_instance` is
    /// replicating changes from.
    ///
    /// An optional `instances` parameter can be provided to only snapshot the specified instances.
    pub async fn snapshot<'b>(
        &'b mut self,
        instances: Option<BTreeSet<Arc<str>>>,
    ) -> Result<
        (
            Lsn,
            impl Stream<Item = (Arc<str>, Result<tiberius::Row, SqlServerError>)> + use<'b, 'a>,
        ),
        SqlServerError,
    > {
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
        tracing::info!(?tables, "got table for capture instance");

        // Before we start a transaction, ensure SQL Server is reporting an LSN.
        //
        // If CDC was very recently enabled (e.g. < 30 seconds ago) then it will
        // report NULL for an LSN. Before we start a transaction, which prevents
        // the LSN from progressing, we want to wait a bit for SQL Server to
        // become ready.
        let (_client, lsn_result) = mz_ore::retry::Retry::default()
            .max_duration(std::time::Duration::from_secs(10))
            .retry_async_with_state(&mut self.client, |_, client| async {
                use mz_ore::retry::RetryResult;
                let result = crate::inspect::get_max_lsn(*client).await;
                let result = match result {
                    Ok(lsn) => RetryResult::Ok(lsn),
                    Err(err @ SqlServerError::NullMaxLsn) => RetryResult::RetryableErr(err),
                    Err(other) => RetryResult::FatalErr(other),
                };
                (client, result)
            })
            .await;
        // Timed out waiting for SQL Server to become ready.
        if let Err(e) = lsn_result {
            return Err(e);
        }

        self.client
            .set_transaction_isolation(TransactionIsolationLevel::Snapshot)
            .await?;
        let txn = self.client.transaction().await?;

        // Get the current LSN of the database.
        let lsn = crate::inspect::get_max_lsn(txn.client).await?;
        tracing::info!(?tables, ?lsn, "starting snapshot");

        // Run a `SELECT` query to snapshot the entire table.
        let stream = async_stream::stream! {
            // TODO(sql_server3): A stream of streams would be better here than
            // returning the name with each result, but the lifetimes are tricky.
            for (capture_instance, schema_name, table_name) in tables {
                tracing::trace!(%capture_instance, %schema_name, %table_name, "snapshot start");

                let query = format!("SELECT * FROM {schema_name}.{table_name};");
                let snapshot = txn.client.query_streaming(&query, &[]);
                let mut snapshot = std::pin::pin!(snapshot);
                while let Some(result) = snapshot.next().await {
                    yield (Arc::clone(&capture_instance), result);
                }

                tracing::trace!(%capture_instance, %schema_name, %table_name, "snapshot end");
            }

            // Slightly awkward, but if the commit fails we need to conform to
            // type of the stream.
            if let Err(e) = txn.commit().await {
                yield ("commit".into(), Err(e));
            }
        };

        Ok((lsn, stream))
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
}
