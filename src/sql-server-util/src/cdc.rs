// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use derivative::Derivative;
use futures::Stream;
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::{Client, SqlServerError};

/// A stream of changes from a table in SQL Server that has CDC enabled.
///
/// SQL Server does not have an API to push or notify consumers of changes, so we periodically
/// poll the upstream source.
///
/// See: <https://learn.microsoft.com/en-us/sql/relational-databases/system-tables/change-data-capture-tables-transact-sql?view=sql-server-ver16>
pub struct CdcStream<'a> {
    /// Client we use for querying SQL Server.
    client: &'a mut Client,
    /// Upstream capture instance we'll list changes from.
    capture_instance: Arc<str>,
    /// How often we poll the upstream for changes.
    poll_interval: Duration,
    /// The log sequence number we start streaming changes from.
    start_lsn: Option<Lsn>,
}

impl<'a> CdcStream<'a> {
    pub(crate) fn new(client: &'a mut Client, capture_instance: Arc<str>) -> Self {
        CdcStream {
            client,
            capture_instance,
            poll_interval: Duration::from_secs(1),
            start_lsn: None,
        }
    }

    /// Set the [`Lsn`] that we should start streaming changes from.
    ///
    /// If the provided [`Lsn`] is not available, the stream will return an error
    /// when first polled.
    pub fn start_lsn(mut self, lsn: Lsn) -> Self {
        self.start_lsn = Some(lsn);
        self
    }

    /// The cadence at which we'll poll the upstream SQL Server database for changes.
    ///
    /// Default is 1 second.
    pub fn poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Consume `self` returning a [`Stream`] of [`CdcEvent`]s.
    pub fn into_stream(mut self) -> impl Stream<Item = Result<CdcEvent, SqlServerError>> + use<'a> {
        async_stream::try_stream! {
            // Determine the LSN to start streaming from.
            let mut curr_lsn = self.determine_start_lsn().await?;

            loop {
                // Measure the tick before we do any operation so the time it takes
                // to query SQL Server is included in the time that we wait.
                let next_tick = tokio::time::Instant::now()
                    .checked_add(self.poll_interval)
                    .expect("tick interval overflowed!");

                // Get the max LSN for the DB.
                let new_lsn = crate::inspect::get_max_lsn(self.client).await?;
                tracing::debug!(?new_lsn, ?curr_lsn, "got max LSN");

                // If the LSN has increased then get all of our changes.
                if new_lsn > curr_lsn {
                    let changes = crate::inspect::get_changes(
                        self.client,
                        &*self.capture_instance,
                        curr_lsn,
                        new_lsn,
                        RowFilterOption::AllUpdateOld,
                    )
                    .await?;

                    // TODO(sql_server3): For very large changes it feels bad collecting
                    // them all in memory, it would be best if we streamed them to the
                    // caller.

                    let mut events: BTreeMap<Lsn, Vec<Operation>> = BTreeMap::default();
                    for change in changes {
                        let (lsn, operation) = Operation::try_parse(change)?;
                        // Group all the operations by their LSN.
                        events.entry(lsn).or_default().push(operation);
                    }

                    for (lsn, changes) in events {
                        // TODO(sql_server1): Handle these events and notify the upstream
                        // that we can cleanup this LSN.
                        let capture_instance = Arc::clone(&self.capture_instance);
                        let mark_complete = Box::new(move || {
                            let _capture_isntance = capture_instance;
                            let _completed_lsn = lsn;
                        });

                        yield CdcEvent { lsn, changes, mark_complete }
                    }

                    // Increment our LSN (`get_changes` is inclusive).
                    let next_lsn = crate::inspect::increment_lsn(self.client, new_lsn).await?;
                    tracing::debug!(?curr_lsn, ?next_lsn, "incrementing LSN");
                    curr_lsn = next_lsn;
                }

                tokio::time::sleep_until(next_tick).await;
            }
        }
    }

    /// Determine the [`Lsn`] to start streaming changes from.
    async fn determine_start_lsn(&mut self) -> Result<Lsn, SqlServerError> {
        match self.start_lsn {
            Some(start_lsn) => {
                let min_lsn =
                    crate::inspect::get_min_lsn(self.client, &*self.capture_instance).await?;

                if start_lsn < min_lsn {
                    // The requested start_lsn is not available!
                    Err(CdcError::LsnNotAvailable {
                        requested: start_lsn,
                        minimum: min_lsn,
                    }
                    .into())
                } else {
                    Ok(start_lsn)
                }
            }
            // If no start_lsn is provided, then begin streaming from the most recent changes.
            None => {
                let max_lsn = crate::inspect::get_max_lsn(self.client).await?;
                Ok(max_lsn)
            }
        }
    }
}

/// A change event from a [`CdcStream`].
#[derive(Derivative)]
#[derivative(Debug)]
pub struct CdcEvent {
    /// The LSN that this change occurred at.
    pub lsn: Lsn,
    /// The change itself.
    pub changes: Vec<Operation>,
    /// When called marks `lsn` as complete allowing the upstream DB to clean up the record.
    #[derivative(Debug = "ignore")]
    pub mark_complete: Box<dyn FnOnce() + Send + Sync>,
}

#[derive(Debug, thiserror::Error)]
pub enum CdcError {
    #[error("the requested LSN '{requested:?}' is less then the minimum '{minimum:?}'")]
    LsnNotAvailable { requested: Lsn, minimum: Lsn },
    #[error("failed to get the required column '{column_name}': {error}")]
    RequiredColumn {
        column_name: &'static str,
        error: String,
    },
}

/// This type is used to represent the progress of each SQL Server instance in
/// the ingestion dataflow.
///
/// A SQL Server LSN is essentially an opaque binary blob that provides a
/// __total order__ to all transations within a database. Technically though an
/// LSN has three components:
///
/// 1. A Virtual Log File (VLF) sequence number, bytes [0, 4)
/// 2. Log block number, bytes [4, 8)
/// 3. Log record number, bytes [8, 10)
///
/// To increment an LSN you need to call the [`sys.fn_cdc_increment_lsn`] T-SQL
/// function.
///
/// [`sys.fn_cdc_increment_lsn`](https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/sys-fn-cdc-increment-lsn-transact-sql?view=sql-server-ver16)
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize, Arbitrary)]
pub struct Lsn([u8; 10]);

impl Lsn {
    /// Return the underlying byte slice for this [`Lsn`].
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Returns `self` as a [`StructuredLsn`].
    pub fn as_structured(&self) -> StructuredLsn {
        let vlf_id: [u8; 4] = self.0[0..4].try_into().expect("known good length");
        let block_id: [u8; 4] = self.0[4..8].try_into().expect("known good length");
        let record_id: [u8; 2] = self.0[8..].try_into().expect("known good length");

        StructuredLsn {
            vlf_id: u32::from_be_bytes(vlf_id),
            block_id: u32::from_be_bytes(block_id),
            record_id: u16::from_be_bytes(record_id),
        }
    }
}

impl Ord for Lsn {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_structured().cmp(&other.as_structured())
    }
}

impl PartialOrd for Lsn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<&[u8]> for Lsn {
    type Error = String;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        let value: [u8; 10] = value
            .try_into()
            .map_err(|_| format!("incorrect length, expected 10 got {}", value.len()))?;
        Ok(Lsn(value))
    }
}

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
            column_name: "lsn".to_string(),
            error: msg,
        })?;
        let operation = match operation {
            1 => Operation::Delete(data),
            2 => Operation::Insert(data),
            3 => Operation::UpdateOld(data),
            4 => Operation::UpdateNew(data),
            other => {
                return Err(SqlServerError::InvalidData {
                    column_name: "operation".to_string(),
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
}
