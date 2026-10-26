// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::pin::pin;

use futures::{Stream as AsyncStream, TryStreamExt};
use mz_postgres_util::Client;
use mz_repr::{Datum, Diff, Row};
use mz_storage_types::sources::MzOffset;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage, TupleData};

use crate::metrics::source::postgres::PgSourceMetrics;
use crate::source::postgres::verify_schema;
use crate::source::postgres::{DefiniteError, SourceOutputInfo, TransientError};

use super::LogicalReplMsg;

/// A decoded row from a transaction with source information.
type DecodedRow = (u32, usize, Result<Row, DefiniteError>, Diff);

/// The outputs of relation `rel` that a message committed at `commit_lsn` applies to.
fn outputs_at(
    table_info: &BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    rel: u32,
    commit_lsn: MzOffset,
) -> impl Iterator<Item = (&usize, &SourceOutputInfo)> {
    table_info
        .get(&rel)
        .into_iter()
        .flatten()
        .filter(move |(_, info)| !info.ignores(commit_lsn))
}

/// Extracts a single transaction from the replication stream delimited by a BEGIN and COMMIT
/// message. The BEGIN message must have already been consumed from the stream before calling this
/// function.
pub(super) fn extract_transaction<'a>(
    stream: impl AsyncStream<Item = Result<LogicalReplMsg, TransientError>> + 'a,
    metadata_client: &'a Client,
    commit_lsn: MzOffset,
    table_info: &'a mut BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    metrics: &'a PgSourceMetrics,
    publication: &'a str,
) -> impl AsyncStream<Item = Result<DecodedRow, TransientError>> + 'a {
    use LogicalReplicationMessage::*;
    let mut row = Row::default();
    async_stream::try_stream!({
        let mut stream = pin!(stream);
        metrics.transactions.inc();
        metrics.lsn.set(commit_lsn.offset);
        while let Some(event) = stream.try_next().await? {
            // We can ignore keepalive messages while processing a transaction because the
            // commit_lsn will drive progress.
            let message = match event {
                ReplicationMessage::XLogData(data) => data.into_data(),
                ReplicationMessage::PrimaryKeepAlive(_) => {
                    metrics.ignored.inc();
                    continue;
                }
                _ => Err(TransientError::UnknownReplicationMessage)?,
            };
            metrics.total.inc();
            match message {
                Insert(body) if !table_info.contains_key(&body.rel_id()) => metrics.ignored.inc(),
                Update(body) if !table_info.contains_key(&body.rel_id()) => metrics.ignored.inc(),
                Delete(body) if !table_info.contains_key(&body.rel_id()) => metrics.ignored.inc(),
                Relation(body) if !table_info.contains_key(&body.rel_id()) => metrics.ignored.inc(),
                Insert(body) => {
                    metrics.inserts.inc();
                    let rel = body.rel_id();
                    for (output, info) in outputs_at(table_info, rel, commit_lsn) {
                        let tuple_data = body.tuple().tuple_data();
                        let Some(ref projection) = info.projection else {
                            panic!("missing projection for {rel}");
                        };
                        let datums = projection.iter().map(|idx| &tuple_data[*idx]);
                        let row = unpack_tuple(datums, &mut row);
                        yield (rel, *output, row, Diff::ONE);
                    }
                }
                Update(body) => match body.old_tuple() {
                    Some(old_tuple) => {
                        metrics.updates.inc();
                        let new_tuple = body.new_tuple();
                        let rel = body.rel_id();
                        for (output, info) in outputs_at(table_info, rel, commit_lsn) {
                            let Some(ref projection) = info.projection else {
                                panic!("missing projection for {rel}");
                            };
                            let old_tuple =
                                projection.iter().map(|idx| &old_tuple.tuple_data()[*idx]);
                            // If the new tuple contains unchanged toast values we reference the old ones
                            let new_tuple = std::iter::zip(
                                projection.iter().map(|idx| &new_tuple.tuple_data()[*idx]),
                                old_tuple.clone(),
                            )
                            .map(|(new, old)| match new {
                                TupleData::UnchangedToast => old,
                                _ => new,
                            });
                            let old_row = unpack_tuple(old_tuple, &mut row);
                            let new_row = unpack_tuple(new_tuple, &mut row);

                            yield (rel, *output, old_row, Diff::MINUS_ONE);
                            yield (rel, *output, new_row, Diff::ONE);
                        }
                    }
                    None => {
                        let rel = body.rel_id();
                        for (output, _) in outputs_at(table_info, rel, commit_lsn) {
                            yield (
                                rel,
                                *output,
                                Err(DefiniteError::DefaultReplicaIdentity),
                                Diff::ONE,
                            );
                        }
                    }
                },
                Delete(body) => match body.old_tuple() {
                    Some(old_tuple) => {
                        metrics.deletes.inc();
                        let rel = body.rel_id();
                        for (output, info) in outputs_at(table_info, rel, commit_lsn) {
                            let Some(ref projection) = info.projection else {
                                panic!("missing projection for {rel}");
                            };
                            let datums = projection.iter().map(|idx| &old_tuple.tuple_data()[*idx]);
                            let row = unpack_tuple(datums, &mut row);
                            yield (rel, *output, row, Diff::MINUS_ONE);
                        }
                    }
                    None => {
                        let rel = body.rel_id();
                        for (output, _) in outputs_at(table_info, rel, commit_lsn) {
                            yield (
                                rel,
                                *output,
                                Err(DefiniteError::DefaultReplicaIdentity),
                                Diff::ONE,
                            );
                        }
                    }
                },
                Relation(body) => {
                    let rel_id = body.rel_id();
                    if let Some(outputs) = table_info.get_mut(&body.rel_id()) {
                        // Because the replication stream doesn't include columns' attnums, we need
                        // to check the current local schema against the current remote schema to
                        // ensure e.g. we haven't received a schema update with the same terminal
                        // column name which is actually a different column.
                        let upstream_info = mz_postgres_util::publication_info(
                            metadata_client,
                            publication,
                            Some(&[rel_id]),
                        )
                        .await?;

                        let mut schema_errors = vec![];

                        outputs.retain(|output_index, info| {
                            match verify_schema(rel_id, info, &upstream_info) {
                                Ok(()) => true,
                                Err(err) => {
                                    schema_errors.push((
                                        rel_id,
                                        *output_index,
                                        Err(err),
                                        Diff::ONE,
                                    ));
                                    false
                                }
                            }
                        });
                        // Recalculate projection vector for the retained valid outputs. Here we
                        // must use the column names in the RelationBody message and not the
                        // upstream_info obtained above, since that one represents the current
                        // schema upstream which may be many versions head of the one we're about
                        // to receive after this Relation message.
                        let column_positions: BTreeMap<_, _> = body
                            .columns()
                            .iter()
                            .enumerate()
                            .map(|(idx, col)| (col.name().unwrap(), idx))
                            .collect();
                        for info in outputs.values_mut() {
                            let projection: Option<Vec<_>> = info
                                .desc
                                .columns
                                .iter()
                                .map(|col| column_positions.get(&*col.name).copied())
                                .collect();
                            match projection {
                                Some(projection) => info.projection = Some(projection),
                                // This message describes a schema from before the output was
                                // created, which need not cover `desc`. Retaining the previous
                                // projection is safe: messages at these LSNs are ignored, and the
                                // schema change that made the columns diverge invalidates the
                                // upstream's relation cache, so another Relation message arrives
                                // ahead of the first message that isn't ignored.
                                None if info.ignores(commit_lsn) => (),
                                None => panic!(
                                    "Relation message for {rel_id} at {commit_lsn} does not \
                                     cover the captured schema of {}",
                                    info.desc.name
                                ),
                            }
                        }
                        for schema_error in schema_errors {
                            yield schema_error;
                        }
                    }
                }
                Truncate(body) => {
                    for &rel_id in body.rel_ids() {
                        if let Some(outputs) = table_info.get_mut(&rel_id) {
                            let mut ignored = BTreeMap::new();
                            for (output, info) in std::mem::take(outputs) {
                                if info.ignores(commit_lsn) {
                                    ignored.insert(output, info);
                                    continue;
                                }
                                yield (
                                    rel_id,
                                    output,
                                    Err(DefiniteError::TableTruncated),
                                    Diff::ONE,
                                );
                            }
                            *outputs = ignored;
                        }
                    }
                }
                Commit(body) => {
                    if commit_lsn != body.commit_lsn().into() {
                        Err(TransientError::InvalidTransaction)?
                    }
                    return;
                }
                // TODO: We should handle origin messages and emit an error as they indicate that
                // the upstream performed a point in time restore so all bets are off about the
                // continuity of the stream.
                Origin(_) | Type(_) => metrics.ignored.inc(),
                Begin(_) => Err(TransientError::NestedTransaction)?,
                // The enum is marked as non_exhaustive. Better to be conservative
                _ => Err(TransientError::UnknownLogicalReplicationMessage)?,
            }
        }
        Err(TransientError::ReplicationEOF)?;
    })
}

/// Unpacks an iterator of TupleData into a list of nullable bytes or an error if this can't be
/// done.
#[inline]
fn unpack_tuple<'a, I>(tuple_data: I, row: &mut Row) -> Result<Row, DefiniteError>
where
    I: IntoIterator<Item = &'a TupleData>,
    I::IntoIter: ExactSizeIterator,
{
    let iter = tuple_data.into_iter();
    let mut packer = row.packer();
    for data in iter {
        let datum = match data {
            TupleData::Text(bytes) => super::super::decode_utf8_text(bytes)?,
            TupleData::Null => Datum::Null,
            TupleData::UnchangedToast => return Err(DefiniteError::MissingToast),
            TupleData::Binary(_) => return Err(DefiniteError::UnexpectedBinaryData),
        };
        packer.push(datum);
    }
    Ok(row.clone())
}
