// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use chrono::format::{DelayedFormat, StrftimeItems};
use chrono::NaiveDateTime;
use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use log::{debug, error, info, warn};
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Operator;
use timely::dataflow::{Scope, Stream};

use dataflow_types::{
    sources::{DebeziumDedupProjection, DebeziumEnvelope, DebeziumMode, DebeziumSourceProjection},
    DataflowError, DecodeError,
};
use expr::GlobalId;
use repr::{Datum, Diff, Row};

use crate::metrics::Metrics;
use crate::source::DecodeResult;

pub(crate) fn render<G: Scope>(
    envelope: &DebeziumEnvelope,
    input: &Stream<G, DecodeResult>,
    debug_name: String,
    metrics: Metrics,
    src_id: GlobalId,
    dataflow_id: usize,
) -> Collection<G, Result<Row, DataflowError>, Diff> {
    let (before_idx, after_idx) = (envelope.before_idx, envelope.after_idx);
    match envelope.mode {
        DebeziumMode::Upsert => {
            let gauge = metrics.debezium_upsert_count_for(src_id, dataflow_id);
            input
                .unary(Pipeline, "envelope-debezium-upsert", move |_, _| {
                    let mut current_values = HashMap::new();
                    let mut data = vec![];
                    move |input, output| {
                        while let Some((cap, refmut_data)) = input.next() {
                            let mut session = output.session(&cap);
                            refmut_data.swap(&mut data);
                            for result in data.drain(..) {
                                let key = match result.key {
                                    Some(Ok(key)) => key,
                                    Some(Err(err)) => {
                                        session.give((Err(err.into()), cap.time().clone(), 1));
                                        continue;
                                    }
                                    None => continue,
                                };

                                // Ignore out of order updates that have been already overwritten
                                if let Some((_, position)) = current_values.get(&key) {
                                    if result.position < *position {
                                        continue;
                                    }
                                }

                                let value = match result.value {
                                    Some(Ok(row)) => match row.iter().nth(after_idx).unwrap() {
                                        Datum::List(after) => {
                                            let mut row = Row::pack(&after);
                                            row.extend(result.metadata.iter());
                                            Some(Ok(row))
                                        }
                                        Datum::Null => None,
                                        d => {
                                            panic!("type error: expected record, found {:?}", d)
                                        }
                                    },
                                    Some(Err(err)) => Some(Err(DataflowError::from(err))),
                                    None => continue,
                                };

                                let retraction = match value {
                                    Some(value) => {
                                        session.give((value.clone(), cap.time().clone(), 1));
                                        current_values.insert(key, (value, result.position))
                                    }
                                    None => current_values.remove(&key),
                                };

                                if let Some((value, _)) = retraction {
                                    session.give((value, cap.time().clone(), -1));
                                }
                            }
                        }
                        gauge.set(current_values.len() as u64);
                    }
                })
                .as_collection()
        }
        _ => input
            .unary(Pipeline, "envelope-debezium", move |_, _| {
                let mut dedup_state = HashMap::new();
                let envelope = envelope.clone();
                let mut data = vec![];

                move |input, output| {
                    while let Some((cap, refmut_data)) = input.next() {
                        let mut session = output.session(&cap);
                        refmut_data.swap(&mut data);
                        for result in data.drain(..) {
                            let key = match result.key.transpose() {
                                Ok(key) => key,
                                Err(err) => {
                                    session.give((Err(err.into()), cap.time().clone(), 1));
                                    continue;
                                }
                            };
                            let value = match result.value {
                                Some(Ok(value)) => value,
                                Some(Err(err)) => {
                                    session.give((Err(err.into()), cap.time().clone(), 1));
                                    continue;
                                }
                                None => continue,
                            };

                            let partition_dedup = dedup_state
                                .entry(result.partition.clone())
                                .or_insert_with(|| {
                                    DebeziumDeduplicationState::new(envelope.clone())
                                });
                            let should_use = match partition_dedup {
                                Some(ref mut s) => {
                                    let (res, state_mutations) = s.should_use_record(
                                        key,
                                        &value,
                                        result.position,
                                        result.upstream_time_millis,
                                        &debug_name,
                                    );

                                    println!("Debezium state mutations: {:?}", state_mutations);
                                    s.apply_mutations(state_mutations);

                                    match res {
                                        Ok(b) => b,
                                        Err(err) => {
                                            session.give((Err(err.into()), cap.time().clone(), 1));
                                            continue;
                                        }
                                    }
                                }
                                None => true,
                            };

                            if should_use {
                                match value.iter().nth(before_idx).unwrap() {
                                    Datum::List(l) => {
                                        session.give((Ok(Row::pack(&l)), cap.time().clone(), -1))
                                    }
                                    Datum::Null => {}
                                    d => panic!("type error: expected record, found {:?}", d),
                                }
                                match value.iter().nth(after_idx).unwrap() {
                                    Datum::List(l) => {
                                        session.give((Ok(Row::pack(&l)), cap.time().clone(), 1))
                                    }
                                    Datum::Null => {}
                                    d => panic!("type error: expected record, found {:?}", d),
                                }
                            }
                        }
                    }
                }
            })
            .as_collection(),
    }
}

/// Track whether or not we should skip a specific debezium message
///
/// The goal of deduplication is to omit sending true duplicates -- the exact
/// same record being sent into materialize twice. That means that we create
/// one deduplicator per timely worker and use use timely key sharding
/// normally. But it also means that no single deduplicator knows the
/// highest-ever seen binlog offset.
#[derive(Debug, PartialEq, Eq)]
struct DebeziumDeduplicationState {
    /// Last recorded binlog position and connector offset
    ///
    /// [`DebeziumEnvelope`] determines whether messages that are not ahead
    /// of the last recorded position will be skipped.
    last_position_and_offset: Option<(RowCoordinates, Option<i64>)>,
    /// Whether or not to track every message we've ever seen
    full: Option<TrackFull>,
    messages_processed: u64,
    // TODO(petrosagg): This is only used when unpacking MySQL row coordinates. The logic was
    // transferred as-is from the previous avro-debezium code. Find a better place to put this or
    // avoid it completely.
    filenames_to_indices: HashMap<String, i64>,
    projection: DebeziumDedupProjection,
}

#[derive(Debug, PartialEq, Eq)]
enum DebeziumStateMutation {
    IncNumMesssagesProcessed,
    SetLastPosition(RowCoordinates),
    SetLastPositionAndOffset(RowCoordinates, Option<i64>),
    SetMaxSeenTime(i64),
    UpdateSeenPositions(RowCoordinates, i64),
    SetStarted(bool),
    SetPaddingStarted(bool),
    AddSeenSnapshotKeys(Row),
    AddFilenameToIndexMapping(String, i64),
    ClearFull,
}

/// A differential update to a [`DebeziumDeduplicationState`]. A collection of these can be used to
/// restore a state to as it was at a given previous time.
type DebeziumStateUpdate = (
    u64,
    Option<(RowCoordinates, Option<i64>)>,
    i64,
    Option<(RowCoordinates, i64)>,
    bool,
    bool,
    Option<Row>,
    Option<(String, i64)>,
);

/// If we need to deal with debezium possibly going back after it hasn't seen things.
/// During normal (non-snapshot) operation, we deduplicate based on binlog position: (pos, row), for MySQL.
/// During the initial snapshot, (pos, row) values are all the same, but primary keys
/// are unique and thus we can get deduplicate based on those.
#[derive(Debug, PartialEq, Eq)]
struct TrackFull {
    /// binlog position to (timestamp that this binlog entry was first seen)
    seen_positions: HashMap<RowCoordinates, i64>,
    seen_snapshot_keys: HashSet<Row>,
    /// The highest-ever seen timestamp, used in logging to let us know how far backwards time might go
    max_seen_time: i64,
    range: Option<TrackRange>,
    started_padding: bool,
    /// Whether we have started full deduplication mode
    started: bool,
}

/// When to start and end full-range tracking
///
/// All values are milliseconds since the unix epoch and are meant to be compared to the
/// `upstream_time_millis` argument to [`DebeziumDeduplicationState::should_use_record`].
///
/// We throw away all tracking data after we see the first record past `end`.
#[derive(Debug, PartialEq, Eq)]
struct TrackRange {
    /// Start pre-filling the seen data before we start trusting it
    ///
    /// At some point we need to start trusting the [`TrackFull::seen_positions`] map more
    /// than we trust the Debezium high water mark. In order to do that, the
    /// `seen_offsets` map must have some data, otherwise all records would show up as
    /// new immediately at the phase transition.
    ///
    /// For example, consider the following series of records, presented vertically in
    /// the order that they were received:
    ///
    /// ```text
    /// ts  val
    /// -------
    /// 1   a
    /// 2   b
    /// 1   a
    /// ```
    ///
    /// If we start tracking at ts 2 and immediately start trusting the hashmap more than
    /// the Debezium high water mark then ts 1 will be falsely double-inserted. So we
    /// need to start building a buffer before we can start trusting it.
    ///
    /// `pad_start` is the upstream_time_millis at we we start building the buffer, and
    /// [`TrackRange::start`] is the point at which we start trusting the buffer.
    /// Currently `pad_start` defaults to 1 hour (wall clock time) before `start`,
    /// as a value that seems overwhelmingly likely to cause the buffer to always have
    /// enough data that it doesn't give incorrect answers.
    pad_start: i64,
    start: i64,
    end: i64,
}

impl TrackFull {
    fn from_keys() -> Self {
        Self {
            seen_positions: Default::default(),
            seen_snapshot_keys: Default::default(),
            max_seen_time: 0,
            range: None,
            started_padding: false,
            started: false,
        }
    }

    fn from_keys_in_range(
        start: NaiveDateTime,
        end: NaiveDateTime,
        pad_start: Option<NaiveDateTime>,
    ) -> Self {
        let mut tracker = Self::from_keys();
        let pad_start = pad_start
            .unwrap_or_else(|| (start - chrono::Duration::hours(1)))
            .timestamp_millis();
        tracker.range = Some(TrackRange {
            pad_start,
            start: start.timestamp_millis(),
            end: end.timestamp_millis(),
        });
        tracker
    }
}

/// See <https://rusanu.com/2012/01/17/what-is-an-lsn-log-sequence-number/>
#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct SqlServerLsn {
    file_seq_num: u32,
    log_block_offset: u32,
    slot_num: u16,
}

impl FromStr for SqlServerLsn {
    type Err = DecodeError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let make_err = || DecodeError::Text(format!("invalid lsn: {}", input));
        // SQL Server change LSNs are 10-byte integers. Debezium
        // encodes them as hex, in the following format: xxxxxxxx:xxxxxxxx:xxxx
        if input.len() != 22 {
            return Err(make_err());
        }
        if input.as_bytes()[8] != b':' || input.as_bytes()[17] != b':' {
            return Err(make_err());
        }
        let file_seq_num = u32::from_str_radix(&input[0..8], 16).or_else(|_| Err(make_err()))?;
        let log_block_offset =
            u32::from_str_radix(&input[9..17], 16).or_else(|_| Err(make_err()))?;
        let slot_num = u16::from_str_radix(&input[18..22], 16).or_else(|_| Err(make_err()))?;

        Ok(Self {
            file_seq_num,
            log_block_offset,
            slot_num,
        })
    }
}

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum RowCoordinates {
    MySql {
        file: i64,
        pos: i64,
        row: i32,
    },
    Postgres {
        last_commit_lsn: Option<u64>,
        lsn: i64,
        total_order: Option<i64>,
    },
    SqlServer {
        change_lsn: SqlServerLsn,
        event_serial_no: i64,
    },
}

impl DebeziumDeduplicationState {
    fn new(envelope: DebeziumEnvelope) -> Option<Self> {
        let (full, projection) = match envelope.mode {
            DebeziumMode::Ordered(projection) => (None, projection),
            DebeziumMode::Full(projection) => (Some(TrackFull::from_keys()), projection),
            DebeziumMode::FullInRange {
                projection,
                start,
                end,
                pad_start,
            } => (
                Some(TrackFull::from_keys_in_range(start, end, pad_start)),
                projection,
            ),
            DebeziumMode::None | DebeziumMode::Upsert => return None,
        };
        Some(DebeziumDeduplicationState {
            last_position_and_offset: None,
            full,
            messages_processed: 0,
            filenames_to_indices: HashMap::new(),
            projection,
        })
    }

    fn extract_total_order(&self, value: &Row) -> Option<i64> {
        match value.iter().nth(self.projection.transaction_idx).unwrap() {
            Datum::List(l) => match l.iter().nth(self.projection.total_order_idx).unwrap() {
                Datum::Int64(n) => Some(n),
                Datum::Null => None,
                d => panic!("type error: expected bigint, found {:?}", d),
            },
            Datum::Null => None,
            d => panic!("type error: expected bigint[], found {:?}", d),
        }
    }

    fn extract_binlog_position(
        &self,
        value: &Row,
        state_mutations: &mut Vec<DebeziumStateMutation>,
    ) -> Result<Option<RowCoordinates>, DecodeError> {
        match value.iter().nth(self.projection.source_idx).unwrap() {
            Datum::List(source) => {
                // While reading a snapshot the row coordinates are useless, so early return None
                match source.iter().nth(self.projection.snapshot_idx).unwrap() {
                    Datum::String(s) if s != "false" => return Ok(None),
                    Datum::True => return Ok(None),
                    _ => {}
                }

                let coords = match self.projection.source_projection {
                    DebeziumSourceProjection::MySql { file, pos, row } => {
                        let filename = match source.iter().nth(file).unwrap() {
                            Datum::String(s) => s,
                            Datum::Null => return Ok(None),
                            d => panic!("type error: expected text, found {:?}", d),
                        };

                        let file = match self.filenames_to_indices.get(filename) {
                            Some(idx) => *idx,
                            None => {
                                let next_idx = self.filenames_to_indices.len() as i64;
                                state_mutations.push(
                                    DebeziumStateMutation::AddFilenameToIndexMapping(
                                        filename.to_owned(),
                                        next_idx,
                                    ),
                                );

                                next_idx
                            }
                        };
                        let pos = match source.iter().nth(pos).unwrap() {
                            Datum::Int64(s) => s,
                            Datum::Null => return Ok(None),
                            d => panic!("type error: expected bigint, found {:?}", d),
                        };
                        let row = match source.iter().nth(row).unwrap() {
                            Datum::Int32(s) => s,
                            Datum::Null => return Ok(None),
                            d => panic!("type error: expected int, found {:?}", d),
                        };

                        RowCoordinates::MySql { file, pos, row }
                    }
                    DebeziumSourceProjection::Postgres { sequence, lsn } => {
                        let last_commit_lsn = match sequence {
                            Some(idx) => {
                                let sequence = match source.iter().nth(idx).unwrap() {
                                    Datum::String(s) => s,
                                    Datum::Null => return Ok(None),
                                    d => panic!("type error: expected text, found {:?}", d),
                                };
                                let make_err = || {
                                    DecodeError::Text(format!("invalid sequence: {:?}", sequence))
                                };
                                let sequence: Vec<Option<&str>> =
                                    serde_json::from_str(sequence).or_else(|_| Err(make_err()))?;

                                match sequence.first().ok_or_else(make_err)? {
                                    Some(s) => Some(u64::from_str(s).or_else(|_| Err(make_err()))?),
                                    None => None,
                                }
                            }
                            None => None,
                        };

                        let lsn = match source.iter().nth(lsn).unwrap() {
                            Datum::Int64(s) => s,
                            Datum::Null => return Ok(None),
                            d => panic!("type error: expected bigint, found {:?}", d),
                        };
                        let total_order = self.extract_total_order(value);

                        RowCoordinates::Postgres {
                            last_commit_lsn,
                            lsn,
                            total_order,
                        }
                    }
                    DebeziumSourceProjection::SqlServer {
                        change_lsn,
                        event_serial_no,
                    } => {
                        let change_lsn = match source.iter().nth(change_lsn).unwrap() {
                            Datum::String(s) => s.parse::<SqlServerLsn>()?,
                            Datum::Null => return Ok(None),
                            d => panic!("type error: expected text, found {:?}", d),
                        };
                        let event_serial_no = match source.iter().nth(event_serial_no).unwrap() {
                            Datum::Int64(s) => s,
                            Datum::Null => return Ok(None),
                            d => panic!("type error: expected bigint, found {:?}", d),
                        };

                        RowCoordinates::SqlServer {
                            change_lsn,
                            event_serial_no,
                        }
                    }
                };
                Ok(Some(coords))
            }
            Datum::Null => Ok(None),
            d => panic!("type error: expected record, found {:?}", d),
        }
    }

    fn should_use_record(
        &self,
        key: Option<Row>,
        value: &Row,
        connector_offset: Option<i64>,
        upstream_time_millis: Option<i64>,
        debug_name: &str,
    ) -> (Result<bool, DecodeError>, Vec<DebeziumStateMutation>) {
        let mut state_mutations = Vec::new();

        let binlog_position = match self.extract_binlog_position(value, &mut state_mutations) {
            Ok(binlog_position) => binlog_position,
            Err(e) => {
                return (Err(e), state_mutations);
            }
        };

        state_mutations.push(DebeziumStateMutation::IncNumMesssagesProcessed);

        // If in the initial snapshot, binlog position is meaningless for detecting
        // duplicates, since it is always the same.
        let should_skip = match &binlog_position {
            None => None,
            Some(position) => match &self.last_position_and_offset {
                Some((old_position, old_offset)) => {
                    if position > old_position {
                        state_mutations
                            .push(DebeziumStateMutation::SetLastPosition(position.clone()));
                        None
                    } else {
                        Some(SkipInfo {
                            old_position: old_position.clone(),
                            old_offset: *old_offset,
                        })
                    }
                }
                None => {
                    state_mutations.push(DebeziumStateMutation::SetLastPositionAndOffset(
                        position.clone(),
                        connector_offset,
                    ));
                    None
                }
            },
        };

        let mut delete_full = false;
        let should_use = match &self.full {
            // Always none if in snapshot, see comment above where `should_skip` is bound.
            None => should_skip.is_none(),
            Some(TrackFull {
                seen_positions,
                seen_snapshot_keys,
                max_seen_time,
                range,
                started_padding,
                started,
            }) => {
                let max_seen_time = max(upstream_time_millis.unwrap_or(0), *max_seen_time);
                state_mutations.push(DebeziumStateMutation::SetMaxSeenTime(max_seen_time));
                if let Some(position) = binlog_position {
                    // first check if we are in a special case of range-bounded track full
                    if let Some(range) = range {
                        if let Some(upstream_time_millis) = upstream_time_millis {
                            if upstream_time_millis < range.pad_start {
                                if *started_padding {
                                    warn!("went back to before padding start, after entering padding \
                                               source={} message_time={} messages_processed={}",
                                              debug_name, fmt_timestamp(upstream_time_millis),
                                              self.messages_processed);
                                }
                                if *started {
                                    warn!("went back to before padding start, after entering full dedupe \
                                               source={} message_time={} messages_processed={}",
                                              debug_name, fmt_timestamp(upstream_time_millis),
                                              self.messages_processed);
                                }
                                state_mutations
                                    .push(DebeziumStateMutation::SetPaddingStarted(false));
                                state_mutations.push(DebeziumStateMutation::SetStarted(false));
                                return (Ok(should_skip.is_none()), state_mutations);
                            }
                            if upstream_time_millis < range.start {
                                // in the padding time range
                                state_mutations
                                    .push(DebeziumStateMutation::SetPaddingStarted(true));
                                if *started {
                                    warn!("went back to before padding start, after entering full dedupe \
                                               source={} message_time={} messages_processed={}",
                                              debug_name, fmt_timestamp(upstream_time_millis),
                                              self.messages_processed);
                                }
                                state_mutations.push(DebeziumStateMutation::SetStarted(false));

                                if seen_positions.get(&position).is_none() {
                                    state_mutations.push(
                                        DebeziumStateMutation::UpdateSeenPositions(
                                            position,
                                            upstream_time_millis,
                                        ),
                                    );
                                }
                                return (Ok(should_skip.is_none()), state_mutations);
                            }
                            if upstream_time_millis <= range.end && !*started {
                                state_mutations.push(DebeziumStateMutation::SetStarted(true));
                                info!(
                                    "starting full deduplication source={} buffer_size={} \
                                         messages_processed={} message_time={}",
                                    debug_name,
                                    seen_positions.len(),
                                    self.messages_processed,
                                    fmt_timestamp(upstream_time_millis)
                                );
                            }
                            if upstream_time_millis > range.end {
                                // don't abort early, but we will clean up after this validation
                                delete_full = true;
                            }
                        } else {
                            warn!("message has no creation time file_position={:?}", position);
                        }
                    }

                    // Now we know that we are in either trackfull or a range-bounded trackfull
                    let seen = seen_positions.get(&position);
                    let (is_new, original_time) = match seen {
                        Some(time) => (false, time),
                        None => (true, upstream_time_millis.as_ref().unwrap_or(&0)),
                    };

                    if !state_mutations
                        .iter()
                        .any(|m| matches!(m, DebeziumStateMutation::UpdateSeenPositions(_, _)))
                    {
                        state_mutations.push(DebeziumStateMutation::UpdateSeenPositions(
                            position.clone(),
                            upstream_time_millis.unwrap_or(0),
                        ));
                    }

                    log_duplication_info(
                        position,
                        connector_offset,
                        upstream_time_millis,
                        debug_name,
                        is_new,
                        &should_skip,
                        original_time,
                        &max_seen_time,
                    );

                    is_new
                } else {
                    let key = match key {
                        Some(key) => key,
                        // No key, so we can't do anything sensible for snapshots.
                        // Return "all OK" and hope their data isn't corrupted.
                        None => return (Ok(true), state_mutations),
                    };

                    let is_new = !seen_snapshot_keys.contains(&key);
                    if is_new {
                        state_mutations.push(DebeziumStateMutation::AddSeenSnapshotKeys(key));
                    } else {
                        warn!(
                                "Snapshot row with key={:?} source={} seen multiple times (most recent message_time={})",
                                key, debug_name, fmt_timestamp(upstream_time_millis)
                            );
                    }
                    is_new
                }
            }
        };

        if delete_full {
            info!(
                "Deleting debezium deduplication tracking data source={} message_time={}",
                debug_name,
                fmt_timestamp(upstream_time_millis)
            );
            state_mutations.push(DebeziumStateMutation::ClearFull);
        }
        (Ok(should_use), state_mutations)
    }

    fn apply_mutations(
        &mut self,
        mutations: Vec<DebeziumStateMutation>,
    ) -> Vec<(DebeziumStateUpdate, Diff)> {
        let mut updates = Vec::new();

        for mutation in mutations {
            self.apply_mutation(mutation, &mut updates);
        }

        updates
    }

    /// Applies the given [`DebeziumStateMutation`] to `self` and returns a `Vec` of
    /// [`DebeziumStateUpdate`] that can be used to record this update in a differential fashion.
    fn apply_mutation(
        &mut self,
        mutation: DebeziumStateMutation,
        updates: &mut Vec<(DebeziumStateUpdate, Diff)>,
    ) {
        match mutation {
            DebeziumStateMutation::IncNumMesssagesProcessed => {
                self.messages_processed += 1;
                updates.push(((1, None, 0, None, false, false, None, None), 1));
            }
            DebeziumStateMutation::SetLastPosition(position) => {
                updates.push((
                    (
                        0,
                        self.last_position_and_offset.clone(),
                        0,
                        None,
                        false,
                        false,
                        None,
                        None,
                    ),
                    -1,
                ));

                match self.last_position_and_offset.as_mut() {
                    Some((old_position, _old_offset)) => {
                        *old_position = position;
                    }
                    None => panic!("no previous position and offset"),
                }

                updates.push((
                    (
                        0,
                        self.last_position_and_offset.clone(),
                        0,
                        None,
                        false,
                        false,
                        None,
                        None,
                    ),
                    1,
                ));
            }
            DebeziumStateMutation::SetLastPositionAndOffset(position, offset) => {
                // Only retract if something was there.
                if self.last_position_and_offset.is_some() {
                    updates.push((
                        (
                            0,
                            self.last_position_and_offset.clone(),
                            0,
                            None,
                            false,
                            false,
                            None,
                            None,
                        ),
                        -1,
                    ));
                }

                self.last_position_and_offset = Some((position, offset));

                updates.push((
                    (
                        0,
                        self.last_position_and_offset.clone(),
                        0,
                        None,
                        false,
                        false,
                        None,
                        None,
                    ),
                    1,
                ));
            }
            DebeziumStateMutation::SetMaxSeenTime(time) => {
                let track_full = match self.full {
                    Some(ref mut state) => state,
                    None => panic!("no full track state"),
                };

                // Only emit updates/retractions for times that are not the default.
                if track_full.max_seen_time != 0 {
                    updates.push((
                        (
                            0,
                            None,
                            track_full.max_seen_time,
                            None,
                            false,
                            false,
                            None,
                            None,
                        ),
                        -1,
                    ));
                }
                if time != 0 {
                    updates.push(((0, None, time, None, false, false, None, None), 1));
                }

                track_full.max_seen_time = time;
            }
            DebeziumStateMutation::UpdateSeenPositions(position, ts) => {
                let track_full = match self.full {
                    Some(ref mut state) => state,
                    None => panic!("no full track state"),
                };

                let previous = track_full.seen_positions.insert(position.clone(), ts);
                if let Some(ts) = previous {
                    updates.push((
                        (
                            0,
                            None,
                            0,
                            Some((position.clone(), ts)),
                            false,
                            false,
                            None,
                            None,
                        ),
                        -1,
                    ));
                }
                updates.push((
                    (0, None, 0, Some((position, ts)), false, false, None, None),
                    1,
                ));
            }
            DebeziumStateMutation::SetStarted(value) => {
                let track_full = match self.full {
                    Some(ref mut state) => state,
                    None => panic!("no full track state"),
                };

                if !false && track_full.started {
                    // Retract a previously emitted `true` update. Which makes us go back to the
                    // base state, which is `false`. No need to emit a "positive" update for that.
                    updates.push(((0, None, 0, None, true, false, None, None), -1));
                }
                if value && !track_full.started {
                    // Emit an update when we go from `false` to `true`. But not otherwise, because
                    // `false` is the base state.
                    updates.push(((0, None, 0, None, true, false, None, None), 1));
                }

                track_full.started = value;
            }
            DebeziumStateMutation::SetPaddingStarted(value) => {
                let track_full = match self.full {
                    Some(ref mut state) => state,
                    None => panic!("no full track state"),
                };

                if !value && track_full.started_padding {
                    // Retract a previously emitted `true` update. Which makes us go back to the
                    // base state, which is `false`. No need to emit a "positive" update for that.
                    updates.push(((0, None, 0, None, false, true, None, None), -1));
                }
                if value && !track_full.started_padding {
                    // Emit an update when we go from `false` to `true`. But not otherwise, because
                    // `false` is the base state.
                    updates.push(((0, None, 0, None, false, true, None, None), 1));
                }

                track_full.started_padding = value;
            }
            DebeziumStateMutation::AddSeenSnapshotKeys(row) => {
                let track_full = match self.full {
                    Some(ref mut state) => state,
                    None => panic!("no full track state"),
                };

                let new = track_full.seen_snapshot_keys.insert(row.clone());
                if new {
                    updates.push(((0, None, 0, None, false, false, Some(row), None), 1));
                }
            }
            DebeziumStateMutation::AddFilenameToIndexMapping(filename, idx) => {
                if let Some(old_idx) = self.filenames_to_indices.insert(filename.clone(), idx) {
                    panic!(
                        "there is already a mapping for {}: {}, trying to insert {}",
                        filename, old_idx, idx
                    );
                }
                updates.push((
                    (0, None, 0, None, false, false, None, Some((filename, idx))),
                    1,
                ));
            }
            DebeziumStateMutation::ClearFull => {
                let track_full = match self.full.take() {
                    Some(state) => state,
                    None => panic!("no full track state"),
                };

                for update in track_full.seen_positions {
                    updates.push(((0, None, 0, Some(update), false, false, None, None), -1));
                }

                for key in track_full.seen_snapshot_keys {
                    updates.push(((0, None, 0, None, false, false, Some(key), None), -1));
                }

                // For these three, only retract if the current state is different from the default
                if track_full.max_seen_time != 0 {
                    updates.push((
                        (
                            0,
                            None,
                            track_full.max_seen_time,
                            None,
                            false,
                            false,
                            None,
                            None,
                        ),
                        -1,
                    ));
                }
                if track_full.started {
                    updates.push(((0, None, 0, None, true, false, None, None), -1));
                }
                if track_full.started_padding {
                    updates.push(((0, None, 0, None, false, true, None, None), -1));
                }
            }
        }
    }
}

/// Helper to track information for logging on deduplication
struct SkipInfo {
    old_position: RowCoordinates,
    old_offset: Option<i64>,
}

#[allow(clippy::too_many_arguments)]
fn log_duplication_info(
    position: RowCoordinates,
    connector_offset: Option<i64>,
    upstream_time_millis: Option<i64>,
    debug_name: &str,
    is_new: bool,
    should_skip: &Option<SkipInfo>,
    original_time: &i64,
    max_seen_time: &i64,
) {
    match (is_new, should_skip) {
        // new item that correctly is past the highest item we've ever seen
        (true, None) => {}
        // new item that violates Debezium "guarantee" that the no new
        // records will ever be sent with a position below the highest
        // position ever seen
        (true, Some(skipinfo)) => {
            // original time is guaranteed to be the same as message time, so
            // that label is omitted from this log message
            warn!(
                "Created a new record behind the highest point in source={} \
                 new deduplication position: {:?}, new connector offset: {}, \
                 old deduplication position: {:?} \
                 message_time={} max_seen_time={}",
                debug_name,
                position,
                connector_offset.unwrap_or(-1),
                skipinfo.old_position,
                fmt_timestamp(upstream_time_millis),
                fmt_timestamp(*max_seen_time),
            );
        }
        // Duplicate item below the highest seen item
        (false, Some(skipinfo)) => {
            debug!(
                "already ingested source={} new deduplication position: {:?}, \
                 old deduplication position: {:?}\
                 connector offset={} message_time={} message_first_seen={} max_seen_time={}",
                debug_name,
                position,
                skipinfo.old_position,
                skipinfo.old_offset.unwrap_or(-1),
                fmt_timestamp(upstream_time_millis),
                fmt_timestamp(*original_time),
                fmt_timestamp(*max_seen_time),
            );
        }
        // already exists, but is past the debezium high water mark.
        //
        // This should be impossible because we set the high-water mark
        // every time we insert something
        (false, None) => {
            error!(
                "We surprisingly are seeing a duplicate record that \
                    is beyond the highest record we've ever seen. {:?} connector offset={} \
                    message_time={} message_first_seen={} max_seen_time={}",
                position,
                connector_offset.unwrap_or(-1),
                fmt_timestamp(upstream_time_millis),
                fmt_timestamp(*original_time),
                fmt_timestamp(*max_seen_time),
            );
        }
    }
}

fn fmt_timestamp(ts: impl Into<Option<i64>>) -> DelayedFormat<StrftimeItems<'static>> {
    let (seconds, nanos) = ts
        .into()
        .map(|ts| (ts / 1000, (ts % 1000) * 1_000_000))
        .unwrap_or((0, 0));
    NaiveDateTime::from_timestamp(seconds, nanos as u32).format("%Y-%m-%dT%H:%S:%S%.f")
}
