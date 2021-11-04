// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! See [`DebeziumDeduplicationStrategy`] for what this module is meant to do

use std::cmp::{max, Ordering};
use std::collections::{HashMap, HashSet};

use anyhow::bail;
use chrono::format::{DelayedFormat, StrftimeItems};
use chrono::NaiveDateTime;
use differential_dataflow::Collection;
use log::{debug, error, info, warn};
use repr::Diff;
use repr::Timestamp;
use repr::{Datum, Row};
use serde::{Deserialize, Serialize};
use timely::dataflow::Scope;

/// Ordered means we can trust Debezium high water marks
///
/// In standard operation, Debezium should always emit messages in position order, but
/// messages may be duplicated.
///
/// For example, this is a legal stream of Debezium event positions:
///
/// ```text
/// 1 2 3 2
/// ```
///
/// Note that `2` appears twice, but the *first* time it appeared it appeared in order.
/// Any position below the highest-ever seen position is guaranteed to be a duplicate,
/// and can be ignored.
///
/// Now consider this stream:
///
/// ```text
/// 1 3 2
/// ```
///
/// In this case, `2` is sent *out* of order, and if it is ignored we will miss important
/// state.
///
/// It is possible for users to do things with multiple databases and multiple Debezium
/// instances pointing at the same Kafka topic that mean that the Debezium guarantees do
/// not hold, in which case we are required to track individual messages, instead of just
/// the highest-ever-seen message.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
pub enum DebeziumDeduplicationStrategy {
    /// Do not perform any deduplication
    ///
    /// This should mostly be used with envelope upsert
    None,
    /// We can trust high water mark
    Ordered,
    /// We need to store some piece of state for every message
    Full,
    FullInRange {
        pad_start: Option<NaiveDateTime>,
        start: NaiveDateTime,
        end: NaiveDateTime,
    },
}

impl DebeziumDeduplicationStrategy {
    /// Create a deduplication strategy with start and end times
    ///
    /// Returns an error if either datetime does not parse, or if there is no time in between them
    pub fn full_in_range(
        start: &str,
        end: &str,
        pad_start: Option<&str>,
    ) -> anyhow::Result<DebeziumDeduplicationStrategy> {
        let fallback_parse = |s: &str| {
            for format in &["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M:%S"] {
                if let Ok(dt) = NaiveDateTime::parse_from_str(s, format) {
                    return Ok(dt);
                }
            }
            if let Ok(d) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                return Ok(d.and_hms(0, 0, 0));
            }

            bail!(
                "UTC DateTime specifier '{}' should match 'YYYY-MM-DD', 'YYYY-MM-DD HH:MM:SS' or \
                   'YYYY-MM-DD HH:MM:SS.FF",
                s
            )
        };

        let start = fallback_parse(start)?;
        let end = fallback_parse(end)?;
        let pad_start = pad_start.map(fallback_parse).transpose()?;

        if start >= end {
            bail!(
                "Debezium deduplication start {} is not before end {}",
                start,
                end
            );
        }
        Ok(DebeziumDeduplicationStrategy::FullInRange {
            start,
            end,
            pad_start,
        })
    }

    pub fn render<G: Scope<Timestamp = Timestamp>>(
        self,
        collection: Collection<G, Row, Diff>,
        debug_name: String,
        worker_index: usize,
        arity: usize,
        key_indices: Option<Vec<usize>>,
    ) -> Collection<G, Row, Diff> {
        let mut state = DebeziumDeduplicationState::new(self, key_indices);
        collection.flat_map(move |row| {
            let should_use = state
                .as_mut()
                .map(|d| {
                    d.should_use_record(
                        None,
                        &debug_name,
                        worker_index,
                        &row,
                        // Debezium decoding always adds two extra columns to the end of the record: one with the deduplication position,
                        // and one with the upstream time in milliseconds.
                        // Since these are the last two datums in the row, they are at `arity - 2` and `arity - 1`, respectively.
                        arity - 2,
                        arity - 1,
                    )
                })
                .unwrap_or(true);
            if should_use {
                let projected = row.into_iter().take(arity - 2);
                Some(Row::pack(projected))
            } else {
                None
            }
        })
    }
}

/// Track whether or not we should skip a specific debezium message
///
/// The goal of deduplication is to omit sending true duplicates -- the exact
/// same record being sent into materialize twice. That means that we create
/// one deduplicator per timely worker and use use timely key sharding
/// normally. But it also means that no single deduplicator knows the
/// highest-ever seen binlog offset.
#[derive(Debug)]
pub(crate) struct DebeziumDeduplicationState {
    /// Last recorded binlog position and connector offset
    ///
    /// [`DebeziumDeduplicationStrategy`] determines whether messages that are not ahead
    /// of the last recorded position will be skipped.
    last_position_and_offset: Option<(Row, Option<i64>)>,
    /// Whether or not to track every message we've ever seen
    full: Option<TrackFull>,
    /// Whether we have printed a warning due to seeing unknown source coordinates
    warned_on_unknown: bool,
    messages_processed: u64,
}

/// If we need to deal with debezium possibly going back after it hasn't seen things.
/// During normal (non-snapshot) operation, we deduplicate based on binlog position: (pos, row), for MySQL.
/// During the initial snapshot, (pos, row) values are all the same, but primary keys
/// are unique and thus we can get deduplicate based on those.
#[derive(Debug)]
struct TrackFull {
    /// binlog position to (timestamp that this binlog entry was first seen)
    seen_positions: HashMap<Row, i64>,
    seen_snapshot_keys: HashSet<Row>,
    /// The highest-ever seen timestamp, used in logging to let us know how far backwards time might go
    max_seen_time: i64,
    key_indices: Option<Vec<usize>>,
    /// Optimization to avoid re-allocating the row over and over when extracting the key.
    key_buf: Row,
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
#[derive(Debug)]
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
    fn from_keys(mut key_indices: Option<Vec<usize>>) -> Self {
        if let Some(key_indices) = key_indices.as_mut() {
            key_indices.sort_unstable();
        }
        Self {
            seen_positions: Default::default(),
            seen_snapshot_keys: Default::default(),
            max_seen_time: 0,
            key_indices,
            key_buf: Default::default(),
            range: None,
            started_padding: false,
            started: false,
        }
    }

    fn from_keys_in_range(
        key_indices: Option<Vec<usize>>,
        start: NaiveDateTime,
        end: NaiveDateTime,
        pad_start: Option<NaiveDateTime>,
    ) -> Self {
        let mut tracker = Self::from_keys(key_indices);
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

fn cmp_by_datum(left: &Row, right: &Row) -> Ordering {
    left.iter().cmp(right.iter())
}

impl DebeziumDeduplicationState {
    pub(crate) fn new(
        strat: DebeziumDeduplicationStrategy,
        key_indices: Option<Vec<usize>>,
    ) -> Option<Self> {
        if matches!(strat, DebeziumDeduplicationStrategy::None) {
            return None;
        }
        let full = match strat {
            DebeziumDeduplicationStrategy::Ordered | DebeziumDeduplicationStrategy::None => None,
            DebeziumDeduplicationStrategy::Full => Some(TrackFull::from_keys(key_indices)),
            DebeziumDeduplicationStrategy::FullInRange {
                start,
                end,
                pad_start,
            } => Some(TrackFull::from_keys_in_range(
                key_indices,
                start,
                end,
                pad_start,
            )),
        };
        Some(DebeziumDeduplicationState {
            last_position_and_offset: Default::default(),
            full,
            warned_on_unknown: false,
            messages_processed: 0,
        })
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn should_use_record(
        &mut self,
        connector_offset: Option<i64>,
        debug_name: &str,
        worker_idx: usize,
        update: &Row,
        binlog_position_index: usize,
        upstream_time_millis_index: usize,
    ) -> bool {
        let binlog_position = update.iter().nth(binlog_position_index).unwrap();
        let upstream_time_millis = match update.iter().nth(upstream_time_millis_index).unwrap() {
            Datum::Int64(i) => Some(i),
            Datum::Null => None,
            _ => panic!(),
        };

        self.messages_processed += 1;

        // If in the initial snapshot, binlog position is meaningless for detecting
        // duplicates, since it is always the same.
        let binlog_position = match binlog_position {
            Datum::Null => None,
            Datum::List(list) => Some(Row::pack(list.iter())),
            _ => unreachable!(),
        };
        let should_skip = match &binlog_position {
            None => None,
            Some(position) => match &mut self.last_position_and_offset {
                Some((old_position, old_offset)) => {
                    if cmp_by_datum(position, old_position) == Ordering::Greater {
                        *old_position = position.clone();
                        None
                    } else {
                        Some(SkipInfo {
                            old_position,
                            old_offset,
                        })
                    }
                }
                None => {
                    self.last_position_and_offset = Some((position.clone(), connector_offset));
                    None
                }
            },
        };

        let mut delete_full = false;
        let should_use = match &mut self.full {
            // Always none if in snapshot, see comment above where `should_skip` is bound.
            None => should_skip.is_none(),
            Some(TrackFull {
                seen_positions,
                seen_snapshot_keys,
                max_seen_time,
                key_indices,
                key_buf,
                range,
                started_padding,
                started,
            }) => {
                *max_seen_time = max(upstream_time_millis.unwrap_or(0), *max_seen_time);
                if binlog_position.is_none() {
                    let key_indices = match key_indices.as_ref() {
                        None => {
                            // No keys, so we can't do anything sensible for snapshots.
                            // Return "all OK" and hope their data isn't corrupted.
                            return true;
                        }
                        Some(ki) => ki,
                    };
                    let mut after_iter = match update.iter().nth(1) {
                        Some(Datum::List(after_elts)) => after_elts.iter(),
                        _ => {
                            error!(
                                "Snapshot row at connector offset {:?}, message_time={} source={} was not an insert.",
                                connector_offset, fmt_timestamp(upstream_time_millis), debug_name);
                            return false;
                        }
                    };
                    let key = {
                        let mut cumsum = 0;
                        for k in key_indices.iter() {
                            let adjusted_idx = *k - cumsum;
                            cumsum += adjusted_idx + 1;
                            key_buf.push(after_iter.nth(adjusted_idx).unwrap());
                        }
                        key_buf.finish_and_reuse()
                    };

                    // Your reaction on reading this code might be:
                    // "Ugh, we are cloning the key row just to support logging a warning!"
                    // But don't worry -- since `Row`s use a 24-byte smallvec, the clone
                    // won't involve an extra allocation unless the key overflows that.
                    //
                    // Anyway, TODO: avoid this via `get_or_insert` once rust-lang/rust#60896 is resolved.
                    let is_new = seen_snapshot_keys.insert(key.clone());
                    if !is_new {
                        warn!(
                                "Snapshot row with key={:?} source={} seen multiple times (most recent message_time={})",
                                key, debug_name, fmt_timestamp(upstream_time_millis)
                            );
                    }
                    is_new
                } else {
                    let position = binlog_position.unwrap();
                    // first check if we are in a special case of range-bounded track full
                    if let Some(range) = range {
                        if let Some(upstream_time_millis) = upstream_time_millis {
                            if upstream_time_millis < range.pad_start {
                                if *started_padding {
                                    warn!("went back to before padding start, after entering padding \
                                               source={}:{} message_time={} messages_processed={}",
                                              debug_name, worker_idx, fmt_timestamp(upstream_time_millis),
                                              self.messages_processed);
                                }
                                if *started {
                                    warn!("went back to before padding start, after entering full dedupe \
                                               source={}:{} message_time={} messages_processed={}",
                                              debug_name, worker_idx, fmt_timestamp(upstream_time_millis),
                                              self.messages_processed);
                                }
                                *started_padding = false;
                                *started = false;
                                return should_skip.is_none();
                            }
                            if upstream_time_millis < range.start {
                                // in the padding time range
                                *started_padding = true;
                                if *started {
                                    warn!("went back to before padding start, after entering full dedupe \
                                               source={}:{} message_time={} messages_processed={}",
                                              debug_name, worker_idx, fmt_timestamp(upstream_time_millis),
                                              self.messages_processed);
                                }
                                *started = false;

                                if seen_positions.get(&position).is_none() {
                                    seen_positions.insert(position, upstream_time_millis);
                                }
                                return should_skip.is_none();
                            }
                            if upstream_time_millis <= range.end && !*started {
                                *started = true;
                                info!(
                                    "starting full deduplication source={}:{} buffer_size={} \
                                         messages_processed={} message_time={}",
                                    debug_name,
                                    worker_idx,
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
                            seen_positions.insert(position.clone(), 0);
                        }
                    }

                    // Now we know that we are in either trackfull or a range-bounded trackfull
                    let seen = seen_positions.entry(position.clone());
                    let is_new = matches!(seen, std::collections::hash_map::Entry::Vacant(_));
                    let original_time = seen.or_insert_with(|| upstream_time_millis.unwrap_or(0));

                    log_duplication_info(
                        position,
                        connector_offset,
                        upstream_time_millis,
                        debug_name,
                        worker_idx,
                        is_new,
                        &should_skip,
                        original_time,
                        max_seen_time,
                    );

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
            self.full = None;
        }
        should_use
    }
}

/// Helper to track information for logging on deduplication
struct SkipInfo<'a> {
    old_position: &'a Row,
    old_offset: &'a Option<i64>,
}

#[allow(clippy::too_many_arguments)]
fn log_duplication_info(
    position: Row,
    connector_offset: Option<i64>,
    upstream_time_millis: Option<i64>,
    debug_name: &str,
    worker_idx: usize,
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
                "Created a new record behind the highest point in source={}:{} \
                 new deduplication position: {:?}, new connector offset: {}, \
                 old deduplication position: {:?} \
                 message_time={} max_seen_time={}",
                debug_name,
                worker_idx,
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
                "already ingested source={}:{} new deduplication position: {:?}, \
                 old deduplication position: {:?}\
                 connector offset={} message_time={} message_first_seen={} max_seen_time={}",
                debug_name,
                worker_idx,
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
