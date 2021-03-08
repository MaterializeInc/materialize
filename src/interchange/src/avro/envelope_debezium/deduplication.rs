// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! See [`DebeziumDeduplicationStrategy`] for what this module is meant to do

use std::borrow::Cow;
use std::cmp::max;
use std::collections::{HashMap, HashSet};

use anyhow::bail;
use chrono::format::{DelayedFormat, StrftimeItems};
use chrono::NaiveDateTime;
use log::{debug, error, info, warn};
use repr::{Datum, Row, RowPacker};
use serde::{Deserialize, Serialize};

use super::RowCoordinates;

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
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub enum DebeziumDeduplicationStrategy {
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
    /// Last recorded (pos, row, offset) for each binlog stream.
    ///
    /// A binlog stream is either a file name (for mysql) or "" for postgres.
    ///
    /// [`DebeziumDeduplicationStrategy`] determines whether messages that are not ahead
    /// of the last recorded pos/row will be skipped.
    binlog_offsets: HashMap<Vec<u8>, (usize, usize, Option<i64>)>,
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
    /// binlog filename to (offset to (timestamp that this binlog entry was first seen))
    seen_offsets: HashMap<Box<[u8]>, HashMap<(usize, usize), i64>>,
    seen_snapshot_keys: HashMap<Box<[u8]>, HashSet<Row>>,
    /// The highest-ever seen timestamp, used in logging to let us know how far backwards time might go
    max_seen_time: i64,
    key_indices: Option<Vec<usize>>,
    /// Optimization to avoid re-allocating the row packer over and over when extracting the key..
    key_buf: RowPacker,
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
    /// At some point we need to start trusting the [`TrackFull::seen_offsets`] map more
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
            seen_offsets: Default::default(),
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

impl DebeziumDeduplicationState {
    pub(crate) fn new(
        strat: DebeziumDeduplicationStrategy,
        key_indices: Option<Vec<usize>>,
    ) -> Self {
        let full = match strat {
            DebeziumDeduplicationStrategy::Ordered => None,
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
        DebeziumDeduplicationState {
            binlog_offsets: Default::default(),
            full,
            warned_on_unknown: false,
            messages_processed: 0,
        }
    }

    #[must_use]
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn should_use_record(
        &mut self,
        file: &[u8],
        row: RowCoordinates,
        coord: Option<i64>,
        upstream_time_millis: Option<i64>,
        debug_name: &str,
        worker_idx: usize,
        is_snapshot: bool,
        update: &Row,
    ) -> bool {
        let (pos, row) = match row {
            RowCoordinates::MySql { pos, row } => (pos, row),
            RowCoordinates::Postgres { lsn, total_order } => (lsn, total_order.unwrap_or(0)),
            RowCoordinates::MSSql {
                change_lsn,
                event_serial_no,
            } => {
                // Consider everything but the file ID to be the offset within the file.
                let offset_in_file =
                    ((change_lsn.log_block_offset as usize) << 16) | (change_lsn.slot_num as usize);
                (offset_in_file, event_serial_no)
            }
            RowCoordinates::Unknown => {
                if !self.warned_on_unknown {
                    self.warned_on_unknown = true;
                    log::warn!("Record with unrecognized source coordinates in {}. You might be using an unsupported upstream database.", debug_name);
                }
                return true;
            }
        };
        self.messages_processed += 1;

        // If in the initial snapshot, binlog (pos, row) is meaningless for detecting
        // duplicates, since it is always the same.
        let should_skip = if is_snapshot {
            None
        } else {
            match self.binlog_offsets.get_mut(file) {
                Some((old_max_pos, old_max_row, old_offset)) => {
                    if (*old_max_pos, *old_max_row) >= (pos, row) {
                        Some(SkipInfo {
                            old_max_pos,
                            old_max_row,
                            old_offset,
                        })
                    } else {
                        // update the debezium high water mark
                        *old_max_pos = pos;
                        *old_max_row = row;
                        *old_offset = coord;
                        None
                    }
                }
                None => {
                    // The extra lookup is fine - this is the cold path.
                    self.binlog_offsets
                        .insert(file.to_owned(), (pos, row, coord));
                    None
                }
            }
        };

        let mut delete_full = false;
        let should_use = match &mut self.full {
            // Always none if in snapshot, see comment above where `should_skip` is bound.
            None => should_skip.is_none(),
            Some(TrackFull {
                seen_offsets,
                seen_snapshot_keys,
                max_seen_time,
                key_indices,
                key_buf,
                range,
                started_padding,
                started,
            }) => {
                *max_seen_time = max(upstream_time_millis.unwrap_or(0), *max_seen_time);
                if is_snapshot {
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
                                "Snapshot row at pos {:?}, message_time={} source={} was not an insert.",
                                coord, fmt_timestamp(upstream_time_millis), debug_name);
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

                    if let Some(seen_keys) = seen_snapshot_keys.get_mut(file) {
                        // Your reaction on reading this code might be:
                        // "Ugh, we are cloning the key row just to support logging a warning!"
                        // But don't worry -- since `Row`s use a 16-byte smallvec, the clone
                        // won't involve an extra allocation unless the key overflows that.
                        //
                        // Anyway, TODO: avoid this via `get_or_insert` once rust-lang/rust#60896 is resolved.
                        let is_new = seen_keys.insert(key.clone());
                        if !is_new {
                            warn!(
                                "Snapshot row with key={:?} source={} seen multiple times (most recent message_time={})",
                                key, debug_name, fmt_timestamp(upstream_time_millis)
                            );
                        }
                        is_new
                    } else {
                        let mut hs = HashSet::new();
                        hs.insert(key);
                        seen_snapshot_keys.insert(file.into(), hs);
                        true
                    }
                } else {
                    if let Some(seen_offsets) = seen_offsets.get_mut(file) {
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

                                    seen_offsets
                                        .entry((pos, row))
                                        .or_insert_with(|| upstream_time_millis);
                                    return should_skip.is_none();
                                }
                                if upstream_time_millis <= range.end && !*started {
                                    *started = true;
                                    info!(
                                        "starting full deduplication source={}:{} buffer_size={} \
                                         messages_processed={} message_time={}",
                                        debug_name,
                                        worker_idx,
                                        seen_offsets.len(),
                                        self.messages_processed,
                                        fmt_timestamp(upstream_time_millis)
                                    );
                                }
                                if upstream_time_millis > range.end {
                                    // don't abort early, but we will clean up after this validation
                                    delete_full = true;
                                }
                            } else {
                                warn!(
                                    "message has no creation time file_position={}:{}:{}",
                                    file_name_repr(file),
                                    pos,
                                    row,
                                );
                                seen_offsets.insert((pos, row), 0);
                            }
                        }

                        // Now we know that we are in either trackfull or a range-bounded trackfull
                        let seen = seen_offsets.entry((pos, row));
                        let is_new = matches!(seen, std::collections::hash_map::Entry::Vacant(_));
                        let original_time =
                            seen.or_insert_with(|| upstream_time_millis.unwrap_or(0));

                        log_duplication_info(
                            file,
                            pos,
                            row,
                            coord,
                            upstream_time_millis,
                            debug_name,
                            worker_idx,
                            is_new,
                            &should_skip,
                            original_time,
                            max_seen_time,
                        );

                        is_new
                    } else {
                        let mut hs = HashMap::new();
                        hs.insert((pos, row), upstream_time_millis.unwrap_or(0));
                        seen_offsets.insert(file.into(), hs);
                        true
                    }
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
    old_max_pos: &'a usize,
    old_max_row: &'a usize,
    old_offset: &'a Option<i64>,
}

#[allow(clippy::too_many_arguments)]
fn log_duplication_info(
    file: &[u8],
    pos: usize,
    row: usize,
    coord: Option<i64>,
    upstream_time_millis: Option<i64>,
    debug_name: &str,
    worker_idx: usize,
    is_new: bool,
    should_skip: &Option<SkipInfo>,
    original_time: &i64,
    max_seen_time: &i64,
) {
    let file_name = file_name_repr(file);
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
                "Created a new record behind the highest point in source={}:{} binlog_file={} \
                 new_record_position={}:{} new_record_kafka_offset={} old_max_position={}:{} \
                 message_time={} max_seen_time={}",
                debug_name,
                worker_idx,
                file_name,
                pos,
                row,
                coord.unwrap_or(-1),
                skipinfo.old_max_pos,
                skipinfo.old_max_row,
                fmt_timestamp(upstream_time_millis),
                fmt_timestamp(*max_seen_time),
            );
        }
        // Duplicate item below the highest seen item
        (false, Some(skipinfo)) => {
            debug!(
                "already ingested source={}:{} binlog_coordinates={}:{}:{} old_binlog={}:{} \
                 kafka_offset={} message_time={} message_first_seen={} max_seen_time={}",
                debug_name,
                worker_idx,
                file_name,
                pos,
                row,
                skipinfo.old_max_pos,
                skipinfo.old_max_row,
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
                    is beyond the highest record we've ever seen. {}:{}:{} kafka_offset={} \
                    message_time={} message_first_seen={} max_seen_time={}",
                file_name,
                pos,
                row,
                coord.unwrap_or(-1),
                fmt_timestamp(upstream_time_millis),
                fmt_timestamp(*original_time),
                fmt_timestamp(*max_seen_time),
            );
        }
    }
}

/// Try to deocde a file name, otherwise use its hex-encoded repr
///
/// We map the top portion of MSSQL LSNs to the file-name part of other
/// system's LSNs.
fn file_name_repr<'a>(file: &'a [u8]) -> Cow<'a, str> {
    match std::str::from_utf8(file) {
        Ok(s) => Cow::from(s),
        Err(_) => Cow::from(hex::encode(file)),
    }
}

fn fmt_timestamp(ts: impl Into<Option<i64>>) -> DelayedFormat<StrftimeItems<'static>> {
    let (seconds, nanos) = ts
        .into()
        .map(|ts| (ts / 1000, (ts % 1000) * 1_000_000))
        .unwrap_or((0, 0));
    NaiveDateTime::from_timestamp(seconds, nanos as u32).format("%Y-%m-%dT%H:%S:%S%.f")
}
