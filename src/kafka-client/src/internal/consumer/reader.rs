// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Decoding of fetched record sets, on the thread that polls.
//!
//! A [`Chunk`] is one partition's record set from one Fetch response, still
//! compressed. A [`Cursor`] walks it one batch at a time: batch headers are
//! read without decompressing, aborted transactional batches and control
//! batches are skipped as whole batches, and only data batches that are
//! served are decompressed, into one buffer per batch that the returned
//! records share.
//!
//! # Transactions
//!
//! Under `read_committed`, the Fetch response lists the aborted transactions
//! that overlap the fetched range as `(producer id, first offset)`. As in the
//! Java client and librdkafka, a transactional batch is skipped if its
//! producer has an aborted transaction that started at or before the batch's
//! last offset and whose abort marker has not been seen yet. The marker (a
//! control batch with an ABORT record) ends the producer's aborted range.
//!
//! # Positions
//!
//! Every record served, every skipped aborted batch and every control batch
//! moves the application position past it. When a batch is exhausted the
//! position moves to the batch's last offset + 1, which skips offsets that
//! compaction removed from the end of the batch.

use bytes::Bytes;

use crate::error::KafkaError;
use crate::internal::compression::CodecError;
use crate::internal::records::{
    self, BatchHeader, BatchIter, ControlRecordKind, Record, RecordIter,
};
use crate::types::RDKafkaErrorCode;

/// The decompression limit for one batch. Brokers cap batches at
/// `message.max.bytes`, typically far below this, so the limit only guards
/// against corrupt or hostile data.
const MAX_DECOMPRESSED: usize = 1 << 30;

/// One partition's record set from one Fetch response.
#[derive(Debug)]
pub(super) struct Chunk {
    pub records: Bytes,
    /// The offset the fetch asked for. Records below it are skipped: brokers
    /// return whole batches, which may start earlier.
    pub fetch_offset: i64,
    /// Aborted transactions as `(producer id, first offset)`, sorted by first
    /// offset. Empty under `read_uncommitted`.
    pub aborted: Vec<(i64, i64)>,
}

/// The record count a batch contributes to queue accounting.
///
/// A compressed legacy wrapper does not reveal its count (-1) and counts as
/// one message.
pub(super) fn accounted_messages(header: &BatchHeader) -> u64 {
    u64::try_from(header.record_count).unwrap_or(1)
}

/// Effects of [`Cursor::next`] that the caller applies to its partition.
#[derive(Debug, Default)]
pub(super) struct Progress {
    /// The new application position, if it moved.
    pub position: Option<i64>,
    /// Size of the batches that were consumed or skipped, as accounted when
    /// the chunk was queued.
    pub bytes: u64,
    /// Message count of the batches that were consumed or skipped.
    pub msgs: u64,
}

/// What [`Cursor::next`] produced.
#[derive(Debug)]
pub(super) enum Step {
    /// A record to serve. The position already accounts for it.
    Record(Record),
    /// A consumer error, e.g. an undecodable batch, which is skipped.
    Error(KafkaError),
    /// The chunk is exhausted.
    Done,
}

/// The batch whose records are being served.
#[derive(Debug)]
struct OpenBatch {
    last_offset: i64,
    size: u64,
    msgs: u64,
    records: RecordIter,
}

/// Iterates over the servable records of a [`Chunk`].
#[derive(Debug)]
pub(super) struct Cursor {
    batches: BatchIter,
    batch: Option<OpenBatch>,
    /// Records below this offset are skipped.
    next_offset: i64,
    read_committed: bool,
    check_crcs: bool,
    /// Aborted transactions not yet reached, by ascending first offset.
    aborted: std::iter::Peekable<std::vec::IntoIter<(i64, i64)>>,
    /// Producers inside an aborted transaction whose marker is pending.
    aborted_producers: Vec<i64>,
}

impl Cursor {
    pub fn new(chunk: Chunk, read_committed: bool, check_crcs: bool) -> Cursor {
        Cursor {
            batches: records::parse_batches(chunk.records),
            batch: None,
            next_offset: chunk.fetch_offset,
            read_committed,
            check_crcs,
            aborted: chunk.aborted.into_iter().peekable(),
            aborted_producers: Vec::new(),
        }
    }

    /// Advances to the next record to serve, recording position and
    /// accounting changes in `progress`.
    pub fn next(&mut self, progress: &mut Progress) -> Step {
        loop {
            if let Some(batch) = &mut self.batch {
                match batch.records.next() {
                    Some(Ok(record)) => {
                        if record.offset < self.next_offset {
                            continue;
                        }
                        self.next_offset = record.offset + 1;
                        progress.position = Some(self.next_offset);
                        return Step::Record(record);
                    }
                    Some(Err(e)) => {
                        let batch = self.batch.take().expect("checked");
                        progress.bytes += batch.size;
                        progress.msgs += batch.msgs;
                        return Step::Error(codec_error(&e));
                    }
                    None => {
                        let batch = self.batch.take().expect("checked");
                        progress.bytes += batch.size;
                        progress.msgs += batch.msgs;
                        self.skip_to(batch.last_offset, progress);
                        continue;
                    }
                }
            }
            let fetched = match self.batches.next() {
                None => return Step::Done,
                // The fetcher did not account for undecodable entries.
                Some(Err(e)) => return Step::Error(codec_error(&e)),
                Some(Ok(fetched)) => fetched,
            };
            let header = fetched.header();
            let size = u64::try_from(header.size).expect("usize fits u64");
            let msgs = accounted_messages(header);
            if header.last_offset < self.next_offset {
                progress.bytes += size;
                progress.msgs += msgs;
                continue;
            }
            if self.check_crcs {
                if let Err(e) = fetched.verify_crc() {
                    progress.bytes += size;
                    progress.msgs += msgs;
                    return Step::Error(codec_error(&e));
                }
            }
            if self.read_committed && header.producer_id >= 0 {
                self.consume_aborted_up_to(header.last_offset);
                if header.is_control {
                    let is_abort = matches!(
                        fetched.control_record(),
                        Ok(Some(c)) if c.kind == ControlRecordKind::Abort
                    );
                    if is_abort {
                        self.aborted_producers.retain(|p| *p != header.producer_id);
                    }
                } else if header.is_transactional
                    && self.aborted_producers.contains(&header.producer_id)
                {
                    progress.bytes += size;
                    progress.msgs += msgs;
                    self.skip_to(header.last_offset, progress);
                    continue;
                }
            }
            if header.is_control {
                // Control records are never served, but move the position.
                progress.bytes += size;
                progress.msgs += msgs;
                self.skip_to(header.last_offset, progress);
                continue;
            }
            match fetched.records(MAX_DECOMPRESSED) {
                Ok(records) => {
                    self.batch = Some(OpenBatch {
                        last_offset: header.last_offset,
                        size,
                        msgs,
                        records,
                    });
                }
                Err(e) => {
                    progress.bytes += size;
                    progress.msgs += msgs;
                    return Step::Error(codec_error(&e));
                }
            }
        }
    }

    /// Moves the position past `last_offset`, if that is ahead.
    fn skip_to(&mut self, last_offset: i64, progress: &mut Progress) {
        if last_offset >= self.next_offset {
            self.next_offset = last_offset + 1;
            progress.position = Some(self.next_offset);
        }
    }

    fn consume_aborted_up_to(&mut self, offset: i64) {
        while let Some((producer_id, _)) = self.aborted.next_if(|(_, first)| *first <= offset) {
            if !self.aborted_producers.contains(&producer_id) {
                self.aborted_producers.push(producer_id);
            }
        }
    }
}

/// The consumer error for an undecodable batch, as librdkafka reports it.
fn codec_error(e: &CodecError) -> KafkaError {
    let code = match e {
        CodecError::UnsupportedCompression(_)
        | CodecError::Decompress { .. }
        | CodecError::TooLarge { .. } => RDKafkaErrorCode::BadCompression,
        _ => RDKafkaErrorCode::BadMessage,
    };
    KafkaError::MessageConsumption(code)
}
