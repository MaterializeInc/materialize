// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Deduplication of batches from idempotent producers.
//!
//! An idempotent producer numbers its records per partition, starting at 0, and stamps each
//! batch with its producer ID, epoch and the sequence number of its first record. A producer that
//! does not learn whether a batch was appended sends the same batch again, with the same numbers.
//! [`ProducerStates`] remembers the last [`RETAINED_BATCHES`] batches of each producer, so that
//! a resent batch is answered with the offsets it was first appended at instead of being
//! appended again.
//!
//! The rules follow the Kafka broker: an older epoch is fenced, a newer epoch starts over at
//! sequence 0, and within an epoch a batch must continue right after the last one.

use std::collections::{BTreeMap, VecDeque};

use crate::ResponseError;

/// How many recent batches are remembered per producer.
///
/// Kafka producers keep at most 5 requests in flight per connection when idempotence is
/// enabled, so only the last 5 batches can be resent.
pub const RETAINED_BATCHES: usize = 5;

/// The idempotence fields of a produced batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatchId {
    /// The producer ID, assigned by `InitProducerId`.
    pub producer_id: i64,
    /// The producer epoch.
    pub epoch: i16,
    /// The sequence number of the batch's first record.
    pub first_sequence: i32,
}

/// A batch that a producer appended, as remembered for deduplication.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendedBatch {
    /// The sequence number of the batch's first record.
    pub first_sequence: i32,
    /// The sequence number of the batch's last record.
    pub last_sequence: i32,
    /// The offset of the batch's first record.
    pub base_offset: i64,
    /// When the batch was appended, in milliseconds since the Unix epoch.
    pub appended_at_ms: i64,
}

/// What to do with a produced batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Append the batch.
    Append,
    /// The batch was already appended at `base_offset`. Acknowledge it with that offset.
    Duplicate { base_offset: i64 },
    /// Reject the batch.
    Reject(ResponseError),
}

/// The recent batches of every idempotent producer that writes to a topic.
#[derive(Debug, Default, Clone)]
pub struct ProducerStates {
    producers: BTreeMap<i64, ProducerState>,
}

#[derive(Debug, Clone)]
struct ProducerState {
    epoch: i16,
    /// The most recent batches, oldest first.
    batches: VecDeque<AppendedBatch>,
}

impl ProducerStates {
    /// Decides what to do with a batch of `record_count` records.
    pub fn admit(&self, id: &ProducerBatchId, record_count: usize) -> Admission {
        let Some(last_sequence) = last_sequence(id.first_sequence, record_count) else {
            return Admission::Reject(ResponseError::InvalidRecord);
        };
        let Some(state) = self.producers.get(&id.producer_id) else {
            // NOTE: Kafka brokers also accept an unknown producer at any sequence, because its
            // state may have expired while the producer was idle.
            return Admission::Append;
        };
        if id.epoch < state.epoch {
            return Admission::Reject(ResponseError::InvalidProducerEpoch);
        }
        if id.epoch > state.epoch {
            return if id.first_sequence == 0 {
                Admission::Append
            } else {
                Admission::Reject(ResponseError::OutOfOrderSequenceNumber)
            };
        }
        if let Some(batch) = state
            .batches
            .iter()
            .find(|b| b.first_sequence == id.first_sequence && b.last_sequence == last_sequence)
        {
            return Admission::Duplicate {
                base_offset: batch.base_offset,
            };
        }
        match state.batches.back() {
            Some(newest) if next_sequence(newest.last_sequence) != id.first_sequence => {
                Admission::Reject(ResponseError::OutOfOrderSequenceNumber)
            }
            _ => Admission::Append,
        }
    }

    /// Records that a batch admitted by [`ProducerStates::admit`] was appended at `base_offset`.
    ///
    /// Returns the batch, and the batches that are no longer remembered because of it, with
    /// their epochs.
    pub fn record(
        &mut self,
        id: &ProducerBatchId,
        record_count: usize,
        base_offset: i64,
        appended_at_ms: i64,
    ) -> (AppendedBatch, Vec<(i16, AppendedBatch)>) {
        let batch = AppendedBatch {
            first_sequence: id.first_sequence,
            last_sequence: last_sequence(id.first_sequence, record_count)
                .expect("admitted batches have a valid sequence range"),
            base_offset,
            appended_at_ms,
        };
        let evicted = self.restore(id.producer_id, id.epoch, batch);
        (batch, evicted)
    }

    /// Adds a batch that was appended earlier, for example when reading back a log.
    ///
    /// Batches must be restored in the order they were appended. Returns the batches that are no
    /// longer remembered because of this one, with their epochs.
    pub fn restore(
        &mut self,
        producer_id: i64,
        epoch: i16,
        batch: AppendedBatch,
    ) -> Vec<(i16, AppendedBatch)> {
        let mut evicted = Vec::new();
        let state = self
            .producers
            .entry(producer_id)
            .or_insert_with(|| ProducerState {
                epoch,
                batches: VecDeque::new(),
            });
        if epoch > state.epoch {
            evicted.extend(state.batches.drain(..).map(|b| (state.epoch, b)));
            state.epoch = epoch;
        }
        state.batches.push_back(batch);
        while state.batches.len() > RETAINED_BATCHES {
            let oldest = state.batches.pop_front().expect("not empty");
            evicted.push((state.epoch, oldest));
        }
        evicted
    }

    /// Forgets producers that have not appended since `cutoff_ms`.
    ///
    /// Returns the forgotten batches, with their producer IDs and epochs.
    pub fn expire(&mut self, cutoff_ms: i64) -> Vec<(i64, i16, AppendedBatch)> {
        let mut evicted = Vec::new();
        self.producers.retain(|producer_id, state| {
            let keep = state
                .batches
                .back()
                .is_some_and(|b| b.appended_at_ms >= cutoff_ms);
            if !keep {
                evicted.extend(
                    state
                        .batches
                        .iter()
                        .map(|b| (*producer_id, state.epoch, *b)),
                );
            }
            keep
        });
        evicted
    }

    /// Returns whether the batch of `producer_id` that starts at `base_offset` is still
    /// remembered.
    pub fn retains(&self, producer_id: i64, base_offset: i64) -> bool {
        self.producers
            .get(&producer_id)
            .is_some_and(|s| s.batches.iter().any(|b| b.base_offset == base_offset))
    }

    /// The number of producers remembered.
    pub fn len(&self) -> usize {
        self.producers.len()
    }

    /// Whether no producer is remembered.
    pub fn is_empty(&self) -> bool {
        self.producers.is_empty()
    }
}

/// Sequence numbers wrap around from `i32::MAX` to 0.
fn next_sequence(sequence: i32) -> i32 {
    if sequence == i32::MAX {
        0
    } else {
        sequence + 1
    }
}

/// Returns the sequence number of the last record of a batch, or `None` if the batch is empty or
/// its sequence range is invalid.
fn last_sequence(first_sequence: i32, record_count: usize) -> Option<i32> {
    if first_sequence < 0 || record_count == 0 {
        return None;
    }
    let count = i64::try_from(record_count).ok()?;
    let span = i64::from(i32::MAX) + 1;
    let last = (i64::from(first_sequence) + count - 1) % span;
    i32::try_from(last).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(producer_id: i64, epoch: i16, first_sequence: i32) -> ProducerBatchId {
        ProducerBatchId {
            producer_id,
            epoch,
            first_sequence,
        }
    }

    #[mz_ore::test]
    fn duplicates_get_their_original_offset() {
        let mut states = ProducerStates::default();
        assert_eq!(states.admit(&id(7, 0, 0), 3), Admission::Append);
        states.record(&id(7, 0, 0), 3, 100, 0);
        assert_eq!(states.admit(&id(7, 0, 3), 2), Admission::Append);
        states.record(&id(7, 0, 3), 2, 103, 0);

        assert_eq!(
            states.admit(&id(7, 0, 0), 3),
            Admission::Duplicate { base_offset: 100 }
        );
        assert_eq!(
            states.admit(&id(7, 0, 3), 2),
            Admission::Duplicate { base_offset: 103 }
        );
        assert_eq!(states.admit(&id(7, 0, 5), 1), Admission::Append);
    }

    #[mz_ore::test]
    fn gaps_and_old_epochs_are_rejected() {
        let mut states = ProducerStates::default();
        states.record(&id(7, 1, 0), 2, 0, 0);
        assert_eq!(
            states.admit(&id(7, 1, 5), 1),
            Admission::Reject(ResponseError::OutOfOrderSequenceNumber)
        );
        assert_eq!(
            states.admit(&id(7, 0, 2), 1),
            Admission::Reject(ResponseError::InvalidProducerEpoch)
        );
        // A new epoch must start over.
        assert_eq!(
            states.admit(&id(7, 2, 2), 1),
            Admission::Reject(ResponseError::OutOfOrderSequenceNumber)
        );
        assert_eq!(states.admit(&id(7, 2, 0), 1), Admission::Append);
        states.record(&id(7, 2, 0), 1, 2, 0);
        assert_eq!(
            states.admit(&id(7, 1, 2), 1),
            Admission::Reject(ResponseError::InvalidProducerEpoch)
        );
    }

    #[mz_ore::test]
    fn only_recent_batches_are_remembered() {
        let mut states = ProducerStates::default();
        let mut evicted = Vec::new();
        for i in 0..10 {
            evicted.extend(states.record(&id(7, 0, i), 1, i64::from(i), 0).1);
        }
        let evicted_offsets: Vec<_> = evicted.iter().map(|(_, b)| b.base_offset).collect();
        assert_eq!(evicted_offsets, vec![0, 1, 2, 3, 4]);
        assert!(!states.retains(7, 4));
        assert!(states.retains(7, 5));
        assert_eq!(
            states.admit(&id(7, 0, 4), 1),
            Admission::Reject(ResponseError::OutOfOrderSequenceNumber)
        );
        assert_eq!(
            states.admit(&id(7, 0, 9), 1),
            Admission::Duplicate { base_offset: 9 }
        );
    }

    #[mz_ore::test]
    fn sequences_wrap_around() {
        let mut states = ProducerStates::default();
        states.record(&id(7, 0, i32::MAX - 1), 2, 0, 0);
        assert_eq!(states.admit(&id(7, 0, 0), 1), Admission::Append);
        assert_eq!(
            states.admit(&id(7, 0, i32::MAX - 1), 2),
            Admission::Duplicate { base_offset: 0 }
        );
    }

    #[mz_ore::test]
    fn idle_producers_expire() {
        let mut states = ProducerStates::default();
        states.record(&id(1, 0, 0), 1, 0, 1_000);
        states.record(&id(2, 0, 0), 1, 1, 5_000);
        let expired = states.expire(2_000);
        assert_eq!(expired.len(), 1);
        assert_eq!(expired[0].0, 1);
        assert!(!states.retains(1, 0));
        assert!(states.retains(2, 1));
        // An expired producer is accepted again, like an unknown one.
        assert_eq!(states.admit(&id(1, 0, 1), 1), Admission::Append);
    }
}
