// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data structures representing topic, partitions and offsets.
//!
//! Semantics follow librdkafka's `rd_kafka_topic_partition_list_t`: adding a
//! partition always appends (duplicates are allowed), and lookups return the
//! first matching element.

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::error::{IsError, KafkaError, KafkaResult, RDKafkaErrorCode};

/// Partition value of a topic without an assigned partition.
const PARTITION_UNASSIGNED: i32 = -1;

const OFFSET_BEGINNING: i64 = -2;
const OFFSET_END: i64 = -1;
const OFFSET_STORED: i64 = -1000;
const OFFSET_INVALID: i64 = -1001;
const OFFSET_TAIL_BASE: i64 = -2000;

/// A Kafka offset.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum Offset {
    /// Start consuming from the beginning of the partition.
    Beginning,
    /// Start consuming from the end of the partition.
    End,
    /// Start consuming from the stored offset.
    Stored,
    /// Offset not assigned or invalid.
    Invalid,
    /// A specific offset to consume from.
    ///
    /// Note that while the offset is a signed integer, negative offsets will be
    /// rejected when passed to librdkafka.
    Offset(i64),
    /// An offset relative to the end of the partition.
    ///
    /// Note that while the offset is a signed integer, negative offsets will
    /// be rejected when passed to librdkafka.
    OffsetTail(i64),
}

impl Offset {
    /// Converts the integer representation of an offset used by librdkafka to
    /// an `Offset`.
    pub fn from_raw(raw_offset: i64) -> Offset {
        match raw_offset {
            OFFSET_BEGINNING => Offset::Beginning,
            OFFSET_END => Offset::End,
            OFFSET_STORED => Offset::Stored,
            OFFSET_INVALID => Offset::Invalid,
            n if n <= OFFSET_TAIL_BASE => Offset::OffsetTail(-(n - OFFSET_TAIL_BASE)),
            n => Offset::Offset(n),
        }
    }

    /// Converts the `Offset` to the internal integer representation used by
    /// librdkafka.
    ///
    /// Returns `None` if the offset cannot be represented in librdkafka's
    /// internal representation.
    pub fn to_raw(self) -> Option<i64> {
        match self {
            Offset::Beginning => Some(OFFSET_BEGINNING),
            Offset::End => Some(OFFSET_END),
            Offset::Stored => Some(OFFSET_STORED),
            Offset::Invalid => Some(OFFSET_INVALID),
            Offset::Offset(n) if n >= 0 => Some(n),
            Offset::OffsetTail(n) if n > 0 => OFFSET_TAIL_BASE.checked_sub(n),
            Offset::Offset(_) | Offset::OffsetTail(_) => None,
        }
    }
}

/// One element of a [`TopicPartitionList`].
///
/// The offset is atomic because rust-rdkafka lets callers change it through
/// elements obtained from a shared reference to the list.
#[derive(Debug)]
struct TplEntry {
    topic: String,
    partition: i32,
    offset: AtomicI64,
    metadata: String,
    error: RDKafkaErrorCode,
}

impl Clone for TplEntry {
    fn clone(&self) -> TplEntry {
        TplEntry {
            topic: self.topic.clone(),
            partition: self.partition,
            offset: AtomicI64::new(self.offset.load(Ordering::Relaxed)),
            metadata: self.metadata.clone(),
            error: self.error,
        }
    }
}

/// A reference to one element of a [`TopicPartitionList`].
pub struct TopicPartitionListElem<'a> {
    entry: &'a TplEntry,
}

impl<'a> TopicPartitionListElem<'a> {
    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.entry.topic
    }

    /// Returns the optional error associated to the specific entry in the TPL.
    pub fn error(&self) -> KafkaResult<()> {
        let code = self.entry.error;
        if code.is_error() {
            Err(KafkaError::OffsetFetch(code))
        } else {
            Ok(())
        }
    }

    /// Returns the partition number.
    pub fn partition(&self) -> i32 {
        self.entry.partition
    }

    /// Returns the offset.
    pub fn offset(&self) -> Offset {
        Offset::from_raw(self.entry.offset.load(Ordering::Relaxed))
    }

    /// Sets the offset.
    ///
    /// Fails with `SetPartitionOffset(InvalidArgument)` if the offset has no
    /// raw representation.
    pub fn set_offset(&mut self, offset: Offset) -> KafkaResult<()> {
        match offset.to_raw() {
            Some(raw) => {
                self.entry.offset.store(raw, Ordering::Relaxed);
                Ok(())
            }
            None => Err(KafkaError::SetPartitionOffset(
                RDKafkaErrorCode::InvalidArgument,
            )),
        }
    }

    /// Returns the optional metadata associated with the entry.
    pub fn metadata(&self) -> &str {
        &self.entry.metadata
    }
}

impl<'a> PartialEq for TopicPartitionListElem<'a> {
    fn eq(&self, other: &TopicPartitionListElem<'a>) -> bool {
        self.topic() == other.topic()
            && self.partition() == other.partition()
            && self.offset() == other.offset()
            && self.metadata() == other.metadata()
    }
}

/// A list of topic partitions with offsets, per-partition errors and
/// metadata.
#[derive(Clone)]
pub struct TopicPartitionList {
    entries: Vec<TplEntry>,
}

impl TopicPartitionList {
    /// Creates a new empty list with default capacity.
    pub fn new() -> TopicPartitionList {
        TopicPartitionList::with_capacity(5)
    }

    /// Creates a new empty list with the specified capacity.
    pub fn with_capacity(capacity: usize) -> TopicPartitionList {
        TopicPartitionList {
            entries: Vec::with_capacity(capacity),
        }
    }

    /// Returns the number of elements in the list.
    pub fn count(&self) -> usize {
        self.entries.len()
    }

    /// Returns the capacity of the list.
    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// Adds a topic with unassigned partitions to the list.
    pub fn add_topic_unassigned<'a>(&'a mut self, topic: &str) -> TopicPartitionListElem<'a> {
        self.add_partition(topic, PARTITION_UNASSIGNED)
    }

    /// Adds a topic and partition to the list, with offset
    /// [`Offset::Invalid`].
    ///
    /// The element is appended even if the list already contains the same
    /// topic and partition.
    pub fn add_partition<'a>(
        &'a mut self,
        topic: &str,
        partition: i32,
    ) -> TopicPartitionListElem<'a> {
        self.push(topic, partition, OFFSET_INVALID);
        TopicPartitionListElem {
            entry: self.entries.last().expect("just pushed"),
        }
    }

    /// Adds a range of partitions, `start_partition..=stop_partition`, to the
    /// list.
    pub fn add_partition_range(&mut self, topic: &str, start_partition: i32, stop_partition: i32) {
        for partition in start_partition..=stop_partition {
            self.push(topic, partition, OFFSET_INVALID);
        }
    }

    /// Sets the offset of the first element matching the topic and partition.
    ///
    /// Fails with `SetPartitionOffset(UnknownPartition)` if there is no such
    /// element and `SetPartitionOffset(InvalidArgument)` if the offset has no
    /// raw representation.
    pub fn set_partition_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: Offset,
    ) -> KafkaResult<()> {
        let Some(raw) = offset.to_raw() else {
            return Err(KafkaError::SetPartitionOffset(
                RDKafkaErrorCode::InvalidArgument,
            ));
        };
        match self.find_entry(topic, partition) {
            Some(entry) => {
                entry.offset.store(raw, Ordering::Relaxed);
                Ok(())
            }
            None => Err(KafkaError::SetPartitionOffset(
                RDKafkaErrorCode::UnknownPartition,
            )),
        }
    }

    /// Adds a topic and partition to the list, with the specified offset.
    ///
    /// NOTE: As in librdkafka this appends a new element and then sets the
    /// offset of the *first* matching element. If the list already contains
    /// the topic and partition, the existing element receives the offset and
    /// the appended duplicate keeps [`Offset::Invalid`].
    pub fn add_partition_offset(
        &mut self,
        topic: &str,
        partition: i32,
        offset: Offset,
    ) -> KafkaResult<()> {
        self.add_partition(topic, partition);
        self.set_partition_offset(topic, partition, offset)
    }

    /// Given a topic name and a partition number, returns the first matching
    /// element.
    pub fn find_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<TopicPartitionListElem<'_>> {
        self.find_entry(topic, partition)
            .map(|entry| TopicPartitionListElem { entry })
    }

    /// Sets all partitions in the list to the specified offset.
    pub fn set_all_offsets(&mut self, offset: Offset) -> Result<(), KafkaError> {
        let raw = offset.to_raw().ok_or(KafkaError::SetPartitionOffset(
            RDKafkaErrorCode::InvalidArgument,
        ))?;
        for entry in &self.entries {
            entry.offset.store(raw, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Returns all the elements of the list.
    pub fn elements(&self) -> Vec<TopicPartitionListElem<'_>> {
        self.entries
            .iter()
            .map(|entry| TopicPartitionListElem { entry })
            .collect()
    }

    /// Returns all the elements of the list that belong to the specified
    /// topic.
    pub fn elements_for_topic<'a>(&'a self, topic: &str) -> Vec<TopicPartitionListElem<'a>> {
        self.entries
            .iter()
            .filter(|entry| entry.topic == topic)
            .map(|entry| TopicPartitionListElem { entry })
            .collect()
    }

    fn push(&mut self, topic: &str, partition: i32, raw_offset: i64) {
        self.entries.push(TplEntry {
            topic: topic.to_string(),
            partition,
            offset: AtomicI64::new(raw_offset),
            metadata: String::new(),
            error: RDKafkaErrorCode::NoError,
        });
    }

    fn find_entry(&self, topic: &str, partition: i32) -> Option<&TplEntry> {
        self.entries
            .iter()
            .find(|entry| entry.partition == partition && entry.topic == topic)
    }

    /// Appends an element with all fields given, for building results. The
    /// offset is in librdkafka's raw encoding (see [`Offset::from_raw`]).
    pub(crate) fn push_entry(
        &mut self,
        topic: &str,
        partition: i32,
        raw_offset: i64,
        metadata: String,
        error: RDKafkaErrorCode,
    ) {
        self.entries.push(TplEntry {
            topic: topic.to_string(),
            partition,
            offset: AtomicI64::new(raw_offset),
            metadata,
            error,
        });
    }

    /// Sets the error of the element at `index`.
    pub(crate) fn set_error_at(&mut self, index: usize, error: RDKafkaErrorCode) {
        self.entries[index].error = error;
    }

    /// Sets the metadata of the element at `index`.
    pub(crate) fn set_metadata_at(&mut self, index: usize, metadata: String) {
        self.entries[index].metadata = metadata;
    }

    /// Sets the offset of the element at `index`, accepting any raw value.
    pub(crate) fn set_raw_offset_at(&mut self, index: usize, raw_offset: i64) {
        *self.entries[index].offset.get_mut() = raw_offset;
    }

    /// The raw offset of the element at `index`, in librdkafka's encoding.
    pub(crate) fn raw_offset_at(&self, index: usize) -> i64 {
        self.entries[index].offset.load(Ordering::Relaxed)
    }
}

impl PartialEq for TopicPartitionList {
    fn eq(&self, other: &TopicPartitionList) -> bool {
        if self.count() != other.count() {
            return false;
        }
        self.elements().iter().all(|elem| {
            if let Some(other_elem) = other.find_partition(elem.topic(), elem.partition()) {
                elem == &other_elem
            } else {
                false
            }
        })
    }
}

impl Default for TopicPartitionList {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for TopicPartitionList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TPL {{")?;
        for (i, elem) in self.elements().iter().enumerate() {
            if i > 0 {
                write!(f, "; ")?;
            }
            write!(
                f,
                "{}/{}: offset={:?} metadata={:?}",
                elem.topic(),
                elem.partition(),
                elem.offset(),
                elem.metadata(),
            )?;
        }
        write!(f, "}}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn offset_conversion() {
        assert_eq!(Offset::Offset(123).to_raw(), Some(123));
        assert_eq!(Offset::from_raw(123), Offset::Offset(123));
        assert_eq!(Offset::OffsetTail(10).to_raw(), Some(-2010));
        assert_eq!(Offset::from_raw(-2010), Offset::OffsetTail(10));
        assert_eq!(Offset::from_raw(-2000), Offset::OffsetTail(0));
        assert_eq!(Offset::from_raw(-2), Offset::Beginning);
        assert_eq!(Offset::from_raw(-1), Offset::End);
        assert_eq!(Offset::from_raw(-1000), Offset::Stored);
        assert_eq!(Offset::from_raw(-1001), Offset::Invalid);
        assert_eq!(Offset::from_raw(-5), Offset::Offset(-5));
        assert_eq!(Offset::Beginning.to_raw(), Some(-2));
        assert_eq!(Offset::End.to_raw(), Some(-1));
        assert_eq!(Offset::Stored.to_raw(), Some(-1000));
        assert_eq!(Offset::Invalid.to_raw(), Some(-1001));
        assert_eq!(Offset::Offset(-1).to_raw(), None);
        assert_eq!(Offset::OffsetTail(0).to_raw(), None);
        assert_eq!(Offset::OffsetTail(i64::MAX).to_raw(), None);
    }

    #[mz_ore::test]
    fn add_partition_offset_find() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition("topic1", 0);
        tpl.add_partition("topic1", 1);
        tpl.add_partition("topic2", 0);
        tpl.add_partition("topic2", 1);

        tpl.set_partition_offset("topic1", 0, Offset::Offset(0))
            .unwrap();
        tpl.set_partition_offset("topic1", 1, Offset::Offset(1))
            .unwrap();
        tpl.set_partition_offset("topic2", 0, Offset::Offset(2))
            .unwrap();
        tpl.set_partition_offset("topic2", 1, Offset::Offset(3))
            .unwrap();

        assert_eq!(tpl.count(), 4);
        assert_eq!(
            tpl.set_partition_offset("topic0", 3, Offset::Offset(0)),
            Err(KafkaError::SetPartitionOffset(
                RDKafkaErrorCode::UnknownPartition
            ))
        );
        assert_eq!(
            tpl.set_partition_offset("topic1", 0, Offset::Offset(-3)),
            Err(KafkaError::SetPartitionOffset(
                RDKafkaErrorCode::InvalidArgument
            ))
        );

        let tp0 = tpl.find_partition("topic1", 0).unwrap();
        let tp1 = tpl.find_partition("topic1", 1).unwrap();
        let tp2 = tpl.find_partition("topic2", 0).unwrap();
        let mut tp3 = tpl.find_partition("topic2", 1).unwrap();

        assert_eq!(tp0.topic(), "topic1");
        assert_eq!(tp0.partition(), 0);
        assert_eq!(tp0.offset(), Offset::Offset(0));
        assert_eq!(tp1.offset(), Offset::Offset(1));
        assert_eq!(tp2.offset(), Offset::Offset(2));
        assert_eq!(tp3.offset(), Offset::Offset(3));
        assert!(tp3.error().is_ok());

        tp3.set_offset(Offset::Offset(1234)).unwrap();
        assert_eq!(tp3.offset(), Offset::Offset(1234));
        assert_eq!(
            tpl.find_partition("topic2", 1).unwrap().offset(),
            Offset::Offset(1234)
        );
        assert_eq!(tpl.elements_for_topic("topic2").len(), 2);
    }

    #[mz_ore::test]
    fn add_partition_offset_existing_appends_duplicate() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset("t", 0, Offset::Offset(5)).unwrap();
        tpl.add_partition_offset("t", 0, Offset::Offset(7)).unwrap();
        let elems = tpl.elements();
        assert_eq!(elems.len(), 2);
        assert_eq!(elems[0].offset(), Offset::Offset(7));
        assert_eq!(elems[1].offset(), Offset::Invalid);
    }

    #[mz_ore::test]
    fn add_partition_range_and_set_all() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_range("topic1", 0, 3);
        assert_eq!(tpl.count(), 4);
        assert!(
            tpl.set_partition_offset("topic1", 4, Offset::Offset(2))
                .is_err()
        );
        tpl.set_all_offsets(Offset::End).unwrap();
        assert!(tpl.elements().iter().all(|e| e.offset() == Offset::End));
        assert!(tpl.set_all_offsets(Offset::Offset(-7)).is_err());
        tpl.add_partition_range("topic2", 3, 2);
        assert_eq!(tpl.count(), 4);
    }

    #[mz_ore::test]
    fn defaults_clone_eq_debug() {
        let mut tpl = TopicPartitionList::new();
        tpl.add_partition("topic1", 0);
        assert_eq!(
            tpl.find_partition("topic1", 0).unwrap().offset(),
            Offset::Invalid
        );
        tpl.add_partition_offset("topic1", 1, Offset::Offset(1))
            .unwrap();
        let cloned = tpl.clone();
        assert_eq!(tpl, cloned);
        // Clones do not share offsets.
        cloned
            .find_partition("topic1", 1)
            .unwrap()
            .set_offset(Offset::Offset(9))
            .unwrap();
        assert_eq!(
            tpl.find_partition("topic1", 1).unwrap().offset(),
            Offset::Offset(1)
        );
        assert_ne!(tpl, cloned);
        assert_eq!(
            format!("{tpl:?}"),
            "TPL {topic1/0: offset=Invalid metadata=\"\"; topic1/1: offset=Offset(1) metadata=\"\"}"
        );
        let unassigned = tpl.add_topic_unassigned("t3");
        assert_eq!(unassigned.partition(), -1);
    }

    #[mz_ore::test]
    fn per_element_errors() {
        let mut tpl = TopicPartitionList::new();
        tpl.push_entry(
            "t",
            0,
            -1,
            "meta".into(),
            RDKafkaErrorCode::UnknownTopicOrPartition,
        );
        let elem = tpl.find_partition("t", 0).unwrap();
        assert_eq!(
            elem.error(),
            Err(KafkaError::OffsetFetch(
                RDKafkaErrorCode::UnknownTopicOrPartition
            ))
        );
        assert_eq!(elem.metadata(), "meta");
        assert_eq!(elem.offset(), Offset::End);
        tpl.set_error_at(0, RDKafkaErrorCode::NoError);
        tpl.set_metadata_at(0, String::new());
        tpl.set_raw_offset_at(0, 10);
        assert_eq!(tpl.raw_offset_at(0), 10);
        assert!(tpl.find_partition("t", 0).unwrap().error().is_ok());
    }
}
