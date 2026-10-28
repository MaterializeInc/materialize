// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Delivery reports.
//!
//! Queued records exist only in their encoded batch, so reports that carry a
//! message's key, payload and headers decode them from it: always for failed
//! deliveries, and for successful ones only if the context sets
//! [`ProducerContext::DELIVERY_REPORT_CONTENTS`].

use std::sync::Arc;

use bytes::Bytes;

use crate::error::KafkaError;
use crate::internal::records::{Record, RecordIter, parse_batches};
use crate::message::{BorrowedHeaders, BorrowedMessage, Header, OwnedHeaders, Timestamp};
use crate::producer::ProducerContext;
use crate::types::RDKafkaErrorCode;

use super::state::{Batch, BatchData, UaMessage, Undelivered};

/// librdkafka's `RD_KAFKA_OFFSET_INVALID`: the offset of a message whose
/// offset is not known.
const OFFSET_INVALID: i64 = -1001;

/// The decompression limit when decoding a batch for reports.
const MAX_DECODED: usize = 1 << 31;

/// Decoded fields of one record.
struct Contents {
    key: Option<Bytes>,
    payload: Option<Bytes>,
    headers: Option<BorrowedHeaders>,
    timestamp: i64,
}

/// Decodes the records of a batch, or `None` if that fails, which it does not
/// for batches this producer encoded.
fn decode<D>(batch: &Batch<D>) -> Option<Vec<Contents>> {
    let records: RecordIter = match &batch.data {
        BatchData::Open(builder) => builder.records(),
        BatchData::Encoded(encoded) => parse_batches(encoded.bytes())
            .next()?
            .ok()?
            .records(MAX_DECODED)
            .ok()?,
    };
    records
        .map(|r| r.ok().map(contents))
        .collect::<Option<Vec<_>>>()
}

fn contents(record: Record) -> Contents {
    let headers = if record.headers.is_empty() {
        None
    } else {
        let mut owned = OwnedHeaders::new_with_capacity(record.headers.len());
        for header in record.headers.iter() {
            let Ok((key, value)) = header else { break };
            owned = owned.insert(Header {
                key: std::str::from_utf8(key).unwrap_or_default(),
                value,
            });
        }
        BorrowedHeaders::from_owned(&owned)
    };
    Contents {
        key: record.key,
        payload: record.value,
        headers,
        timestamp: record.timestamp,
    }
}

/// Reports a delivered batch. `base_offset` and `log_append_time` are -1 when
/// unknown (acks=0, or a duplicate of an earlier send).
pub(super) fn delivered<C: ProducerContext>(
    context: &C,
    only_errors: bool,
    topic: &Arc<str>,
    partition: i32,
    batch: Batch<C::DeliveryOpaque>,
    base_offset: i64,
    log_append_time: i64,
) {
    if only_errors {
        return;
    }
    let contents = if C::DELIVERY_REPORT_CONTENTS {
        decode(&batch)
    } else {
        None
    };
    let mut contents = contents.map(Vec::into_iter);
    for (i, opaque) in batch.opaques.into_iter().enumerate() {
        let offset = if base_offset >= 0 {
            base_offset + i64::try_from(i).expect("fits")
        } else {
            OFFSET_INVALID
        };
        let c = contents.as_mut().and_then(Iterator::next);
        let timestamp = if log_append_time >= 0 {
            Timestamp::LogAppendTime(log_append_time)
        } else {
            match &c {
                Some(c) => Timestamp::CreateTime(c.timestamp),
                None => Timestamp::NotAvailable,
            }
        };
        let (key, payload, headers) = match c {
            Some(c) => (c.key, c.payload, c.headers),
            None => (None, None, None),
        };
        let message = BorrowedMessage::new(
            Arc::clone(topic),
            partition,
            offset,
            timestamp,
            key,
            payload,
            headers,
        );
        context.delivery(&Ok(message), opaque);
    }
}

/// Reports records that will not be delivered, with their contents.
pub(super) fn failed<C: ProducerContext>(
    context: &C,
    topic: &Arc<str>,
    partition: i32,
    records: Undelivered<C::DeliveryOpaque>,
    error: RDKafkaErrorCode,
) {
    let error = KafkaError::MessageProduction(error);
    match records {
        Undelivered::Batch(batch) => {
            let mut contents = decode(&batch).map(Vec::into_iter);
            for opaque in batch.opaques {
                let c = contents.as_mut().and_then(Iterator::next);
                let (key, payload, headers, timestamp) = match c {
                    Some(c) => (
                        c.key,
                        c.payload,
                        c.headers,
                        Timestamp::CreateTime(c.timestamp),
                    ),
                    None => (None, None, None, Timestamp::NotAvailable),
                };
                let message = BorrowedMessage::new(
                    Arc::clone(topic),
                    partition,
                    OFFSET_INVALID,
                    timestamp,
                    key,
                    payload,
                    headers,
                );
                context.delivery(&Err((error.clone(), message)), opaque);
            }
        }
        Undelivered::Ua(m) => {
            let UaMessage {
                key,
                payload,
                headers,
                timestamp,
                opaque,
                ..
            } = m;
            let message = BorrowedMessage::new(
                Arc::clone(topic),
                partition,
                OFFSET_INVALID,
                Timestamp::CreateTime(timestamp),
                key.map(Bytes::from),
                payload.map(Bytes::from),
                headers.as_ref().and_then(BorrowedHeaders::from_owned),
            );
            context.delivery(&Err((error, message)), opaque);
        }
    }
}
