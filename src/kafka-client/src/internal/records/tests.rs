// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use itertools::Itertools;
use rand::{RngExt, SeedableRng};

use super::*;

const ALL_CODECS: [Compression; 5] = [
    Compression::None,
    Compression::Gzip,
    Compression::Snappy,
    Compression::Lz4,
    Compression::Zstd,
];

const MAX: usize = 64 << 20;

/// An owned record, for comparisons.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Rec {
    timestamp: i64,
    key: Option<Vec<u8>>,
    value: Option<Vec<u8>>,
    headers: Vec<(String, Option<Vec<u8>>)>,
}

impl Rec {
    fn new(timestamp: i64, key: Option<&[u8]>, value: Option<&[u8]>) -> Self {
        Rec {
            timestamp,
            key: key.map(<[u8]>::to_vec),
            value: value.map(<[u8]>::to_vec),
            headers: vec![],
        }
    }

    fn header(mut self, key: &str, value: Option<&[u8]>) -> Self {
        self.headers
            .push((key.to_owned(), value.map(<[u8]>::to_vec)));
        self
    }

    fn append_to(&self, builder: &mut BatchBuilder) -> usize {
        let headers = self
            .headers
            .iter()
            .map(|(k, v)| RecordHeader {
                key: k,
                value: v.as_deref(),
            })
            .collect::<Vec<_>>();
        let expected = builder.encoded_record_size(
            self.timestamp,
            self.key.as_deref(),
            self.value.as_deref(),
            &headers,
        );
        let written = builder.append(
            self.timestamp,
            self.key.as_deref(),
            self.value.as_deref(),
            &headers,
        );
        assert_eq!(written, expected);
        written
    }

    fn from_record(r: &Record) -> Self {
        Rec {
            timestamp: r.timestamp,
            key: r.key.as_ref().map(|b| b.to_vec()),
            value: r.value.as_ref().map(|b| b.to_vec()),
            headers: r
                .headers
                .iter()
                .map(|h| {
                    let (k, v) = h.expect("valid header");
                    (
                        String::from_utf8(k.to_vec()).expect("utf-8 key"),
                        v.map(<[u8]>::to_vec),
                    )
                })
                .collect(),
        }
    }
}

fn encode(
    c: Compression,
    recs: &[Rec],
    transactional: bool,
    ident: ProducerIdentity,
    base_sequence: i32,
) -> EncodedBatch {
    let mut builder = BatchBuilder::new(c, None, transactional);
    let mut size = BATCH_HEADER_LEN;
    for r in recs {
        size += r.append_to(&mut builder);
    }
    assert_eq!(builder.uncompressed_size(), size);
    assert_eq!(builder.record_count(), i32::try_from(recs.len()).unwrap());
    builder.build(ident, base_sequence).expect("build")
}

fn decode(data: Bytes) -> Vec<(BatchHeader, Vec<Record>)> {
    let mut iter = parse_batches(data);
    let out = iter
        .by_ref()
        .map(|batch| {
            let batch = batch.expect("valid batch");
            batch.verify_crc().expect("valid crc");
            let records = batch
                .records(MAX)
                .expect("records")
                .collect::<Result<Vec<_>, _>>()
                .expect("valid records");
            (batch.header().clone(), records)
        })
        .collect();
    assert_eq!(iter.truncated_bytes(), 0);
    out
}

fn sample_records() -> Vec<Rec> {
    let big: Vec<u8> = (0..200_000_u32)
        .map(|i| u8::try_from(i % 97).unwrap())
        .collect();
    let mut recs = vec![
        Rec::new(1_700_000_000_000, Some(b"key"), Some(b"value")),
        Rec::new(1_700_000_000_005, None, Some(b"null key")),
        Rec::new(1_700_000_000_001, Some(b""), Some(b"empty key")),
        Rec::new(1_700_000_000_002, Some(b"null value"), None),
        Rec::new(1_700_000_000_003, Some(b"empty value"), Some(b"")),
        Rec::new(1_600_000_000_000, None, None),
        Rec::new(1_700_000_000_004, Some(b"headers"), Some(b"v"))
            .header("a", Some(b"1"))
            .header("null", None)
            .header("empty", Some(b""))
            .header("", Some(b"empty key"))
            .header("a", Some(b"duplicate")),
        Rec::new(1_700_000_000_010, Some(b"big"), Some(&big)),
        Rec::new(-1, Some(b"no timestamp"), Some(b"x")),
    ];
    for i in 0..1000 {
        let key = format!("key-{i}");
        let value = format!("value-{i}-{}", "x".repeat(i % 50));
        let mut r = Rec::new(
            1_700_000_001_000 + i64::try_from(i).unwrap(),
            Some(key.as_bytes()),
            Some(value.as_bytes()),
        );
        if i % 3 == 0 {
            r = r.header("materialize-timestamp", Some(i.to_string().as_bytes()));
        }
        recs.push(r);
    }
    recs
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn round_trip_every_codec() {
    let recs = sample_records();
    let n = i32::try_from(recs.len()).unwrap();
    for c in ALL_CODECS {
        for transactional in [false, true] {
            let ident = ProducerIdentity {
                producer_id: 4711,
                producer_epoch: 3,
            };
            let batch = encode(c, &recs, transactional, ident, 42);
            assert_eq!(batch.compression(), c);
            assert_eq!(batch.record_count(), n);
            assert_eq!(batch.base_sequence(), 42);
            assert_eq!(batch.last_sequence(), 42 + n - 1);
            assert_eq!(batch.producer_identity(), ident);
            let decoded = decode(batch.bytes());
            assert_eq!(decoded.len(), 1);
            let (header, records) = &decoded[0];
            assert_eq!(
                *header,
                BatchHeader {
                    base_offset: 0,
                    last_offset: i64::from(n - 1),
                    partition_leader_epoch: -1,
                    magic: 2,
                    compression: c,
                    timestamp_type: TimestampType::CreateTime,
                    is_transactional: transactional,
                    is_control: false,
                    producer_id: 4711,
                    producer_epoch: 3,
                    base_sequence: 42,
                    first_timestamp: 1_700_000_000_000,
                    max_timestamp: 1_700_000_001_999,
                    record_count: n,
                    size: batch.len(),
                },
                "{c}"
            );
            let got = records.iter().map(Rec::from_record).collect::<Vec<_>>();
            assert!(got == recs, "{c}");
            for (i, r) in records.iter().enumerate() {
                assert_eq!(r.offset, i64::try_from(i).unwrap());
                assert_eq!(r.timestamp_type, TimestampType::CreateTime);
                assert_eq!(r.headers.len(), recs[i].headers.len());
                assert_eq!(r.headers.is_empty(), recs[i].headers.is_empty());
            }
        }
    }
}

#[mz_ore::test]
fn single_record_layout() {
    let mut builder = BatchBuilder::new(Compression::None, None, false);
    builder.append(1000, Some(b"k"), Some(b"v"), &[]);
    let batch = builder.build(ProducerIdentity::NONE, -1).unwrap();
    let mut expected = vec![];
    expected.extend_from_slice(&0_i64.to_be_bytes()); // base offset
    expected.extend_from_slice(&58_i32.to_be_bytes()); // length
    expected.extend_from_slice(&(-1_i32).to_be_bytes()); // leader epoch
    expected.push(2); // magic
    expected.extend_from_slice(&[0; 4]); // crc, filled in below
    expected.extend_from_slice(&0_i16.to_be_bytes()); // attributes
    expected.extend_from_slice(&0_i32.to_be_bytes()); // last offset delta
    expected.extend_from_slice(&1000_i64.to_be_bytes()); // base timestamp
    expected.extend_from_slice(&1000_i64.to_be_bytes()); // max timestamp
    expected.extend_from_slice(&(-1_i64).to_be_bytes()); // producer id
    expected.extend_from_slice(&(-1_i16).to_be_bytes()); // producer epoch
    expected.extend_from_slice(&(-1_i32).to_be_bytes()); // base sequence
    expected.extend_from_slice(&1_i32.to_be_bytes()); // record count
    // length 8, attributes, ts delta 0, offset delta 0, key "k", value "v",
    // no headers.
    expected.extend_from_slice(&[0x10, 0, 0, 0, 0x02, b'k', 0x02, b'v', 0]);
    let crc = crc32c::crc32c(&expected[ATTRIBUTES..]);
    expected[CRC..CRC + 4].copy_from_slice(&crc.to_be_bytes());
    assert_eq!(batch.bytes(), expected);
}

#[mz_ore::test]
fn empty_batch_is_rejected() {
    let builder = BatchBuilder::new(Compression::Lz4, None, false);
    assert!(builder.is_empty());
    assert_eq!(builder.uncompressed_size(), BATCH_HEADER_LEN);
    assert_eq!(
        builder.build(ProducerIdentity::NONE, -1).unwrap_err(),
        CodecError::EmptyBatch
    );
}

#[mz_ore::test]
fn incompressible_batch_is_sent_uncompressed() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(7);
    let noise: Vec<u8> = (0..10_000).map(|_| rng.random()).collect();
    for c in [
        Compression::Gzip,
        Compression::Lz4,
        Compression::Zstd,
        Compression::Snappy,
    ] {
        let batch = encode(
            c,
            &[Rec::new(1, None, Some(&noise))],
            false,
            ProducerIdentity::NONE,
            -1,
        );
        assert_eq!(batch.compression(), Compression::None, "{c}");
        let decoded = decode(batch.bytes());
        assert_eq!(decoded[0].0.compression, Compression::None);
        assert_eq!(decoded[0].1[0].value.as_deref(), Some(&noise[..]));
    }
}

#[mz_ore::test]
fn restamp_rewrites_identity_and_crc() {
    let recs = sample_records();
    for c in [Compression::None, Compression::Lz4] {
        let mut batch = encode(c, &recs[..20], true, ProducerIdentity::NONE, -1);
        let before = batch.bytes();
        let ident = ProducerIdentity {
            producer_id: 99,
            producer_epoch: 7,
        };
        // An outstanding `Bytes` forces a copy and keeps the old contents.
        batch.restamp(ident, i32::MAX - 5);
        assert_eq!(batch.producer_identity(), ident);
        assert_eq!(batch.base_sequence(), i32::MAX - 5);
        assert_eq!(batch.last_sequence(), 13);
        let after = decode(batch.bytes());
        assert_eq!(after[0].0.producer_id, 99);
        assert_eq!(after[0].0.producer_epoch, 7);
        assert_eq!(after[0].0.base_sequence, i32::MAX - 5);
        let old = decode(before.clone());
        assert_eq!(old[0].0.producer_id, -1);
        assert_eq!(before.len(), batch.len());
        assert_eq!(
            before[ATTRIBUTES..PRODUCER_ID],
            batch.bytes()[ATTRIBUTES..PRODUCER_ID]
        );
        assert_eq!(
            before[BATCH_HEADER_LEN..],
            batch.bytes()[BATCH_HEADER_LEN..]
        );
        drop(before);
        drop(old);
        // Unique: rewritten in place.
        batch.restamp(ProducerIdentity::NONE, -1);
        let again = decode(batch.bytes());
        assert_eq!(again[0].0.producer_id, -1);
        assert_eq!(again[0].0.base_sequence, -1);
    }
}

#[mz_ore::test]
fn sequence_wraps() {
    assert_eq!(increment_sequence(5, 0), 5);
    assert_eq!(increment_sequence(5, 3), 8);
    assert_eq!(increment_sequence(i32::MAX, 0), i32::MAX);
    assert_eq!(increment_sequence(i32::MAX, 1), 0);
    assert_eq!(increment_sequence(i32::MAX - 1, 3), 1);
}

/// Rewrites header fields of an encoded v2 batch and fixes its CRC.
fn patch(batch: &[u8], f: impl FnOnce(&mut [u8])) -> Bytes {
    let mut data = batch.to_vec();
    f(&mut data);
    let crc = crc32c::crc32c(&data[ATTRIBUTES..]);
    data[CRC..CRC + 4].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(data)
}

#[mz_ore::test]
fn log_append_time_uses_max_timestamp() {
    let recs = &sample_records()[..10];
    let batch = encode(Compression::None, recs, false, ProducerIdentity::NONE, -1);
    let data = patch(&batch.bytes(), |d| {
        let attributes = get_i16(d, ATTRIBUTES) | ATTR_LOG_APPEND_TIME;
        put_i16(d, ATTRIBUTES, attributes);
        put_i64(d, MAX_TIMESTAMP, 1_800_000_000_000);
    });
    let decoded = decode(data);
    assert_eq!(decoded[0].0.timestamp_type, TimestampType::LogAppendTime);
    for r in &decoded[0].1 {
        assert_eq!(r.timestamp, 1_800_000_000_000);
        assert_eq!(r.timestamp_type, TimestampType::LogAppendTime);
    }
}

#[mz_ore::test]
fn record_count_must_match() {
    let recs = &sample_records()[..10];
    let batch = encode(Compression::None, recs, false, ProducerIdentity::NONE, -1);
    for count in [9, 11, -1] {
        let data = patch(&batch.bytes(), |d| put_i32(d, RECORD_COUNT, count));
        let batch = parse_batches(data).next().unwrap().unwrap();
        match batch.records(MAX) {
            Ok(records) => {
                let results = records.collect::<Vec<_>>();
                assert!(results.last().unwrap().is_err(), "{count}");
                assert_eq!(results.iter().filter(|r| r.is_err()).count(), 1);
            }
            Err(_) => assert_eq!(count, -1),
        }
    }
}

/// A control batch as transaction coordinators write it.
fn control_batch(key: &[u8]) -> Bytes {
    let mut builder = BatchBuilder::new(Compression::None, None, true);
    // Value: version 0, coordinator epoch 5.
    builder.append(1, Some(key), Some(&[0, 0, 0, 0, 0, 5]), &[]);
    let batch = builder
        .build(
            ProducerIdentity {
                producer_id: 1,
                producer_epoch: 0,
            },
            -1,
        )
        .unwrap();
    patch(&batch.bytes(), |d| {
        let attributes = get_i16(d, ATTRIBUTES) | ATTR_CONTROL;
        put_i16(d, ATTRIBUTES, attributes);
    })
}

#[mz_ore::test]
fn control_records() {
    let cases: [(&[u8], Result<Option<ControlRecord>, ()>); 6] = [
        (
            &[0, 0, 0, 0],
            Ok(Some(ControlRecord {
                version: 0,
                kind: ControlRecordKind::Abort,
            })),
        ),
        (
            &[0, 0, 0, 1],
            Ok(Some(ControlRecord {
                version: 0,
                kind: ControlRecordKind::Commit,
            })),
        ),
        (
            &[0, 0, 0, 7],
            Ok(Some(ControlRecord {
                version: 0,
                kind: ControlRecordKind::Unknown(7),
            })),
        ),
        (
            &[0, 1, 0, 1, 0xff],
            Ok(Some(ControlRecord {
                version: 1,
                kind: ControlRecordKind::Commit,
            })),
        ),
        (&[0, 0, 0], Err(())),
        (&[0xff, 0xff, 0, 1], Err(())),
    ];
    for (key, expected) in cases {
        let batch = parse_batches(control_batch(key)).next().unwrap().unwrap();
        assert!(batch.header().is_control);
        assert!(batch.header().is_transactional);
        assert_eq!(batch.control_record().map_err(|_| ()), expected, "{key:?}");
    }
    let data = encode(
        Compression::None,
        &sample_records()[..1],
        true,
        ProducerIdentity::NONE,
        -1,
    );
    let batch = parse_batches(data.bytes()).next().unwrap().unwrap();
    assert_eq!(batch.control_record(), Ok(None));
}

/// Encodes a legacy (magic 0 or 1) message with a valid CRC.
fn legacy_message(
    offset: i64,
    magic: i8,
    attributes: u8,
    timestamp: i64,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
) -> Vec<u8> {
    let mut body = vec![u8::reinterpret_cast(magic), attributes];
    if magic == 1 {
        body.extend_from_slice(&timestamp.to_be_bytes());
    }
    for b in [key, value] {
        match b {
            None => body.extend_from_slice(&(-1_i32).to_be_bytes()),
            Some(b) => {
                body.extend_from_slice(&i32::try_from(b.len()).unwrap().to_be_bytes());
                body.extend_from_slice(b);
            }
        }
    }
    let mut out = offset.to_be_bytes().to_vec();
    out.extend_from_slice(&i32::try_from(body.len() + 4).unwrap().to_be_bytes());
    out.extend_from_slice(&crc32fast::hash(&body).to_be_bytes());
    out.extend_from_slice(&body);
    out
}

fn xerial_snappy(input: &[u8]) -> Vec<u8> {
    let mut out = b"\x82SNAPPY\x00".to_vec();
    out.extend_from_slice(&1_u32.to_be_bytes());
    out.extend_from_slice(&1_u32.to_be_bytes());
    for chunk in input.chunks(100) {
        let block = snap::raw::Encoder::new().compress_vec(chunk).unwrap();
        out.extend_from_slice(&u32::try_from(block.len()).unwrap().to_be_bytes());
        out.extend_from_slice(&block);
    }
    out
}

/// Breaks the LZ4 frame header checksum the way Kafka < 0.10 did.
fn break_lz4_hc(frame: &mut [u8]) {
    assert_eq!(frame[4] & 0x08, 0, "no content size");
    let [_, hc, _, _] = twox_hash::XxHash32::oneshot(0, &frame[..6]).to_le_bytes();
    frame[6] = hc;
}

fn simple(r: &Record) -> (i64, i64, TimestampType, Option<Vec<u8>>, Option<Vec<u8>>) {
    (
        r.offset,
        r.timestamp,
        r.timestamp_type,
        r.key.as_ref().map(|b| b.to_vec()),
        r.value.as_ref().map(|b| b.to_vec()),
    )
}

#[mz_ore::test]
fn legacy_uncompressed() {
    let mut data = legacy_message(5, 0, 0, 0, Some(b"k0"), Some(b"v0"));
    data.extend(legacy_message(6, 1, 0, 1234, None, Some(b"")));
    data.extend(legacy_message(7, 1, 0, 0, Some(b""), None));
    data.extend(legacy_message(
        8,
        1,
        ATTR_LOG_APPEND_TIME.to_be_bytes()[1],
        999,
        None,
        None,
    ));
    let decoded = decode(Bytes::from(data));
    let headers = decoded.iter().map(|(h, _)| h.clone()).collect::<Vec<_>>();
    assert_eq!(
        headers
            .iter()
            .map(|h| (
                h.magic,
                h.base_offset,
                h.last_offset,
                h.record_count,
                h.timestamp_type
            ))
            .collect::<Vec<_>>(),
        [
            (0, 5, 5, 1, TimestampType::NotAvailable),
            (1, 6, 6, 1, TimestampType::CreateTime),
            (1, 7, 7, 1, TimestampType::CreateTime),
            (1, 8, 8, 1, TimestampType::LogAppendTime),
        ]
    );
    assert!(
        headers
            .iter()
            .all(|h| h.producer_id == -1 && !h.is_control && !h.is_transactional)
    );
    let records = decoded
        .iter()
        .flat_map(|(_, r)| r.iter().map(simple))
        .collect::<Vec<_>>();
    assert_eq!(
        records,
        [
            (
                5,
                -1,
                TimestampType::NotAvailable,
                Some(b"k0".to_vec()),
                Some(b"v0".to_vec())
            ),
            (6, 1234, TimestampType::CreateTime, None, Some(vec![])),
            // librdkafka reports a magic 1 timestamp of 0 as not available.
            (7, -1, TimestampType::NotAvailable, Some(vec![]), None),
            (8, 999, TimestampType::LogAppendTime, None, None),
        ]
    );
    assert!(decoded.iter().all(|(_, r)| r[0].headers.is_empty()));
}

#[mz_ore::test]
fn legacy_compressed_magic0() {
    let inner = [
        legacy_message(10, 0, 0, 0, Some(b"a"), Some(b"1")),
        legacy_message(11, 0, 0, 0, None, Some(b"2")),
        legacy_message(12, 0, 0, 0, Some(b"c"), None),
    ]
    .concat();
    for c in [Compression::Gzip, Compression::Snappy, Compression::Lz4] {
        for variant in 0..2 {
            let mut compressed = vec![];
            compression::compress(c, None, &inner, &mut compressed).unwrap();
            match (c, variant) {
                (Compression::Snappy, 1) => compressed = xerial_snappy(&inner),
                (Compression::Lz4, 1) => break_lz4_hc(&mut compressed),
                _ => {}
            }
            let attributes = u8::try_from(c.attribute_bits()).unwrap();
            let data = legacy_message(12, 0, attributes, 0, None, Some(&compressed));
            let decoded = decode(Bytes::from(data));
            let (header, records) = &decoded[0];
            assert_eq!(header.compression, c);
            assert_eq!(header.record_count, -1);
            assert_eq!((header.base_offset, header.last_offset), (12, 12));
            assert_eq!(
                records.iter().map(simple).collect::<Vec<_>>(),
                [
                    (
                        10,
                        -1,
                        TimestampType::NotAvailable,
                        Some(b"a".to_vec()),
                        Some(b"1".to_vec())
                    ),
                    (
                        11,
                        -1,
                        TimestampType::NotAvailable,
                        None,
                        Some(b"2".to_vec())
                    ),
                    (
                        12,
                        -1,
                        TimestampType::NotAvailable,
                        Some(b"c".to_vec()),
                        None
                    ),
                ],
                "{c} {variant}"
            );
        }
    }
}

#[mz_ore::test]
fn legacy_compressed_magic1_relative_offsets() {
    let inner = [
        legacy_message(0, 1, 0, 1000, Some(b"a"), Some(b"1")),
        legacy_message(1, 1, 0, 1001, Some(b"b"), Some(b"2")),
        legacy_message(2, 1, 0, 0, Some(b"c"), Some(b"3")),
    ]
    .concat();
    for c in [Compression::Gzip, Compression::Snappy, Compression::Lz4] {
        let mut compressed = vec![];
        compression::compress(c, None, &inner, &mut compressed).unwrap();
        let attributes = u8::try_from(c.attribute_bits()).unwrap();
        let create = legacy_message(102, 1, attributes, 1001, None, Some(&compressed));
        let decoded = decode(Bytes::from(create));
        assert_eq!(decoded[0].0.timestamp_type, TimestampType::CreateTime);
        assert_eq!(
            decoded[0].1.iter().map(simple).collect::<Vec<_>>(),
            [
                (
                    100,
                    1000,
                    TimestampType::CreateTime,
                    Some(b"a".to_vec()),
                    Some(b"1".to_vec())
                ),
                (
                    101,
                    1001,
                    TimestampType::CreateTime,
                    Some(b"b".to_vec()),
                    Some(b"2".to_vec())
                ),
                (
                    102,
                    -1,
                    TimestampType::NotAvailable,
                    Some(b"c".to_vec()),
                    Some(b"3".to_vec())
                ),
            ],
            "{c}"
        );
        let log_append = legacy_message(
            102,
            1,
            attributes | ATTR_LOG_APPEND_TIME.to_be_bytes()[1],
            5555,
            None,
            Some(&compressed),
        );
        let decoded = decode(Bytes::from(log_append));
        assert!(
            decoded[0]
                .1
                .iter()
                .all(|r| r.timestamp == 5555 && r.timestamp_type == TimestampType::LogAppendTime)
        );
        assert_eq!(
            decoded[0].1.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [100, 101, 102]
        );
    }
}

#[mz_ore::test]
fn legacy_errors() {
    let inner = legacy_message(0, 1, 0, 1, Some(b"a"), Some(b"1"));
    // Magic 1 frames need a proper LZ4 header checksum.
    let mut lz4 = vec![];
    compression::compress(Compression::Lz4, None, &inner, &mut lz4).unwrap();
    break_lz4_hc(&mut lz4);
    let data = legacy_message(0, 1, 3, 1, None, Some(&lz4));
    let batch = parse_batches(Bytes::from(data)).next().unwrap().unwrap();
    assert!(batch.records(MAX).is_err());

    // Nested compression.
    let mut gz = vec![];
    compression::compress(Compression::Gzip, None, &inner, &mut gz).unwrap();
    let nested = legacy_message(0, 1, 1, 1, None, Some(&gz));
    let mut gz2 = vec![];
    compression::compress(Compression::Gzip, None, &nested, &mut gz2).unwrap();
    let data = legacy_message(0, 1, 1, 1, None, Some(&gz2));
    let batch = parse_batches(Bytes::from(data)).next().unwrap().unwrap();
    assert!(batch.records(MAX).is_err());

    // Decompression limit.
    let data = legacy_message(0, 1, 1, 1, None, Some(&gz));
    let batch = parse_batches(Bytes::from(data)).next().unwrap().unwrap();
    assert!(batch.records(inner.len()).is_ok());
    assert_eq!(
        batch.records(inner.len() - 1).unwrap_err(),
        CodecError::TooLarge {
            limit: inner.len() - 1
        }
    );

    // A size field that disagrees with the key and value.
    let mut data = legacy_message(0, 1, 0, 1, Some(b"a"), Some(b"1"));
    data.push(0);
    let len = get_i32(&data, BATCH_LENGTH) + 1;
    put_i32(&mut data, BATCH_LENGTH, len);
    assert!(parse_batches(Bytes::from(data)).next().unwrap().is_err());

    // CRC.
    let mut data = legacy_message(0, 0, 0, 0, Some(b"a"), Some(b"1"));
    let batch = parse_batches(Bytes::from(data.clone()))
        .next()
        .unwrap()
        .unwrap();
    assert_eq!(batch.verify_crc(), Ok(()));
    let last = data.len() - 1;
    data[last] ^= 1;
    let batch = parse_batches(Bytes::from(data)).next().unwrap().unwrap();
    assert!(matches!(
        batch.verify_crc(),
        Err(CodecError::CrcMismatch { .. })
    ));
}

#[mz_ore::test]
fn v2_crc_mismatch() {
    let batch = encode(
        Compression::None,
        &sample_records()[..3],
        false,
        ProducerIdentity::NONE,
        -1,
    );
    let mut data = batch.bytes().to_vec();
    let last = data.len() - 1;
    data[last] ^= 1;
    let batch = parse_batches(Bytes::from(data)).next().unwrap().unwrap();
    assert!(matches!(
        batch.verify_crc(),
        Err(CodecError::CrcMismatch { .. })
    ));
}

/// A record set mixing every codec and both legacy formats.
fn mixed_record_set() -> (Vec<u8>, Vec<usize>) {
    let recs = sample_records();
    let mut data = vec![];
    let mut ends = vec![];
    for (i, c) in ALL_CODECS.into_iter().enumerate() {
        let batch = encode(
            c,
            &recs[i * 7..i * 7 + 7],
            i % 2 == 0,
            ProducerIdentity::NONE,
            -1,
        );
        data.extend_from_slice(&batch.bytes());
        ends.push(data.len());
    }
    data.extend(legacy_message(100, 0, 0, 0, Some(b"k"), Some(b"v")));
    ends.push(data.len());
    let inner = legacy_message(0, 1, 0, 5, Some(b"k"), None);
    let mut gz = vec![];
    compression::compress(Compression::Gzip, None, &inner, &mut gz).unwrap();
    data.extend(legacy_message(101, 1, 1, 5, None, Some(&gz)));
    ends.push(data.len());
    data.extend_from_slice(&control_batch(&[0, 0, 0, 1]));
    ends.push(data.len());
    (data, ends)
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn truncated_tails_are_dropped() {
    let (data, ends) = mixed_record_set();
    let data = Bytes::from(data);
    for cut in 0..=data.len() {
        let mut iter = parse_batches(data.slice(..cut));
        let n = iter
            .by_ref()
            .inspect(|b| assert!(b.is_ok(), "complete batch"))
            .count();
        let complete = ends.iter().filter(|end| **end <= cut).count();
        assert_eq!(n, complete, "cut {cut}");
        let consumed = ends.get(complete.wrapping_sub(1)).copied().unwrap_or(0);
        assert_eq!(iter.truncated_bytes(), cut - consumed, "cut {cut}");
    }
}

#[mz_ore::test]
fn unsupported_magic_is_skipped() {
    let batch = encode(
        Compression::None,
        &sample_records()[..2],
        false,
        ProducerIdentity::NONE,
        -1,
    );
    let mut bogus = batch.bytes().to_vec();
    bogus[MAGIC] = 3;
    put_i64(&mut bogus, BASE_OFFSET, 77);
    let data = [batch.bytes().to_vec(), bogus, batch.bytes().to_vec()].concat();
    let results = parse_batches(Bytes::from(data)).collect::<Vec<_>>();
    assert_eq!(results.len(), 3);
    assert!(results[0].is_ok());
    assert_eq!(
        results[1].as_ref().unwrap_err(),
        &CodecError::UnsupportedMagic {
            magic: 3,
            offset: 77
        }
    );
    assert!(results[2].is_ok());
}

#[mz_ore::test]
fn negative_length_stops_iteration() {
    let batch = encode(
        Compression::None,
        &sample_records()[..2],
        false,
        ProducerIdentity::NONE,
        -1,
    );
    let mut bogus = batch.bytes().to_vec();
    put_i32(&mut bogus, BATCH_LENGTH, -5);
    let data = [batch.bytes().to_vec(), bogus, batch.bytes().to_vec()].concat();
    let results = parse_batches(Bytes::from(data)).collect::<Vec<_>>();
    assert_eq!(results.len(), 2);
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
}

/// Exercises every decoding path on `data`, which may be garbage.
fn exercise(data: Bytes, max: usize) {
    for batch in parse_batches(data) {
        let Ok(batch) = batch else { continue };
        let _ = batch.verify_crc();
        let _ = batch.control_record();
        let Ok(records) = batch.records(max) else {
            continue;
        };
        for record in records {
            let Ok(record) = record else { continue };
            for header in record.headers.iter() {
                let _ = header;
            }
        }
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn garbage_never_panics() {
    let mut rng = rand::rngs::StdRng::seed_from_u64(1);
    let (valid, _) = mixed_record_set();
    for i in 0..3000 {
        let mut data = valid.clone();
        match i % 4 {
            0 => {
                for _ in 0..rng.random_range(1..8) {
                    let at = rng.random_range(0..data.len());
                    data[at] = rng.random();
                }
            }
            1 => {
                // Corrupt a header field but keep framing plausible.
                let at = rng.random_range(0..BATCH_HEADER_LEN);
                data[at] ^= 1 << rng.random_range(0..8);
            }
            2 => {
                let len = rng.random_range(0..200);
                data = (0..len).map(|_| rng.random()).collect();
            }
            _ => {
                // Garbage records behind a valid header.
                let batch = encode(
                    Compression::None,
                    &sample_records()[..3],
                    false,
                    ProducerIdentity::NONE,
                    -1,
                );
                data = batch.bytes().to_vec();
                for b in &mut data[BATCH_HEADER_LEN..] {
                    if rng.random_bool(0.2) {
                        *b = rng.random();
                    }
                }
            }
        }
        exercise(Bytes::from(data), 1 << 20);
    }
}

#[mz_ore::test]
fn huge_declared_lengths_do_not_allocate() {
    // A batch claiming 2^31 - 1 bytes is treated as truncated.
    let mut data = vec![0; 64];
    put_i32(&mut data, BATCH_LENGTH, i32::MAX);
    data[MAGIC] = 2;
    let mut iter = parse_batches(Bytes::from(data));
    assert!(iter.next().is_none());
    assert_eq!(iter.truncated_bytes(), 64);

    // A huge header count with the bytes for only one header.
    let headers = RecordHeaders {
        count: 1 << 30,
        data: Bytes::from_static(b"\x02k\x01"),
    };
    let results = headers.iter().collect::<Vec<_>>();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0], Ok((&b"k"[..], None)));
    assert!(results[1].is_err());

    // Declared key and value lengths beyond the record.
    for record in [
        &[0x0c, 0, 0, 0, 0x7e, 0, 0][..],
        &[0x0c, 0, 0, 0, 0x01, 0x7e, 0],
    ] {
        let mut v2 = V2Records {
            buf: Bytes::copy_from_slice(record),
            pos: 0,
            remaining: 1,
            base_offset: 0,
            base_timestamp: 0,
            log_append_time: false,
            max_timestamp: 0,
        };
        assert!(v2.next().unwrap().is_err(), "{record:?}");
        assert!(v2.next().is_none());
    }
}

// Cross-implementation fixtures: record sets fetched (Fetch v11, so exactly as
// the broker stores them) from Apache Kafka 4.2 and Redpanda 26.1:
//
// * `rdkafka-*`: `fixture_records` produced by librdkafka 2.12.1 (the version
//   Materialize uses) with each `compression.type`. `rdkafka-txn-*` are three
//   transactions (committed, aborted, committed) with their control batches,
//   fetched read_uncommitted. `rdkafka-logappendtime-lz4` is from a topic with
//   `message.timestamp.type=LogAppendTime`.
// * `java-recompressed-*`: `fixture_records` produced uncompressed by
//   librdkafka to Kafka topics with a `compression.type`, so the broker's Java
//   codecs (snappy-java framing, KafkaLZ4BlockOutputStream, zstd-jni)
//   recompressed them.
// * `java-producer-*`: `java_producer_records` sent by the Java console
//   producer (idempotent, with `--compression-codec`).

/// The records the librdkafka fixtures contain.
fn fixture_records() -> Vec<Rec> {
    let mut recs = vec![
        Rec::new(1_700_000_000_000, Some(b"key-0"), Some(b"value-0"))
            .header("h1", Some(b"v1"))
            .header("h-null", None)
            .header("h-empty", Some(b"")),
        Rec::new(1_700_000_000_001, None, Some(b"null key")),
        Rec::new(1_700_000_000_002, Some(b""), Some(b"empty key")),
        Rec::new(1_700_000_000_003, Some(b"null value"), None),
        Rec::new(1_700_000_000_004, Some(b"empty value"), Some(b"")),
    ];
    for i in 5..30 {
        let value = format!("value-{i}-{}", "abcdefgh".repeat(i % 10));
        recs.push(Rec::new(
            1_700_000_000_000 + i64::try_from(i).unwrap(),
            Some(format!("key-{i}").as_bytes()),
            Some(value.as_bytes()),
        ));
    }
    recs
}

/// The records of the Java console producer fixtures, with timestamp 0 as the
/// producer chose wall clock times.
fn java_producer_records() -> Vec<Rec> {
    fixture_records()
        .into_iter()
        .enumerate()
        .map(|(i, mut r)| {
            r.timestamp = 0;
            if i > 0 {
                r = r.header("h", Some(b"x"));
            }
            // The script feeding the console producer repeated the pattern
            // once instead of zero times.
            if i >= 10 && i % 10 == 0 {
                r.value = Some(format!("value-{i}-abcdefgh").into_bytes());
            }
            r
        })
        .collect()
}

/// Decodes a fixture record set: all records of all batches, the batch
/// headers, and checks every CRC.
fn decode_fixture(data: &'static [u8]) -> (Vec<BatchHeader>, Vec<Record>) {
    let mut headers = vec![];
    let mut records = vec![];
    let mut iter = parse_batches(Bytes::from_static(data));
    for batch in iter.by_ref() {
        let batch = batch.expect("valid batch");
        batch.verify_crc().expect("valid crc");
        if !batch.header().is_control {
            for r in batch.records(MAX).expect("records") {
                records.push(r.expect("valid record"));
            }
        }
        headers.push(batch.header().clone());
    }
    assert_eq!(iter.truncated_bytes(), 0);
    (headers, records)
}

/// The decompressed records section of each batch in `data`.
fn payloads(data: &'static [u8]) -> Vec<Vec<u8>> {
    parse_batches(Bytes::from_static(data))
        .map(|batch| {
            let batch = batch.unwrap();
            let payload = &batch.bytes()[BATCH_HEADER_LEN..];
            compression::decompress(batch.header().compression, payload, MAX).unwrap()
        })
        .collect()
}

const RDKAFKA_NONE: &[u8] = include_bytes!("testdata/rdkafka-none.bin");

#[mz_ore::test]
fn librdkafka_fixtures() {
    let fixtures: [(&[u8], Compression); 5] = [
        (RDKAFKA_NONE, Compression::None),
        (
            include_bytes!("testdata/rdkafka-gzip.bin"),
            Compression::Gzip,
        ),
        (
            include_bytes!("testdata/rdkafka-snappy.bin"),
            Compression::Snappy,
        ),
        (include_bytes!("testdata/rdkafka-lz4.bin"), Compression::Lz4),
        (
            include_bytes!("testdata/rdkafka-zstd.bin"),
            Compression::Zstd,
        ),
    ];
    for (data, codec) in fixtures {
        let (headers, records) = decode_fixture(data);
        assert_eq!(headers.len(), 1, "{codec}");
        let h = &headers[0];
        assert_eq!(h.compression, codec);
        assert_eq!((h.base_offset, h.last_offset, h.record_count), (0, 29, 30));
        assert_eq!(h.timestamp_type, TimestampType::CreateTime);
        assert!(!h.is_transactional && !h.is_control);
        assert_eq!(
            (h.producer_id, h.producer_epoch, h.base_sequence),
            (-1, -1, -1)
        );
        assert_eq!(h.first_timestamp, 1_700_000_000_000);
        assert_eq!(h.max_timestamp, 1_700_000_000_029);
        assert_eq!(
            records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            (0..30).collect::<Vec<_>>()
        );
        let got = records.iter().map(Rec::from_record).collect::<Vec<_>>();
        assert_eq!(got, fixture_records(), "{codec}");
        assert_eq!(payloads(data), payloads(RDKAFKA_NONE), "{codec}");
    }
}

/// Our uncompressed encoding is byte-for-byte librdkafka's, from the CRC on.
/// Before the CRC, the broker rewrote the base offset and leader epoch.
#[mz_ore::test]
fn encoding_matches_librdkafka() {
    let ours = encode(
        Compression::None,
        &fixture_records(),
        false,
        ProducerIdentity::NONE,
        -1,
    );
    assert_eq!(ours.bytes()[CRC..], RDKAFKA_NONE[CRC..]);
    assert_eq!(
        ours.bytes()[..PARTITION_LEADER_EPOCH],
        RDKAFKA_NONE[..PARTITION_LEADER_EPOCH]
    );
}

#[mz_ore::test]
fn java_recompressed_fixtures() {
    let fixtures: [(&[u8], Compression); 4] = [
        (
            include_bytes!("testdata/java-recompressed-gzip.bin"),
            Compression::Gzip,
        ),
        (
            include_bytes!("testdata/java-recompressed-snappy.bin"),
            Compression::Snappy,
        ),
        (
            include_bytes!("testdata/java-recompressed-lz4.bin"),
            Compression::Lz4,
        ),
        (
            include_bytes!("testdata/java-recompressed-zstd.bin"),
            Compression::Zstd,
        ),
    ];
    for (data, codec) in fixtures {
        let (headers, records) = decode_fixture(data);
        assert_eq!(headers.len(), 1, "{codec}");
        assert_eq!(headers[0].compression, codec);
        let got = records.iter().map(Rec::from_record).collect::<Vec<_>>();
        assert_eq!(got, fixture_records(), "{codec}");
        assert_eq!(payloads(data), payloads(RDKAFKA_NONE), "{codec}");
    }
    // The Java client frames snappy (librdkafka does not).
    let snappy = include_bytes!("testdata/java-recompressed-snappy.bin");
    assert!(snappy[BATCH_HEADER_LEN..].starts_with(b"\x82SNAPPY\x00"));
}

#[mz_ore::test]
fn java_producer_fixtures() {
    let fixtures: [(&[u8], Compression); 4] = [
        (
            include_bytes!("testdata/java-producer-gzip.bin"),
            Compression::Gzip,
        ),
        (
            include_bytes!("testdata/java-producer-snappy.bin"),
            Compression::Snappy,
        ),
        (
            include_bytes!("testdata/java-producer-lz4.bin"),
            Compression::Lz4,
        ),
        (
            include_bytes!("testdata/java-producer-zstd.bin"),
            Compression::Zstd,
        ),
    ];
    for (data, codec) in fixtures {
        let (headers, records) = decode_fixture(data);
        assert_eq!(headers.len(), 1, "{codec}");
        let h = &headers[0];
        assert_eq!(h.compression, codec);
        assert_eq!(h.record_count, 30);
        // The Java producer is idempotent by default.
        assert!(h.producer_id >= 0);
        assert_eq!((h.producer_epoch, h.base_sequence), (0, 0));
        assert!(!h.is_transactional);
        assert!(
            records
                .iter()
                .all(|r| r.timestamp_type == TimestampType::CreateTime
                    && (h.first_timestamp..=h.max_timestamp).contains(&r.timestamp))
        );
        let got = records
            .iter()
            .map(|r| Rec {
                timestamp: 0,
                ..Rec::from_record(r)
            })
            .collect::<Vec<_>>();
        assert_eq!(got, java_producer_records(), "{codec}");
    }
}

#[mz_ore::test]
fn log_append_time_fixture() {
    let data = include_bytes!("testdata/rdkafka-logappendtime-lz4.bin");
    let (headers, records) = decode_fixture(data);
    let h = &headers[0];
    assert_eq!(h.timestamp_type, TimestampType::LogAppendTime);
    assert!(h.max_timestamp > 1_700_000_000_029);
    assert_eq!(records.len(), 30);
    for (r, want) in records.iter().zip_eq(fixture_records()) {
        assert_eq!(r.timestamp, h.max_timestamp);
        assert_eq!(r.timestamp_type, TimestampType::LogAppendTime);
        assert_eq!(
            Rec {
                timestamp: want.timestamp,
                ..Rec::from_record(r)
            },
            want
        );
    }
}

#[mz_ore::test]
fn transactional_fixtures() {
    let fixtures: [(&[u8], i64); 2] = [
        (include_bytes!("testdata/rdkafka-txn-kafka.bin"), 0),
        // Redpanda's hidden transaction fence batches leave offset gaps.
        (include_bytes!("testdata/rdkafka-txn-redpanda.bin"), 1),
    ];
    let recs = fixture_records();
    for (data, first_offset) in fixtures {
        let batches = parse_batches(Bytes::from_static(data))
            .map(|b| b.unwrap())
            .collect::<Vec<_>>();
        let kinds = batches
            .iter()
            .map(|b| b.control_record().unwrap().map(|c| c.kind))
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [
                None,
                Some(ControlRecordKind::Commit),
                None,
                Some(ControlRecordKind::Abort),
                None,
                Some(ControlRecordKind::Commit),
            ]
        );
        let producer_id = batches[0].header().producer_id;
        assert!(producer_id >= 0);
        for b in &batches {
            let h = b.header();
            b.verify_crc().unwrap();
            assert!(h.is_transactional);
            assert_eq!(h.is_control, b.control_record().unwrap().is_some());
            assert_eq!((h.producer_id, h.producer_epoch), (producer_id, 0));
        }
        assert_eq!(batches[0].header().base_offset, first_offset);
        let data_batches = [
            (&batches[0], 0..5),
            (&batches[2], 5..8),
            (&batches[4], 8..10),
        ];
        let mut sequence = 0;
        for (batch, range) in data_batches {
            assert_eq!(batch.header().base_sequence, sequence);
            sequence += i32::try_from(range.len()).unwrap();
            let got = batch
                .records(MAX)
                .unwrap()
                .map(|r| Rec::from_record(&r.unwrap()))
                .collect::<Vec<_>>();
            assert_eq!(got, recs[range]);
        }
    }
}

/// Encode and decode throughput for 1 KiB records in 1 MiB batches. Run with
/// `cargo test --profile optimized -p mz-kafka-client -- --ignored
/// codec_throughput --nocapture`.
#[mz_ore::test]
#[ignore]
fn codec_throughput() {
    use std::time::Instant;

    // JSON-ish values of random words and numbers, which LZ4 compresses about
    // 2x, like typical topic data.
    let mut rng = rand::rngs::StdRng::seed_from_u64(3);
    let words: Vec<String> = (0..256)
        .map(|_| {
            let len = rng.random_range(3..10);
            (0..len)
                .map(|_| char::from(rng.random_range(b'a'..=b'z')))
                .collect()
        })
        .collect();
    let records: Vec<(Vec<u8>, Vec<u8>)> = (0..1000)
        .map(|i| {
            let mut value = format!("{{\"id\":{i},\"fields\":{{").into_bytes();
            while value.len() < 1000 {
                let word = &words[rng.random_range(0..words.len())];
                let n: u32 = rng.random();
                value.extend_from_slice(format!("\"{word}\":{n},").as_bytes());
            }
            value.truncate(1022);
            value.extend_from_slice(b"}}");
            (format!("key-{i}").into_bytes(), value)
        })
        .collect();
    let headers = [RecordHeader {
        key: "materialize-timestamp",
        value: Some(b"1700000000000"),
    }];

    for c in ALL_CODECS {
        for with_headers in [false, true] {
            let headers: &[RecordHeader<'_>] = if with_headers { &headers } else { &[] };
            let start = Instant::now();
            let mut iterations = 0;
            let mut batch = None;
            while start.elapsed().as_secs_f64() < 1.0 {
                let mut builder = BatchBuilder::with_capacity(c, None, false, 1100 * records.len());
                for (i, (k, v)) in records.iter().enumerate() {
                    let ts = 1_700_000_000_000 + i64::try_from(i).unwrap();
                    builder.append(ts, Some(k), Some(v), headers);
                }
                batch = Some(builder.build(ProducerIdentity::NONE, -1).unwrap());
                iterations += 1;
            }
            let encode = start.elapsed().as_secs_f64();
            let batch = batch.unwrap().bytes();

            let start = Instant::now();
            let mut decode_iterations = 0;
            let mut checksum = 0;
            while start.elapsed().as_secs_f64() < 1.0 {
                for b in parse_batches(batch.clone()) {
                    for r in b.unwrap().records(MAX).unwrap() {
                        let r = r.unwrap();
                        checksum += r.key.map_or(0, |k| k.len()) + r.value.map_or(0, |v| v.len());
                        for h in r.headers.iter() {
                            checksum += h.unwrap().0.len();
                        }
                    }
                }
                decode_iterations += 1;
            }
            let decode = start.elapsed().as_secs_f64();
            assert!(checksum > 0);
            let n = f64::from(u32::try_from(records.len()).unwrap());
            let uncompressed: usize = records.iter().map(|(k, v)| k.len() + v.len()).sum();
            let mib = f64::from(u32::try_from(uncompressed).unwrap()) / f64::from(1 << 20);
            println!(
                "{c:>6} headers={with_headers:<5} batch {:>7} B | encode {:>6.2} M rec/s {:>7.0} MiB/s | decode {:>6.2} M rec/s {:>7.0} MiB/s",
                batch.len(),
                n * f64::from(iterations) / encode / 1e6,
                mib * f64::from(iterations) / encode,
                n * f64::from(decode_iterations) / decode / 1e6,
                mib * f64::from(decode_iterations) / decode,
            );
        }
    }
}
