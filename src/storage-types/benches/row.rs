// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use mz_persist::indexed::columnar::{ColumnarRecords, ColumnarRecordsBuilder};
use mz_persist::metrics::ColumnarMetrics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::columnar::{PartDecoder, PartEncoder, Schema};
use mz_persist_types::part::{Part, PartBuilder};
use mz_persist_types::Codec;
use mz_repr::{ColumnType, Datum, ProtoRow, RelationDesc, Row, ScalarType};
use mz_storage_types::sources::SourceData;
use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn encode_legacy(data: &[SourceData]) -> ColumnarRecords {
    let mut buf = ColumnarRecordsBuilder::default();
    let mut key_buf = Vec::new();
    for data in data.iter() {
        key_buf.clear();
        data.encode(&mut key_buf);
        assert!(buf.push(((&key_buf, &[]), 1i64.to_le_bytes(), 1i64.to_le_bytes())));
    }
    buf.finish(&ColumnarMetrics::disconnected())
}

fn decode_legacy(part: &ColumnarRecords) -> SourceData {
    let mut storage = Some(ProtoRow::default());
    let mut data = SourceData(Ok(Row::default()));
    for ((key, _val), _ts, _diff) in part.iter() {
        SourceData::decode_from(&mut data, key, &mut storage).unwrap();
        black_box(&data);
    }
    data
}

fn encode_structured(schema: &RelationDesc, data: &[SourceData]) -> Part {
    let mut part = PartBuilder::new::<SourceData, _, _, _>(schema, &UnitSchema);
    let mut part_mut = part.get_mut();
    let mut encoder = <RelationDesc as Schema<SourceData>>::encoder(schema, part_mut.key).unwrap();
    for data in data.iter() {
        encoder.encode(data);
    }
    drop(encoder);
    for _ in data.iter() {
        part_mut.ts.push(1u64);
        part_mut.diff.push(1i64);
    }
    part.finish().unwrap()
}

fn decode_structured(schema: &RelationDesc, part: &Part, len: usize) -> SourceData {
    let decoder = <RelationDesc as Schema<SourceData>>::decoder(schema, part.key_ref()).unwrap();
    let mut data = SourceData(Ok(Row::default()));
    for idx in 0..len {
        decoder.decode(idx, &mut data);
        black_box(&data);
    }
    data
}

fn bench_roundtrip(c: &mut Criterion, name: &str, schema: &RelationDesc, data: &[SourceData]) {
    c.bench_function(&format!("roundtrip_{}_encode_legacy", name), |b| {
        b.iter(|| std::hint::black_box(encode_legacy(data)));
    });
    c.bench_function(&format!("roundtrip_{}_encode_structured", name), |b| {
        b.iter(|| std::hint::black_box(encode_structured(schema, data)));
    });
    let legacy = encode_legacy(data);
    let structured = encode_structured(schema, data);
    c.bench_function(&format!("roundtrip_{}_decode_legacy", name), |b| {
        b.iter(|| std::hint::black_box(decode_legacy(&legacy)));
    });
    c.bench_function(&format!("roundtrip_{}_decode_structured", name), |b| {
        b.iter(|| std::hint::black_box(decode_structured(schema, &structured, data.len())));
    });
}

fn benches_roundtrip(c: &mut Criterion) {
    let num_rows = 16 * 1024;
    let mut rng: StdRng = SeedableRng::seed_from_u64(1);

    {
        let schema = RelationDesc::from_names_and_types(vec![
            (
                "a",
                ColumnType {
                    nullable: false,
                    scalar_type: ScalarType::UInt64,
                },
            ),
            (
                "b",
                ColumnType {
                    nullable: true,
                    scalar_type: ScalarType::UInt64,
                },
            ),
        ]);
        let data = (0..num_rows)
            .map(|_| {
                let row = Row::pack(vec![
                    Datum::from(rng.gen::<u64>()),
                    Datum::from(rng.gen::<Option<u64>>()),
                ]);
                SourceData(Ok(row))
            })
            .collect::<Vec<_>>();
        bench_roundtrip(c, "int64", &schema, &data);
    }

    {
        let schema = RelationDesc::from_names_and_types(vec![
            (
                "a",
                ColumnType {
                    nullable: false,
                    scalar_type: ScalarType::Bytes,
                },
            ),
            (
                "b",
                ColumnType {
                    nullable: true,
                    scalar_type: ScalarType::Bytes,
                },
            ),
        ]);
        let data = (0..num_rows)
            .map(|_| {
                let str_len = rng.gen_range(0..10);
                let row = Row::pack(vec![
                    Datum::from(Alphanumeric.sample_string(&mut rng, str_len).as_bytes()),
                    Datum::from(
                        Some(Alphanumeric.sample_string(&mut rng, str_len).as_bytes())
                            .filter(|_| rng.gen::<bool>()),
                    ),
                ]);
                SourceData(Ok(row))
            })
            .collect::<Vec<_>>();
        bench_roundtrip(c, "bytes", &schema, &data);
    }

    {
        let schema = RelationDesc::from_names_and_types(vec![
            (
                "a",
                ColumnType {
                    nullable: false,
                    scalar_type: ScalarType::String,
                },
            ),
            (
                "b",
                ColumnType {
                    nullable: true,
                    scalar_type: ScalarType::String,
                },
            ),
        ]);
        let data = (0..num_rows)
            .map(|_| {
                let str_len = rng.gen_range(0..10);
                let row = Row::pack(vec![
                    Datum::from(Alphanumeric.sample_string(&mut rng, str_len).as_str()),
                    Datum::from(
                        Some(Alphanumeric.sample_string(&mut rng, str_len).as_str())
                            .filter(|_| rng.gen::<bool>()),
                    ),
                ]);
                SourceData(Ok(row))
            })
            .collect::<Vec<_>>();
        bench_roundtrip(c, "string", &schema, &data);
    }
}

criterion_group!(benches, benches_roundtrip);
criterion_main!(benches);
