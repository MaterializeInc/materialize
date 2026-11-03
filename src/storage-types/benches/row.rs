// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_alloc_default as _;
use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use mz_persist::indexed::columnar::{ColumnarRecords, ColumnarRecordsBuilder};
use mz_persist::metrics::ColumnarMetrics;
use mz_persist_types::Codec;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::columnar::{ColumnDecoder, Schema};
use mz_persist_types::part::{Part, PartBuilder};
use mz_repr::adt::date::Date;
use mz_repr::adt::numeric::{self, Numeric};
use mz_repr::{Datum, ProtoRow, RelationDesc, Row, SqlColumnType, SqlScalarType};
use mz_storage_types::sources::SourceData;
use rand::distr::{Alphanumeric, Distribution, SampleString, StandardUniform};
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};

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

fn decode_legacy(part: &ColumnarRecords, schema: &RelationDesc) -> SourceData {
    let mut storage = Some(ProtoRow::default());
    let mut data = SourceData(Ok(Row::default()));
    for ((key, _val), _ts, _diff) in part.iter() {
        SourceData::decode_from(&mut data, key, &mut storage, schema).unwrap();
        black_box(&data);
    }
    data
}

fn random_option<T>(rng: &mut StdRng) -> Option<T>
where
    StandardUniform: Distribution<T>,
{
    if rng.random::<bool>() {
        Some(rng.random())
    } else {
        None
    }
}

fn bench_roundtrip(c: &mut Criterion, name: &str, schema: &RelationDesc, data: &[SourceData]) {
    c.bench_function(&format!("roundtrip_{}_encode_legacy", name), |b| {
        b.iter(|| std::hint::black_box(encode_legacy(data)));
    });
    let legacy = encode_legacy(data);
    c.bench_function(&format!("roundtrip_{}_decode_legacy", name), |b| {
        b.iter(|| std::hint::black_box(decode_legacy(&legacy, schema)));
    });
}

fn benches_roundtrip(c: &mut Criterion) {
    let num_rows = 16 * 1024;
    let mut rng: StdRng = SeedableRng::seed_from_u64(1);

    {
        let schema = RelationDesc::from_names_and_types(vec![
            (
                "a",
                SqlColumnType {
                    nullable: false,
                    scalar_type: SqlScalarType::UInt64,
                },
            ),
            (
                "b",
                SqlColumnType {
                    nullable: true,
                    scalar_type: SqlScalarType::UInt64,
                },
            ),
        ]);
        let data = (0..num_rows)
            .map(|_| {
                let row = Row::pack(vec![
                    Datum::from(rng.random::<u64>()),
                    Datum::from(random_option::<u64>(&mut rng)),
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
                SqlColumnType {
                    nullable: false,
                    scalar_type: SqlScalarType::Bytes,
                },
            ),
            (
                "b",
                SqlColumnType {
                    nullable: true,
                    scalar_type: SqlScalarType::Bytes,
                },
            ),
        ]);
        let data = (0..num_rows)
            .map(|_| {
                let str_len = rng.random_range(0..10);
                let row = Row::pack(vec![
                    Datum::from(Alphanumeric.sample_string(&mut rng, str_len).as_bytes()),
                    Datum::from(
                        Some(Alphanumeric.sample_string(&mut rng, str_len).as_bytes())
                            .filter(|_| rng.random::<bool>()),
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
                SqlColumnType {
                    nullable: false,
                    scalar_type: SqlScalarType::String,
                },
            ),
            (
                "b",
                SqlColumnType {
                    nullable: true,
                    scalar_type: SqlScalarType::String,
                },
            ),
        ]);
        let data = (0..num_rows)
            .map(|_| {
                let str_len = rng.random_range(0..10);
                let row = Row::pack(vec![
                    Datum::from(Alphanumeric.sample_string(&mut rng, str_len).as_str()),
                    Datum::from(
                        Some(Alphanumeric.sample_string(&mut rng, str_len).as_str())
                            .filter(|_| rng.random::<bool>()),
                    ),
                ]);
                SourceData(Ok(row))
            })
            .collect::<Vec<_>>();
        bench_roundtrip(c, "string", &schema, &data);
    }
}

fn encode_part(schema: &RelationDesc, data: &[SourceData]) -> Part {
    let mut builder = PartBuilder::new(schema, &UnitSchema);
    for data in data {
        builder.push(data, &(), 1u64, 1i64);
    }
    builder.finish()
}

/// A TPC-H `lineitem`-shaped relation: four `numeric(12,2)` columns, which dominate the persist
/// write path's encoding and stats cost, next to integers, dates, and short strings.
fn lineitem(num_rows: usize, rng: &mut StdRng) -> (RelationDesc, Vec<SourceData>) {
    let col = |scalar_type| SqlColumnType {
        nullable: false,
        scalar_type,
    };
    let numeric = || {
        col(SqlScalarType::Numeric {
            max_scale: Some(numeric::NumericMaxScale::try_from(2i64).unwrap()),
        })
    };
    let schema = RelationDesc::from_names_and_types(vec![
        ("l_orderkey", col(SqlScalarType::Int32)),
        ("l_partkey", col(SqlScalarType::Int32)),
        ("l_suppkey", col(SqlScalarType::Int32)),
        ("l_linenumber", col(SqlScalarType::Int32)),
        ("l_quantity", numeric()),
        ("l_extendedprice", numeric()),
        ("l_discount", numeric()),
        ("l_tax", numeric()),
        ("l_returnflag", col(SqlScalarType::String)),
        ("l_linestatus", col(SqlScalarType::String)),
        ("l_shipdate", col(SqlScalarType::Date)),
        ("l_commitdate", col(SqlScalarType::Date)),
        ("l_receiptdate", col(SqlScalarType::Date)),
        ("l_shipinstruct", col(SqlScalarType::String)),
        ("l_shipmode", col(SqlScalarType::String)),
        ("l_comment", col(SqlScalarType::String)),
    ]);
    let mut cx = numeric::cx_datum();
    let mut cents = |rng: &mut StdRng, max: i64| -> Numeric {
        let mut n = cx.from_i64(rng.random_range(0..max));
        n.set_exponent(n.exponent() - 2);
        numeric::rescale(&mut n, 2).unwrap();
        n
    };
    let date = |rng: &mut StdRng| Date::from_pg_epoch(rng.random_range(-2500..0)).unwrap();
    let flags = ["A", "N", "R"];
    let modes = ["AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP", "TRUCK"];
    let data = (0..num_rows)
        .map(|_| {
            let comment_len = rng.random_range(10..44);
            let comment = Alphanumeric.sample_string(rng, comment_len);
            let row = Row::pack(vec![
                Datum::Int32(rng.random_range(0..6_000_000)),
                Datum::Int32(rng.random_range(0..200_000)),
                Datum::Int32(rng.random_range(0..10_000)),
                Datum::Int32(rng.random_range(1..8)),
                Datum::from(cents(rng, 5_000)),
                Datum::from(cents(rng, 10_000_000)),
                Datum::from(cents(rng, 11)),
                Datum::from(cents(rng, 9)),
                Datum::String(flags[rng.random_range(0..flags.len())]),
                Datum::String(flags[rng.random_range(0..2)]),
                Datum::Date(date(rng)),
                Datum::Date(date(rng)),
                Datum::Date(date(rng)),
                Datum::String("DELIVER IN PERSON"),
                Datum::String(modes[rng.random_range(0..modes.len())]),
                Datum::String(&comment),
            ]);
            SourceData(Ok(row))
        })
        .collect();
    (schema, data)
}

fn benches_lineitem(c: &mut Criterion) {
    let mut rng: StdRng = SeedableRng::seed_from_u64(1);
    let (schema, data) = lineitem(16 * 1024, &mut rng);

    c.bench_function("lineitem_encode_structured", |b| {
        b.iter(|| black_box(encode_part(&schema, &data)));
    });
    let part = encode_part(&schema, &data);
    c.bench_function("lineitem_stats", |b| {
        b.iter(|| {
            let decoder = Schema::<SourceData>::decoder_any(&schema, part.key.as_ref()).unwrap();
            black_box(decoder.stats())
        });
    });
}

criterion_group!(benches, benches_roundtrip, benches_lineitem);
criterion_main!(benches);
