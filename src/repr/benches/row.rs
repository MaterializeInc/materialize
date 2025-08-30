// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::hint::black_box;

use arrow::array::StructArray;
use criterion::{Bencher, Criterion, Throughput, criterion_group, criterion_main};
use mz_persist::indexed::columnar::{ColumnarRecords, ColumnarRecordsBuilder};
use mz_persist::metrics::ColumnarMetrics;
use mz_persist_types::Codec;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::columnar::{ColumnDecoder, Schema};
use mz_persist_types::part::{Part, PartBuilder};
use mz_repr::adt::date::Date;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{Datum, ProtoRow, RelationDesc, Row, SqlColumnType, SqlScalarType};
use rand::distributions::{Alphanumeric, DistString};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

fn bench_sort_datums(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    b.iter_with_setup(|| rows.clone(), |mut rows| rows.sort())
}

fn bench_sort_row(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let rows = rows.into_iter().map(Row::pack).collect::<Vec<_>>();
    b.iter_with_setup(|| rows.clone(), |mut rows| rows.sort())
}

fn bench_sort_iter(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let rows = rows.into_iter().map(Row::pack).collect::<Vec<_>>();
    b.iter_with_setup(
        || rows.clone(),
        |mut rows| {
            rows.sort_by(move |a, b| {
                for (a, b) in a.iter().zip(b.iter()) {
                    match a.cmp(&b) {
                        Ordering::Equal => (),
                        non_equal => return non_equal,
                    }
                }
                Ordering::Equal
            });
        },
    )
}

fn bench_sort_unpack(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let rows = rows.into_iter().map(Row::pack).collect::<Vec<_>>();
    b.iter_with_setup(
        || rows.clone(),
        |mut rows| {
            #[allow(clippy::unnecessary_sort_by)]
            rows.sort_by(move |a, b| a.unpack().cmp(&b.unpack()));
        },
    )
}

fn bench_sort_unpacked(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let arity = rows[0].len();
    let rows = rows.into_iter().map(Row::pack).collect::<Vec<_>>();
    b.iter_with_setup(
        || rows.clone(),
        |rows| {
            let mut unpacked = vec![];
            for row in &rows {
                unpacked.extend(&*row);
            }
            let mut slices = unpacked.chunks(arity).collect::<Vec<_>>();
            slices.sort();
            slices.into_iter().map(Row::pack).collect::<Vec<_>>()
        },
    )
}

fn bench_filter_unpacked(filter: Datum, rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let rows = rows.into_iter().map(Row::pack).collect::<Vec<_>>();
    b.iter_with_setup(
        || rows.clone(),
        |mut rows| rows.retain(|row| row.unpack()[0] == filter),
    )
}

fn bench_filter_packed(filter: Datum, rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    let filter = Row::pack_slice(&[filter]);
    let rows = rows.into_iter().map(Row::pack).collect::<Vec<_>>();
    b.iter_with_setup(
        || rows.clone(),
        |mut rows| rows.retain(|row| row.unpack()[0] == filter.unpack_first()),
    )
}

fn bench_pack_pack(rows: Vec<Vec<Datum>>, b: &mut Bencher) {
    b.iter(|| rows.iter().map(Row::pack).collect::<Vec<_>>())
}

fn seeded_rng() -> StdRng {
    SeedableRng::from_seed([
        224, 38, 155, 23, 190, 65, 147, 224, 136, 172, 167, 36, 125, 199, 232, 59, 191, 4, 243,
        175, 114, 47, 213, 46, 85, 226, 227, 35, 238, 119, 237, 21,
    ])
}

pub fn bench_sort(c: &mut Criterion) {
    let num_rows = 10_000;

    let mut rng = seeded_rng();
    let int_rows = (0..num_rows)
        .map(|_| {
            vec![
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
            ]
        })
        .collect::<Vec<_>>();
    let numeric_rows = (0..num_rows)
        .map(|_| {
            vec![
                Datum::Numeric(rng.r#gen::<i32>().into()),
                Datum::Numeric(rng.r#gen::<i32>().into()),
                Datum::Numeric(rng.r#gen::<i32>().into()),
                Datum::Numeric(rng.r#gen::<i32>().into()),
                Datum::Numeric(rng.r#gen::<i32>().into()),
                Datum::Numeric(rng.r#gen::<i32>().into()),
            ]
        })
        .collect::<Vec<_>>();

    let mut rng = seeded_rng();
    let byte_data = (0..num_rows)
        .map(|_| {
            let i: i32 = rng.r#gen();
            format!("{} and then {} and then {}", i, i + 1, i + 2).into_bytes()
        })
        .collect::<Vec<_>>();
    let byte_rows = byte_data
        .iter()
        .map(|bytes| vec![Datum::Bytes(bytes)])
        .collect::<Vec<_>>();

    c.bench_function("sort_datums_ints", |b| {
        bench_sort_datums(int_rows.clone(), b)
    });
    c.bench_function("sort_row_ints", |b| bench_sort_row(int_rows.clone(), b));
    c.bench_function("sort_iter_ints", |b| bench_sort_iter(int_rows.clone(), b));
    c.bench_function("sort_unpack_ints", |b| {
        bench_sort_unpack(int_rows.clone(), b)
    });
    c.bench_function("sort_unpacked_ints", |b| {
        bench_sort_unpacked(int_rows.clone(), b)
    });

    c.bench_function("sort_datums_numeric", |b| {
        bench_sort_datums(numeric_rows.clone(), b)
    });
    c.bench_function("sort_row_numeric", |b| {
        bench_sort_row(numeric_rows.clone(), b)
    });
    c.bench_function("sort_iter_numeric", |b| {
        bench_sort_iter(numeric_rows.clone(), b)
    });
    c.bench_function("sort_unpack_numeric", |b| {
        bench_sort_unpack(numeric_rows.clone(), b)
    });
    c.bench_function("sort_unpacked_numeric", |b| {
        bench_sort_unpacked(numeric_rows.clone(), b)
    });

    c.bench_function("sort_datums_bytes", |b| {
        bench_sort_datums(byte_rows.clone(), b)
    });
    c.bench_function("sort_row_bytes", |b| bench_sort_row(byte_rows.clone(), b));
    c.bench_function("sort_iter_bytes", |b| bench_sort_iter(byte_rows.clone(), b));
    c.bench_function("sort_unpack_bytes", |b| {
        bench_sort_unpack(byte_rows.clone(), b)
    });
    c.bench_function("sort_unpacked_bytes", |b| {
        bench_sort_unpacked(byte_rows.clone(), b)
    });
}

pub fn bench_pack(c: &mut Criterion) {
    let num_rows = 10_000;

    let mut rng = seeded_rng();
    let int_rows = (0..num_rows)
        .map(|_| {
            vec![
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
            ]
        })
        .collect::<Vec<_>>();

    let mut rng = seeded_rng();
    let byte_data = (0..num_rows)
        .map(|_| {
            let i: i32 = rng.r#gen();
            format!("{} and then {} and then {}", i, i + 1, i + 2).into_bytes()
        })
        .collect::<Vec<_>>();
    let byte_rows = byte_data
        .iter()
        .map(|bytes| vec![Datum::Bytes(bytes)])
        .collect::<Vec<_>>();

    c.bench_function("pack_pack_ints", |b| bench_pack_pack(int_rows.clone(), b));

    c.bench_function("pack_pack_bytes", |b| bench_pack_pack(byte_rows.clone(), b));
}

fn bench_filter(c: &mut Criterion) {
    let num_rows = 10_000;
    let mut rng = seeded_rng();
    let mut random_date =
        || Date::from_pg_epoch(rng.gen_range(Date::LOW_DAYS..=Date::HIGH_DAYS)).unwrap();
    let mut rng = seeded_rng();
    let date_rows = (0..num_rows)
        .map(|_| {
            vec![
                Datum::Date(random_date()),
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
                Datum::Int32(rng.r#gen()),
            ]
        })
        .collect::<Vec<_>>();
    let filter = random_date();

    c.bench_function("filter_unpacked", |b| {
        bench_filter_unpacked(Datum::Date(filter), date_rows.clone(), b)
    });
    c.bench_function("filter_packed", |b| {
        bench_filter_packed(Datum::Date(filter), date_rows.clone(), b)
    });
}

fn encode_legacy(rows: &[Row]) -> ColumnarRecords {
    let mut buf = ColumnarRecordsBuilder::default();
    let mut key_buf = Vec::new();
    for row in rows.iter() {
        key_buf.clear();
        row.encode(&mut key_buf);
        assert!(buf.push(((&key_buf, &[]), 1i64.to_le_bytes(), 1i64.to_le_bytes())));
    }
    buf.finish(&ColumnarMetrics::disconnected())
}

fn decode_legacy(part: &ColumnarRecords, schema: &RelationDesc) -> Row {
    let mut storage = Some(ProtoRow::default());
    let mut row = Row::default();
    for ((key, _val), _ts, _diff) in part.iter() {
        Row::decode_from(&mut row, key, &mut storage, schema).unwrap();
        black_box(&row);
    }
    row
}

fn encode_structured2(schema: &RelationDesc, rows: &[Row]) -> Part {
    let mut builder = PartBuilder::new(schema, &UnitSchema);
    for row in rows.iter() {
        builder.push(row, &(), 1u64, 1i64);
    }
    builder.finish()
}

fn bench_roundtrip(c: &mut Criterion) {
    let num_rows = 50_000;
    let mut rng = seeded_rng();
    let rows = (0..num_rows)
        .map(|_| {
            let str_len = rng.gen_range(0..10);
            Row::pack(vec![
                Datum::from(rng.r#gen::<bool>()),
                Datum::from(rng.r#gen::<Option<bool>>()),
                Datum::from(Alphanumeric.sample_string(&mut rng, str_len).as_str()),
                Datum::from(
                    Some(Alphanumeric.sample_string(&mut rng, str_len).as_str())
                        .filter(|_| rng.r#gen::<bool>()),
                ),
            ])
        })
        .collect::<Vec<_>>();
    let schema = RelationDesc::from_names_and_types(vec![
        (
            "a",
            SqlColumnType {
                nullable: false,
                scalar_type: SqlScalarType::Bool,
            },
        ),
        (
            "b",
            SqlColumnType {
                nullable: true,
                scalar_type: SqlScalarType::Bool,
            },
        ),
        (
            "c",
            SqlColumnType {
                nullable: false,
                scalar_type: SqlScalarType::String,
            },
        ),
        (
            "d",
            SqlColumnType {
                nullable: true,
                scalar_type: SqlScalarType::String,
            },
        ),
    ]);

    c.bench_function("roundtrip_encode_legacy", |b| {
        b.iter(|| std::hint::black_box(encode_legacy(&rows)));
    });
    c.bench_function("roundtrip_encode_structured2", |b| {
        let mut builder = PartBuilder::new(&schema, &UnitSchema);
        b.iter(|| {
            for row in rows.iter() {
                builder.push(row, &(), 1u64, 1i64);
            }
            std::hint::black_box(&mut builder);
        });
    });

    let legacy = encode_legacy(&rows);
    let structured2 = encode_structured2(&schema, &rows);
    c.bench_function("roundtrip_decode_legacy", |b| {
        b.iter(|| std::hint::black_box(decode_legacy(&legacy, &schema)));
    });
    c.bench_function("roundtrip_decode_structured2", |b| {
        let col = structured2
            .key
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");
        let decoder =
            <RelationDesc as Schema<Row>>::decoder(&schema, col.clone()).expect("success");
        let mut row = Row::default();

        b.iter(|| {
            for idx in 0..rows.len() {
                decoder.decode(idx, &mut row);
                // We create a packer which clears the row.
                let _ = row.packer();
            }
            std::hint::black_box(&mut row);
        });
    });
}

fn bench_json(c: &mut Criterion) {
    const NUM_ROWS: u64 = 10_000;

    let mut group = c.benchmark_group("json");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let mut row = Row::default();
    row.packer().push_dict_with(|packer| {
        packer.push(Datum::String("details"));
        packer.push_dict_with(|packer| {
            packer.push(Datum::String("sha"));
            packer.push(Datum::String("i am not a GIT sha but that's okay"));

            packer.push(Datum::String("status"));
            packer.push(Datum::String("improvement (maybe?)"));

            packer.push(Datum::String("counter"));
            packer.push(Datum::from(Numeric::from(60000u64)));

            packer.push(Datum::String("enemies"));
            packer.push(Datum::JsonNull);

            packer.push(Datum::String("hungry"));
            packer.push(Datum::True);

            packer.push(Datum::String("timing_info"));
            packer.push_list([
                Datum::from(Numeric::from(1.0001f64)),
                Datum::from(Numeric::from(0.998f64)),
                Datum::from(Numeric::from(1.10000004f64)),
                Datum::from(Numeric::from(1.5555555f64)),
                Datum::from(Numeric::from(0.9999191f64)),
            ]);
        });

        packer.push(Datum::String("event-type"));
        packer.push(Datum::String("benchmarking"));

        packer.push(Datum::String("other_fields"));
        packer.push_dict_with(|packer| {
            packer.push(Datum::String("bar"));
            packer.push(Datum::String("I hope this JSON blob is large enough"));

            packer.push(Datum::String("baz"));
            packer.push(Datum::String("I am running out of ideas"));

            packer.push(Datum::String("foo"));
            packer.push_dict_with(|packer| {
                packer.push(Datum::String("three letter thing"));
                packer.push(Datum::String("this will have to do"));

                packer.push(Datum::String("my number"));
                packer.push(Datum::from(Numeric::from(6.345789f64)));
            })
        })
    });

    group.bench_function("encode/structured2", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", SqlScalarType::Jsonb.nullable(false))]);
        b.iter(|| {
            let mut builder = PartBuilder::new(&schema, &UnitSchema);
            for _ in 0..NUM_ROWS {
                std::hint::black_box(&mut builder).push(&row, &(), 1u64, 1i64);
            }
            let part = builder.finish();
            std::hint::black_box(part.key);
        });
    });

    group.bench_function("decode/structured2", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", SqlScalarType::Jsonb.nullable(false))]);
        let mut builder = PartBuilder::new(&schema, &UnitSchema);
        builder.push(&row, &(), 1u64, 1i64);
        let part = builder.finish();

        let col = part
            .key
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");
        let decoder =
            <RelationDesc as Schema<Row>>::decoder(&schema, col.clone()).expect("success");
        let mut row = Row::default();

        b.iter(|| {
            for _ in 0..NUM_ROWS {
                decoder.decode(0, std::hint::black_box(&mut row));
                std::hint::black_box(&mut row);
            }
        });
    });
}

fn bench_string(c: &mut Criterion) {
    const NUM_ROWS: u64 = 10_000;

    let mut group = c.benchmark_group("string");
    group.throughput(Throughput::Elements(NUM_ROWS));

    let row = Row::pack_slice(&[Datum::String(
        "I am a string that could be in a real row but I'm a fake row!",
    )]);

    group.bench_function("encode/structured2", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", SqlScalarType::String.nullable(false))]);
        b.iter(|| {
            let mut builder = PartBuilder::new(&schema, &UnitSchema);
            for _ in 0..NUM_ROWS {
                std::hint::black_box(&mut builder).push(&row, &(), 1u64, 1i64);
            }
            let part = builder.finish();
            std::hint::black_box(part.key);
        });
    });

    group.bench_function("decode/structured2", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", SqlScalarType::String.nullable(false))]);
        let mut builder = PartBuilder::new(&schema, &UnitSchema);
        builder.push(&row, &(), 1u64, 1i64);
        let part = builder.finish();

        let col = part
            .key
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");
        let decoder =
            <RelationDesc as Schema<Row>>::decoder(&schema, col.clone()).expect("success");
        let mut row = Row::default();

        b.iter(|| {
            for _ in 0..NUM_ROWS {
                decoder.decode(0, std::hint::black_box(&mut row));
                std::hint::black_box(&mut row);
            }
        });
    });
}

criterion_group!(
    benches,
    bench_sort,
    bench_pack,
    bench_filter,
    bench_roundtrip,
    bench_json,
    bench_string
);
criterion_main!(benches);
