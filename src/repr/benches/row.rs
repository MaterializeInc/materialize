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
use std::iter::Peekable;
use std::ops::Range;

use arrow::array::{Array, ArrayRef, AsArray, StructArray};
use arrow::compute::{SortColumn, SortOptions};
use arrow::datatypes::DataType;
use arrow_row::SortField;
use criterion::{criterion_group, criterion_main, Bencher, Criterion, Throughput};
use itertools::Itertools;
use mz_persist::indexed::columnar::{ColumnarRecords, ColumnarRecordsBuilder};
use mz_persist::metrics::ColumnarMetrics;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::columnar::{ColumnDecoder, ColumnEncoder, PartDecoder, Schema, Schema2};
use mz_persist_types::part::{Part, Part2, PartBuilder, PartBuilder2};
use mz_persist_types::stats::PartStats;
use mz_persist_types::Codec;
use mz_repr::adt::date::Date;
use mz_repr::adt::numeric::Numeric;
use mz_repr::{ColumnType, Datum, ProtoRow, RelationDesc, Row, ScalarType};
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
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
            ]
        })
        .collect::<Vec<_>>();
    let numeric_rows = (0..num_rows)
        .map(|_| {
            vec![
                Datum::Numeric(rng.gen::<i32>().into()),
                Datum::Numeric(rng.gen::<i32>().into()),
                Datum::Numeric(rng.gen::<i32>().into()),
                Datum::Numeric(rng.gen::<i32>().into()),
                Datum::Numeric(rng.gen::<i32>().into()),
                Datum::Numeric(rng.gen::<i32>().into()),
            ]
        })
        .collect::<Vec<_>>();

    let mut rng = seeded_rng();
    let byte_data = (0..num_rows)
        .map(|_| {
            let i: i32 = rng.gen();
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
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
            ]
        })
        .collect::<Vec<_>>();

    let mut rng = seeded_rng();
    let byte_data = (0..num_rows)
        .map(|_| {
            let i: i32 = rng.gen();
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
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
                Datum::Int32(rng.gen()),
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

fn decode_legacy(part: &ColumnarRecords) -> Row {
    let mut storage = Some(ProtoRow::default());
    let mut row = Row::default();
    for ((key, _val), _ts, _diff) in part.iter() {
        Row::decode_from(&mut row, key, &mut storage).unwrap();
        black_box(&row);
    }
    row
}

fn encode_structured(schema: &RelationDesc, rows: &[Row]) -> Part {
    let mut builder = PartBuilder::new(schema, &UnitSchema).expect("success");
    for row in rows.iter() {
        builder.push(row, &(), 1u64, 1i64);
    }
    builder.finish()
}

fn encode_structured2(schema: &RelationDesc, rows: &[Row]) -> Part2 {
    let mut builder = PartBuilder2::new(schema, &UnitSchema);
    for row in rows.iter() {
        builder.push(row, &(), 1, 1);
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
                Datum::from(rng.gen::<bool>()),
                Datum::from(rng.gen::<Option<bool>>()),
                Datum::from(Alphanumeric.sample_string(&mut rng, str_len).as_str()),
                Datum::from(
                    Some(Alphanumeric.sample_string(&mut rng, str_len).as_str())
                        .filter(|_| rng.gen::<bool>()),
                ),
            ])
        })
        .collect::<Vec<_>>();
    let schema = RelationDesc::from_names_and_types(vec![
        (
            "a",
            ColumnType {
                nullable: false,
                scalar_type: ScalarType::Bool,
            },
        ),
        (
            "b",
            ColumnType {
                nullable: true,
                scalar_type: ScalarType::Bool,
            },
        ),
        (
            "c",
            ColumnType {
                nullable: false,
                scalar_type: ScalarType::String,
            },
        ),
        (
            "d",
            ColumnType {
                nullable: true,
                scalar_type: ScalarType::String,
            },
        ),
    ]);

    c.bench_function("roundtrip_encode_legacy", |b| {
        b.iter(|| std::hint::black_box(encode_legacy(&rows)));
    });
    c.bench_function("roundtrip_encode_structured", |b| {
        let mut builder = PartBuilder::new(&schema, &UnitSchema).expect("success");
        b.iter(|| {
            for row in rows.iter() {
                builder.push(row, &(), 1u64, 1i64);
            }
            std::hint::black_box(&mut builder);
        });
    });
    c.bench_function("roundtrip_encode_structured2", |b| {
        let mut builder = PartBuilder2::new(&schema, &UnitSchema);
        b.iter(|| {
            for row in rows.iter() {
                builder.push(row, &(), 1, 1);
            }
            std::hint::black_box(&mut builder);
        });
    });

    let legacy = encode_legacy(&rows);
    let structured = encode_structured(&schema, &rows);
    let structured2 = encode_structured2(&schema, &rows);
    c.bench_function("roundtrip_decode_legacy", |b| {
        b.iter(|| std::hint::black_box(decode_legacy(&legacy)));
    });
    c.bench_function("roundtrip_decode_structured", |b| {
        let ((), decoder) = schema.decoder(structured.key_ref()).unwrap();
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
    c.bench_function("roundtrip_decode_structured2", |b| {
        let col = structured2
            .key
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");
        let decoder =
            <RelationDesc as Schema2<Row>>::decoder(&schema, col.clone()).expect("success");
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

    let mut builder = PartBuilder2::new(&schema, &UnitSchema);
    for row in rows.iter() {
        builder.push(row, &(), 1, 1);
    }
    let part = builder.finish();
    let key_cols = part.key.as_struct().columns();

    c.bench_function("sort_structured2_repr", |b| {
        let decoder =
            <RelationDesc as Schema2<Row>>::decoder(&schema, part.key.as_struct().clone()).unwrap();

        b.iter(|| {
            let mut unpacked = vec![];
            for idx in 0..rows.len() {
                let mut row = Row::default();
                decoder.decode(idx, &mut row);
                unpacked.push(row);
            }
            unpacked.sort_by(|a, b| a.iter().cmp(b.iter()));

            let mut encoder = <RelationDesc as Schema2<Row>>::encoder(&schema).unwrap();
            for row in &rows {
                encoder.append(row);
            }
            let (finished, _) = encoder.finish();
            std::hint::black_box(finished)
        });
    });
    c.bench_function("sort_structured2_lexsort", |b| {
        let sort_cols: Vec<_> = key_cols
            .iter()
            .map(|c| SortColumn {
                values: c.clone(),
                options: None,
            })
            .collect();
        b.iter(|| {
            let sorted = arrow::compute::lexsort(&sort_cols, None).unwrap();
            std::hint::black_box(sorted)
        });
    });

    c.bench_function("sort_structured2_row", |b| {
        let converter = arrow_row::RowConverter::new(
            key_cols
                .iter()
                .map(|c| SortField::new(c.data_type().clone()))
                .collect(),
        )
        .unwrap();
        b.iter(|| {
            let rows = converter.convert_columns(key_cols).unwrap();
            let mut sort: Vec<_> = rows.iter().collect();
            sort.sort();
            let sorted = converter.convert_rows(sort).unwrap();
            std::hint::black_box(sorted)
        });
    });

    let sort_range = |range: Range<usize>| {
        let sliced: Vec<_> = key_cols
            .iter()
            .map(|c| SortColumn {
                values: c.slice(range.start, range.end - range.start),
                options: None,
            })
            .collect();
        arrow::compute::lexsort(&sliced, None).unwrap()
    };
    let first_cols = sort_range(0..(num_rows / 2));
    let second_cols = sort_range((num_rows / 2)..num_rows);
    struct MergeIter<A: Iterator>(Peekable<A>, Peekable<A>);

    impl<A: Iterator> Iterator for MergeIter<A>
    where
        A::Item: Ord,
    {
        type Item = A::Item;

        fn next(&mut self) -> Option<Self::Item> {
            match (self.0.peek(), self.1.peek()) {
                (None, None) => None,
                (Some(_), None) => self.0.next(),
                (None, Some(_)) => self.1.next(),
                (Some(a), Some(b)) => {
                    if a < b {
                        self.0.next()
                    } else {
                        self.1.next()
                    }
                }
            }
        }
    }
    c.bench_function("sort_structured2_merge_repr", |b| {
        let decode = |arrays: &[ArrayRef]| {
            let array =
                StructArray::new(part.key.as_struct().fields().clone(), arrays.to_vec(), None);
            let decoder = <RelationDesc as Schema2<Row>>::decoder(&schema, array).unwrap();
            let mut unpacked = vec![];
            for idx in 0..(num_rows / 2) {
                let mut row = Row::default();
                decoder.decode(idx, &mut row);
                unpacked.push(row);
            }
            unpacked
        };

        #[derive(PartialEq, Eq)]
        struct AsDatums<'a>(&'a Row);

        impl<'a> PartialOrd for AsDatums<'a> {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }

        impl<'a> Ord for AsDatums<'a> {
            fn cmp(&self, other: &Self) -> Ordering {
                self.0.iter().cmp(other.0.iter())
            }
        }

        b.iter(|| {
            let left = decode(&first_cols);
            let right = decode(&second_cols);

            let merged = MergeIter(
                left.iter().map(AsDatums).peekable(),
                right.iter().map(AsDatums).peekable(),
            );

            let mut encoder = <RelationDesc as Schema2<Row>>::encoder(&schema).unwrap();
            for AsDatums(row) in merged {
                encoder.append(row);
            }
            let (finished, _) = encoder.finish();
            std::hint::black_box(finished)
        });
    });
    c.bench_function("sort_structured2_merge_lexsort", |b| {
        b.iter(|| {
            let concat: Vec<_> = first_cols
                .iter()
                .zip(second_cols.iter())
                .map(|(a, b)| SortColumn {
                    values: arrow::compute::concat(&[a, b]).unwrap(),
                    options: None,
                })
                .collect();
            let sorted = arrow::compute::lexsort(&concat, None).unwrap();
            std::hint::black_box(sorted)
        });
    });

    c.bench_function("sort_structured2_merge_row", |b| {
        let converter = arrow_row::RowConverter::new(
            key_cols
                .iter()
                .map(|c| SortField::new(c.data_type().clone()))
                .collect(),
        )
        .unwrap();

        b.iter(|| {
            let first_rows = converter.convert_columns(&first_cols).unwrap();
            let second_rows = converter.convert_columns(&second_cols).unwrap();
            let sort = MergeIter(first_rows.iter().peekable(), second_rows.iter().peekable());
            let sorted = converter.convert_rows(sort).unwrap();
            std::hint::black_box(sorted)
        });
    });

    c.bench_function("sort_structured2_interleave_repr", |b| {
        let decode = |arrays: &[ArrayRef]| {
            let array =
                StructArray::new(part.key.as_struct().fields().clone(), arrays.to_vec(), None);
            let decoder = <RelationDesc as Schema2<Row>>::decoder(&schema, array).unwrap();
            let mut unpacked = vec![];
            for idx in 0..(num_rows / 2) {
                let mut row = Row::default();
                decoder.decode(idx, &mut row);
                unpacked.push(row);
            }
            unpacked
        };

        b.iter(|| {
            let left = decode(&first_cols);
            let right = decode(&second_cols);

            let mut encoder = <RelationDesc as Schema2<Row>>::encoder(&schema).unwrap();
            for row in left.iter().interleave(right.iter()) {
                encoder.append(row);
            }
            let (finished, _) = encoder.finish();
            std::hint::black_box(finished)
        });
    });
    c.bench_function("sort_structured2_interleave_lexsort", |b| {
        let indices: Vec<_> = (0..num_rows).map(|i| (i % 2, i / 2)).collect();
        b.iter(|| {
            let concat: Vec<_> = first_cols
                .iter()
                .zip(second_cols.iter())
                .map(|(a, b)| arrow::compute::interleave(&[a, b], &indices).unwrap())
                .collect();
            std::hint::black_box(concat)
        });
    });

    c.bench_function("sort_structured2_interleave_row", |b| {
        let converter = arrow_row::RowConverter::new(
            key_cols
                .iter()
                .map(|c| SortField::new(c.data_type().clone()))
                .collect(),
        )
        .unwrap();

        b.iter(|| {
            let first_rows = converter.convert_columns(&first_cols).unwrap();
            let second_rows = converter.convert_columns(&second_cols).unwrap();
            let sort = first_rows.iter().interleave(second_rows.iter());
            let sorted = converter.convert_rows(sort).unwrap();
            std::hint::black_box(sorted)
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

    group.bench_function("encode/structured", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", ScalarType::Jsonb.nullable(false))]);
        b.iter(|| {
            let mut builder = PartBuilder::new(&schema, &UnitSchema).unwrap();
            for _ in 0..NUM_ROWS {
                std::hint::black_box(&mut builder).push(&row, &(), 1i64, 1u64);
            }
            let part = builder.finish();
            let stats = PartStats::new(std::hint::black_box(&part)).unwrap();
            std::hint::black_box((part, stats));
        });
    });

    group.bench_function("encode/structured2", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", ScalarType::Jsonb.nullable(false))]);
        b.iter(|| {
            let mut builder = PartBuilder2::new(&schema, &UnitSchema);
            for _ in 0..NUM_ROWS {
                std::hint::black_box(&mut builder).push(&row, &(), 1, 1);
            }
            let part = builder.finish();
            std::hint::black_box((part.key, part.key_stats));
        });
    });

    group.bench_function("decode/structured", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", ScalarType::Jsonb.nullable(false))]);
        let mut builder = PartBuilder::new(&schema, &UnitSchema).unwrap();
        builder.push(&row, &(), 1i64, 1u64);
        let part = builder.finish();

        let decoder =
            <RelationDesc as Schema<Row>>::decoder(&schema, part.key_ref()).expect("success");
        let mut row = Row::default();

        b.iter(|| {
            for _ in 0..NUM_ROWS {
                decoder.decode(0, std::hint::black_box(&mut row));
                std::hint::black_box(&mut row);
            }
        });
    });

    group.bench_function("decode/structured2", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", ScalarType::Jsonb.nullable(false))]);
        let mut builder = PartBuilder2::new(&schema, &UnitSchema);
        builder.push(&row, &(), 1, 1);
        let part = builder.finish();

        let col = part
            .key
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");
        let decoder =
            <RelationDesc as Schema2<Row>>::decoder(&schema, col.clone()).expect("success");
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

    group.bench_function("encode/structured", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", ScalarType::String.nullable(false))]);
        b.iter(|| {
            let mut builder = PartBuilder::new(&schema, &UnitSchema).unwrap();
            for _ in 0..NUM_ROWS {
                std::hint::black_box(&mut builder).push(&row, &(), 1i64, 1u64);
            }
            let part = builder.finish();
            let stats = PartStats::new(std::hint::black_box(&part)).unwrap();
            std::hint::black_box((part, stats));
        });
    });

    group.bench_function("encode/structured2", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", ScalarType::String.nullable(false))]);
        b.iter(|| {
            let mut builder = PartBuilder2::new(&schema, &UnitSchema);
            for _ in 0..NUM_ROWS {
                std::hint::black_box(&mut builder).push(&row, &(), 1, 1);
            }
            let part = builder.finish();
            std::hint::black_box((part.key, part.key_stats));
        });
    });

    group.bench_function("decode/structured", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", ScalarType::String.nullable(false))]);
        let mut builder = PartBuilder::new(&schema, &UnitSchema).unwrap();
        builder.push(&row, &(), 1i64, 1u64);
        let part = builder.finish();

        let decoder =
            <RelationDesc as Schema<Row>>::decoder(&schema, part.key_ref()).expect("success");
        let mut row = Row::default();

        b.iter(|| {
            for _ in 0..NUM_ROWS {
                decoder.decode(0, std::hint::black_box(&mut row));
                std::hint::black_box(&mut row);
            }
        });
    });

    group.bench_function("decode/structured2", |b| {
        let schema =
            RelationDesc::from_names_and_types(vec![("a", ScalarType::String.nullable(false))]);
        let mut builder = PartBuilder2::new(&schema, &UnitSchema);
        builder.push(&row, &(), 1, 1);
        let part = builder.finish();

        let col = part
            .key
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("struct array");
        let decoder =
            <RelationDesc as Schema2<Row>>::decoder(&schema, col.clone()).expect("success");
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
