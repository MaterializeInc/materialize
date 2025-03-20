// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use mz_persist_types::columnar::FixedSizeCodec;
use mz_proto::chrono::ProtoNaiveTime;
use mz_proto::{ProtoType, RustType};
use mz_repr::adt::datetime::PackedNaiveTime;
use mz_repr::adt::interval::{Interval, PackedInterval, ProtoInterval};
use mz_repr::adt::mz_acl_item::{
    AclItem, AclMode, MzAclItem, PackedAclItem, PackedMzAclItem, ProtoAclItem, ProtoMzAclItem,
};
use mz_repr::adt::numeric::{Numeric, PackedNumeric};
use mz_repr::adt::system::Oid;
use mz_repr::adt::timestamp::PackedNaiveDateTime;
use mz_repr::role_id::RoleId;
use mz_repr::ProtoNumeric;
use prost::Message;

fn bench_interval(c: &mut Criterion) {
    let mut group = c.benchmark_group("Interval");
    group.throughput(Throughput::Elements(1));

    const INTERVAL: Interval = Interval::new(1, 1, 0);
    group.bench_function("encode", |b| {
        let mut buf = [0u8; 32];
        b.iter(|| {
            let packed = PackedInterval::from_value(std::hint::black_box(INTERVAL));
            std::hint::black_box(&mut buf[..PackedInterval::SIZE])
                .copy_from_slice(packed.as_bytes());
        })
    });
    group.bench_function("encode/proto", |b| {
        let mut buf = vec![0u8; 64];
        b.iter(|| {
            let proto = std::hint::black_box(INTERVAL).into_proto();
            proto.encode(std::hint::black_box(&mut buf)).unwrap();
            buf.clear();
        });
    });

    const PACKED: [u8; 16] = [128, 0, 0, 1, 128, 0, 0, 1, 128, 0, 0, 0, 0, 0, 0, 0];
    group.bench_function("decode", |b| {
        b.iter(|| {
            let packed = PackedInterval::from_bytes(std::hint::black_box(&PACKED)).unwrap();
            let normal = std::hint::black_box(packed).into_value();
            std::hint::black_box(normal);
        })
    });
    const ENCODED: [u8; 4] = [8, 1, 16, 1];
    group.bench_function("decode/proto", |b| {
        b.iter(|| {
            let proto = ProtoInterval::decode(std::hint::black_box(&ENCODED[..])).unwrap();
            let normal: Interval = std::hint::black_box(proto).into_rust().unwrap();
            std::hint::black_box(normal);
        })
    });

    group.finish();
}

fn bench_time(c: &mut Criterion) {
    let mut group = c.benchmark_group("NaiveTime");
    group.throughput(Throughput::Elements(1));

    let naive_time = NaiveTime::from_hms_opt(1, 1, 1).unwrap();
    group.bench_function("encode", |b| {
        let mut buf = [0u8; 32];
        b.iter(|| {
            let packed = PackedNaiveTime::from_value(std::hint::black_box(naive_time));
            std::hint::black_box(&mut buf[..PackedNaiveTime::SIZE])
                .copy_from_slice(packed.as_bytes());
        })
    });
    group.bench_function("encode/proto", |b| {
        let mut buf = vec![0u8; 64];
        b.iter(|| {
            let proto = std::hint::black_box(naive_time).into_proto();
            proto.encode(std::hint::black_box(&mut buf)).unwrap();
            buf.clear();
        });
    });

    const PACKED: [u8; 8] = [0, 0, 14, 77, 0, 0, 0, 0];
    group.bench_function("decode", |b| {
        b.iter(|| {
            let packed = PackedNaiveTime::from_bytes(std::hint::black_box(&PACKED)).unwrap();
            let normal = std::hint::black_box(packed).into_value();
            std::hint::black_box(normal);
        })
    });
    const ENCODED: [u8; 3] = [8, 205, 28];
    group.bench_function("decode/proto", |b| {
        b.iter(|| {
            let proto = ProtoNaiveTime::decode(std::hint::black_box(&ENCODED[..])).unwrap();
            let normal: NaiveTime = std::hint::black_box(proto).into_rust().unwrap();
            std::hint::black_box(normal);
        })
    });

    group.finish()
}

fn bench_acl_item(c: &mut Criterion) {
    let mut group = c.benchmark_group("AclItem");
    group.throughput(Throughput::Elements(1));

    let acl_item = AclItem {
        grantee: Oid(1),
        grantor: Oid(2),
        acl_mode: AclMode::all(),
    };
    group.bench_function("encode", |b| {
        let mut buf = [0u8; 32];
        b.iter(|| {
            let packed = PackedAclItem::from_value(std::hint::black_box(acl_item));
            std::hint::black_box(&mut buf[..PackedAclItem::SIZE])
                .copy_from_slice(packed.as_bytes());
        })
    });
    group.bench_function("encode/proto", |b| {
        let mut buf = vec![0u8; 64];
        b.iter(|| {
            let proto = std::hint::black_box(acl_item).into_proto();
            proto.encode(std::hint::black_box(&mut buf)).unwrap();
            buf.clear();
        })
    });

    const PACKED: [u8; 16] = [0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0, 224, 0, 3, 15];
    group.bench_function("decode", |b| {
        b.iter(|| {
            let packed = PackedAclItem::from_bytes(std::hint::black_box(&PACKED)).unwrap();
            let normal = std::hint::black_box(packed).into_value();
            std::hint::black_box(normal);
        })
    });
    const ENCODED: [u8; 12] = [8, 1, 16, 2, 26, 6, 8, 143, 134, 128, 128, 14];
    group.bench_function("decode/proto", |b| {
        b.iter(|| {
            let proto = ProtoAclItem::decode(std::hint::black_box(&ENCODED[..])).unwrap();
            let normal: AclItem = std::hint::black_box(proto).into_rust().unwrap();
            std::hint::black_box(normal);
        })
    });

    group.finish();
}

fn bench_mz_acl_item(c: &mut Criterion) {
    let mut group = c.benchmark_group("MzAclItem");
    group.throughput(Throughput::Elements(1));

    let acl_item = MzAclItem {
        grantee: RoleId::User(1),
        grantor: RoleId::User(2),
        acl_mode: AclMode::all(),
    };
    group.bench_function("encode", |b| {
        let mut buf = [0u8; 32];
        b.iter(|| {
            let packed = PackedMzAclItem::from_value(std::hint::black_box(acl_item));
            std::hint::black_box(&mut buf[..PackedMzAclItem::SIZE])
                .copy_from_slice(packed.as_bytes());
        })
    });
    group.bench_function("encode/proto", |b| {
        let mut buf = vec![0u8; 64];
        b.iter(|| {
            let proto = std::hint::black_box(acl_item).into_proto();
            proto.encode(std::hint::black_box(&mut buf)).unwrap();
            buf.clear();
        });
    });

    const PACKED: [u8; 32] = [
        0, 0, 1, 44, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 44, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 224,
        0, 3, 15,
    ];
    group.bench_function("decode", |b| {
        b.iter(|| {
            let packed = PackedMzAclItem::from_bytes(std::hint::black_box(&PACKED)).unwrap();
            let normal = std::hint::black_box(packed).into_value();
            std::hint::black_box(normal);
        })
    });
    const ENCODED: [u8; 16] = [10, 2, 16, 1, 18, 2, 16, 2, 26, 6, 8, 143, 134, 128, 128, 14];
    group.bench_function("decode/proto", |b| {
        b.iter(|| {
            let proto = ProtoMzAclItem::decode(std::hint::black_box(&ENCODED[..])).unwrap();
            let normal: MzAclItem = std::hint::black_box(proto).into_rust().unwrap();
            std::hint::black_box(normal);
        })
    });

    group.finish();
}

fn bench_numeric(c: &mut Criterion) {
    let mut group = c.benchmark_group("Numeric");
    group.throughput(Throughput::Elements(1));

    let val = Numeric::from(-101);
    group.bench_function("encode", |b| {
        let mut buf = [0u8; 64];
        b.iter(|| {
            let packed = PackedNumeric::from_value(std::hint::black_box(val));
            std::hint::black_box(&mut buf[..PackedNumeric::SIZE])
                .copy_from_slice(packed.as_bytes());
        })
    });
    group.bench_function("encode/proto", |b| {
        let mut buf = vec![0u8; 64];
        b.iter(|| {
            let (bcd, scale) = std::hint::black_box(val).to_packed_bcd().unwrap();
            ProtoNumeric { bcd, scale }.encode(&mut buf).unwrap();
            buf.clear();
        });
    });

    const PACKED: [u8; 40] = [
        3, 0, 0, 0, 0, 0, 0, 0, 101, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 128, 0, 0, 0, 0, 0,
    ];
    group.bench_function("decode", |b| {
        b.iter(|| {
            let packed = PackedNumeric::from_bytes(std::hint::black_box(&PACKED)).unwrap();
            let normal = std::hint::black_box(packed).into_value();
            std::hint::black_box(normal);
        })
    });
    const ENCODED: [u8; 4] = [10, 2, 16, 29];
    group.bench_function("decode/proto", |b| {
        b.iter(|| {
            let proto = ProtoNumeric::decode(std::hint::black_box(&ENCODED[..])).unwrap();
            let normal =
                std::hint::black_box(Numeric::from_packed_bcd(&proto.bcd, proto.scale)).unwrap();
            std::hint::black_box(normal);
        })
    });

    group.finish();
}

#[allow(clippy::useless_vec)]
fn bench_timestamp(c: &mut Criterion) {
    let mut group = c.benchmark_group("Timestamp");
    group.throughput(Throughput::Elements(1));

    let date = NaiveDate::from_ymd_opt(2024, 6, 30).unwrap();
    let time = NaiveTime::from_hms_opt(12, 30, 1).unwrap();
    let val = NaiveDateTime::new(date, time);

    group.bench_function("encode", |b| {
        let mut buf = vec![0u8; 64];
        b.iter(|| {
            let packed = PackedNaiveDateTime::from_value(std::hint::black_box(val));
            std::hint::black_box(&mut buf[..PackedNaiveDateTime::SIZE])
                .copy_from_slice(packed.as_bytes());
        })
    });
    group.bench_function("encode/micros", |b| {
        b.iter(|| {
            let encoded = std::hint::black_box(val).and_utc().timestamp_micros();
            std::hint::black_box(encoded);
        })
    });
    group.bench_function("encode/proto", |b| {
        let mut buf = vec![0u8; 64];
        b.iter(|| {
            let proto = std::hint::black_box(val).into_proto();
            proto.encode(&mut buf).unwrap();
            buf.clear();
        });
    });
}

criterion_group!(
    benches,
    bench_interval,
    bench_time,
    bench_acl_item,
    bench_mz_acl_item,
    bench_numeric,
    bench_timestamp
);
criterion_main!(benches);
