// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.
//
// Benchmarks for decoding input format

use criterion::{criterion_group, criterion_main};

pub mod avro;
pub mod protobuf;

criterion_group!(benches, avro::bench_avro, protobuf::bench_protobuf);
criterion_main!(benches);
