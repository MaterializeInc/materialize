// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use criterion::{black_box, Criterion, Throughput};
use futures::executor::block_on;
use protobuf::{Message, MessageField};

use interchange::protobuf::{DecodedDescriptors, Decoder, NormalizedProtobufMessageName};

use gen::benchmark::{Connector, Record, Value};

mod gen {
    include!(concat!(env!("OUT_DIR"), "/protobuf/mod.rs"));
}

pub fn bench_protobuf(c: &mut Criterion) {
    let mut value = Value::new();
    value.l_orderkey = 1;
    value.l_orderkey = 155_190;
    value.l_suppkey = 7706;
    value.l_linenumber = 1;
    value.l_quantity = 17.0;
    value.l_extendedprice = 21168.23;
    value.l_discount = 0.04;
    value.l_tax = 0.02;
    value.l_returnflag = "N".into();
    value.l_linestatus = "O".into();
    value.l_shipdate = 9567;
    value.l_commitdate = 9537;
    value.l_receiptdate = 9537;
    value.l_shipinstruct = "DELIVER IN PERSON".into();
    value.l_shipmode = "TRUCK".into();
    value.l_comment = "egular courts above the".into();

    let mut connector = Connector::new();
    connector.version = "0.9.5.Final".into();
    connector.connector = "mysql".into();
    connector.name = "tcph".into();
    connector.server_id = 0;
    connector.ts_sec = 0;
    connector.gtid = "".into();
    connector.file = "binlog.000004".into();
    connector.pos = 951_896_181;
    connector.row = 0;
    connector.snapshot = true;
    connector.thread = 0;
    connector.db = "tcph".into();
    connector.table = "lineitem".into();
    connector.query = "".into();

    let mut record = Record::new();
    record.tcph_tcph_lineitem_value = MessageField::some(value);
    record.source = MessageField::some(connector);
    record.op = "c".into();
    record.ts_ms = 1_560_886_948_093;

    let buf = record
        .write_to_bytes()
        .expect("record failed to serialize to bytes");
    let len = buf.len() as u64;
    let mut decoder = Decoder::new(
        DecodedDescriptors::from_bytes(
            gen::FILE_DESCRIPTOR_SET_DATA,
            NormalizedProtobufMessageName::new(".bench.Record".to_string()),
        )
        .unwrap(),
        None,
    );

    let mut bg = c.benchmark_group("protobuf");
    bg.throughput(Throughput::Bytes(len));
    bg.bench_function("decode", move |b| {
        b.iter(|| black_box(block_on(decoder.decode(&buf)).unwrap()))
    });
    bg.finish();
}
