// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use criterion::{black_box, Criterion, Throughput};

use interchange::protobuf::test_util::gen::benchmark::{
    file_descriptor_proto, Connector, Record, Value,
};
use interchange::protobuf::Decoder;
use protobuf::descriptor::{FileDescriptorProto, FileDescriptorSet};
use protobuf::{Message, RepeatedField};
use serde_protobuf::descriptor::Descriptors;

fn get_decoder(message_name: &str) -> Decoder {
    let mut repeated_field = RepeatedField::<FileDescriptorProto>::new();
    let file_descriptor_proto = file_descriptor_proto().clone();
    repeated_field.push(file_descriptor_proto);

    let mut file_descriptor_set: FileDescriptorSet = FileDescriptorSet::new();
    file_descriptor_set.set_file(repeated_field);

    let descriptors = Descriptors::from_proto(&file_descriptor_set);
    Decoder::new(descriptors, message_name)
}

pub fn bench_protobuf(c: &mut Criterion) {
    let mut value = Value::new();
    value.set_l_orderkey(1);
    value.set_l_orderkey(155_190);
    value.set_l_suppkey(7706);
    value.set_l_linenumber(1);
    value.set_l_quantity(17.0);
    value.set_l_extendedprice(21168.23);
    value.set_l_discount(0.04);
    value.set_l_tax(0.02);
    value.set_l_returnflag("N".into());
    value.set_l_linestatus("O".into());
    value.set_l_shipdate(9567);
    value.set_l_commitdate(9537);
    value.set_l_receiptdate(9537);
    value.set_l_shipinstruct("DELIVER IN PERSON".into());
    value.set_l_shipmode("TRUCK".into());
    value.set_l_comment("egular courts above the".into());

    let mut connector = Connector::new();
    connector.set_version("0.9.5.Final".into());
    connector.set_connector("mysql".into());
    connector.set_name("tcph".into());
    connector.set_server_id(0);
    connector.set_ts_sec(0);
    connector.set_gtid("".into());
    connector.set_file("binlog.000004".into());
    connector.set_pos(951_896_181);
    connector.set_row(0);
    connector.set_snapshot(true);
    connector.set_thread(0);
    connector.set_db("tcph".into());
    connector.set_table("lineitem".into());
    connector.set_query("".into());

    let mut record = Record::new();
    record.set_tcph_tcph_lineitem_value(value);
    record.set_source(connector);
    record.set_op("c".into());
    record.set_ts_ms(1_560_886_948_093);

    let buf = record
        .write_to_bytes()
        .expect("record failed to serialize to bytes");
    let len = buf.len() as u64;
    let mut decoder = get_decoder(".bench.Record");

    let mut bg = c.benchmark_group("protobuf");
    bg.throughput(Throughput::Bytes(len));
    bg.bench_function("decode", move |b| {
        b.iter(|| black_box(decoder.decode(&buf).unwrap()))
    });
    bg.finish();
}
