// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use avro_rs::types::Value as AvroValue;
use byteorder::{NetworkEndian, WriteBytesExt};
use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

use interchange::avro::{parse_schema, Decoder};

fn bench_interchange(c: &mut Criterion) {
    let schema_str = r#"
{
  "type": "record",
  "name": "Envelope",
  "namespace": "tpch.tpch.lineitem",
  "fields": [
    {
      "name": "before",
      "type": [
        "null",
        {
          "type": "record",
          "name": "Value",
          "fields": [
            {
              "name": "l_orderkey",
              "type": "int"
            },
            {
              "name": "l_partkey",
              "type": "int"
            },
            {
              "name": "l_suppkey",
              "type": "int"
            },
            {
              "name": "l_linenumber",
              "type": "int"
            },
            {
              "name": "l_quantity",
              "type": "double"
            },
            {
              "name": "l_extendedprice",
              "type": "double"
            },
            {
              "name": "l_discount",
              "type": "double"
            },
            {
              "name": "l_tax",
              "type": "double"
            },
            {
              "name": "l_returnflag",
              "type": "string"
            },
            {
              "name": "l_linestatus",
              "type": "string"
            },
            {
              "name": "l_shipdate",
              "type": {
                "type": "int",
                "connect.version": 1,
                "connect.name": "org.apache.kafka.connect.data.Date",
                "logicalType": "date"
              }
            },
            {
              "name": "l_commitdate",
              "type": {
                "type": "int",
                "connect.version": 1,
                "connect.name": "org.apache.kafka.connect.data.Date",
                "logicalType": "date"
              }
            },
            {
              "name": "l_receiptdate",
              "type": {
                "type": "int",
                "connect.version": 1,
                "connect.name": "org.apache.kafka.connect.data.Date",
                "logicalType": "date"
              }
            },
            {
              "name": "l_shipinstruct",
              "type": "string"
            },
            {
              "name": "l_shipmode",
              "type": "string"
            },
            {
              "name": "l_comment",
              "type": "string"
            }
          ],
          "connect.name": "tpch.tpch.lineitem.Value"
        }
      ],
      "default": null
    },
    {
      "name": "after",
      "type": [
        "null",
        "Value"
      ],
      "default": null
    },
    {
      "name": "source",
      "type": {
        "type": "record",
        "name": "Source",
        "namespace": "io.debezium.connector.mysql",
        "fields": [
          {
            "name": "version",
            "type": [
              "null",
              "string"
            ],
            "default": null
          },
          {
            "name": "connector",
            "type": [
              "null",
              "string"
            ],
            "default": null
          },
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "server_id",
            "type": "long"
          },
          {
            "name": "ts_sec",
            "type": "long"
          },
          {
            "name": "gtid",
            "type": [
              "null",
              "string"
            ],
            "default": null
          },
          {
            "name": "file",
            "type": "string"
          },
          {
            "name": "pos",
            "type": "long"
          },
          {
            "name": "row",
            "type": "int"
          },
          {
            "name": "snapshot",
            "type": [
              {
                "type": "boolean",
                "connect.default": false
              },
              "null"
            ],
            "default": false
          },
          {
            "name": "thread",
            "type": [
              "null",
              "long"
            ],
            "default": null
          },
          {
            "name": "db",
            "type": [
              "null",
              "string"
            ],
            "default": null
          },
          {
            "name": "table",
            "type": [
              "null",
              "string"
            ],
            "default": null
          },
          {
            "name": "query",
            "type": [
              "null",
              "string"
            ],
            "default": null
          }
        ],
        "connect.name": "io.debezium.connector.mysql.Source"
      }
    },
    {
      "name": "op",
      "type": "string"
    },
    {
      "name": "ts_ms",
      "type": [
        "null",
        "long"
      ],
      "default": null
    }
  ],
  "connect.name": "tpch.tpch.lineitem.Envelope"
}
"#;
    let schema = parse_schema(schema_str).unwrap();

    #[rustfmt::skip]
    let record = AvroValue::Record(vec![
        ("before".into(), AvroValue::Union(Box::new(AvroValue::Null))),
        ("after".into(), AvroValue::Union(Box::new(AvroValue::Record(vec![
            ("l_orderkey".into(), AvroValue::Int(1)),
            ("l_partkey".into(), AvroValue::Int(155_190)),
            ("l_suppkey".into(), AvroValue::Int(7706)),
            ("l_linenumber".into(), AvroValue::Int(1)),
            ("l_quantity".into(), AvroValue::Double(17.0)),
            ("l_extendedprice".into(), AvroValue::Double(21168.23)),
            ("l_discount".into(), AvroValue::Double(0.04)),
            ("l_tax".into(), AvroValue::Double(0.02)),
            ("l_returnflag".into(), AvroValue::String("N".into())),
            ("l_linestatus".into(), AvroValue::String("O".into())),
            ("l_shipdate".into(), AvroValue::Int(9567)),
            ("l_commitdate".into(), AvroValue::Int(9537)),
            ("l_receiptdate".into(), AvroValue::Int(9567)),
            ("l_shipinstruct".into(), AvroValue::String("DELIVER IN PERSON".into())),
            ("l_shipmode".into(), AvroValue::String("TRUCK".into())),
            ("l_comment".into(), AvroValue::String("egular courts above the".into())),
        ])))),
        ("source".into(), AvroValue::Record(vec![
            ("version".into(), AvroValue::Union(Box::new(AvroValue::String("0.9.5.Final".into())))),
            ("connector".into(), AvroValue::Union(Box::new(AvroValue::String("mysql".into())))),
            ("name".into(), AvroValue::String("tpch".into())),
            ("server_id".into(), AvroValue::Long(0)),
            ("ts_sec".into(), AvroValue::Long(0)),
            ("gtid".into(), AvroValue::Union(Box::new(AvroValue::Null))),
            ("file".into(), AvroValue::String("binlog.000004".into())),
            ("pos".into(), AvroValue::Long(951_896_181)),
            ("row".into(), AvroValue::Int(0)),
            ("snapshot".into(), AvroValue::Union(Box::new(AvroValue::Boolean(true)))),
            ("thread".into(), AvroValue::Union(Box::new(AvroValue::Null))),
            ("db".into(), AvroValue::Union(Box::new(AvroValue::String("tpch".into())))),
            ("table".into(), AvroValue::Union(Box::new(AvroValue::String("lineitem".into())))),
            ("query".into(), AvroValue::Union(Box::new(AvroValue::Null))),
        ])),
        ("op".into(), AvroValue::String("c".into())),
        ("ts_ms".into(), AvroValue::Union(Box::new(AvroValue::Long(1_560_886_948_093)))),
    ]);

    let mut buf = Vec::new();
    buf.write_u8(0).unwrap();
    buf.write_i32::<NetworkEndian>(0).unwrap();
    buf.extend(avro_rs::to_avro_datum(&schema, record).unwrap());
    let len = buf.len() as u64;

    let mut decoder = Decoder::new(schema_str, None);

    let mut bg = c.benchmark_group("avro");
    bg.throughput(Throughput::Bytes(len));
    bg.bench_function("decode", move |b| {
        b.iter(|| black_box(decoder.decode(&buf).unwrap()))
    });
    bg.finish();
}

criterion_group!(benches, bench_interchange);
criterion_main!(benches);
