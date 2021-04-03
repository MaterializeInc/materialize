// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Prometheus metrics that reflect the catalog

use expr::GlobalId;
use lazy_static::lazy_static;

use prometheus::{register_uint_gauge, register_uint_gauge_vec, UIntGauge, UIntGaugeVec};

use dataflow_types::{ExternalSourceConnector, SinkConnector, SourceConnector};

use crate::catalog::{CatalogItem, Sink, SinkConnectorState, Source};

lazy_static! {
    static ref SOURCES: UIntGaugeVec = register_uint_gauge_vec!(
        "mz_source_count",
        "The number of user-defined sources of a given type currently in use.",
        &["type"]
    )
    .unwrap();
    static ref SOURCE_COUNT_AVRO_OCF: UIntGauge = SOURCES.with_label_values(&["avro-ocf"]);
    static ref SOURCE_COUNT_FILE: UIntGauge = SOURCES.with_label_values(&["file"]);
    static ref SOURCE_COUNT_KAFKA: UIntGauge = SOURCES.with_label_values(&["kafka"]);
    static ref SOURCE_COUNT_KINESIS: UIntGauge = SOURCES.with_label_values(&["kinesis"]);
    static ref SOURCE_COUNT_PUBNUB: UIntGauge = SOURCES.with_label_values(&["pubnub"]);
    static ref SOURCE_COUNT_POSTGRES: UIntGauge = SOURCES.with_label_values(&["postgres"]);
    static ref SOURCE_COUNT_S3: UIntGauge = SOURCES.with_label_values(&["s3"]);
    static ref SOURCE_COUNT_TABLE: UIntGauge = SOURCES.with_label_values(&["table"]);
    static ref SINKS: UIntGaugeVec = register_uint_gauge_vec!(
        "mz_sink_count",
        "The number of user-defined sinks of a given type currently in use.",
        &["type"]
    )
    .unwrap();
    static ref SINK_COUNT_TAIL: UIntGauge = SINKS.with_label_values(&["tail"]);
    static ref SINK_COUNT_KAFKA: UIntGauge = SINKS.with_label_values(&["kafka"]);
    static ref SINK_COUNT_AVRO_OCF: UIntGauge = SINKS.with_label_values(&["avro-ocf"]);
    static ref VIEW_COUNT: UIntGauge = register_uint_gauge!(
        "mz_view_count",
        "The number of user-defined views that are currently in use."
    )
    .unwrap();
}

pub(super) fn item_created(id: GlobalId, item: &CatalogItem) {
    if id.is_system() {
        return;
    }
    match item {
        CatalogItem::Table(_) => SOURCE_COUNT_TABLE.inc(),
        CatalogItem::Source(Source { connector, .. }) => match connector {
            SourceConnector::External { connector, .. } => match connector {
                ExternalSourceConnector::AvroOcf(_) => SOURCE_COUNT_AVRO_OCF.inc(),
                ExternalSourceConnector::File(_) => SOURCE_COUNT_FILE.inc(),
                ExternalSourceConnector::Kafka(_) => SOURCE_COUNT_KAFKA.inc(),
                ExternalSourceConnector::Kinesis(_) => SOURCE_COUNT_KINESIS.inc(),
                ExternalSourceConnector::Postgres(_) => SOURCE_COUNT_POSTGRES.inc(),
                ExternalSourceConnector::PubNub(_) => SOURCE_COUNT_PUBNUB.inc(),
                ExternalSourceConnector::S3(_) => SOURCE_COUNT_S3.inc(),
            },
            SourceConnector::Local => {} // nothing interesting to users here
        },
        CatalogItem::Sink(Sink { connector, .. }) => match connector {
            SinkConnectorState::Pending(_) => {}
            SinkConnectorState::Ready(connector) => match connector {
                SinkConnector::Kafka(_) => SINK_COUNT_KAFKA.inc(),
                SinkConnector::Tail(_) => SINK_COUNT_TAIL.inc(),
                SinkConnector::AvroOcf(_) => SINK_COUNT_AVRO_OCF.inc(),
            },
        },
        CatalogItem::View(_) => VIEW_COUNT.inc(),
        CatalogItem::Index(_) | CatalogItem::Type(_) | CatalogItem::Func(_) => {}
    }
}

pub(super) fn item_dropped(id: GlobalId, item: &CatalogItem) {
    if id.is_system() {
        return;
    }
    match item {
        CatalogItem::Table(_) => SOURCE_COUNT_TABLE.dec(),
        CatalogItem::Source(Source { connector, .. }) => match connector {
            SourceConnector::External { connector, .. } => match connector {
                ExternalSourceConnector::AvroOcf(_) => SOURCE_COUNT_AVRO_OCF.dec(),
                ExternalSourceConnector::File(_) => SOURCE_COUNT_FILE.dec(),
                ExternalSourceConnector::Kafka(_) => SOURCE_COUNT_KAFKA.dec(),
                ExternalSourceConnector::Kinesis(_) => SOURCE_COUNT_KINESIS.dec(),
                ExternalSourceConnector::Postgres(_) => SOURCE_COUNT_POSTGRES.dec(),
                ExternalSourceConnector::PubNub(_) => SOURCE_COUNT_PUBNUB.dec(),
                ExternalSourceConnector::S3(_) => SOURCE_COUNT_S3.dec(),
            },
            SourceConnector::Local => {} // nothing interesting to users here
        },
        CatalogItem::Sink(Sink { connector, .. }) => match connector {
            SinkConnectorState::Pending(_) => {}
            SinkConnectorState::Ready(connector) => match connector {
                SinkConnector::Kafka(_) => SINK_COUNT_KAFKA.dec(),
                SinkConnector::Tail(_) => SINK_COUNT_TAIL.dec(),
                SinkConnector::AvroOcf(_) => SINK_COUNT_AVRO_OCF.dec(),
            },
        },
        CatalogItem::View(_) => VIEW_COUNT.dec(),
        CatalogItem::Index(_) | CatalogItem::Type(_) | CatalogItem::Func(_) => {}
    }
}
