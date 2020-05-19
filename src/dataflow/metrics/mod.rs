// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use lazy_static::lazy_static;

use prometheus::{register_int_counter_vec, IntCounterVec};
use prometheus_static_metric::make_static_metric;

make_static_metric! {
    pub struct EventsRead: IntCounter {
        "format" => { avro, csv, protobuf, raw },
        "status" => { success, error }
    }
}

lazy_static! {
    static ref EVENTS_COUNTER_INTERNAL: IntCounterVec = register_int_counter_vec!(
        "mz_dataflow_events_read_total",
        "Count of events we have read from the wire",
        &["format", "status"]
    )
    .unwrap();
    pub static ref EVENTS_COUNTER: EventsRead = EventsRead::from(&EVENTS_COUNTER_INTERNAL);
    pub static ref SCHEDULING_ELAPSED_NS: IntCounterVec = register_int_counter_vec!(
        "mz_dataflow_operator_scheduling_elapsed_ns",
        "Nanoseconds spent in each operator",
        &["worker_id", "operator_id"]
    )
    .unwrap();
}
