// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Native types for the generated structs in [`super::gen`]
//!
//! Primarily exist to make deserializing from serde easier and more deterministic

use protobuf::RepeatedField;
use serde::Deserialize;

use crate::protobuf::gen::{billing, simple};
use crate::protobuf::{DbgMsg, DynMessage, FromMessage, ToMessage};

// Billing demo

impl ToMessage for Batch {
    fn to_message(self) -> DynMessage {
        Box::new(billing::Batch::from(self))
    }
}

impl FromMessage for Batch {
    type MessageType = billing::Batch;
}

impl DbgMsg for billing::Batch {}

#[derive(Clone, Debug, Deserialize)]
pub enum Resource {
    Mem,
    Cpu,
    Disk,
}

impl From<Resource> for billing::Resource {
    fn from(r: Resource) -> billing::Resource {
        match r {
            Resource::Mem => billing::Resource::MEM,
            Resource::Cpu => billing::Resource::CPU,
            Resource::Disk => billing::Resource::DISK,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
enum Units {
    Bytes,
    Millis,
    Units,
}

impl From<Units> for billing::Units {
    fn from(r: Units) -> billing::Units {
        match r {
            Units::Bytes => billing::Units::BYTES,
            Units::Millis => billing::Units::MILLIS,
            Units::Units => billing::Units::UNITS,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct Measurement {
    resource: Resource,
    measured_value: i64,
}

impl From<Measurement> for billing::Measurement {
    fn from(m: Measurement) -> billing::Measurement {
        billing::Measurement {
            resource: m.resource.into(),
            measured_value: m.measured_value,
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
struct Record_ {
    interval_start: String,
    interval_end: String,

    meter: String,
    value: u32,

    measurements: Vec<Measurement>,
}

impl From<Record_> for billing::Record {
    fn from(r: Record_) -> billing::Record {
        billing::Record {
            interval_start: r.interval_start,
            interval_end: r.interval_end,

            meter: r.meter,
            value: r.value,

            measurements: iter_into(r.measurements),
            ..Default::default()
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct Batch {
    id: String,
    interval_start: String,
    interval_end: String,

    records: Vec<Record_>,
}

impl From<Batch> for billing::Batch {
    fn from(r: Batch) -> billing::Batch {
        billing::Batch {
            id: r.id,
            interval_start: r.interval_start,
            interval_end: r.interval_end,

            records: iter_into(r.records),
            ..Default::default()
        }
    }
}

// testdrive stubs

#[derive(Clone, Debug, Deserialize)]
pub struct Struct {
    int: i32,
    bad_int: i32,
    bin: Binary,
    st: String,
}

impl From<Struct> for simple::Struct {
    fn from(s: Struct) -> simple::Struct {
        simple::Struct {
            int: s.int,
            bad_int: s.bad_int,
            bin: s.bin.into(),
            st: s.st,
            ..Default::default()
        }
    }
}

impl ToMessage for Struct {
    fn to_message(self) -> DynMessage {
        Box::new(simple::Struct::from(self))
    }
}

impl FromMessage for Struct {
    type MessageType = simple::Struct;
}

impl DbgMsg for simple::Struct {}

#[derive(Clone, Debug, Deserialize)]
pub enum Binary {
    Zero,
    One,
}

impl From<Binary> for simple::Binary {
    fn from(b: Binary) -> simple::Binary {
        match b {
            Binary::Zero => simple::Binary::ZERO,
            Binary::One => simple::Binary::ONE,
        }
    }
}

fn iter_into<T>(items: Vec<impl Into<T>>) -> RepeatedField<T> {
    RepeatedField::from_vec(items.into_iter().map(Into::into).collect())
}
