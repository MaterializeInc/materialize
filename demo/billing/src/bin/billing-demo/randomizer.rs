// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::prelude::*;
use chrono::DateTime;
use prost_types::Timestamp;
use rand::distributions::Distribution;
use rand::seq::SliceRandom;
use rand::Rng;
use rand_distr::Normal;
use uuid::Uuid;

use crate::gen::billing::{Batch, Record, ResourceInfo};

pub static NUM_CLIENTS: u32 = 100;

pub trait Randomizer {
    fn random(rng: &mut impl Rng) -> Self;
}

pub struct RecordState {
    pub last_time: DateTime<Utc>,
}

fn protobuf_timestamp(time: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: time.timestamp(),
        nanos: time.timestamp_subsec_nanos() as i32,
    }
}

/// Construct a Batch that depends on `state`
///
/// In particular this will have somewhat sensible values for all fields, and
/// will be the next time slice after `state.last_time`, incrementing `last_time` to now
pub fn random_batch(rng: &mut impl Rng, state: &mut RecordState) -> Batch {
    let id = Uuid::new_v4();

    let dur_val = rng.gen_range(15..1_000);
    let dur = chrono::Duration::seconds(dur_val);
    let interval_start_time = state.last_time.clone();
    let interval_start = protobuf_timestamp(state.last_time);
    state.last_time = state.last_time.checked_add_signed(dur).unwrap();
    let interval_end = protobuf_timestamp(state.last_time);

    let mut records = vec![];

    for _ in 0..rng.gen_range(1..50) {
        records.push(random_record(rng, interval_start_time, dur_val));
    }

    Batch {
        id: id.to_string(),
        interval_start: Some(interval_start),
        interval_end: Some(interval_end),
        records,
    }
}

fn random_record(rng: &mut impl Rng, start_at: DateTime<Utc>, max_secs: i64) -> Record {
    let start_offset = rng.gen_range(0..max_secs - 1);
    let interval_start = start_at
        .checked_add_signed(chrono::Duration::seconds(start_offset))
        .unwrap();
    let interval_end = interval_start
        .checked_add_signed(chrono::Duration::seconds(
            rng.gen_range(start_offset..max_secs),
        ))
        .unwrap();

    static POSSIBLE_METERS: [&str; 1] = ["execution_time_ms"];
    let meter = (*POSSIBLE_METERS.choose(rng).unwrap()).to_string();

    let n = Normal::new(50.0, 10.0).unwrap();
    let mut val;
    loop {
        val = n.sample(rng);
        if (1.0..1000.0).contains(&val) {
            break;
        }
    }

    Record {
        id: Uuid::new_v4().to_string(),
        interval_start: Some(protobuf_timestamp(interval_start)),
        interval_end: Some(protobuf_timestamp(interval_end)),
        meter,
        value: val as i32,
        info: Some(ResourceInfo::random(rng)),
    }
}

impl Randomizer for ResourceInfo {
    fn random(rng: &mut impl Rng) -> ResourceInfo {
        static POSSIBLE_CPUS: &[i32] = &[1, 2];
        static POSSIBLE_MEM: &[i32] = &[8, 16];
        static POSSIBLE_DISK: &[i32] = &[128];
        ResourceInfo {
            cpu_num: *POSSIBLE_CPUS.choose(rng).unwrap(),
            memory_gb: *POSSIBLE_MEM.choose(rng).unwrap(),
            disk_gb: *POSSIBLE_DISK.choose(rng).unwrap(),
            client_id: rng.gen_range(1..NUM_CLIENTS as i32),
            vm_id: rng.gen_range(1000..2000),
        }
    }
}
