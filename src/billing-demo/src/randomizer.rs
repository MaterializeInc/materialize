// Copyright 2020 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use chrono::prelude::*;
use protobuf::RepeatedField;
use rand::distributions::Distribution;
use rand::seq::SliceRandom;
use rand::Rng;
use rand_distr::Normal;
use uuid_b64::UuidB64;

use crate::gen::billing::{Batch, Record, ResourceInfo};

pub trait Randomizer {
    fn random(rng: &mut impl Rng) -> Self;
}

pub struct RecordState {
    last_time: DateTime<Utc>,
}

impl RecordState {
    pub fn new() -> RecordState {
        RecordState {
            last_time: Utc::now() - chrono::Duration::seconds(60 * 60 * 24 * 7),
        }
    }
}

/// Construct a Batch that depends on `state`
///
/// In particular this will have somewhat sensible values for all fields, and
/// will be the next time slice after `state.last_time`, incrementing `last_time` to now
pub fn random_batch(rng: &mut impl Rng, state: &mut RecordState) -> Batch {
    let id = UuidB64::new();

    let dur_val = rng.gen_range(15, 1_000);
    let dur = chrono::Duration::seconds(dur_val);
    let interval_start = state.last_time;
    let interval_end = interval_start.checked_add_signed(dur).unwrap();
    state.last_time = interval_end;

    let mut records = RepeatedField::<Record>::new();

    for _ in 0..rng.gen_range(1, 50) {
        records.push(random_record(rng, interval_start.clone(), dur_val));
    }

    let mut batch = Batch::new();
    batch.set_id(id.to_string());
    batch.set_interval_start(interval_start.to_rfc3339());
    batch.set_interval_end(interval_end.to_rfc3339());
    batch.set_records(records);

    batch
}

fn random_record(rng: &mut impl Rng, start_at: DateTime<Utc>, max_secs: i64) -> Record {
    let start_offset = rng.gen_range(0, max_secs - 1);
    let interval_start = start_at
        .checked_add_signed(chrono::Duration::seconds(start_offset))
        .unwrap();
    let interval_end = interval_start
        .checked_add_signed(chrono::Duration::seconds(
            rng.gen_range(start_offset, max_secs),
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

    let mut record = Record::new();
    record.set_id(UuidB64::new().to_string());
    record.set_interval_start(interval_start.to_rfc3339());
    record.set_interval_end(interval_end.to_rfc3339());
    record.set_meter(meter);
    record.set_value(val as u32);
    record.set_info(ResourceInfo::random(rng));

    record
}

impl Randomizer for ResourceInfo {
    fn random(rng: &mut impl Rng) -> ResourceInfo {
        static POSSIBLE_CPUS: &[i32] = &[1, 2];
        static POSSIBLE_MEM: &[i32] = &[8, 16];
        static POSSIBLE_DISK: &[i32] = &[128];

        let mut resource_info = ResourceInfo::new();
        resource_info.set_cpu_num(*POSSIBLE_CPUS.choose(rng).unwrap());
        resource_info.set_memory_gb(*POSSIBLE_MEM.choose(rng).unwrap());
        resource_info.set_disk_gb(*POSSIBLE_DISK.choose(rng).unwrap());
        resource_info.set_client_id(rng.gen_range(1, 100));
        resource_info.set_vm_id(rng.gen_range(1000, 2000));

        resource_info
    }
}
