// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use rand::prelude::SmallRng;
use rand::{Rng, SeedableRng};

use crate::nemesis::{
    Input, ReadSnapshotReq, Req, ReqId, SealReq, SnapshotId, TakeSnapshotReq, WriteReq,
};

pub struct Generator {
    seed: u64,
    req_id: u64,
    snap_id: u64,
}

// TODO: Make Generator configurable, so individual tests can be written to
// stress interesting combinations of random traffic.
impl Generator {
    pub fn from_seed(seed: u64) -> Self {
        Generator {
            seed,
            req_id: 0,
            snap_id: 0,
        }
    }

    pub fn gen(&mut self) -> Input {
        let req_id = ReqId(self.req_id);
        self.req_id += 1;
        let mut rng = SmallRng::seed_from_u64(self.seed + req_id.0);
        let req = loop {
            break match rng.gen_range(0..=3) {
                0 => {
                    let stream = rng.gen_range(0..5).to_string();
                    let key = rng.gen_range('a'..'e').to_string();
                    // TODO: dd/timely allow for a lot of generality here, but
                    // mz sources mostly just introduce data in order. Once the
                    // Generator is made more configurable, we should probably
                    // break this into two buckets, each modeled separately.
                    // First is one that picks a time within +N of the current
                    // capability of the input. Second is one that picks a time
                    // less than the current input (which should receive an
                    // error when run).
                    let ts = rng.gen_range(0..100);
                    // TODO: Tables and sources generally will be using -1 and
                    // 1. 0, -2, and 2 might find some more interesting edge
                    // cases. Is it worth expanding this to something with a
                    // wider range (and possibly a non-uniform distribution)
                    // once we start thinking of operator persistence? Perhaps
                    // overflows, what else might be relevant here then?
                    let diff = rng.gen_range(-2..=2);
                    Req::Write(WriteReq {
                        stream,
                        update: ((key, ()), ts, diff),
                    })
                }
                1 => {
                    let stream = rng.gen_range(0..5).to_string();
                    let ts = rng.gen_range(0..100);
                    Req::Seal(SealReq { stream, ts })
                }
                2 => {
                    let stream = rng.gen_range(0..5).to_string();
                    let snap = SnapshotId(self.snap_id);
                    self.snap_id += 1;
                    Req::TakeSnapshot(TakeSnapshotReq { stream, snap })
                }
                3 => {
                    if self.snap_id == 0 {
                        continue;
                    }
                    let snap = SnapshotId(rng.gen_range(0..self.snap_id));
                    Req::ReadSnapshot(ReadSnapshotReq { snap })
                }
                _ => unreachable!(),
            };
        };
        Input { req_id, req }
    }
}

impl Iterator for Generator {
    type Item = Input;

    fn next(&mut self) -> Option<Self::Item> {
        Some(self.gen())
    }
}
