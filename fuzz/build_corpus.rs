// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use walkdir::WalkDir;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::io::prelude::*;
use std::io::Read;

const CHANCE_TO_PICK_QUERY: u32 = 100;
const MAX_QUERIES: u32 = 10_000;

fn main() {
    let mut query_num = 0;
    let mut rng: SmallRng = SeedableRng::seed_from_u64(42);
    for entry in WalkDir::new("../sqllogictest/test/") {
        let entry = entry.unwrap();
        if entry.path().is_file() {
            println!("Reading {:?}", entry.path());
            let mut contents = String::new();
            File::open(&entry.path())
                .unwrap()
                .read_to_string(&mut contents)
                .unwrap();
            for line in contents.lines() {
                if line.starts_with("SELECT") && rng.gen_ratio(1, CHANCE_TO_PICK_QUERY) {
                    let filename = &format!("./corpus/fuzz_sqllogictest/imported-{}", query_num);
                    // println!("Writing {}", filename);
                    File::create(filename)
                        .unwrap()
                        .write_all(line.as_bytes())
                        .unwrap();
                    query_num += 1;
                    if query_num >= MAX_QUERIES {
                        return;
                    }
                }
            }
        }
    }
}
