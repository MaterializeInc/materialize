// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use walkdir::WalkDir;

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use std::fs::File;
use std::io::prelude::*;
use std::io::Read;

const CHANCE_TO_PICK_QUERY: u32 = 1000;

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
            for record in sqllogictest::parser::parse_records(&contents) {
                match record {
                    Ok(sqllogictest::ast::Record::Query { sql, .. })
                    | Ok(sqllogictest::ast::Record::Statement { sql, .. }) => {
                        if rng.gen_ratio(1, CHANCE_TO_PICK_QUERY) {
                            let filename =
                                &format!("./corpus/fuzz_sqllogictest/imported-{}", query_num);
                            File::create(filename)
                                .unwrap()
                                .write_all(sql.as_bytes())
                                .unwrap();
                            query_num += 1;
                        }
                    }
                    _ => (),
                }
            }
        }
    }
}
