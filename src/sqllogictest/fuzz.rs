// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Fuzz testing via sqllogictest.

use coord::SqlResponse;
use futures::Future;

use crate::runner::State;

pub fn fuzz(sqls: &str) {
    let mut state = State::start().unwrap();
    for sql in sqls.split(';') {
        if let Ok(plan) = state.plan_sql(sql) {
            if let SqlResponse::SendRows { typ, rx } = state.run_plan(plan) {
                for row in rx.wait().unwrap() {
                    for (typ, datum) in typ.column_types.iter().zip(row.into_iter()) {
                        assert!(datum.is_instance_of(typ));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::fs::File;
    use std::io::Read;

    use walkdir::WalkDir;

    #[test]
    fn fuzz_artifacts() {
        let mut input = String::new();
        for entry in WalkDir::new("../../fuzz/artifacts/fuzz_sqllogictest/") {
            let entry = entry.unwrap();
            if entry.path().is_file() && entry.file_name() != ".gitignore" {
                input.clear();
                File::open(&entry.path())
                    .unwrap()
                    .read_to_string(&mut input)
                    .unwrap();
                fuzz(&input);
            }
        }
    }
}
