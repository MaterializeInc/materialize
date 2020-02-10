// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Fuzz testing via sqllogictest.

use futures::executor::block_on;

use coord::ExecuteResponse;

use crate::runner::State;

pub fn fuzz(sqls: &str) {
    let mut state = State::start().unwrap();
    for sql in sqls.split(';') {
        if let Ok((Some(desc), ExecuteResponse::SendRows(rx))) = state.run_sql(sql) {
            for row in block_on(rx).unwrap().unwrap_rows() {
                for (typ, datum) in desc.iter_types().zip(row.iter()) {
                    assert!(datum.is_instance_of(typ));
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
