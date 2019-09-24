// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Fuzz testing via sqllogictest.

use std::borrow::ToOwned;

use futures::Future;

use coord::SqlResponse;

pub fn fuzz(sqls: &str) {
    let mut state = crate::runner::FullState::start().unwrap();
    for sql in sqls.split(';') {
        if let Ok(plan) = state
            .planner
            .handle_command(&mut state.session, sql.to_owned())
        {
            let sql_response = state.coord.sequence_plan(plan, state.conn_id, None);
            if let SqlResponse::SendRows { typ, rx } = sql_response {
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
