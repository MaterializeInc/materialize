// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

use tempfile::NamedTempFile;

#[test]
fn datadriven() {
    datadriven::walk("tests/testdata", |f| {
        f.run(|test_case| {
            let mut f = NamedTempFile::new().unwrap();
            f.write_all(test_case.input.as_bytes()).unwrap();
            walkabout::load(f.path())
                .map(|ir| match test_case.directive.as_str() {
                    "visit" => walkabout::gen_visit(&ir),
                    "visit-mut" => walkabout::gen_visit_mut(&ir),
                    other => panic!("unknown directive: {}", other),
                })
                .unwrap_or_else(|e| format!("error: {}\n", e))
        })
    })
}
