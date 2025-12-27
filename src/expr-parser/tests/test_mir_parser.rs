// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_expr_parser::{TestCatalog, handle_define, handle_roundtrip};

#[mz_ore::test]
fn run_roundtrip_tests() {
    // Interpret datadriven tests.
    datadriven::walk("tests/test_mir_parser", |f| {
        let mut catalog = TestCatalog::default();
        f.run(|test_case| -> String {
            match test_case.directive.as_str() {
                "define" => handle_define(&mut catalog, &test_case.input),
                "roundtrip" => handle_roundtrip(&catalog, &test_case.input),
                _ => format!("unknown directive: {}", test_case.directive),
            }
        })
    });
}
