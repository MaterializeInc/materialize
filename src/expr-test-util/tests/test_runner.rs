// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod test {
    use expr_test_util::*;

    #[test]
    fn run() {
        datadriven::walk("tests/testdata", |f| {
            let mut catalog = TestCatalog::default();
            f.run(move |s| -> String {
                match s.directive.as_str() {
                    "cat" => match catalog.handle_test_command(&s.input) {
                        Ok(()) => String::from("ok\n"),
                        Err(err) => format!("error: {}\n", err),
                    },
                    // tests that we can build `MirScalarExpr`s
                    "build-scalar" => match build_scalar(&s.input) {
                        Ok(scalar) => format!("{}\n", scalar.to_string()),
                        Err(err) => format!("error: {}\n", err),
                    },
                    "build" => match build_rel(&s.input, &catalog) {
                        // Generally, explanations for fully optimized queries
                        // are not allowed to have whitespace at the end;
                        // however, a partially optimized query can.
                        // Since clippy rejects test results with trailing
                        // whitespace, remove whitespace before comparing results.
                        Ok(rel) => format!(
                            "{}\n",
                            catalog
                                .generate_explanation(&rel, s.args.get("format"))
                                .trim_end()
                        ),
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
