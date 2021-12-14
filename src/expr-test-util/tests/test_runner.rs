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
    use lowertest::{from_json, TestDeserializeContext};
    use ore::result::ResultExt;
    use ore::str::separated;
    use serde::de::DeserializeOwned;
    use serde::Serialize;

    fn roundtrip<F, T, G, C>(
        s: &str,
        type_name: &str,
        build_obj: F,
        ctx_gen: G,
    ) -> Result<T, String>
    where
        T: DeserializeOwned + Serialize + Eq + Clone,
        F: Fn(&str) -> Result<T, String>,
        C: TestDeserializeContext,
        G: Fn() -> C,
    {
        let result: T = build_obj(s)?;
        let json = serde_json::to_value(result.clone()).map_err_to_string()?;
        let new_s = from_json(&json, type_name, &RTI, &mut ctx_gen());
        let new_result = build_obj(&new_s)?;
        if new_result.eq(&result) {
            Ok(result)
        } else {
            Err(format!(
                "Round trip failed. New spec:\n{}
            Original {type_name}:\n{}
            New {type_name}:\n{}
            JSON for original {type_name}:\n{}",
                new_s,
                serde_json::to_string_pretty(&result).unwrap(),
                serde_json::to_string_pretty(&new_result).unwrap(),
                json.to_string(),
                type_name = type_name
            ))
        }
    }

    #[test]
    fn run_roundtrip_tests() {
        datadriven::walk("tests/testdata", |f| {
            let mut catalog = TestCatalog::default();
            f.run(move |s| -> String {
                match s.directive.as_str() {
                    "cat" => match catalog.handle_test_command(&s.input) {
                        Ok(()) => String::from("ok\n"),
                        Err(err) => format!("error: {}\n", err),
                    },
                    // tests that we can build `MirScalarExpr`s
                    "build-scalar" => {
                        match roundtrip(
                            &s.input,
                            "MirScalarExpr",
                            build_scalar,
                            MirScalarExprDeserializeContext::default,
                        ) {
                            Ok(scalar) => format!("{}\n", scalar),
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    "build" => {
                        match roundtrip(
                            &s.input,
                            "MirRelationExpr",
                            |s| build_rel(s, &catalog),
                            || MirRelationExprDeserializeContext::new(&catalog),
                        ) {
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
                        }
                    }
                    "rel-to-test" => {
                        let (spec, source_defs) = json_to_spec(&s.input, &catalog);
                        format!(
                            "cat\n{}\n----\nok\n\n{}\n",
                            separated("\n", source_defs),
                            spec
                        )
                    }
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
