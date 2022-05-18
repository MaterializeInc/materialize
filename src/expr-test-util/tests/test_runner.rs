// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod test {
    use mz_expr_test_util::*;
    use mz_lowertest::{serialize, MzReflect, TestDeserializeContext};
    use mz_ore::result::ResultExt;
    use serde::de::DeserializeOwned;
    use serde::Serialize;

    /// Build a object of type `T` from the test spec, turn it back into a
    /// test spec, construct an object from the new test spec, and see if the
    /// two objects are the same.
    fn roundtrip<F, T, G, C>(
        s: &str,
        type_name: &str,
        build_obj: F,
        ctx_gen: G,
    ) -> Result<T, String>
    where
        T: DeserializeOwned + MzReflect + Serialize + Eq + Clone,
        F: Fn(&str) -> Result<T, String>,
        C: TestDeserializeContext,
        G: Fn() -> C,
    {
        let result: T = build_obj(s)?;
        let json = serde_json::to_value(result.clone()).map_err_to_string()?;
        let new_s = serialize::<T, _>(&json, type_name, &mut ctx_gen());
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
                json,
                type_name = type_name
            ))
        }
    }

    /// An extended version of `roundtrip`.
    ///
    /// Only works for `MirRelationExpr`.
    ///
    /// When converting the relation back to the test spec, this method also
    /// figures out the catalog commands required to create the relation from a
    /// blank catalog.
    fn roundtrip_with_catalog(orig_spec: &str, orig_catalog: &TestCatalog) -> Result<(), String> {
        let orig_rel = build_rel(orig_spec, &orig_catalog)?;
        let json = serde_json::to_value(orig_rel.clone()).map_err_to_string()?;
        let mut new_catalog = TestCatalog::default();
        let (new_spec, source_defs) = json_to_spec(&json.to_string(), &new_catalog);
        for source_def in source_defs {
            new_catalog.handle_test_command(&source_def)?;
        }
        let new_rel = build_rel(&new_spec, &new_catalog)?;
        if new_rel.eq(&orig_rel) {
            Ok(())
        } else {
            Err(format!(
                "Round trip failed. New spec:\n{}
            Original relation:\n{}
            New relation:\n{}
            JSON for original relation:\n{}",
                new_spec,
                serde_json::to_string_pretty(&orig_rel).unwrap(),
                serde_json::to_string_pretty(&new_rel).unwrap(),
                json,
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
                                generate_explanation(&catalog, &rel, s.args.get("format"))
                                    .trim_end()
                            ),
                            Err(err) => format!("error: {}\n", err),
                        }
                    }
                    "rel-to-test" => match roundtrip_with_catalog(&s.input, &catalog) {
                        Ok(()) => "ok\n".to_string(),
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
