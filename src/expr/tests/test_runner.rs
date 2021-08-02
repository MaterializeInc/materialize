// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

mod test {
    use expr::canonicalize::canonicalize_predicates;
    use expr::MirScalarExpr;
    use expr_test_util::*;
    use lowertest::{deserialize, tokenize};
    use ore::str::separated;
    use repr::RelationType;

    fn reduce(s: &str) -> Result<MirScalarExpr, String> {
        let mut input_stream = tokenize(&s)?.into_iter();
        let mut ctx = MirScalarExprDeserializeContext::default();
        let mut scalar: MirScalarExpr =
            deserialize(&mut input_stream, "MirScalarExpr", &RTI, &mut ctx)?;
        let typ: RelationType = deserialize(&mut input_stream, "RelationType", &RTI, &mut ctx)?;
        scalar.reduce(&typ);
        Ok(scalar)
    }

    fn test_canonicalize_pred(s: &str) -> Result<Vec<MirScalarExpr>, String> {
        let mut input_stream = tokenize(&s)?.into_iter();
        let mut ctx = MirScalarExprDeserializeContext::default();
        let mut predicates: Vec<MirScalarExpr> =
            deserialize(&mut input_stream, "Vec<MirScalarExpr>", &RTI, &mut ctx)?;
        let typ: RelationType = deserialize(&mut input_stream, "RelationType", &RTI, &mut ctx)?;
        canonicalize_predicates(&mut predicates, &typ);
        Ok(predicates)
    }

    #[test]
    fn run() {
        datadriven::walk("tests/testdata", |f| {
            f.run(move |s| -> String {
                match s.directive.as_str() {
                    // tests simplification of scalars
                    "reduce" => match reduce(&s.input) {
                        Ok(scalar) => {
                            format!("{}\n", scalar.to_string())
                        }
                        Err(err) => format!("error: {}\n", err),
                    },
                    "canonicalize" => match test_canonicalize_pred(&s.input) {
                        Ok(preds) => {
                            format!("{}\n", separated("\n", preds.iter().map(|p| p.to_string())))
                        }
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
