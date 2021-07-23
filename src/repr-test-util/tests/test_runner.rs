// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#[cfg(test)]
mod tests {
    use lowertest::tokenize;
    use repr::{Datum, ScalarType};
    use repr_test_util::{get_datum_from_str, get_scalar_type_or_default};

    fn build_datum<'a>(s: &'a str) -> Result<Datum<'a>, String> {
        let s = s.trim();
        let (litval, scalar_type) = s.split_at(
            s.find(|c| [' ', '\t', '\n'].contains(&c))
                .unwrap_or_else(|| s.len()),
        );
        get_datum_from_str(litval, &build_scalar_type_inner(litval, scalar_type)?)
    }

    fn build_scalar_type(s: &str) -> Result<ScalarType, String> {
        build_scalar_type_inner("", s)
    }

    fn build_scalar_type_inner(litval: &str, s: &str) -> Result<ScalarType, String> {
        get_scalar_type_or_default(litval, &mut tokenize(s)?.into_iter())
    }

    #[test]
    fn run() {
        datadriven::walk("tests/testdata", |f| {
            f.run(move |s| -> String {
                match s.directive.as_str() {
                    "build-scalar-type" => match build_scalar_type(&s.input) {
                        Ok(scalar_type) => format!("{:?}\n", scalar_type),
                        Err(err) => format!("error: {}\n", err),
                    },
                    "build-datum" => match build_datum(&s.input) {
                        Ok(datum) => format!("{:?}\n", datum),
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
