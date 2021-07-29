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
    use repr::ScalarType;
    use repr_test_util::{
        datum_to_test_spec, get_datum_from_str, get_scalar_type_or_default, unquote_string,
    };

    fn build_datum(s: &str) -> Result<String, String> {
        // 1) Convert test spec to the datum.
        let s = s.trim();
        let (litval, scalar_type) = if s.starts_with('"') {
            s.split_at(
                s[1..]
                    .rfind(|c| '"' == c)
                    .map(|i| i + 2)
                    .unwrap_or_else(|| s.len()),
            )
        } else {
            s.split_at(
                s.find(|c| [' ', '\t', '\n'].contains(&c))
                    .unwrap_or_else(|| s.len()),
            )
        };
        let scalar_type = build_scalar_type_inner(litval, scalar_type)?;
        let unquoted_litval = unquote_string(&litval, &scalar_type);
        let datum = get_datum_from_str(&unquoted_litval[..], &scalar_type)?;
        // 2) It should be possible to convert the datum back to the test spec.
        let roundtrip_s = datum_to_test_spec(datum);
        if roundtrip_s != litval {
            Err(format!(
                "Round trip failed. Old spec: {}. New spec: {}.",
                litval, roundtrip_s
            ))
        } else {
            Ok(format!("{:?}", datum))
        }
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
                        Ok(result) => format!("{}\n", result),
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
