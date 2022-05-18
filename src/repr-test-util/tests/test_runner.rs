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
    use mz_lowertest::{deserialize_optional, tokenize, GenericTestDeserializeContext};
    use mz_ore::str::separated;
    use mz_repr::ScalarType;
    use mz_repr_test_util::*;

    fn build_datum(s: &str) -> Result<String, String> {
        // 1) Convert test spec to the row containing the datum.
        let mut stream_iter = tokenize(s)?.into_iter();
        let litval =
            extract_literal_string(&stream_iter.next().ok_or("Empty test")?, &mut stream_iter)?
                .unwrap();
        let scalar_type = get_scalar_type_or_default(&litval[..], &mut stream_iter)?;
        let row = test_spec_to_row(std::iter::once((&litval[..], &scalar_type)))?;
        // 2) It should be possible to unpack the row and then convert the datum
        // back to the test spec.
        let datum = row.unpack_first();
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

    fn build_row(s: &str) -> Result<String, String> {
        let mut stream_iter = tokenize(s)?.into_iter();
        let litvals = parse_vec_of_literals(
            &stream_iter
                .next()
                .ok_or_else(|| "Empty row spec".to_string())?,
        )?;
        let scalar_types: Option<Vec<ScalarType>> = deserialize_optional(
            &mut stream_iter,
            "Vec<ScalarType>",
            &mut GenericTestDeserializeContext::default(),
        )?;
        let scalar_types = if let Some(scalar_types) = scalar_types {
            scalar_types
        } else {
            litvals
                .iter()
                .map(|l| get_scalar_type_or_default(l, &mut std::iter::empty()))
                .collect::<Result<Vec<_>, String>>()?
        };
        let row = test_spec_to_row(litvals.iter().map(|s| &s[..]).zip(scalar_types.iter()))?;
        let roundtrip_litvals = row
            .unpack()
            .into_iter()
            .map(datum_to_test_spec)
            .collect::<Vec<_>>();
        if roundtrip_litvals != litvals {
            Err(format!(
                "Round trip failed. Old spec: [{}]. New spec: [{}].",
                separated(" ", litvals),
                separated(" ", roundtrip_litvals)
            ))
        } else {
            Ok(format!(
                "{}",
                separated("\n", row.unpack().into_iter().map(|d| format!("{:?}", d)))
            ))
        }
    }

    fn build_scalar_type(s: &str) -> Result<ScalarType, String> {
        get_scalar_type_or_default("", &mut tokenize(s)?.into_iter())
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
                    "build-row" => match build_row(&s.input) {
                        Ok(result) => format!("{}\n", result),
                        Err(err) => format!("error: {}\n", err),
                    },
                    _ => panic!("unknown directive: {}", s.directive),
                }
            })
        });
    }
}
