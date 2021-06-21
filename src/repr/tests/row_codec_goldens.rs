// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use datadriven::{walk, TestCase};
use lowertest::{deserialize_optional, tokenize, GenericTestDeserializeContext};
use persist_types::Codec;
use repr::{Row, ScalarType};
use repr_test_util::{get_scalar_type_or_default, parse_vec_of_literals, test_spec_to_row, RTI};

fn one_golden(tc: &TestCase) -> Result<String, String> {
    let row_before = {
        let s = tc.input.trim();
        let mut stream_iter = tokenize(s)?.into_iter();
        let litvals = parse_vec_of_literals(
            &stream_iter
                .next()
                .ok_or_else(|| "Empty row spec".to_string())?,
        )?;
        let scalar_types: Option<Vec<ScalarType>> = deserialize_optional(
            &mut stream_iter,
            "Vec<ScalarType>",
            &RTI,
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
        test_spec_to_row(litvals.iter().map(|s| &s[..]).zip(scalar_types.iter()))?
    };
    let mut encoded_row = Vec::new();
    row_before.encode(&mut encoded_row);
    let encoded_base64 = base64::encode(&encoded_row);
    let row_after = Row::decode(&encoded_row)
        .map_err(|err| format!("error decoding back into row {}: {}", &encoded_base64, err))?;
    assert_eq!(
        &row_before, &row_after,
        "Row did not roundtrip through encoding encoded={}",
        &encoded_base64
    );
    Ok(format!("{}\n", encoded_base64))
}

// This test verifies that Row's current implementation of Codec::decode can
// faithfully recover data written by historical versions of its Codec::encode.
// This is critical because we use this format for permanent storage and
// violating it would result in customers being unable to read historical data.
//
// NB: At the moment, persistence is still being built and there has only ever
// been version of encode (the current one) and we haven't actually committed to
// backward compatibility. Once we do, any changes to Datum semantics will
// require that we create a path for backward compatibility in the Codec::decode
// for this.
//
// NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE
//
// Do not update any of the expected values in this file without looping in the
// persistence team to sanity check that its done correctly. See the above
// comment for why.
//
// NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE NOTICE
#[test]
fn row_codec_goldens() {
    walk("tests/testdata", |f| {
        f.run(|tc| -> String {
            assert_eq!(tc.directive, "row-codec-golden", "unknown directive");
            one_golden(tc).unwrap_or_else(|err| format!("ERROR: {}\n", err))
        })
    });
}
