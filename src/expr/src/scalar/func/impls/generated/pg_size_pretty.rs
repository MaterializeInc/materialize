// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.


#[derive(
    proptest_derive::Arbitrary,
    Ord,
    PartialOrd,
    Clone,
    Debug,
    Eq,
    PartialEq,
    serde::Serialize,
    serde::Deserialize,
    Hash,
    mz_lowertest::MzReflect
)]
pub struct PgSizePretty;
impl<'a> crate::func::EagerUnaryFunc<'a> for PgSizePretty {
    type Input = Numeric;
    type Output = Result<String, EvalError>;
    fn call(&self, a: Self::Input) -> Self::Output {
        let mut a = a;
        {
            let mut cx = numeric::cx_datum();
            let units = ["bytes", "kB", "MB", "GB", "TB", "PB"];
            for (pos, unit) in units.iter().rev().skip(1).rev().enumerate() {
                if Numeric::from(-10239.5) < a && a < Numeric::from(10239.5) {
                    if pos > 0 {
                        cx.round(&mut a);
                    }
                    return Ok(format!("{} {unit}", a.to_standard_notation_string()));
                }
                cx.div(&mut a, &Numeric::from(1024));
                numeric::munge_numeric(&mut a).unwrap();
            }
            cx.round(&mut a);
            Ok(format!("{} {}", a.to_standard_notation_string(), units.last().unwrap()))
        }
    }
    fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
        let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
        let nullable = output.nullable;
        output.nullable(nullable || (propagates_nulls && input_type.nullable))
    }
}
impl std::fmt::Display for PgSizePretty {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("pg_size_pretty")
    }
}
