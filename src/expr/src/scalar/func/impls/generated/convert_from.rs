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
pub struct ConvertFrom;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for ConvertFrom {
    type Input1 = Datum<'a>;
    type Input2 = Datum<'a>;
    type Output = Result<Datum<'a>, EvalError>;
    fn call(
        &self,
        a: Self::Input1,
        b: Self::Input2,
        temp_storage: &'a mz_repr::RowArena,
    ) -> Self::Output {
        {
            let encoding_name = b
                .unwrap_str()
                .to_lowercase()
                .replace('_', "-")
                .into_boxed_str();
            if encoding_from_whatwg_label(&encoding_name).map(|e| e.name())
                != Some("utf-8")
            {
                return Err(EvalError::InvalidEncodingName(encoding_name));
            }
            match str::from_utf8(a.unwrap_bytes()) {
                Ok(from) => Ok(Datum::String(from)),
                Err(e) => {
                    Err(EvalError::InvalidByteSequence {
                        byte_sequence: e.to_string().into(),
                        encoding_name,
                    })
                }
            }
        }
    }
    fn output_type(
        &self,
        input_type_a: mz_repr::ColumnType,
        input_type_b: mz_repr::ColumnType,
    ) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = <String>::as_column_type();
        let propagates_nulls = crate::func::binary::EagerBinaryFunc::propagates_nulls(
            self,
        );
        let nullable = output.nullable;
        output
            .nullable(
                nullable
                    || (propagates_nulls
                        && (input_type_a.nullable || input_type_b.nullable)),
            )
    }
    fn introduces_nulls(&self) -> bool {
        <String as ::mz_repr::DatumType<'_, ()>>::nullable()
    }
    fn propagates_nulls(&self) -> bool {
        true
    }
}
impl std::fmt::Display for ConvertFrom {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("convert_from")
    }
}
