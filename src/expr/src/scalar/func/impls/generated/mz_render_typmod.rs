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
pub struct MzRenderTypmod;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for MzRenderTypmod {
    type Input1 = Datum<'a>;
    type Input2 = Datum<'a>;
    type Output = Result<Datum<'a>, EvalError>;
    fn call(
        &self,
        oid: Self::Input1,
        typmod: Self::Input2,
        temp_storage: &'a mz_repr::RowArena,
    ) -> Self::Output {
        {
            let oid = oid.unwrap_uint32();
            let typmod = typmod.unwrap_int32();
            let s = match Type::from_oid_and_typmod(oid, typmod) {
                Ok(typ) => typ.constraint().display_or("").to_string(),
                Err(_) if typmod >= 0 => format!("({typmod})"),
                Err(_) => "".into(),
            };
            Ok(Datum::String(temp_storage.push_string(s)))
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
        false
    }
    fn propagates_nulls(&self) -> bool {
        true
    }
}
impl std::fmt::Display for MzRenderTypmod {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("mz_render_typmod")
    }
}
