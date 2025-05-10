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
pub struct PrettySql;
impl<'a> crate::func::binary::EagerBinaryFunc<'a> for PrettySql {
    type Input1 = Datum<'a>;
    type Input2 = Datum<'a>;
    type Output = Result<Datum<'a>, EvalError>;
    fn call(
        &self,
        sql: Self::Input1,
        width: Self::Input2,
        temp_storage: &'a mz_repr::RowArena,
    ) -> Self::Output {
        {
            let sql = sql.unwrap_str();
            let width = width.unwrap_int32();
            let width = usize::try_from(width)
                .map_err(|_| EvalError::PrettyError("invalid width".into()))?;
            let pretty = pretty_str(
                    sql,
                    PrettyConfig {
                        width,
                        format_mode: FormatMode::Simple,
                    },
                )
                .map_err(|e| EvalError::PrettyError(e.to_string().into()))?;
            let pretty = temp_storage.push_string(pretty);
            Ok(Datum::String(pretty))
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
impl std::fmt::Display for PrettySql {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(stringify!(pretty_sql))
    }
}
