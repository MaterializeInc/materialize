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
pub struct MzFormatPrivileges;
impl<'a> crate::func::EagerUnaryFunc<'a> for MzFormatPrivileges {
    type Input = String;
    type Output = Result<ArrayRustType<String>, EvalError>;
    fn call(&self, privileges: Self::Input) -> Self::Output {
        AclMode::from_str(&privileges)
            .map(|acl_mode| {
                ArrayRustType(
                    acl_mode
                        .explode()
                        .into_iter()
                        .map(|privilege| privilege.to_string())
                        .collect(),
                )
            })
            .map_err(|e: anyhow::Error| EvalError::InvalidPrivileges(
                e.to_string().into(),
            ))
    }
    fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
        use mz_repr::AsColumnType;
        let output = Self::Output::as_column_type();
        let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
        let nullable = output.nullable;
        output.nullable(nullable || (propagates_nulls && input_type.nullable))
    }
}
impl std::fmt::Display for MzFormatPrivileges {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("mz_format_privileges")
    }
}
