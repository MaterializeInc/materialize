// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/// This macro generates a definition of a UnaryFunc variant and automatically implements
/// `UnaryFuncTrait` for it based on the provided function.
///
/// The macro can be applied to unary functions that adhere to the following constraints:
/// * The input is eagerly evaluatated
/// * The function doesn't preserve uniquencess
///
/// Functions that don't adhere to the above constraints need to have their UnaryFuncTrait
/// implementation defined manually and have full control over their eval phase and other
/// characteristics.
///
/// The macro implements a set of elision rules for automatically determining whether a given
/// function propagates or introduces nulls. The rules are summarized in the following table:
///
/// Input type  | Output type  | Propagates NULL  | Introduces NULL | Preserves uniqueness
/// ------------+--------------+------------------+-----------------+---------------------
/// `Option<T>` | `Option<T>`  | Not elided       | Not elided      | Not elided
/// `Option<T>` | `T`          | `false`          | `false`         | false
/// `T`         | `Option<T>`  | `true`           | `true`          | false
/// `T`         | `T`          | `true`           | `false`         | false
///
/// For the non elided case the attributes `sqlname`, `propagates_nulls`, `introduces_nulls`, and
/// `preserves_uniqueness` must be specified:
///
/// ```
/// sqlfunc!(
///     #[sqlname = "foo"]
///     #[propagates_nulls = false]
///     #[introduces_nulls = true]
///     #[preserves_uniqueness = true]
///     fn foo(a: Option<bool>) -> Option<bool> {
///         unimplemented!()
///     }
/// );
/// ```
///
///
/// ```
/// /// This SQL function will be named "booltoi32". It propagates nulls and doesn't introduce them
/// sqlfunc!(
///     fn booltoi32(a: bool) -> i32 {
///         a as i32
///     }
/// );
/// ```
///
/// A custom name can be given as the first parameter to the macro:
/// ```
/// /// This SQL function will be named "isnull". It doesn't propagate nor introduce nulls
/// sqlfunc!(
///     #[sqlname = "isnull"]
///     fn is_null(a: Option<i32>) -> bool {
///         a.is_none()
///     }
/// );
/// ```
macro_rules! sqlfunc {
    (
        #[sqlname = $name:expr]
        #[propagates_nulls = $propagates_nulls:literal]
        #[introduces_nulls = $introduces_nulls:literal]
        #[preserves_uniqueness = $preserves_uniqueness:literal]
        fn $fn_name:ident($param_name:ident: Option<$param_ty:ty>) -> Option<$ret_ty:ty>
            $body:block
    ) => {
        use std::convert::TryInto;
        use std::fmt;

        use paste::paste;
        use repr::{ColumnType, Datum, FromTy, RowArena, ScalarType};
        use serde::{Deserialize, Serialize};
        use lowertest::MzStructReflect;

        use crate::scalar::func::UnaryFuncTrait;
        use crate::{EvalError, MirScalarExpr};

        paste! {
            #[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect)]
            pub struct [<$fn_name:camel>];

            impl UnaryFuncTrait for [<$fn_name:camel>] {
                fn eval<'a>(
                    &'a self,
                    datums: &[Datum<'a>],
                    temp_storage: &'a RowArena,
                    a: &'a MirScalarExpr,
                ) -> Result<Datum<'a>, EvalError> {
                    // Define the provided function privately
                    fn $fn_name($param_name: Option<$param_ty>) -> Option<$ret_ty> {
                        $body
                    }

                    // Evaluate the argument and convert it to the concrete type that the
                    // implementation expects. This cannot fail here because the expression is
                    // already typechecked.
                    let a: Option<$param_ty> = a
                        .eval(datums, temp_storage)?
                        .try_into()
                        .expect("expression already typechecked");

                    Ok($fn_name(a).into())
                }

                fn output_type(&self, input_type: ColumnType) -> ColumnType {
                    // Use the FromTy trait to convert the Rust type into our runtime type
                    // representation
                    <ScalarType as FromTy<$ret_ty>>::from_ty()
                        .nullable($propagates_nulls && input_type.nullable || $introduces_nulls)
                }

                fn propagates_nulls(&self) -> bool {
                    $propagates_nulls
                }

                fn introduces_nulls(&self) -> bool {
                    $introduces_nulls
                }

                fn preserves_uniqueness(&self) -> bool {
                    $preserves_uniqueness
                }
            }

            impl From<[<$fn_name:camel>]> for crate::UnaryFunc {
                fn from(variant: [<$fn_name:camel>]) -> Self {
                    Self::[<$fn_name:camel>](variant)
                }
            }

            impl fmt::Display for [<$fn_name:camel>] {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    f.write_str($name)
                }
            }
        }
    };

    // First, expand the function name into an attribute, if it was omitted
    (
        fn $fn_name:ident $($tail:tt)*
    ) => {
        sqlfunc!(
            #[sqlname = stringify!($fn_name)]
            fn $fn_name $($tail)*
        );
    };

    // Then, check if omitting the attributes is ok
    (
        #[sqlname = $name:expr]
        fn $fn_name:ident($param_name:ident: Option<$param_ty:ty>) -> Option<$ret_ty:ty>
            $body:block
    ) => {
        compile_error!(
            "missing `propagates_nulls` and `introduces_nulls` attributes.\n       \
            These properties cannot be elided when both input and output are optional"
        );
    };

    // The function takes optional arguments and returns non optional, therefore it doesn't
    // propagate nulls
    (
        #[sqlname = $name:expr]
        fn $fn_name:ident($param_name:ident: Option<$param_ty:ty>) -> $ret_ty:ty
            $body:block
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[propagates_nulls = false]
            #[introduces_nulls = false]
            #[preserves_uniqueness = false]
            fn $fn_name($param_name: Option<$param_ty>) -> Option<$ret_ty> {
                Some($body)
            }
        );
    };

    // The function takes non-optional arguments and returns optional, therefore it propagates
    // nulls and introduces nulls too
    (
        #[sqlname = $name:expr]
        fn $fn_name:ident($param_name:ident: $param_ty:ty) -> Option<$ret_ty:ty>
            $body:block
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[propagates_nulls = true]
            #[introduces_nulls = true]
            #[preserves_uniqueness = false]
            fn $fn_name($param_name: Option<$param_ty>) -> Option<$ret_ty> {
                let $param_name = $param_name?;
                $body
            }
        );
    };

    // The function takes non-optional arguments and returns non optional, therefore it propagates
    // nulls but doesn't introduce them
    (
        #[sqlname = $name:expr]
        fn $fn_name:ident($param_name:ident: $param_ty:ty) -> $ret_ty:ty
            $body:block
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[propagates_nulls = true]
            #[introduces_nulls = false]
            #[preserves_uniqueness = false]
            fn $fn_name($param_name: Option<$param_ty>) -> Option<$ret_ty> {
                let $param_name = $param_name?;
                Some($body)
            }
        );
    };
}

/// Temporary macro that generates the equivalent of what enum_dispatch will do in the future. We
/// need this manual macro implementation to delegate to the previous manual implementation for
/// variants that use the old definitions.
///
/// Once everything is handled by this macro we can remove it and replace it with `enum_dispatch`
macro_rules! derive_unary {
    ($($name:ident),*) => {
        impl UnaryFunc {
            pub fn eval<'a>(
                &'a self,
                datums: &[Datum<'a>],
                temp_storage: &'a RowArena,
                a: &'a MirScalarExpr,
            ) -> Result<Datum<'a>, EvalError> {
                match self {
                    $(Self::$name(f) => f.eval(datums, temp_storage, a),)*
                    _ => self.eval_manual(datums, temp_storage, a),
                }
            }

            pub fn output_type(&self, input_type: ColumnType) -> ColumnType {
                match self {
                    $(Self::$name(f) => f.output_type(input_type),)*
                    _ => self.output_type_manual(input_type),
                }
            }
            pub fn propagates_nulls(&self) -> bool {
                match self {
                    $(Self::$name(f) => f.propagates_nulls(),)*
                    _ => self.propagates_nulls_manual(),
                }
            }
            pub fn introduces_nulls(&self) -> bool {
                match self {
                    $(Self::$name(f) => f.introduces_nulls(),)*
                    _ => self.introduces_nulls_manual(),
                }
            }
            pub fn preserves_uniqueness(&self) -> bool {
                match self {
                    $(Self::$name(f) => f.preserves_uniqueness(),)*
                    _ => self.preserves_uniqueness_manual(),
                }
            }
        }

        impl fmt::Display for UnaryFunc {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self {
                    $(Self::$name(func) => func.fmt(f),)*
                    _ => self.fmt_manual(f),
                }
            }
        }
    }
}
