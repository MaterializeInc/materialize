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
        fn $fn_name:ident($param_name:ident: $param_ty:ty) -> Result<Option<$ret_ty:ty>, EvalError>
            $body:block
    ) => {
        paste::paste! {
            pub fn $fn_name($param_name: $param_ty) -> Result<Option<$ret_ty>, crate::EvalError> {
                $body
            }

            mod [<__ $fn_name _impl>] {
                use std::fmt;

                use repr::{ColumnType, Datum, FromTy, RowArena, ScalarType, WithArena};
                use serde::{Deserialize, Serialize};
                use lowertest::MzStructReflect;

                use crate::scalar::func::UnaryFuncTrait;
                use crate::{EvalError, MirScalarExpr};

                #[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, MzStructReflect)]
                pub struct [<$fn_name:camel>];

                /// Utility function that converts the return type the passed function into its
                /// runtime representation
                fn output_type_of<T, R, E>(_: fn(T) -> Result<Option<R>, E>) -> ScalarType
                where ScalarType: FromTy<R> {
                    <ScalarType as FromTy<R>>::from_ty()
                }

                impl UnaryFuncTrait for [<$fn_name:camel>] {
                    fn eval<'a>(
                        &'a self,
                        datums: &[Datum<'a>],
                        temp_storage: &'a RowArena,
                        a: &'a MirScalarExpr,
                    ) -> Result<Datum<'a>, EvalError> {
                        // Evaluate the argument and convert it to the concrete type that the
                        // implementation expects. This cannot fail here because the expression is
                        // already typechecked.
                        let a: $param_ty = a
                            .eval(datums, temp_storage)?
                            .try_into()
                            .expect("expression already typechecked");

                        // Then we call the provided function that will return a Result<T, _>,
                        // where T is some concrete, owned type
                        let result = super::$fn_name(a);

                        // Finally, we convert T into a Datum<'a> by wrapping the returned value
                        // into a `WithArena` with the temporary storage provided to eval and
                        // delegating to the respective `Into` implementation
                        result.map(move |r| WithArena::new(temp_storage, r).into())
                    }

                    fn output_type(&self, input_type: ColumnType) -> ColumnType {
                        // Use the FromTy trait to convert the Rust return type into our runtime
                        // type representation
                        output_type_of(super::$fn_name)
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

                impl fmt::Display for [<$fn_name:camel>] {
                    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                        f.write_str($name)
                    }
                }
            }
            pub use [<__ $fn_name _impl>]::[<$fn_name:camel>];
        }
    };

    // Here follow 3 stages of transformations for the cases where the user didn't provide a fully
    // specified function signature as expected by the rule above.

    // Stage 0: Expand the function name into an attribute, if it was omitted
    (
        fn $fn_name:ident $($tail:tt)*
    ) => {
        sqlfunc!(
            #[sqlname = stringify!($fn_name)]
            fn $fn_name $($tail)*
        );
    };

    // Stage 1: Convert function to fallible, if it wasn't already

    // It is important that we check if the return type matches Result<Option<T>>, Result<T>,
    // Option<T>, and T in that order because once a type is matched by a `ty` typed metavariable
    // it can no longer be destructured syntactically. So if `$ret_ty:ty` matched
    // `Result<Option<T>>` as a unit we would no longer be able to infer information about the
    // structure of the type and therefore wouldn't be able to apply the elision rules.
    (
        #[sqlname = $name:expr]
        fn $fn_name:ident $params:tt -> Result<Option<$ret_ty:ty>, EvalError>
            $body:block
    ) => {
        sqlfunc!(
            @stage2
            #[sqlname = $name]
            fn $fn_name $params -> Result<Option<$ret_ty>, EvalError> {
                $body
            }
        );
    };
    (
        #[sqlname = $name:expr]
        fn $fn_name:ident $params:tt -> Result<$ret_ty:ty, EvalError>
            $body:block
    ) => {
        sqlfunc!(
            @stage2
            #[sqlname = $name]
            fn $fn_name $params -> Result<$ret_ty, EvalError> {
                $body
            }
        );
    };
    (
        #[sqlname = $name:expr]
        fn $fn_name:ident $params:tt -> Option<$ret_ty:ty>
            $body:block
    ) => {
        sqlfunc!(
            @stage2
            #[sqlname = $name]
            fn $fn_name $params -> Result<Option<$ret_ty>, EvalError> {
                Ok($body)
            }
        );
    };
    (
        #[sqlname = $name:expr]
        fn $fn_name:ident $params:tt -> $ret_ty:ty
            $body:block
    ) => {
        sqlfunc!(
            @stage2
            #[sqlname = $name]
            fn $fn_name $params -> Result<$ret_ty, EvalError> {
                Ok($body)
            }
        );
    };


    // Stage 2: Apply the elision rules to find out if the function propagates or introduces nulls

    // We can't infer in this case, bail out with a nice error message
    (
        @stage2
        #[sqlname = $name:expr]
        fn $fn_name:ident($param_name:ident: Option<$param_ty:ty>) -> Result<Option<$ret_ty:ty>, EvalError>
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
        @stage2
        #[sqlname = $name:expr]
        fn $fn_name:ident($param_name:ident: Option<$param_ty:ty>) -> Result<$ret_ty:ty, EvalError>
            $body:block
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[propagates_nulls = false]
            #[introduces_nulls = false]
            #[preserves_uniqueness = false]
            fn $fn_name($param_name: Option<$param_ty>) -> Result<Option<$ret_ty>, EvalError> {
                ($body).map(Some)
            }
        );
    };

    // The function takes non-optional arguments and returns optional, therefore it propagates
    // nulls and introduces nulls too
    (
        @stage2
        #[sqlname = $name:expr]
        fn $fn_name:ident($param_name:ident: $param_ty:ty) -> Result<Option<$ret_ty:ty>, EvalError>
            $body:block
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[propagates_nulls = true]
            #[introduces_nulls = true]
            #[preserves_uniqueness = false]
            fn $fn_name($param_name: Option<$param_ty>) -> Result<Option<$ret_ty>, EvalError> {
                let $param_name = match $param_name {
                    Some(v) => v,
                    None => return Ok(None),
                };
                $body
            }
        );
    };

    // The function takes non-optional arguments and returns non optional, therefore it propagates
    // nulls but doesn't introduce them
    (
        @stage2
        #[sqlname = $name:expr]
        fn $fn_name:ident($param_name:ident: $param_ty:ty) -> Result<$ret_ty:ty, EvalError>
            $body:block
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[propagates_nulls = true]
            #[introduces_nulls = false]
            #[preserves_uniqueness = false]
            fn $fn_name($param_name: Option<$param_ty>) -> Result<Option<$ret_ty>, EvalError> {
                let $param_name = match $param_name {
                    Some(v) => v,
                    None => return Ok(None),
                };
                ($body).map(Some)
            }
        );
    };
}

#[cfg(test)]
mod test {
    use crate::scalar::func::UnaryFuncTrait;
    use repr::ScalarType;

    sqlfunc!(
        #[sqlname = "INFALLIBLE"]
        fn infallible1(a: f32) -> f32 {
            a
        }
    );

    sqlfunc!(
        fn infallible2(a: Option<f32>) -> f32 {
            a.unwrap_or_default()
        }
    );

    sqlfunc!(
        fn infallible3(a: f32) -> Option<f32> {
            Some(a)
        }
    );

    #[test]
    fn elision_rules_infallible() {
        assert_eq!(format!("{}", Infallible1), "INFALLIBLE");
        assert!(Infallible1.propagates_nulls());
        assert!(!Infallible1.introduces_nulls());

        assert!(!Infallible2.propagates_nulls());
        assert!(!Infallible2.introduces_nulls());

        assert!(Infallible3.propagates_nulls());
        assert!(Infallible3.introduces_nulls());
    }

    #[test]
    fn output_types_infallible() {
        assert_eq!(
            Infallible1.output_type(ScalarType::Float32.nullable(true)),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible1.output_type(ScalarType::Float32.nullable(false)),
            ScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Infallible2.output_type(ScalarType::Float32.nullable(true)),
            ScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Infallible2.output_type(ScalarType::Float32.nullable(false)),
            ScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Infallible3.output_type(ScalarType::Float32.nullable(true)),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible3.output_type(ScalarType::Float32.nullable(false)),
            ScalarType::Float32.nullable(true)
        );
    }

    sqlfunc!(
        fn fallible1(a: f32) -> Result<f32, EvalError> {
            Ok(a)
        }
    );

    sqlfunc!(
        fn fallible2(a: Option<f32>) -> Result<f32, EvalError> {
            Ok(a.unwrap_or_default())
        }
    );

    sqlfunc!(
        fn fallible3(a: f32) -> Result<Option<f32>, EvalError> {
            Ok(Some(a))
        }
    );

    #[test]
    fn elision_rules_fallible() {
        assert!(Fallible1.propagates_nulls());
        assert!(!Fallible1.introduces_nulls());

        assert!(!Fallible2.propagates_nulls());
        assert!(!Fallible2.introduces_nulls());

        assert!(Fallible3.propagates_nulls());
        assert!(Fallible3.introduces_nulls());
    }

    #[test]
    fn output_types_fallible() {
        assert_eq!(
            Fallible1.output_type(ScalarType::Float32.nullable(true)),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible1.output_type(ScalarType::Float32.nullable(false)),
            ScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Fallible2.output_type(ScalarType::Float32.nullable(true)),
            ScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Fallible2.output_type(ScalarType::Float32.nullable(false)),
            ScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Fallible3.output_type(ScalarType::Float32.nullable(true)),
            ScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible3.output_type(ScalarType::Float32.nullable(false)),
            ScalarType::Float32.nullable(true)
        );
    }

    sqlfunc!(
        #[sqlname = "foobar"]
        #[propagates_nulls = false]
        #[introduces_nulls = true]
        #[preserves_uniqueness = true]
        fn explicit(a: Option<f64>) -> Result<Option<f64>, EvalError> {
            Ok(a)
        }
    );

    #[test]
    fn explicit_annotation() {
        assert_eq!(format!("{}", Explicit), "foobar");
        assert!(!Explicit.propagates_nulls());
        assert!(Explicit.introduces_nulls());
        assert!(Explicit.preserves_uniqueness());
    }
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

        $(
            impl From<$name> for crate::UnaryFunc {
                fn from(variant: $name) -> Self {
                    Self::$name(variant)
                }
            }
        )*
    }
}
