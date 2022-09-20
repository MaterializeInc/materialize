// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

macro_rules! sqlfunc {
    // Expand the function name into an attribute if it was omitted
    (
        fn $fn_name:ident $($tail:tt)*
    ) => {
        sqlfunc!(
            #[sqlname = stringify!($fn_name)]
            fn $fn_name $($tail)*
        );
    };

    // Add the uniqueness attribute if it was omitted
    (
        #[sqlname = $name:expr]
        fn $fn_name:ident $($tail:tt)*
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[preserves_uniqueness = false]
            fn $fn_name $($tail)*
        );
    };

    // Add lifetime parameter if it was omitted
    (
        #[sqlname = $name:expr]
        #[preserves_uniqueness = $preserves_uniqueness:expr]
        fn $fn_name:ident ($($params:tt)*) $($tail:tt)*
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[preserves_uniqueness = $preserves_uniqueness]
            fn $fn_name<'a>($($params)*) $($tail)*
        );
    };

    // Normalize mut arguments to non-mut ones
    (
        #[sqlname = $name:expr]
        #[preserves_uniqueness = $preserves_uniqueness:expr]
        fn $fn_name:ident<$lt:lifetime>(mut $param_name:ident: $input_ty:ty $(,)?) -> $output_ty:ty
            $body:block
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[preserves_uniqueness = $preserves_uniqueness]
            fn $fn_name<$lt>($param_name: $input_ty) -> $output_ty {
                let mut $param_name = $param_name;
                $body
            }
        );
    };

    (
        #[sqlname = $name:expr]
        #[preserves_uniqueness = $preserves_uniqueness:expr]
        fn $fn_name:ident<$lt:lifetime>($param_name:ident: $input_ty:ty $(,)?) -> $output_ty:ty
            $body:block
    ) => {
        paste::paste! {
            #[derive(proptest_derive::Arbitrary, Ord, PartialOrd, Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Hash, mz_lowertest::MzReflect)]
            pub struct [<$fn_name:camel>];

            impl<'a> crate::func::EagerUnaryFunc<'a> for [<$fn_name:camel>] {
                type Input = $input_ty;
                type Output = $output_ty;

                fn call(&self, a: Self::Input) -> Self::Output {
                    $fn_name(a)
                }

                fn output_type(&self, input_type: mz_repr::ColumnType) -> mz_repr::ColumnType {
                    use mz_repr::AsColumnType;
                    let output = Self::Output::as_column_type();
                    let propagates_nulls = crate::func::EagerUnaryFunc::propagates_nulls(self);
                    let nullable = output.nullable;
                    // The output is nullable if it is nullable by itself or the input is nullable
                    // and this function propagates nulls
                    output.nullable(nullable || (propagates_nulls && input_type.nullable))
                }

                fn preserves_uniqueness(&self) -> bool {
                    $preserves_uniqueness
                }
            }

            impl std::fmt::Display for [<$fn_name:camel>] {
                fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                    f.write_str($name)
                }
            }

            pub fn $fn_name<$lt>($param_name: $input_ty) -> $output_ty {
                $body
            }
        }
    };
}

#[cfg(test)]
mod test {
    use crate::scalar::func::LazyUnaryFunc;
    use crate::EvalError;
    use mz_repr::ScalarType;

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
}

/// Temporary macro that generates the equivalent of what enum_dispatch will do in the future. We
/// need this manual macro implementation to delegate to the previous manual implementation for
/// variants that use the old definitions.
///
/// Once everything is handled by this macro we can remove it and replace it with `enum_dispatch`
macro_rules! derive_unary {
    ($($name:ident),*) => {
        #[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize, Hash, mz_lowertest::MzReflect)]
        pub enum UnaryFunc {
            $($name($name),)*
        }

        impl UnaryFunc {
            pub fn eval<'a>(
                &'a self,
                datums: &[Datum<'a>],
                temp_storage: &'a RowArena,
                a: &'a MirScalarExpr,
            ) -> Result<Datum<'a>, EvalError> {
                match self {
                    $(Self::$name(f) => f.eval(datums, temp_storage, a),)*
                }
            }

            pub fn output_type(&self, input_type: ColumnType) -> ColumnType {
                match self {
                    $(Self::$name(f) => LazyUnaryFunc::output_type(f, input_type),)*
                }
            }
            pub fn propagates_nulls(&self) -> bool {
                match self {
                    $(Self::$name(f) => LazyUnaryFunc::propagates_nulls(f),)*
                }
            }
            pub fn introduces_nulls(&self) -> bool {
                match self {
                    $(Self::$name(f) => LazyUnaryFunc::introduces_nulls(f),)*
                }
            }
            pub fn preserves_uniqueness(&self) -> bool {
                match self {
                    $(Self::$name(f) => LazyUnaryFunc::preserves_uniqueness(f),)*
                }
            }
        }

        impl fmt::Display for UnaryFunc {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self {
                    $(Self::$name(func) => func.fmt(f),)*
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
