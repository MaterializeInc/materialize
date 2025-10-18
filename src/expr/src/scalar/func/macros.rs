// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// Convenience macro for generating `inverse` values.
macro_rules! to_unary {
    ($f:expr) => {
        Some(crate::UnaryFunc::from($f))
    };
}

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

    // Add both uniqueness + inverse attributes if they were omitted
    (
        #[sqlname = $name:expr]
        fn $fn_name:ident $($tail:tt)*
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[preserves_uniqueness = false]
            #[inverse = None]
            fn $fn_name $($tail)*
        );
    };

    (
        #[sqlname = $name:expr]
        #[is_monotone = $is_monotone:expr]
        fn $fn_name:ident $($tail:tt)*
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[preserves_uniqueness = false]
            #[is_monotone = $is_monotone]
            fn $fn_name $($tail)*
        );
    };

    // Add the inverse attribute if it was omitted
    (
        #[sqlname = $name:expr]
        #[preserves_uniqueness = $preserves_uniqueness:expr]
        fn $fn_name:ident $($tail:tt)*
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[preserves_uniqueness = $preserves_uniqueness]
            #[inverse = None]
            fn $fn_name $($tail)*
        );
    };

    (
        #[sqlname = $name:expr]
        #[preserves_uniqueness = $preserves_uniqueness:expr]
        #[is_monotone = $is_monotone:expr]
        fn $fn_name:ident $($tail:tt)*
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[preserves_uniqueness = $preserves_uniqueness]
            #[inverse = None]
            #[is_monotone = $is_monotone]
            fn $fn_name $($tail)*
        );
    };

    // Add the monotone attribute if it was omitted
    (
        #[sqlname = $name:expr]
        #[preserves_uniqueness = $preserves_uniqueness:expr]
        #[inverse = $inverse:expr]
        fn $fn_name:ident $($tail:tt)*
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[preserves_uniqueness = $preserves_uniqueness]
            #[inverse = $inverse]
            #[is_monotone = false]
            fn $fn_name $($tail)*
        );
    };

    // Add lifetime parameter if it was omitted
    (
        #[sqlname = $name:expr]
        #[preserves_uniqueness = $preserves_uniqueness:expr]
        #[inverse = $inverse:expr]
        #[is_monotone = $is_monotone:expr]
        fn $fn_name:ident ($($params:tt)*) $($tail:tt)*
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[preserves_uniqueness = $preserves_uniqueness]
            #[inverse = $inverse]
            #[is_monotone = $is_monotone]
            fn $fn_name<'a>($($params)*) $($tail)*
        );
    };

    // Normalize mut arguments to non-mut ones
    (
        #[sqlname = $name:expr]
        #[preserves_uniqueness = $preserves_uniqueness:expr]
        #[inverse = $inverse:expr]
        #[is_monotone = $is_monotone:expr]
        fn $fn_name:ident<$lt:lifetime>(mut $param_name:ident: $input_ty:ty $(,)?) -> $output_ty:ty
            $body:block
    ) => {
        sqlfunc!(
            #[sqlname = $name]
            #[preserves_uniqueness = $preserves_uniqueness]
            #[inverse = $inverse]
            #[is_monotone = $is_monotone]
            fn $fn_name<$lt>($param_name: $input_ty) -> $output_ty {
                let mut $param_name = $param_name;
                $body
            }
        );
    };

    (
        #[sqlname = $name:expr]
        #[preserves_uniqueness = $preserves_uniqueness:expr]
        #[inverse = $inverse:expr]
        #[is_monotone = $is_monotone:expr]
        fn $fn_name:ident<$lt:lifetime>($param_name:ident: $input_ty:ty $(,)?) -> $output_ty:ty
            $body:block
    ) => {
        paste::paste! {
            #[mz_expr_derive::sqlfunc(
                sqlname = $name,
                preserves_uniqueness = $preserves_uniqueness,
                inverse = $inverse,
                is_monotone = $is_monotone,
            )]
            #[allow(clippy::extra_unused_lifetimes)]
            pub fn $fn_name<$lt>($param_name: $input_ty) -> $output_ty {
                $body
            }

            mod $fn_name {
                #[cfg(test)]
                #[mz_ore::test]
                // unsupported operation: extern static `pidfd_spawnp` is not supported by Miri
                #[cfg_attr(miri, ignore)]
                fn test_sqlfunc_macro() {
                    use crate::func::EagerUnaryFunc;
                    let f = super::[<$fn_name:camel>];
                    let input_type = mz_repr::SqlColumnType {
                        scalar_type: mz_repr::SqlScalarType::Float32,
                        nullable: true,
                    };
                    let output_type_nullable = f.output_type(input_type);

                    let input_type = mz_repr::SqlColumnType {
                        scalar_type: mz_repr::SqlScalarType::Float32,
                        nullable: false,
                    };
                    let output_type_nonnullable = f.output_type(input_type);

                    let preserves_uniqueness = f.preserves_uniqueness();
                    let inverse = f.inverse();
                    let is_monotone = f.is_monotone();
                    let propagates_nulls = f.propagates_nulls();
                    let introduces_nulls = f.introduces_nulls();
                    let could_error = f.could_error();

                    #[derive(Debug)]
                    #[allow(unused)]
                    struct Info {
                        output_type_nullable: mz_repr::SqlColumnType,
                        output_type_nonnullable: mz_repr::SqlColumnType,
                        preserves_uniqueness: bool,
                        inverse: Option<crate::UnaryFunc>,
                        is_monotone: bool,
                        propagates_nulls: bool,
                        introduces_nulls: bool,
                        could_error: bool,
                    }

                    let info = Info {
                        output_type_nullable,
                        output_type_nonnullable,
                        preserves_uniqueness,
                        inverse,
                        is_monotone,
                        propagates_nulls,
                        introduces_nulls,
                        could_error,
                    };

                    insta::assert_debug_snapshot!(info);
                }

            }
        }
    };
}

#[cfg(test)]
mod test {
    use mz_repr::SqlScalarType;

    use crate::EvalError;
    use crate::scalar::func::LazyUnaryFunc;

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

    #[mz_ore::test]
    fn elision_rules_infallible() {
        assert_eq!(format!("{}", Infallible1), "INFALLIBLE");
        assert!(Infallible1.propagates_nulls());
        assert!(!Infallible1.introduces_nulls());

        assert!(!Infallible2.propagates_nulls());
        assert!(!Infallible2.introduces_nulls());

        assert!(Infallible3.propagates_nulls());
        assert!(Infallible3.introduces_nulls());
    }

    #[mz_ore::test]
    fn output_types_infallible() {
        assert_eq!(
            Infallible1.output_type(SqlScalarType::Float32.nullable(true)),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible1.output_type(SqlScalarType::Float32.nullable(false)),
            SqlScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Infallible2.output_type(SqlScalarType::Float32.nullable(true)),
            SqlScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Infallible2.output_type(SqlScalarType::Float32.nullable(false)),
            SqlScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Infallible3.output_type(SqlScalarType::Float32.nullable(true)),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Infallible3.output_type(SqlScalarType::Float32.nullable(false)),
            SqlScalarType::Float32.nullable(true)
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

    #[mz_ore::test]
    fn elision_rules_fallible() {
        assert!(Fallible1.propagates_nulls());
        assert!(!Fallible1.introduces_nulls());

        assert!(!Fallible2.propagates_nulls());
        assert!(!Fallible2.introduces_nulls());

        assert!(Fallible3.propagates_nulls());
        assert!(Fallible3.introduces_nulls());
    }

    #[mz_ore::test]
    fn output_types_fallible() {
        assert_eq!(
            Fallible1.output_type(SqlScalarType::Float32.nullable(true)),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible1.output_type(SqlScalarType::Float32.nullable(false)),
            SqlScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Fallible2.output_type(SqlScalarType::Float32.nullable(true)),
            SqlScalarType::Float32.nullable(false)
        );
        assert_eq!(
            Fallible2.output_type(SqlScalarType::Float32.nullable(false)),
            SqlScalarType::Float32.nullable(false)
        );

        assert_eq!(
            Fallible3.output_type(SqlScalarType::Float32.nullable(true)),
            SqlScalarType::Float32.nullable(true)
        );
        assert_eq!(
            Fallible3.output_type(SqlScalarType::Float32.nullable(false)),
            SqlScalarType::Float32.nullable(true)
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
                output: &mut Vec<Datum<'a>>,
            ) -> Result<(), EvalError> {
                match self {
                    $(Self::$name(f) => f.eval(datums, temp_storage, a, output),)*
                }
            }

            pub fn output_type(&self, input_type: SqlColumnType) -> SqlColumnType {
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
            pub fn inverse(&self) -> Option<UnaryFunc> {
                match self {
                    $(Self::$name(f) => LazyUnaryFunc::inverse(f),)*
                }
            }
            pub fn is_monotone(&self) -> bool {
                match self {
                    $(Self::$name(f) => LazyUnaryFunc::is_monotone(f),)*
                }
            }
            pub fn could_error(&self) -> bool {
                match self {
                    $(Self::$name(f) => LazyUnaryFunc::could_error(f),)*
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

/// This is not complete yet, pending conversion of all binary funcs to an implementation of
/// `LazyBinaryFunc`.
macro_rules! derive_binary_from {
    ($($name:ident $( ( $variant:ident ) )?),* $(,)?) => {
        $(
            derive_binary_from!(from @ $name $( ( $variant ) )?);
        )*
    };
    (from @ $name:ident ( $variant:ident ) ) => {
        impl From<$variant> for crate::BinaryFunc {
            fn from(variant: $variant) -> Self {
                Self::$name(variant)
            }
        }
    };
    (from @ $name:ident) => {
        derive_binary_from!(from @ $name ( $name ) );
    };
}
