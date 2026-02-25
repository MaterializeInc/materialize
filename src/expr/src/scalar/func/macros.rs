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

#[cfg(test)]
mod test {
    use mz_expr_derive::sqlfunc;
    use mz_repr::SqlScalarType;

    use crate::EvalError;
    use crate::scalar::func::LazyUnaryFunc;

    #[sqlfunc(sqlname = "INFALLIBLE")]
    fn infallible1(a: f32) -> f32 {
        a
    }

    #[sqlfunc]
    fn infallible2(a: Option<f32>) -> f32 {
        a.unwrap_or_default()
    }

    #[sqlfunc]
    fn infallible3(a: f32) -> Option<f32> {
        Some(a)
    }

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

    #[sqlfunc]
    fn fallible1(a: f32) -> Result<f32, EvalError> {
        Ok(a)
    }

    #[sqlfunc]
    fn fallible2(a: Option<f32>) -> Result<f32, EvalError> {
        Ok(a.unwrap_or_default())
    }

    #[sqlfunc]
    fn fallible3(a: f32) -> Result<Option<f32>, EvalError> {
        Ok(Some(a))
    }

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
        #[derive(
            Ord, PartialOrd, Clone, Debug, Eq, PartialEq,
            serde::Serialize, serde::Deserialize, Hash,
            mz_lowertest::MzReflect,
        )]
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

/// Generates the `VariadicFunc` enum, `Display` impl (delegating to inner),
/// and `From<InnerType>` impls for each variant.
///
/// All variants must use explicit `Name(Type)` syntax. When the variant name equals
/// the inner type name, write e.g. `And(And)`.
macro_rules! derive_variadic {
    ($($name:ident ( $variant:ident )),* $(,)?) => {
        #[derive(
            Ord, PartialOrd, Clone, Debug, Eq, PartialEq,
            serde::Serialize, serde::Deserialize, Hash,
            mz_lowertest::MzReflect,
        )]
        pub enum VariadicFunc {
            $($name($variant),)*
        }

        impl std::fmt::Display for VariadicFunc {
            fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                match self {
                    $(Self::$name(func) => func.fmt(f),)*
                }
            }
        }

        $(
            impl From<$variant> for crate::VariadicFunc {
                fn from(variant: $variant) -> Self {
                    Self::$name(variant)
                }
            }
        )*
    }
}

/// Generates the `BinaryFunc` enum, its `impl` block (with 8 delegating methods),
/// `Display` impl, and `From<InnerType>` impls for each variant.
///
/// All variants must use explicit `Name(Type)` syntax. When the variant name equals
/// the inner type name, write e.g. `AddInt16(AddInt16)`.
macro_rules! derive_binary {
    ($($name:ident ( $variant:ident )),* $(,)?) => {
        #[derive(
            Ord, PartialOrd, Clone, Debug, Eq, PartialEq,
            serde::Serialize, serde::Deserialize, Hash,
            mz_lowertest::MzReflect,
        )]
        pub enum BinaryFunc {
            $($name($variant),)*
        }

        impl BinaryFunc {
            pub fn eval<'a>(
                &'a self,
                datums: &[Datum<'a>],
                temp_storage: &'a RowArena,
                exprs: &[&'a MirScalarExpr],
            ) -> Result<Datum<'a>, EvalError> {
                match self {
                    $(Self::$name(f) => f.eval(datums, temp_storage, exprs),)*
                }
            }

            pub fn output_type(&self, input_types: &[SqlColumnType]) -> SqlColumnType {
                match self {
                    $(Self::$name(f) => {
                        LazyBinaryFunc::output_type(f, input_types)
                    },)*
                }
            }

            pub fn propagates_nulls(&self) -> bool {
                match self {
                    $(Self::$name(f) => LazyBinaryFunc::propagates_nulls(f),)*
                }
            }

            pub fn introduces_nulls(&self) -> bool {
                match self {
                    $(Self::$name(f) => LazyBinaryFunc::introduces_nulls(f),)*
                }
            }

            pub fn is_infix_op(&self) -> bool {
                match self {
                    $(Self::$name(f) => LazyBinaryFunc::is_infix_op(f),)*
                }
            }

            pub fn negate(&self) -> Option<BinaryFunc> {
                match self {
                    $(Self::$name(f) => LazyBinaryFunc::negate(f),)*
                }
            }

            pub fn could_error(&self) -> bool {
                match self {
                    $(Self::$name(f) => LazyBinaryFunc::could_error(f),)*
                }
            }

            pub fn is_monotone(&self) -> (bool, bool) {
                match self {
                    $(Self::$name(f) => LazyBinaryFunc::is_monotone(f),)*
                }
            }
        }

        impl fmt::Display for BinaryFunc {
            fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                match self {
                    $(Self::$name(func) => func.fmt(f),)*
                }
            }
        }

        $(
            impl From<$variant> for crate::BinaryFunc {
                fn from(variant: $variant) -> Self {
                    Self::$name(variant)
                }
            }
        )*
    }
}
