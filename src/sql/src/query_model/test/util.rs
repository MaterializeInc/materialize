// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::query_model::model::*;
use mz_repr::*;

pub(crate) fn cref(quantifier_id: QuantifierId, position: usize) -> ColumnReference {
    ColumnReference {
        quantifier_id,
        position,
    }
}

pub(crate) mod qgm {
    use super::*;

    pub(crate) fn get(id: u64) -> Get {
        Get {
            id: mz_repr::GlobalId::User(id),
            unique_keys: vec![],
        }
    }
}

// #[allow(dead_code)]
pub(crate) mod exp {
    use super::*;

    pub(crate) fn cref(quantifier_id: QuantifierId, position: usize) -> BoxScalarExpr {
        BoxScalarExpr::ColumnReference(ColumnReference {
            quantifier_id,
            position,
        })
    }

    pub(crate) fn add(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::AddInt32,
            expr1: Box::new(lhs),
            expr2: Box::new(rhs),
        }
    }

    pub(crate) fn sub(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::SubInt32,
            expr1: Box::new(lhs),
            expr2: Box::new(rhs),
        }
    }

    pub(crate) fn mul(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::MulInt32,
            expr1: Box::new(lhs),
            expr2: Box::new(rhs),
        }
    }

    pub(crate) fn div(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::DivInt32,
            expr1: Box::new(lhs),
            expr2: Box::new(rhs),
        }
    }

    pub(crate) fn gt(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::Gt,
            expr1: Box::new(lhs),
            expr2: Box::new(rhs),
        }
    }

    pub(crate) fn gte(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::Gte,
            expr1: Box::new(lhs),
            expr2: Box::new(rhs),
        }
    }

    pub(crate) fn lt(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::Lt,
            expr1: Box::new(lhs),
            expr2: Box::new(rhs),
        }
    }

    pub(crate) fn lte(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::Lte,
            expr1: Box::new(lhs),
            expr2: Box::new(rhs),
        }
    }

    pub(crate) fn eq(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::Eq,
            expr1: Box::new(lhs),
            expr2: Box::new(rhs),
        }
    }

    pub(crate) fn not_eq(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallBinary {
            func: mz_expr::BinaryFunc::NotEq,
            expr1: Box::new(lhs),
            expr2: Box::new(rhs),
        }
    }

    pub(crate) fn or(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallVariadic {
            func: mz_expr::VariadicFunc::Or,
            exprs: vec![lhs, rhs],
        }
    }

    pub(crate) fn and(lhs: BoxScalarExpr, rhs: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallVariadic {
            func: mz_expr::VariadicFunc::And,
            exprs: vec![lhs, rhs],
        }
    }

    pub(crate) fn not(expr: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallUnary {
            func: mz_expr::UnaryFunc::Not(mz_expr::func::Not),
            expr: Box::new(expr),
        }
    }

    pub(crate) fn isnull(expr: BoxScalarExpr) -> BoxScalarExpr {
        BoxScalarExpr::CallUnary {
            func: mz_expr::UnaryFunc::IsNull(mz_expr::func::IsNull),
            expr: Box::new(expr),
        }
    }

    pub(crate) fn base(position: usize, column_type: ColumnType) -> BoxScalarExpr {
        BoxScalarExpr::BaseColumn(BaseColumn {
            position,
            column_type,
        })
    }

    pub(crate) mod lit {
        use super::*;

        pub(crate) fn int32(value: i32) -> BoxScalarExpr {
            BoxScalarExpr::Literal(Row::pack(&[Datum::Int32(value)]), typ::int32(true))
        }
    }
}

pub(crate) mod typ {
    use super::*;

    pub(crate) fn int32(nullable: bool) -> ColumnType {
        ColumnType {
            scalar_type: ScalarType::Int32,
            nullable,
        }
    }

    pub(crate) fn bool(nullable: bool) -> ColumnType {
        ColumnType {
            scalar_type: ScalarType::Bool,
            nullable,
        }
    }
}
