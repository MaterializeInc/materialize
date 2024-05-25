use std::ops::Neg;

use num_bigint::BigInt;
use syn::spanned::Spanned;

pub fn get_discriminant(def: &syn::Expr) -> Result<BigInt, crate::Error> {
    match def {
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Byte(x),
            ..
        }) => Ok(BigInt::from(x.value())),
        syn::Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Int(x),
            ..
        }) => Ok(x.base10_parse().unwrap()),
        syn::Expr::Group(syn::ExprGroup { ref expr, .. }) => get_discriminant(expr),
        syn::Expr::Unary(x) => {
            let val = get_discriminant(&x.expr)?;
            match x.op {
                syn::UnOp::Neg(_) => Ok(val.neg()),
                _ => Err(crate::Error::UnsupportedDiscriminant(def.span())),
            }
        }
        expr => Err(crate::Error::UnsupportedDiscriminant(expr.span())),
    }
}
