// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// TODO(benesch): remove this module if upstream adds implementations of FromSql
// to tuples directly: https://github.com/sfackler/rust-postgres/pull/626.

use std::error::Error;

use postgres_types::{private, FromSql, Kind, Type, WrongType};

/// A wrapper for tuples that implements [`FromSql`] for PostgreSQL composite
/// types.
#[derive(Debug, PartialEq, Eq)]
pub struct Record<T>(pub T);

macro_rules! impl_tuple {
    ($n:expr; $($ty_ident:ident),*; $($var_ident:ident),*) => {
        impl<'a, $($ty_ident),*> FromSql<'a> for Record<($($ty_ident,)*)>
        where
            $($ty_ident: FromSql<'a>),*
        {
            fn from_sql(
                _: &Type,
                mut raw: &'a [u8],
            ) -> Result<Record<($($ty_ident,)*)>, Box<dyn Error + Sync + Send>> {
                let num_fields = private::read_be_i32(&mut raw)?;
                if num_fields as usize != $n {
                    return Err(format!(
                        "Postgres record field count does not match Rust tuple length: {} vs {}",
                        num_fields,
                        $n,
                    ).into());
                }

                $(
                    let oid = private::read_be_i32(&mut raw)? as u32;
                    let ty = match Type::from_oid(oid) {
                        None => {
                            return Err(format!(
                                "cannot decode OID {} inside of anonymous record",
                                oid,
                            ).into());
                        }
                        Some(ty) if !$ty_ident::accepts(&ty) => {
                            return Err(Box::new(WrongType::new::<$ty_ident>(ty.clone())));
                        }
                        Some(ty) => ty,
                    };
                    let $var_ident = private::read_value(&ty, &mut raw)?;
                )*

                Ok(Record(($($var_ident,)*)))
            }

            fn accepts(ty: &Type) -> bool {
                match ty.kind() {
                    Kind::Pseudo => *ty == Type::RECORD,
                    Kind::Composite(fields) => fields.len() == $n,
                    _ => false,
                }
            }
        }
    };
}

impl_tuple!(0; ; );
impl_tuple!(1; T0; v0);
impl_tuple!(2; T0, T1; v0, v1);
impl_tuple!(3; T0, T1, T2; v0, v1, v2);
impl_tuple!(4; T0, T1, T2, T3; v0, v1, v2, v3);
