// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Functions on the `inet` and `cidr` types. Both are [`Inet`] datums, so
//! these functions accept either. Functions that return `cidr` override the
//! output type.

use mz_expr_derive::sqlfunc;
use mz_repr::SqlScalarType;
use mz_repr::adt::inet::{Inet, InetError};
use mz_repr::strconv;

use crate::EvalError;

impl From<InetError> for EvalError {
    fn from(e: InetError) -> EvalError {
        match e {
            InetError::OutOfRange => EvalError::InetOutOfRange,
            e => EvalError::InvalidParameterValue(e.to_string().into()),
        }
    }
}

#[sqlfunc(sqlname = "text_to_inet", preserves_uniqueness = false)]
fn cast_string_to_inet<'a>(a: &'a str) -> Result<Inet, EvalError> {
    strconv::parse_inet(a).map_err(Into::into)
}

#[sqlfunc(
    sqlname = "text_to_cidr",
    preserves_uniqueness = false,
    output_type_expr = SqlScalarType::Cidr.nullable(input_type.nullable)
)]
fn cast_string_to_cidr<'a>(a: &'a str) -> Result<Inet, EvalError> {
    strconv::parse_cidr(a).map_err(Into::into)
}

/// `text(inet)`, which is also the `inet -> text` cast: always prints the mask.
#[sqlfunc(sqlname = "text", preserves_uniqueness = true)]
fn cast_inet_to_string(a: Inet) -> String {
    a.display(Inet::fmt_cidr).to_string()
}

/// `cidr(inet)` and `network(inet)`: clears the host bits.
#[sqlfunc(
    sqlname = "network",
    is_monotone = true,
    output_type_expr = SqlScalarType::Cidr.nullable(input_type.nullable)
)]
fn inet_network(a: Inet) -> Inet {
    a.network()
}

#[sqlfunc(
    sqlname = "cidr_to_inet",
    preserves_uniqueness = true,
    is_eliminable_cast = true,
    is_monotone = true
)]
fn cast_cidr_to_inet(a: Inet) -> Inet {
    a
}

#[sqlfunc(sqlname = "host")]
fn inet_host(a: Inet) -> String {
    a.display(Inet::fmt_addr).to_string()
}

#[sqlfunc(sqlname = "abbrev")]
fn inet_abbrev(a: Inet) -> String {
    a.to_string()
}

#[sqlfunc(sqlname = "abbrev")]
fn cidr_abbrev(a: Inet) -> String {
    a.display(Inet::fmt_cidr_abbrev).to_string()
}

#[sqlfunc(sqlname = "masklen")]
fn inet_masklen(a: Inet) -> i32 {
    i32::from(a.masklen())
}

#[sqlfunc(sqlname = "family")]
fn inet_family(a: Inet) -> i32 {
    a.family()
}

#[sqlfunc(sqlname = "broadcast")]
fn inet_broadcast(a: Inet) -> Inet {
    a.broadcast()
}

#[sqlfunc(sqlname = "netmask")]
fn inet_netmask(a: Inet) -> Inet {
    a.netmask()
}

#[sqlfunc(sqlname = "hostmask")]
fn inet_hostmask(a: Inet) -> Inet {
    a.hostmask()
}

#[sqlfunc(sqlname = "~", preserves_uniqueness = true)]
fn inet_not(a: Inet) -> Inet {
    a.bitnot()
}

#[sqlfunc(sqlname = "set_masklen", propagates_nulls = true)]
fn inet_set_masklen(a: Inet, bits: i32) -> Result<Inet, EvalError> {
    Ok(a.set_masklen(bits)?)
}

#[sqlfunc(
    sqlname = "set_masklen",
    propagates_nulls = true,
    output_type_expr = "SqlScalarType::Cidr.nullable(input_types[0].nullable || input_types[1].nullable)"
)]
fn cidr_set_masklen(a: Inet, bits: i32) -> Result<Inet, EvalError> {
    Ok(a.set_masklen_cidr(bits)?)
}

#[sqlfunc(sqlname = "<<", is_infix_op = true, propagates_nulls = true)]
fn inet_subnet(a: Inet, b: Inet) -> bool {
    a.is_strict_subnet_of(&b)
}

#[sqlfunc(sqlname = "<<=", is_infix_op = true, propagates_nulls = true)]
fn inet_subnet_eq(a: Inet, b: Inet) -> bool {
    a.is_subnet_of(&b)
}

#[sqlfunc(sqlname = ">>", is_infix_op = true, propagates_nulls = true)]
fn inet_supernet(a: Inet, b: Inet) -> bool {
    b.is_strict_subnet_of(&a)
}

#[sqlfunc(sqlname = ">>=", is_infix_op = true, propagates_nulls = true)]
fn inet_supernet_eq(a: Inet, b: Inet) -> bool {
    b.is_subnet_of(&a)
}

#[sqlfunc(sqlname = "&&", is_infix_op = true, propagates_nulls = true)]
fn inet_overlap(a: Inet, b: Inet) -> bool {
    a.overlaps(&b)
}

#[sqlfunc(sqlname = "inet_same_family", propagates_nulls = true)]
fn inet_same_family(a: Inet, b: Inet) -> bool {
    a.is_ipv6() == b.is_ipv6()
}

#[sqlfunc(
    sqlname = "inet_merge",
    propagates_nulls = true,
    output_type_expr = "SqlScalarType::Cidr.nullable(input_types[0].nullable || input_types[1].nullable)"
)]
fn inet_merge(a: Inet, b: Inet) -> Result<Inet, EvalError> {
    Ok(a.merge(&b)?)
}

#[sqlfunc(sqlname = "&", is_infix_op = true, propagates_nulls = true)]
fn inet_and(a: Inet, b: Inet) -> Result<Inet, EvalError> {
    Ok(a.bitand(&b)?)
}

#[sqlfunc(sqlname = "|", is_infix_op = true, propagates_nulls = true)]
fn inet_or(a: Inet, b: Inet) -> Result<Inet, EvalError> {
    Ok(a.bitor(&b)?)
}

#[sqlfunc(sqlname = "+", is_infix_op = true, propagates_nulls = true)]
fn add_inet_int64(a: Inet, b: i64) -> Result<Inet, EvalError> {
    Ok(a.add(b)?)
}

#[sqlfunc(sqlname = "-", is_infix_op = true, propagates_nulls = true)]
fn sub_inet_int64(a: Inet, b: i64) -> Result<Inet, EvalError> {
    Ok(a.sub_int(b)?)
}

#[sqlfunc(sqlname = "-", is_infix_op = true, propagates_nulls = true)]
fn sub_inet(a: Inet, b: Inet) -> Result<i64, EvalError> {
    Ok(a.sub(&b)?)
}
