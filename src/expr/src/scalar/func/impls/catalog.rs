// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Scalar functions that decode durable catalog data for the builtin catalog
//! views. The decoders live in `mz_catalog_decode`, documented there.

use mz_catalog_decode::{audit_log, create_sql, durable};
use mz_expr_derive::sqlfunc;
use mz_repr::ArrayRustType;
use mz_repr::adt::jsonb::{Jsonb, JsonbRef};
use mz_repr::adt::mz_acl_item::MzAclItem;

use crate::EvalError;

fn catalog_err(e: String) -> EvalError {
    EvalError::InvalidCatalogJson(e.into())
}

fn to_jsonb(val: serde_json::Value) -> Jsonb {
    Jsonb::from_serde_json(val).expect("valid JSONB")
}

#[sqlfunc]
fn parse_catalog_id<'a>(a: JsonbRef<'a>) -> Result<String, EvalError> {
    durable::catalog_id(a).map_err(catalog_err)
}

#[sqlfunc]
fn parse_catalog_privileges<'a>(a: JsonbRef<'a>) -> Result<ArrayRustType<MzAclItem>, EvalError> {
    durable::privileges(a)
        .map(ArrayRustType)
        .map_err(catalog_err)
}

/// Returns the PostgreSQL ACL char-code string (e.g. `"ar"`) of a catalog
/// `AclMode`.
#[sqlfunc]
fn parse_catalog_acl_mode<'a>(a: JsonbRef<'a>) -> Result<String, EvalError> {
    durable::acl_mode(a)
        .map(|mode| mode.to_string())
        .map_err(catalog_err)
}

#[sqlfunc]
fn parse_catalog_audit_log_details<'a>(a: JsonbRef<'a>) -> Result<Jsonb, EvalError> {
    audit_log::details(a).map_err(catalog_err)
}

#[sqlfunc]
fn parse_catalog_create_sql<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
    create_sql::item_details(a)
        .map(to_jsonb)
        .map_err(catalog_err)
}

#[sqlfunc]
fn parse_catalog_item_references<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
    create_sql::item_references(a)
        .map(to_jsonb)
        .map_err(catalog_err)
}

#[sqlfunc]
fn parse_postgres_source_details<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
    create_sql::postgres_source_details(a)
        .map(to_jsonb)
        .map_err(catalog_err)
}

#[sqlfunc]
fn parse_kafka_source_details<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
    create_sql::kafka_source_details(a)
        .map(to_jsonb)
        .map_err(catalog_err)
}

#[sqlfunc]
fn parse_source_export_details<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
    create_sql::source_export_details(a)
        .map(to_jsonb)
        .map_err(catalog_err)
}

#[sqlfunc]
fn parse_connection_details<'a>(a: &'a str) -> Result<Jsonb, EvalError> {
    create_sql::connection_details(a)
        .map(to_jsonb)
        .map_err(catalog_err)
}
