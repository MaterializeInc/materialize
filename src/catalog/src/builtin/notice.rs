// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::sync::LazyLock;

use mz_pgrepr::oid;
use mz_repr::namespaces::MZ_INTERNAL_SCHEMA;
use mz_repr::{RelationDesc, SqlScalarType};
use mz_sql::catalog::NameReference;

use crate::builtin::{Builtin, BuiltinIndex, BuiltinTable, BuiltinView, MONITOR_SELECT};

use super::{MONITOR_REDACTED_SELECT, SUPPORT_SELECT};

pub static MZ_OPTIMIZER_NOTICES: LazyLock<BuiltinTable> = LazyLock::new(|| {
    use SqlScalarType::{List, String, TimestampTz};

    BuiltinTable {
        name: "mz_optimizer_notices",
        schema: MZ_INTERNAL_SCHEMA,
        oid: oid::TABLE_MZ_OPTIMIZER_NOTICES_OID,
        desc: RelationDesc::builder()
            .with_column("id", String.nullable(false))
            .with_column("notice_type", String.nullable(false))
            .with_column("message", String.nullable(false))
            .with_column("hint", String.nullable(false))
            .with_column("action", String.nullable(true))
            .with_column("redacted_message", String.nullable(true))
            .with_column("redacted_hint", String.nullable(true))
            .with_column("redacted_action", String.nullable(true))
            .with_column("action_type", String.nullable(true))
            .with_column("object_id", String.nullable(true))
            .with_column(
                "dependency_ids",
                List {
                    element_type: Box::new(String),
                    custom_id: None,
                }
                .nullable(false),
            )
            .with_column(
                "created_at",
                TimestampTz { precision: None }.nullable(false),
            )
            .with_key(vec![0])
            .finish(),
        column_comments: BTreeMap::new(),
        is_retained_metrics_object: false,
        access: vec![MONITOR_SELECT],
    }
});

/// An [`MZ_NOTICES`] that is made safe to be viewed by Materialize staff
/// because it binds the `redacted_~` from [`MZ_NOTICES`] as `~`.
///
/// This view is provisioned to accomodate the union of notice types (optimizer,
/// sources and sinks, etc). Even though at the moment is only hosts optimizer
/// notices, the idea is to evolve it over time as sketched in the design doc[^1].
///
/// [^1]: <https://github.com/MaterializeInc/materialize/blob/main/doc/developer/design/20231113_optimizer_notice_catalog.md>
pub static MZ_NOTICES: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_notices",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_NOTICES_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("notice_type", SqlScalarType::String.nullable(false))
        .with_column("message", SqlScalarType::String.nullable(false))
        .with_column("hint", SqlScalarType::String.nullable(false))
        .with_column("action", SqlScalarType::String.nullable(true))
        .with_column("redacted_message", SqlScalarType::String.nullable(true))
        .with_column("redacted_hint", SqlScalarType::String.nullable(true))
        .with_column("redacted_action", SqlScalarType::String.nullable(true))
        .with_column("action_type", SqlScalarType::String.nullable(true))
        .with_column("object_id", SqlScalarType::String.nullable(true))
        .with_column(
            "created_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "Materialize's unique ID for this notice."),
        ("notice_type", "The notice type."),
        (
            "message",
            "A brief description of the issue highlighted by this notice.",
        ),
        (
            "hint",
            "A high-level hint that tells the user what can be improved.",
        ),
        ("action", "A concrete action that will resolve the notice."),
        (
            "redacted_message",
            "A redacted version of the `message` column. `NULL` if no redaction is needed.",
        ),
        (
            "redacted_hint",
            "A redacted version of the `hint` column. `NULL` if no redaction is needed.",
        ),
        (
            "redacted_action",
            "A redacted version of the `action` column. `NULL` if no redaction is needed.",
        ),
        (
            "action_type",
            "The type of the `action` string (`sql_statements` for a valid SQL string or `plain_text` for plain text).",
        ),
        (
            "object_id",
            "The ID of the materialized view or index. Corresponds to `mz_objects.id`. For global notices, this column is `NULL`.",
        ),
        (
            "created_at",
            "The time at which the notice was created. Note that some notices are re-created on `environmentd` restart.",
        ),
    ]),
    sql: "SELECT
    n.id,
    n.notice_type,
    n.message,
    n.hint,
    n.action,
    n.redacted_message,
    n.redacted_hint,
    n.redacted_action,
    n.action_type,
    n.object_id,
    n.created_at
FROM
    mz_internal.mz_optimizer_notices n
",
    access: vec![MONITOR_SELECT],
});

/// A redacted version of [`MZ_NOTICES`] that is made safe to be viewed by
/// Materialize staff because it binds the `redacted_~` from [`MZ_NOTICES`] as
/// `~`.
pub static MZ_NOTICES_REDACTED: LazyLock<BuiltinView> = LazyLock::new(|| BuiltinView {
    name: "mz_notices_redacted",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::VIEW_MZ_NOTICES_REDACTED_OID,
    desc: RelationDesc::builder()
        .with_column("id", SqlScalarType::String.nullable(false))
        .with_column("notice_type", SqlScalarType::String.nullable(false))
        .with_column("message", SqlScalarType::String.nullable(false))
        .with_column("hint", SqlScalarType::String.nullable(false))
        .with_column("action", SqlScalarType::String.nullable(true))
        .with_column("action_type", SqlScalarType::String.nullable(true))
        .with_column("object_id", SqlScalarType::String.nullable(true))
        .with_column(
            "created_at",
            SqlScalarType::TimestampTz { precision: None }.nullable(false),
        )
        .with_key(vec![0])
        .finish(),
    column_comments: BTreeMap::from_iter([
        ("id", "Materialize's unique ID for this notice."),
        ("notice_type", "The notice type."),
        (
            "message",
            "A redacted brief description of the issue highlighted by this notice.",
        ),
        (
            "hint",
            "A redacted high-level hint that tells the user what can be improved.",
        ),
        (
            "action",
            "A redacted concrete action that will resolve the notice.",
        ),
        (
            "action_type",
            "The type of the `action` string (`sql_statements` for a valid SQL string or `plain_text` for plain text).",
        ),
        (
            "object_id",
            "The ID of the materialized view or index. Corresponds to `mz_objects.id`. For global notices, this column is `NULL`.",
        ),
        (
            "created_at",
            "The time at which the notice was created. Note that some notices are re-created on `environmentd` restart.",
        ),
    ]),
    sql: "SELECT
    id,
    notice_type,
    coalesce(redacted_message, message) as message,
    coalesce(redacted_hint, hint) as hint,
    coalesce(redacted_action, action) as action,
    action_type,
    object_id,
    created_at
FROM
    mz_internal.mz_notices
",
    access: vec![SUPPORT_SELECT, MONITOR_REDACTED_SELECT, MONITOR_SELECT],
});

pub const MZ_NOTICES_IND: BuiltinIndex = BuiltinIndex {
    name: "mz_notices_ind",
    schema: MZ_INTERNAL_SCHEMA,
    oid: oid::INDEX_MZ_NOTICES_IND_OID,
    sql: "IN CLUSTER mz_catalog_server ON mz_internal.mz_notices(id)",
    is_retained_metrics_object: false,
};

/// An iterator over [`Builtin`] objects for optimization hints.
///
/// Used in the [`super::BUILTINS_STATIC`] initializer.
pub(super) fn builtins() -> impl Iterator<Item = Builtin<NameReference>> {
    [
        Builtin::Table(&MZ_OPTIMIZER_NOTICES),
        Builtin::View(&MZ_NOTICES),
        Builtin::View(&MZ_NOTICES_REDACTED),
        Builtin::Index(&MZ_NOTICES_IND),
    ]
    .into_iter()
}
