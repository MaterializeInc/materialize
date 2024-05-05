// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Planning functions related to running MySQL ingestions.

use mz_proto::RustType;
use mz_repr::RelationDesc;
use mz_sql_parser::ast::{
    CreateSourceConnection, CreateSourceStatement, MySqlConfigOption, MySqlConfigOptionName,
    SourceEnvelope, UnresolvedItemName,
};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::Connection;
use mz_storage_types::sources::envelope::{KeyEnvelope, UnplannedSourceEnvelope};
use mz_storage_types::sources::mysql::{
    MySqlSourceConnection, MySqlSourceDetails, ProtoMySqlSourceDetails,
};
use mz_storage_types::sources::{GenericSourceConnection, SourceConnection, SourceDesc};
use prost::Message;

use crate::ast::display::AstDisplay;
use crate::names::Aug;
use crate::plan::error::PlanError;
use crate::plan::statement::StatementContext;
use crate::plan::with_options::TryFromValue;

generate_extracted_config!(
    MySqlConfigOption,
    (Details, String),
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![])),
    (IgnoreColumns, Vec::<UnresolvedItemName>, Default(vec![]))
);

pub(super) fn plan_create_source_desc_mysql(
    scx: &StatementContext,
    stmt: &CreateSourceStatement<Aug>,
) -> Result<(SourceDesc<ReferencedConnection>, RelationDesc), PlanError> {
    mz_ore::soft_assert_or_log!(
        stmt.referenced_subsources.is_none(),
        "referenced subsources must be cleared in purification"
    );

    let CreateSourceStatement {
        name: _,
        in_cluster: _,
        col_names: _,
        connection,
        envelope,
        if_not_exists: _,
        format,
        key_constraint: _,
        include_metadata,
        with_options: _,
        referenced_subsources: _,
        progress_subsource: _,
    } = &stmt;

    for (check, feature) in [
        (
            matches!(envelope, Some(SourceEnvelope::None) | None),
            "ENVELOPE other than NONE",
        ),
        (format.is_none(), "FORMAT"),
        (include_metadata.is_empty(), "INCLUDE metadata"),
    ] {
        if !check {
            bail_never_supported!(format!("{} with MySQL source", feature));
        }
    }

    let CreateSourceConnection::MySql {
        connection,
        options,
    } = connection
    else {
        panic!("must be MySQL connection")
    };

    let connection_item = scx.get_item_by_resolved_name(connection)?;
    match connection_item.connection()? {
        Connection::MySql(connection) => connection,
        _ => sql_bail!(
            "{} is not a MySQL connection",
            scx.catalog.resolve_full_name(connection_item.name())
        ),
    };
    let MySqlConfigOptionExtracted {
        details,
        text_columns,
        ignore_columns,
        seen: _,
    } = options.clone().try_into()?;

    let details = details
        .as_ref()
        .ok_or_else(|| sql_err!("internal error: MySQL source missing details"))?;
    let details = hex::decode(details).map_err(|e| sql_err!("{}", e))?;
    let details = ProtoMySqlSourceDetails::decode(&*details).map_err(|e| sql_err!("{}", e))?;
    let details = MySqlSourceDetails::from_proto(details).map_err(|e| sql_err!("{}", e))?;

    let text_columns = text_columns
        .into_iter()
        .map(|name| name.try_into().map_err(|e| sql_err!("{}", e)))
        .collect::<Result<Vec<_>, _>>()?;
    let ignore_columns = ignore_columns
        .into_iter()
        .map(|name| name.try_into().map_err(|e| sql_err!("{}", e)))
        .collect::<Result<Vec<_>, _>>()?;

    let connection = GenericSourceConnection::<ReferencedConnection>::from(MySqlSourceConnection {
        connection: connection_item.id(),
        connection_id: connection_item.id(),
        details,
        text_columns,
        ignore_columns,
    });

    mz_ore::soft_assert_no_log!(
        connection.metadata_columns().is_empty(),
        "PG connections do not contain metadata columns"
    );

    let (envelope, relation_desc) = UnplannedSourceEnvelope::None(KeyEnvelope::None).desc(
        None,
        RelationDesc::empty(),
        RelationDesc::empty(),
    )?;

    mz_ore::soft_assert_eq_or_log!(
        relation_desc,
        RelationDesc::empty(),
        "PostgreSQL source's primary source must have an empty relation desc"
    );

    let source_desc = SourceDesc::<ReferencedConnection> {
        connection,
        encoding: None,
        envelope,
    };

    Ok((source_desc, relation_desc))
}
