// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Data definition language (DDL).
//!
//! This module houses the handlers for statements that modify the catalog, like
//! `ALTER`, `CREATE`, and `DROP`.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;
use std::iter;
use std::time::Duration;

use itertools::{Either, Itertools};
use mz_adapter_types::compaction::{CompactionWindow, DEFAULT_LOGICAL_COMPACTION_WINDOW_DURATION};
use mz_controller_types::{
    is_cluster_size_v2, ClusterId, ReplicaId, DEFAULT_REPLICA_LOGGING_INTERVAL,
};
use mz_expr::refresh_schedule::{RefreshEvery, RefreshSchedule};
use mz_expr::{CollectionPlan, UnmaterializableFunc};
use mz_interchange::avro::{AvroSchemaGenerator, AvroSchemaOptions, DocTarget};
use mz_ore::cast::CastFrom;
use mz_ore::collections::HashSet;
use mz_ore::soft_panic_or_log;
use mz_ore::str::StrExt;
use mz_repr::adt::interval::Interval;
use mz_repr::adt::mz_acl_item::{MzAclItem, PrivilegeMap};
use mz_repr::optimize::OptimizerFeatureOverrides;
use mz_repr::role_id::RoleId;
use mz_repr::{
    ColumnName, ColumnType, GlobalId, RelationDesc, RelationType, ScalarType, Timestamp,
};
use mz_sql_parser::ast::display::comma_separated;
use mz_sql_parser::ast::{
    self, AlterClusterAction, AlterClusterStatement, AlterConnectionAction, AlterConnectionOption,
    AlterConnectionOptionName, AlterConnectionStatement, AlterIndexAction, AlterIndexStatement,
    AlterObjectRenameStatement, AlterObjectSwapStatement, AlterRetainHistoryStatement,
    AlterRoleOption, AlterRoleStatement, AlterSecretStatement, AlterSetClusterStatement,
    AlterSinkStatement, AlterSourceAction, AlterSourceAddSubsourceOption,
    AlterSourceAddSubsourceOptionName, AlterSourceStatement, AlterSystemResetAllStatement,
    AlterSystemResetStatement, AlterSystemSetStatement, AvroSchema, ClusterFeature,
    ClusterFeatureName, ClusterOption, ClusterOptionName, ClusterScheduleOptionValue, ColumnOption,
    CommentObjectType, CommentStatement, CreateClusterReplicaStatement, CreateClusterStatement,
    CreateConnectionOption, CreateConnectionOptionName, CreateConnectionStatement,
    CreateConnectionType, CreateDatabaseStatement, CreateIndexStatement,
    CreateMaterializedViewStatement, CreateRoleStatement, CreateSchemaStatement,
    CreateSecretStatement, CreateSinkConnection, CreateSinkOption, CreateSinkOptionName,
    CreateSinkStatement, CreateSourceOptionName, CreateTableStatement, CreateTypeAs,
    CreateTypeListOption, CreateTypeListOptionName, CreateTypeMapOption, CreateTypeMapOptionName,
    CreateTypeStatement, CreateViewStatement, CreateWebhookSourceStatement, CsrConfigOption,
    CsrConfigOptionName, CsrConnection, CsrConnectionAvro, DocOnIdentifier, DocOnSchema,
    DropObjectsStatement, DropOwnedStatement, Expr, Format, Ident, IfExistsBehavior, IndexOption,
    IndexOptionName, KafkaSinkConfigOption, MaterializedViewOption, MaterializedViewOptionName,
    QualifiedReplica, RefreshAtOptionValue, RefreshEveryOptionValue, RefreshOptionValue,
    ReplicaDefinition, ReplicaOption, ReplicaOptionName, RoleAttribute, SetRoleVar, Statement,
    TableConstraint, TableOption, TableOptionName, UnresolvedDatabaseName, UnresolvedItemName,
    UnresolvedObjectName, UnresolvedSchemaName, Value, ViewDefinition, WithOptionValue,
};
use mz_sql_parser::ident;
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::connections::Connection;
use mz_storage_types::sinks::{
    KafkaIdStyle, KafkaSinkConnection, KafkaSinkFormat, SinkEnvelope, StorageSinkConnection,
};
use mz_storage_types::sources::Timeline;

use crate::ast::display::AstDisplay;
use crate::catalog::{
    CatalogCluster, CatalogDatabase, CatalogError, CatalogItem, CatalogItemType,
    CatalogRecordField, CatalogType, CatalogTypeDetails, ObjectType, SystemObjectType,
};
use crate::kafka_util::KafkaSinkConfigOptionExtracted;
use crate::names::{
    Aug, CommentObjectId, DatabaseId, ObjectId, PartialItemName, QualifiedItemName,
    ResolvedClusterName, ResolvedColumnName, ResolvedDataType, ResolvedDatabaseSpecifier,
    ResolvedItemName, SchemaSpecifier, SystemObjectId,
};
use crate::normalize::{self, ident};
use crate::plan::error::PlanError;
use crate::plan::query::{plan_expr, scalar_type_from_catalog, ExprContext, QueryLifetime};
use crate::plan::scope::Scope;
use crate::plan::statement::ddl::connection::{INALTERABLE_OPTIONS, MUTUALLY_EXCLUSIVE_SETS};
use crate::plan::statement::{scl, StatementContext, StatementDesc};
use crate::plan::typeconv::CastContext;
use crate::plan::with_options::{OptionalDuration, TryFromValue};
use crate::plan::WebhookValidation;
use crate::plan::{
    plan_utils, query, transform_ast, AlterClusterPlan, AlterClusterRenamePlan,
    AlterClusterReplicaRenamePlan, AlterClusterSwapPlan, AlterConnectionPlan, AlterItemRenamePlan,
    AlterNoopPlan, AlterOptionParameter, AlterRetainHistoryPlan, AlterRolePlan,
    AlterSchemaRenamePlan, AlterSchemaSwapPlan, AlterSecretPlan, AlterSetClusterPlan,
    AlterSystemResetAllPlan, AlterSystemResetPlan, AlterSystemSetPlan, ClusterSchedule,
    CommentPlan, ComputeReplicaConfig, ComputeReplicaIntrospectionConfig, CreateClusterManagedPlan,
    CreateClusterPlan, CreateClusterReplicaPlan, CreateClusterUnmanagedPlan, CreateClusterVariant,
    CreateConnectionPlan, CreateDatabasePlan, CreateIndexPlan, CreateMaterializedViewPlan,
    CreateRolePlan, CreateSchemaPlan, CreateSecretPlan, CreateSinkPlan, CreateSourcePlan,
    CreateTablePlan, CreateTypePlan, CreateViewPlan, DataSourceDesc, DropObjectsPlan,
    DropOwnedPlan, Index, MaterializedView, Params, Plan, PlanClusterOption, PlanNotice,
    QueryContext, ReplicaConfig, Secret, Sink, Source, Table, Type, VariableValue, View,
    WebhookBodyFormat, WebhookHeaderFilters, WebhookHeaders,
};
use crate::session::vars;
use crate::session::vars::{
    ENABLE_CLUSTER_SCHEDULE_REFRESH, ENABLE_KAFKA_SINK_HEADERS, ENABLE_REFRESH_EVERY_MVS,
};

mod connection;
pub mod source;

// TODO: Figure out what the maximum number of columns we can actually support is, and set that.
//
// The real max is probably higher than this, but it's easier to relax a constraint than make it
// more strict.
const MAX_NUM_COLUMNS: usize = 256;

const MANAGED_REPLICA_PATTERN: once_cell::sync::Lazy<regex::Regex> =
    once_cell::sync::Lazy::new(|| regex::Regex::new(r"^r(\d)+$").unwrap());

pub fn describe_create_database(
    _: &StatementContext,
    _: CreateDatabaseStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_database(
    _: &StatementContext,
    CreateDatabaseStatement {
        name,
        if_not_exists,
    }: CreateDatabaseStatement,
) -> Result<Plan, PlanError> {
    Ok(Plan::CreateDatabase(CreateDatabasePlan {
        name: normalize::ident(name.0),
        if_not_exists,
    }))
}

pub fn describe_create_schema(
    _: &StatementContext,
    _: CreateSchemaStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_schema(
    scx: &StatementContext,
    CreateSchemaStatement {
        mut name,
        if_not_exists,
    }: CreateSchemaStatement,
) -> Result<Plan, PlanError> {
    if name.0.len() > 2 {
        sql_bail!("schema name {} has more than two components", name);
    }
    let schema_name = normalize::ident(
        name.0
            .pop()
            .expect("names always have at least one component"),
    );
    let database_spec = match name.0.pop() {
        None => match scx.catalog.active_database() {
            Some(id) => ResolvedDatabaseSpecifier::Id(id.clone()),
            None => sql_bail!("no database specified and no active database"),
        },
        Some(n) => match scx.resolve_database(&UnresolvedDatabaseName(n.clone())) {
            Ok(database) => ResolvedDatabaseSpecifier::Id(database.id()),
            Err(_) => sql_bail!("invalid database {}", n.as_str()),
        },
    };
    Ok(Plan::CreateSchema(CreateSchemaPlan {
        database_spec,
        schema_name,
        if_not_exists,
    }))
}

pub fn describe_create_table(
    _: &StatementContext,
    _: CreateTableStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_table(
    scx: &StatementContext,
    stmt: CreateTableStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateTableStatement {
        name,
        columns,
        constraints,
        if_not_exists,
        temporary,
        with_options,
    } = &stmt;

    let names: Vec<_> = columns
        .iter()
        .map(|c| normalize::column_name(c.name.clone()))
        .collect();

    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    // Build initial relation type that handles declared data types
    // and NOT NULL constraints.
    let mut column_types = Vec::with_capacity(columns.len());
    let mut defaults = Vec::with_capacity(columns.len());
    let mut keys = Vec::new();

    for (i, c) in columns.into_iter().enumerate() {
        let aug_data_type = &c.data_type;
        let ty = query::scalar_type_from_sql(scx, aug_data_type)?;
        let mut nullable = true;
        let mut default = Expr::null();
        for option in &c.options {
            match &option.option {
                ColumnOption::NotNull => nullable = false,
                ColumnOption::Default(expr) => {
                    // Ensure expression can be planned and yields the correct
                    // type.
                    let mut expr = expr.clone();
                    transform_ast::transform(scx, &mut expr)?;
                    let _ = query::plan_default_expr(scx, &expr, &ty)?;
                    default = expr.clone();
                }
                ColumnOption::Unique { is_primary } => {
                    keys.push(vec![i]);
                    if *is_primary {
                        nullable = false;
                    }
                }
                other => {
                    bail_unsupported!(format!("CREATE TABLE with column constraint: {}", other))
                }
            }
        }
        column_types.push(ty.nullable(nullable));
        defaults.push(default);
    }

    let mut seen_primary = false;
    'c: for constraint in constraints {
        match constraint {
            TableConstraint::Unique {
                name: _,
                columns,
                is_primary,
                nulls_not_distinct,
            } => {
                if seen_primary && *is_primary {
                    sql_bail!(
                        "multiple primary keys for table {} are not allowed",
                        name.to_ast_string_stable()
                    );
                }
                seen_primary = *is_primary || seen_primary;

                let mut key = vec![];
                for column in columns {
                    let column = normalize::column_name(column.clone());
                    match names.iter().position(|name| *name == column) {
                        None => sql_bail!("unknown column in constraint: {}", column),
                        Some(i) => {
                            let nullable = &mut column_types[i].nullable;
                            if *is_primary {
                                if *nulls_not_distinct {
                                    sql_bail!(
                                        "[internal error] PRIMARY KEY does not support NULLS NOT DISTINCT"
                                    );
                                }

                                *nullable = false;
                            } else if !(*nulls_not_distinct || !*nullable) {
                                // Non-primary key unique constraints are only keys if all of their
                                // columns are `NOT NULL` or the constraint is `NULLS NOT DISTINCT`.
                                break 'c;
                            }

                            key.push(i);
                        }
                    }
                }

                if *is_primary {
                    keys.insert(0, key);
                } else {
                    keys.push(key);
                }
            }
            TableConstraint::ForeignKey { .. } => {
                // Foreign key constraints are not presently enforced. We allow
                // them with feature flags for sqllogictest's sake.
                scx.require_feature_flag(&vars::ENABLE_TABLE_FOREIGN_KEY)?
            }
            TableConstraint::Check { .. } => {
                // Check constraints are not presently enforced. We allow them
                // with feature flags for sqllogictest's sake.
                scx.require_feature_flag(&vars::ENABLE_TABLE_CHECK_CONSTRAINT)?
            }
        }
    }

    if !keys.is_empty() {
        // Unique constraints are not presently enforced. We allow them with feature flags for
        // sqllogictest's sake.
        scx.require_feature_flag(&vars::ENABLE_TABLE_KEYS)?
    }

    let typ = RelationType::new(column_types).with_keys(keys);

    let temporary = *temporary;
    let name = if temporary {
        scx.allocate_temporary_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    } else {
        scx.allocate_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    };

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    // For PostgreSQL compatibility, we need to prevent creating tables when
    // there is an existing object *or* type of the same name.
    if let (false, Ok(item)) = (
        if_not_exists,
        scx.catalog.resolve_item_or_type(&partial_name),
    ) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    let desc = RelationDesc::new(typ, names);

    let create_sql = normalize::create_statement(scx, Statement::CreateTable(stmt.clone()))?;

    let options = plan_table_options(scx, with_options.clone())?;
    let compaction_window = options.iter().find_map(|o| {
        #[allow(irrefutable_let_patterns)]
        if let crate::plan::TableOption::RetainHistory(lcw) = o {
            Some(lcw.clone())
        } else {
            None
        }
    });

    let table = Table {
        create_sql,
        desc,
        defaults,
        temporary,
        compaction_window,
    };
    Ok(Plan::CreateTable(CreateTablePlan {
        name,
        table,
        if_not_exists: *if_not_exists,
    }))
}

pub fn describe_create_webhook_source(
    _: &StatementContext,
    _: CreateWebhookSourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_webhook_source(
    scx: &StatementContext,
    mut stmt: CreateWebhookSourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    // We will rewrite the cluster if one is not provided, so we must use the `in_cluster` value
    // we plan to normalize when we canonicalize the create statement.
    let in_cluster = source_sink_cluster_config(scx, "source", &mut stmt.in_cluster)?;
    let create_sql =
        normalize::create_statement(scx, Statement::CreateWebhookSource(stmt.clone()))?;

    let CreateWebhookSourceStatement {
        name,
        if_not_exists,
        body_format,
        include_headers,
        validate_using,
        // We resolved `in_cluster` above, so we want to ignore it here.
        in_cluster: _,
    } = stmt;

    let validate_using = validate_using
        .map(|stmt| query::plan_webhook_validate_using(scx, stmt))
        .transpose()?;
    if let Some(WebhookValidation { expression, .. }) = &validate_using {
        // If the validation expression doesn't reference any part of the request, then we should
        // return an error because it's almost definitely wrong.
        if !expression.contains_column() {
            return Err(PlanError::WebhookValidationDoesNotUseColumns);
        }
        // Validation expressions cannot contain unmaterializable functions, except `now()`. We
        // allow calls to `now()` because some webhook providers recommend rejecting requests that
        // are older than a certain threshold.
        if expression.contains_unmaterializable_except(&[UnmaterializableFunc::CurrentTimestamp]) {
            return Err(PlanError::WebhookValidationNonDeterministic);
        }
    }

    let body_format = match body_format {
        Format::Bytes => WebhookBodyFormat::Bytes,
        Format::Json { array } => WebhookBodyFormat::Json { array },
        Format::Text => WebhookBodyFormat::Text,
        // TODO(parkmycar): Make an issue to support more types, or change this to NeverSupported.
        ty => {
            return Err(PlanError::Unsupported {
                feature: format!("{ty} is not a valid BODY FORMAT for a WEBHOOK source"),
                issue_no: None,
            })
        }
    };

    let mut column_ty = vec![
        // Always include the body of the request as the first column.
        ColumnType {
            scalar_type: ScalarType::from(body_format),
            nullable: false,
        },
    ];
    let mut column_names = vec!["body".to_string()];

    let mut headers = WebhookHeaders::default();

    // Include a `headers` column, possibly filtered.
    if let Some(filters) = include_headers.column {
        column_ty.push(ColumnType {
            scalar_type: ScalarType::Map {
                value_type: Box::new(ScalarType::String),
                custom_id: None,
            },
            nullable: false,
        });
        column_names.push("headers".to_string());

        let (allow, block): (BTreeSet<_>, BTreeSet<_>) =
            filters.into_iter().partition_map(|filter| {
                if filter.block {
                    itertools::Either::Right(filter.header_name)
                } else {
                    itertools::Either::Left(filter.header_name)
                }
            });
        headers.header_column = Some(WebhookHeaderFilters { allow, block });
    }

    // Map headers to specific columns.
    for header in include_headers.mappings {
        let scalar_type = header
            .use_bytes
            .then_some(ScalarType::Bytes)
            .unwrap_or(ScalarType::String);
        column_ty.push(ColumnType {
            scalar_type,
            nullable: true,
        });
        column_names.push(header.column_name.into_string());

        let column_idx = column_ty.len() - 1;
        // Double check we're consistent with column names.
        assert_eq!(
            column_idx,
            column_names.len() - 1,
            "header column names and types don't match"
        );
        headers
            .mapped_headers
            .insert(column_idx, (header.header_name, header.use_bytes));
    }

    // Validate our columns.
    let mut unique_check = HashSet::with_capacity(column_names.len());
    for name in &column_names {
        if !unique_check.insert(name) {
            return Err(PlanError::AmbiguousColumn(name.clone().into()));
        }
    }
    if column_names.len() > MAX_NUM_COLUMNS {
        return Err(PlanError::TooManyColumns {
            max_num_columns: MAX_NUM_COLUMNS,
            req_num_columns: column_names.len(),
        });
    }

    let typ = RelationType::new(column_ty);
    let desc = RelationDesc::new(typ, column_names);

    // Check for an object in the catalog with this same name
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name)?)?;
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (false, Ok(item)) = (if_not_exists, scx.catalog.resolve_item(&partial_name)) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    // Note(parkmycar): We don't currently support specifying a timeline for Webhook sources. As
    // such, we always use a default of EpochMilliseconds.
    let timeline = Timeline::EpochMilliseconds;

    Ok(Plan::CreateSource(CreateSourcePlan {
        name,
        source: Source {
            create_sql,
            data_source: DataSourceDesc::Webhook {
                validate_using,
                body_format,
                headers,
            },
            desc,
            compaction_window: None,
        },
        if_not_exists,
        timeline,
        in_cluster: Some(in_cluster),
    }))
}

/// Determine the cluster ID to use for this item.
///
/// If `in_cluster` is `None` we will update it to refer to the default cluster.
/// Because of this, do not normalize/canonicalize the create SQL statement
/// until after calling this function.
fn source_sink_cluster_config(
    scx: &StatementContext,
    ty: &'static str,
    in_cluster: &mut Option<ResolvedClusterName>,
) -> Result<ClusterId, PlanError> {
    let cluster = match in_cluster {
        None => {
            let cluster = scx.catalog.resolve_cluster(None)?;
            *in_cluster = Some(ResolvedClusterName {
                id: cluster.id(),
                print_name: None,
            });
            cluster
        }
        Some(in_cluster) => scx.catalog.get_cluster(in_cluster.id),
    };

    if cluster.replica_ids().len() > 1 {
        sql_bail!("cannot create {ty} in cluster with more than one replica")
    }

    Ok(cluster.id())
}

pub fn describe_create_view(
    _: &StatementContext,
    _: CreateViewStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_view(
    scx: &StatementContext,
    def: &mut ViewDefinition<Aug>,
    params: &Params,
    temporary: bool,
) -> Result<(QualifiedItemName, View), PlanError> {
    let create_sql = normalize::create_statement(
        scx,
        Statement::CreateView(CreateViewStatement {
            if_exists: IfExistsBehavior::Error,
            temporary,
            definition: def.clone(),
        }),
    )?;

    let ViewDefinition {
        name,
        columns,
        query,
    } = def;

    let query::PlannedRootQuery {
        mut expr,
        mut desc,
        finishing,
        scope: _,
    } = query::plan_root_query(scx, query.clone(), QueryLifetime::View)?;
    // We get back a trivial finishing, because `plan_root_query` applies the given finishing.
    // Note: Earlier, we were thinking to maybe persist the finishing information with the view
    // here to help with materialize#724. However, in the meantime, there might be a better
    // approach to solve materialize#724:
    // https://github.com/MaterializeInc/materialize/issues/724#issuecomment-1688293709
    assert!(finishing.is_trivial(expr.arity()));

    expr.bind_parameters(params)?;

    let name = if temporary {
        scx.allocate_temporary_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    } else {
        scx.allocate_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?
    };

    plan_utils::maybe_rename_columns(
        format!("view {}", scx.catalog.resolve_full_name(&name)),
        &mut desc,
        columns,
    )?;
    let names: Vec<ColumnName> = desc.iter_names().cloned().collect();

    if let Some(dup) = names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    let view = View {
        create_sql,
        expr,
        column_names: names,
        temporary,
    };

    Ok((name, view))
}

pub fn plan_create_view(
    scx: &StatementContext,
    mut stmt: CreateViewStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    let CreateViewStatement {
        temporary,
        if_exists,
        definition,
    } = &mut stmt;
    let (name, view) = plan_view(scx, definition, params, *temporary)?;

    // Override the statement-level IfExistsBehavior with Skip if this is
    // explicitly requested in the PlanContext (the default is `false`).
    let ignore_if_exists_errors = scx.pcx().map_or(false, |pcx| pcx.ignore_if_exists_errors);

    let replace = if *if_exists == IfExistsBehavior::Replace && !ignore_if_exists_errors {
        let if_exists = true;
        let cascade = false;
        if let Some(id) = plan_drop_item(
            scx,
            ObjectType::View,
            if_exists,
            definition.name.clone(),
            cascade,
        )? {
            if view.expr.depends_on().contains(&id) {
                let item = scx.catalog.get_item(&id);
                sql_bail!(
                    "cannot replace view {0}: depended upon by new {0} definition",
                    scx.catalog.resolve_full_name(item.name())
                );
            }
            Some(id)
        } else {
            None
        }
    } else {
        None
    };
    let drop_ids = replace
        .map(|id| {
            scx.catalog
                .item_dependents(id)
                .into_iter()
                .map(|id| id.unwrap_item_id())
                .collect()
        })
        .unwrap_or_default();

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    // For PostgreSQL compatibility, we need to prevent creating views when
    // there is an existing object *or* type of the same name.
    if let (Ok(item), IfExistsBehavior::Error, false) = (
        scx.catalog.resolve_item_or_type(&partial_name),
        *if_exists,
        ignore_if_exists_errors,
    ) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    Ok(Plan::CreateView(CreateViewPlan {
        name,
        view,
        replace,
        drop_ids,
        if_not_exists: *if_exists == IfExistsBehavior::Skip,
        ambiguous_columns: *scx.ambiguous_columns.borrow(),
    }))
}

pub fn describe_create_materialized_view(
    _: &StatementContext,
    _: CreateMaterializedViewStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_materialized_view(
    scx: &StatementContext,
    mut stmt: CreateMaterializedViewStatement<Aug>,
    params: &Params,
) -> Result<Plan, PlanError> {
    let cluster_id =
        crate::plan::statement::resolve_cluster_for_materialized_view(scx.catalog, &stmt)?;
    stmt.in_cluster = Some(ResolvedClusterName {
        id: cluster_id,
        print_name: None,
    });

    let create_sql =
        normalize::create_statement(scx, Statement::CreateMaterializedView(stmt.clone()))?;

    let partial_name = normalize::unresolved_item_name(stmt.name)?;
    let name = scx.allocate_qualified_name(partial_name.clone())?;

    let query::PlannedRootQuery {
        mut expr,
        mut desc,
        finishing,
        scope: _,
    } = query::plan_root_query(scx, stmt.query, QueryLifetime::MaterializedView)?;
    // We get back a trivial finishing, see comment in `plan_view`.
    assert!(finishing.is_trivial(expr.arity()));

    expr.bind_parameters(params)?;

    plan_utils::maybe_rename_columns(
        format!("materialized view {}", scx.catalog.resolve_full_name(&name)),
        &mut desc,
        &stmt.columns,
    )?;
    let column_names: Vec<ColumnName> = desc.iter_names().cloned().collect();

    let MaterializedViewOptionExtracted {
        assert_not_null,
        retain_history,
        refresh,
        seen: _,
    }: MaterializedViewOptionExtracted = stmt.with_options.try_into()?;

    let refresh_schedule = {
        let mut refresh_schedule = RefreshSchedule::empty();
        let mut on_commits_seen = 0;
        for refresh_option_value in refresh {
            if !matches!(refresh_option_value, RefreshOptionValue::OnCommit) {
                scx.require_feature_flag(&ENABLE_REFRESH_EVERY_MVS)?;
            }
            match refresh_option_value {
                RefreshOptionValue::OnCommit => {
                    on_commits_seen += 1;
                }
                RefreshOptionValue::AtCreation => {
                    soft_panic_or_log!("REFRESH AT CREATION should have been purified away");
                    sql_bail!("INTERNAL ERROR: REFRESH AT CREATION should have been purified away")
                }
                RefreshOptionValue::At(RefreshAtOptionValue { mut time }) => {
                    transform_ast::transform(scx, &mut time)?; // Desugar the expression
                    let ecx = &ExprContext {
                        qcx: &QueryContext::root(scx, QueryLifetime::OneShot),
                        name: "REFRESH AT",
                        scope: &Scope::empty(),
                        relation_type: &RelationType::empty(),
                        allow_aggregates: false,
                        allow_subqueries: false,
                        allow_parameters: false,
                        allow_windows: false,
                    };
                    let hir = plan_expr(ecx, &time)?.cast_to(
                        ecx,
                        CastContext::Assignment,
                        &ScalarType::MzTimestamp,
                    )?;
                    // (mz_now was purified away to a literal earlier)
                    let timestamp = hir
                        .into_literal_mz_timestamp()
                        .ok_or_else(|| PlanError::InvalidRefreshAt)?;
                    refresh_schedule.ats.push(timestamp);
                }
                RefreshOptionValue::Every(RefreshEveryOptionValue {
                    interval,
                    aligned_to,
                }) => {
                    let interval = Interval::try_from_value(Value::Interval(interval))?;
                    if interval.as_microseconds() <= 0 {
                        sql_bail!("REFRESH interval must be positive; got: {}", interval);
                    }
                    if interval.months != 0 {
                        // This limitation is because we want Intervals to be cleanly convertable
                        // to a unix epoch timestamp difference. When the interval involves months, then
                        // this is not true anymore, because months have variable lengths.
                        // See `Timestamp::round_up`.
                        sql_bail!("REFRESH interval must not involve units larger than days");
                    }
                    let interval = interval.duration()?;
                    if u64::try_from(interval.as_millis()).is_err() {
                        sql_bail!("REFRESH interval too large");
                    }

                    let mut aligned_to = match aligned_to {
                        Some(aligned_to) => aligned_to,
                        None => {
                            soft_panic_or_log!(
                                "ALIGNED TO should have been filled in by purification"
                            );
                            sql_bail!(
                                "INTERNAL ERROR: ALIGNED TO should have been filled in by purification"
                            )
                        }
                    };

                    // Desugar the `aligned_to` expression
                    transform_ast::transform(scx, &mut aligned_to)?;

                    let ecx = &ExprContext {
                        qcx: &QueryContext::root(scx, QueryLifetime::OneShot),
                        name: "REFRESH EVERY ... ALIGNED TO",
                        scope: &Scope::empty(),
                        relation_type: &RelationType::empty(),
                        allow_aggregates: false,
                        allow_subqueries: false,
                        allow_parameters: false,
                        allow_windows: false,
                    };
                    let aligned_to_hir = plan_expr(ecx, &aligned_to)?.cast_to(
                        ecx,
                        CastContext::Assignment,
                        &ScalarType::MzTimestamp,
                    )?;
                    // (mz_now was purified away to a literal earlier)
                    let aligned_to_const = aligned_to_hir
                        .into_literal_mz_timestamp()
                        .ok_or_else(|| PlanError::InvalidRefreshEveryAlignedTo)?;

                    refresh_schedule.everies.push(RefreshEvery {
                        interval,
                        aligned_to: aligned_to_const,
                    });
                }
            }
        }

        if on_commits_seen > 1 {
            sql_bail!("REFRESH ON COMMIT cannot be specified multiple times");
        }
        if on_commits_seen > 0 && refresh_schedule != RefreshSchedule::empty() {
            sql_bail!("REFRESH ON COMMIT is not compatible with any of the other REFRESH options");
        }

        if refresh_schedule == RefreshSchedule::empty() {
            None
        } else {
            Some(refresh_schedule)
        }
    };

    let as_of = stmt.as_of.map(Timestamp::from);

    let compaction_window = retain_history
        .map(|cw| {
            scx.require_feature_flag(&vars::ENABLE_LOGICAL_COMPACTION_WINDOW)?;
            Ok::<_, PlanError>(cw.try_into()?)
        })
        .transpose()?;
    let mut non_null_assertions = assert_not_null
        .into_iter()
        .map(normalize::column_name)
        .map(|assertion_name| {
            column_names
                .iter()
                .position(|col| col == &assertion_name)
                .ok_or_else(|| {
                    sql_err!(
                        "column {} in ASSERT NOT NULL option not found",
                        assertion_name.as_str().quoted()
                    )
                })
        })
        .collect::<Result<Vec<_>, _>>()?;
    non_null_assertions.sort();
    if let Some(dup) = non_null_assertions.iter().duplicates().next() {
        let dup = &column_names[*dup];
        sql_bail!(
            "duplicate column {} in non-null assertions",
            dup.as_str().quoted()
        );
    }

    if let Some(dup) = column_names.iter().duplicates().next() {
        sql_bail!("column {} specified more than once", dup.as_str().quoted());
    }

    // Override the statement-level IfExistsBehavior with Skip if this is
    // explicitly requested in the PlanContext (the default is `false`).
    let if_exists = match scx.pcx().map(|pcx| pcx.ignore_if_exists_errors) {
        Ok(true) => IfExistsBehavior::Skip,
        _ => stmt.if_exists,
    };

    let mut replace = None;
    let mut if_not_exists = false;
    match if_exists {
        IfExistsBehavior::Replace => {
            let if_exists = true;
            let cascade = false;
            let replace_id = plan_drop_item(
                scx,
                ObjectType::MaterializedView,
                if_exists,
                partial_name.into(),
                cascade,
            )?;
            if let Some(id) = replace_id {
                if expr.depends_on().contains(&id) {
                    let item = scx.catalog.get_item(&id);
                    sql_bail!(
                        "cannot replace materialized view {0}: depended upon by new {0} definition",
                        scx.catalog.resolve_full_name(item.name())
                    );
                }
                replace = Some(id);
            }
        }
        IfExistsBehavior::Skip => if_not_exists = true,
        IfExistsBehavior::Error => (),
    }
    let drop_ids = replace
        .map(|id| {
            scx.catalog
                .item_dependents(id)
                .into_iter()
                .map(|id| id.unwrap_item_id())
                .collect()
        })
        .unwrap_or_default();

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    // For PostgreSQL compatibility, we need to prevent creating materialized
    // views when there is an existing object *or* type of the same name.
    if let (IfExistsBehavior::Error, Ok(item)) =
        (if_exists, scx.catalog.resolve_item_or_type(&partial_name))
    {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    Ok(Plan::CreateMaterializedView(CreateMaterializedViewPlan {
        name,
        materialized_view: MaterializedView {
            create_sql,
            expr,
            column_names,
            cluster_id,
            non_null_assertions,
            compaction_window,
            refresh_schedule,
            as_of,
        },
        replace,
        drop_ids,
        if_not_exists,
        ambiguous_columns: *scx.ambiguous_columns.borrow(),
    }))
}

generate_extracted_config!(
    MaterializedViewOption,
    (AssertNotNull, Ident, AllowMultiple),
    (RetainHistory, Duration),
    (Refresh, RefreshOptionValue<Aug>, AllowMultiple)
);

pub fn describe_create_sink(
    _: &StatementContext,
    _: CreateSinkStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(CreateSinkOption, (Snapshot, bool));

pub fn plan_create_sink(
    scx: &StatementContext,
    mut stmt: CreateSinkStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateSinkStatement {
        name,
        in_cluster: _,
        from,
        connection,
        format,
        envelope,
        if_not_exists,
        with_options,
    } = stmt.clone();

    const ALLOWED_WITH_OPTIONS: &[CreateSinkOptionName] = &[CreateSinkOptionName::Snapshot];

    if let Some(op) = with_options
        .iter()
        .find(|op| !ALLOWED_WITH_OPTIONS.contains(&op.name))
    {
        scx.require_feature_flag_w_dynamic_desc(
            &vars::ENABLE_CREATE_SOURCE_DENYLIST_WITH_OPTIONS,
            format!("CREATE SINK...WITH ({}..)", op.name.to_ast_string()),
            format!(
                "permitted options are {}",
                comma_separated(ALLOWED_WITH_OPTIONS)
            ),
        )?;
    }

    let envelope = match envelope {
        Some(ast::SinkEnvelope::Upsert) => SinkEnvelope::Upsert,
        Some(ast::SinkEnvelope::Debezium) => SinkEnvelope::Debezium,
        None => sql_bail!("ENVELOPE clause is required"),
    };

    // Check for an object in the catalog with this same name
    let Some(name) = name else {
        return Err(PlanError::MissingName(CatalogItemType::Sink));
    };
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name)?)?;
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (false, Ok(item)) = (if_not_exists, scx.catalog.resolve_item(&partial_name)) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    let from_name = &from;
    let from = scx.get_item_by_resolved_name(&from)?;
    let desc = from.desc(&scx.catalog.resolve_full_name(from.name()))?;
    let key_indices = match &connection {
        CreateSinkConnection::Kafka { key, .. } => {
            if let Some(key) = key.clone() {
                let key_columns = key
                    .key_columns
                    .into_iter()
                    .map(normalize::column_name)
                    .collect::<Vec<_>>();
                let mut uniq = BTreeSet::new();
                for col in key_columns.iter() {
                    if !uniq.insert(col) {
                        sql_bail!("Repeated column name in sink key: {}", col);
                    }
                }
                let indices = key_columns
                    .iter()
                    .map(|col| -> anyhow::Result<usize> {
                        let name_idx = desc
                            .get_by_name(col)
                            .map(|(idx, _type)| idx)
                            .ok_or_else(|| sql_err!("No such column: {}", col))?;
                        if desc.get_unambiguous_name(name_idx).is_none() {
                            sql_err!("Ambiguous column: {}", col);
                        }
                        Ok(name_idx)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let is_valid_key =
                    desc.typ().keys.iter().any(|key_columns| {
                        key_columns.iter().all(|column| indices.contains(column))
                    });

                if !is_valid_key && envelope == SinkEnvelope::Upsert {
                    if key.not_enforced {
                        scx.catalog
                            .add_notice(PlanNotice::UpsertSinkKeyNotEnforced {
                                key: key_columns.clone(),
                                name: name.item.clone(),
                            })
                    } else {
                        return Err(PlanError::UpsertSinkWithInvalidKey {
                            name: from_name.full_name_str(),
                            desired_key: key_columns.iter().map(|c| c.to_string()).collect(),
                            valid_keys: desc
                                .typ()
                                .keys
                                .iter()
                                .map(|key| {
                                    key.iter()
                                        .map(|col| desc.get_name(*col).as_str().into())
                                        .collect()
                                })
                                .collect(),
                        });
                    }
                }
                Some(indices)
            } else {
                None
            }
        }
    };

    let headers_index = match &connection {
        CreateSinkConnection::Kafka {
            headers: Some(headers),
            ..
        } => {
            scx.require_feature_flag(&ENABLE_KAFKA_SINK_HEADERS)?;

            match envelope {
                SinkEnvelope::Upsert => (),
                SinkEnvelope::Debezium => {
                    sql_bail!("HEADERS option is not supported with ENVELOPE DEBEZIUM")
                }
            };

            let headers = normalize::column_name(headers.clone());
            let (idx, ty) = desc
                .get_by_name(&headers)
                .ok_or_else(|| sql_err!("HEADERS column ({}) is unknown", headers))?;

            if desc.get_unambiguous_name(idx).is_none() {
                sql_bail!("HEADERS column ({}) is ambiguous", headers);
            }

            match &ty.scalar_type {
                ScalarType::Map { value_type, .. }
                    if matches!(&**value_type, ScalarType::String | ScalarType::Bytes) => {}
                _ => sql_bail!(
                    "HEADERS column must have type map[text => text] or map[text => bytea]"
                ),
            }

            Some(idx)
        }
        _ => None,
    };

    // pick the first valid natural relation key, if any
    let relation_key_indices = desc.typ().keys.get(0).cloned();

    let key_desc_and_indices = key_indices.map(|key_indices| {
        let cols = desc
            .iter()
            .map(|(name, ty)| (name.clone(), ty.clone()))
            .collect::<Vec<_>>();
        let (names, types): (Vec<_>, Vec<_>) =
            key_indices.iter().map(|&idx| cols[idx].clone()).unzip();
        let typ = RelationType::new(types);
        (RelationDesc::new(typ, names), key_indices)
    });

    if key_desc_and_indices.is_none() && envelope == SinkEnvelope::Upsert {
        return Err(PlanError::UpsertSinkWithoutKey);
    }

    let connection_builder = match connection {
        CreateSinkConnection::Kafka {
            connection,
            options,
            ..
        } => kafka_sink_builder(
            scx,
            connection,
            options,
            format,
            relation_key_indices,
            key_desc_and_indices,
            headers_index,
            desc.into_owned(),
            envelope,
            from.id(),
        )?,
    };

    let CreateSinkOptionExtracted { snapshot, seen: _ } = with_options.try_into()?;

    // WITH SNAPSHOT defaults to true
    let with_snapshot = snapshot.unwrap_or(true);

    // We will rewrite the cluster if one is not provided, so we must use the
    // `in_cluster` value we plan to normalize when we canonicalize the create
    // statement.
    let in_cluster = source_sink_cluster_config(scx, "sink", &mut stmt.in_cluster)?;
    let create_sql = normalize::create_statement(scx, Statement::CreateSink(stmt))?;

    Ok(Plan::CreateSink(CreateSinkPlan {
        name,
        sink: Sink {
            create_sql,
            from: from.id(),
            connection: connection_builder,
            envelope,
        },
        with_snapshot,
        if_not_exists,
        in_cluster,
    }))
}

/// Creating this by hand instead of using generate_extracted_config! macro
/// because the macro doesn't support parameterized enums. See <https://github.com/MaterializeInc/materialize/issues/22213>
#[derive(Debug, Default, PartialEq, Clone)]
pub struct CsrConfigOptionExtracted {
    seen: ::std::collections::BTreeSet<CsrConfigOptionName<Aug>>,
    pub(crate) avro_key_fullname: Option<String>,
    pub(crate) avro_value_fullname: Option<String>,
    pub(crate) null_defaults: bool,
    pub(crate) value_doc_options: BTreeMap<DocTarget, String>,
    pub(crate) key_doc_options: BTreeMap<DocTarget, String>,
}

impl std::convert::TryFrom<Vec<CsrConfigOption<Aug>>> for CsrConfigOptionExtracted {
    type Error = crate::plan::PlanError;
    fn try_from(v: Vec<CsrConfigOption<Aug>>) -> Result<CsrConfigOptionExtracted, Self::Error> {
        let mut extracted = CsrConfigOptionExtracted::default();
        let mut common_doc_comments = BTreeMap::new();
        for option in v {
            if !extracted.seen.insert(option.name.clone()) {
                return Err(PlanError::Unstructured({
                    format!("{} specified more than once", option.name)
                }));
            }
            let option_name = option.name.clone();
            let option_name_str = option_name.to_ast_string();
            let better_error = |e: PlanError| {
                PlanError::Unstructured(format!("invalid {}: {}", option_name.to_ast_string(), e))
            };
            match option.name {
                CsrConfigOptionName::AvroKeyFullname => {
                    extracted.avro_key_fullname =
                        <Option<String>>::try_from_value(option.value).map_err(better_error)?;
                }
                CsrConfigOptionName::AvroValueFullname => {
                    extracted.avro_value_fullname =
                        <Option<String>>::try_from_value(option.value).map_err(better_error)?;
                }
                CsrConfigOptionName::NullDefaults => {
                    extracted.null_defaults =
                        <bool>::try_from_value(option.value).map_err(better_error)?;
                }
                CsrConfigOptionName::AvroDocOn(doc_on) => {
                    let value = String::try_from_value(option.value.ok_or_else(|| {
                        PlanError::InvalidOptionValue {
                            option_name: option_name_str,
                            err: Box::new(PlanError::Unstructured("cannot be empty".to_string())),
                        }
                    })?)
                    .map_err(better_error)?;
                    let key = match doc_on.identifier {
                        DocOnIdentifier::Column(ResolvedColumnName::Column {
                            relation: ResolvedItemName::Item { id, .. },
                            name,
                            index: _,
                        }) => DocTarget::Field {
                            object_id: id,
                            column_name: name,
                        },
                        DocOnIdentifier::Type(ResolvedItemName::Item { id, .. }) => {
                            DocTarget::Type(id)
                        }
                        _ => unreachable!(),
                    };

                    match doc_on.for_schema {
                        DocOnSchema::KeyOnly => {
                            extracted.key_doc_options.insert(key, value);
                        }
                        DocOnSchema::ValueOnly => {
                            extracted.value_doc_options.insert(key, value);
                        }
                        DocOnSchema::All => {
                            common_doc_comments.insert(key, value);
                        }
                    }
                }
            }
        }

        for (key, value) in common_doc_comments {
            if !extracted.key_doc_options.contains_key(&key) {
                extracted.key_doc_options.insert(key.clone(), value.clone());
            }
            if !extracted.value_doc_options.contains_key(&key) {
                extracted.value_doc_options.insert(key, value);
            }
        }
        Ok(extracted)
    }
}

fn kafka_sink_builder(
    scx: &StatementContext,
    connection: ResolvedItemName,
    options: Vec<KafkaSinkConfigOption<Aug>>,
    format: Option<Format<Aug>>,
    relation_key_indices: Option<Vec<usize>>,
    key_desc_and_indices: Option<(RelationDesc, Vec<usize>)>,
    headers_index: Option<usize>,
    value_desc: RelationDesc,
    envelope: SinkEnvelope,
    sink_from: GlobalId,
) -> Result<StorageSinkConnection<ReferencedConnection>, PlanError> {
    // Get Kafka connection.
    let connection_item = scx.get_item_by_resolved_name(&connection)?;
    let connection_id = connection_item.id();
    match connection_item.connection()? {
        Connection::Kafka(_) => (),
        _ => sql_bail!(
            "{} is not a kafka connection",
            scx.catalog.resolve_full_name(connection_item.name())
        ),
    };

    let KafkaSinkConfigOptionExtracted {
        topic,
        compression_type,
        progress_group_id_prefix,
        transactional_id_prefix,
        legacy_ids,
        seen: _,
    }: KafkaSinkConfigOptionExtracted = options.try_into()?;

    let transactional_id = match (transactional_id_prefix, legacy_ids) {
        (Some(_), Some(true)) => {
            sql_bail!("LEGACY IDS cannot be used at the same time as TRANSACTIONAL ID PREFIX")
        }
        (None, Some(true)) => KafkaIdStyle::Legacy,
        (prefix, _) => KafkaIdStyle::Prefix(prefix),
    };

    let progress_group_id = match (progress_group_id_prefix, legacy_ids) {
        (Some(_), Some(true)) => {
            sql_bail!("LEGACY IDS cannot be used at the same time as PROGRESS GROUP ID PREFIX")
        }
        (None, Some(true)) => KafkaIdStyle::Legacy,
        (prefix, _) => KafkaIdStyle::Prefix(prefix),
    };

    let topic_name = topic.ok_or_else(|| sql_err!("KAFKA CONNECTION must specify TOPIC"))?;

    let format = match format {
        Some(Format::Avro(AvroSchema::Csr {
            csr_connection:
                CsrConnectionAvro {
                    connection:
                        CsrConnection {
                            connection,
                            options,
                        },
                    seed,
                    key_strategy,
                    value_strategy,
                },
        })) => {
            if seed.is_some() {
                sql_bail!("SEED option does not make sense with sinks");
            }
            if key_strategy.is_some() {
                sql_bail!("KEY STRATEGY option does not make sense with sinks");
            }
            if value_strategy.is_some() {
                sql_bail!("VALUE STRATEGY option does not make sense with sinks");
            }

            let item = scx.get_item_by_resolved_name(&connection)?;
            let csr_connection = match item.connection()? {
                Connection::Csr(_) => item.id(),
                _ => {
                    sql_bail!(
                        "{} is not a schema registry connection",
                        scx.catalog
                            .resolve_full_name(item.name())
                            .to_string()
                            .quoted()
                    )
                }
            };
            let CsrConfigOptionExtracted {
                avro_key_fullname,
                avro_value_fullname,
                null_defaults,
                key_doc_options,
                value_doc_options,
                ..
            } = options.try_into()?;

            if key_desc_and_indices.is_none() && avro_key_fullname.is_some() {
                sql_bail!("Cannot specify AVRO KEY FULLNAME without a corresponding KEY field");
            }

            if key_desc_and_indices.is_some()
                && (avro_key_fullname.is_some() ^ avro_value_fullname.is_some())
            {
                sql_bail!("Must specify both AVRO KEY FULLNAME and AVRO VALUE FULLNAME when specifying generated schema names");
            }

            let options = AvroSchemaOptions {
                avro_key_fullname,
                avro_value_fullname,
                set_null_defaults: null_defaults,
                is_debezium: matches!(envelope, SinkEnvelope::Debezium),
                sink_from: Some(sink_from),
                value_doc_options,
                key_doc_options,
            };

            let schema_generator = AvroSchemaGenerator::new(
                key_desc_and_indices
                    .as_ref()
                    .map(|(desc, _indices)| desc.clone()),
                value_desc.clone(),
                options,
            )?;
            let value_schema = schema_generator.value_writer_schema().to_string();
            let key_schema = schema_generator
                .key_writer_schema()
                .map(|key_schema| key_schema.to_string());

            KafkaSinkFormat::Avro {
                key_schema,
                value_schema,
                csr_connection,
            }
        }
        Some(Format::Json { array: false }) => KafkaSinkFormat::Json,
        Some(Format::Json { array: true }) => bail_unsupported!("JSON ARRAY format in sinks"),
        Some(format) => bail_unsupported!(format!("sink format {:?}", format)),
        None => bail_unsupported!("sink without format"),
    };

    Ok(StorageSinkConnection::Kafka(KafkaSinkConnection {
        connection_id,
        connection: connection_id,
        format,
        topic: topic_name,
        relation_key_indices,
        key_desc_and_indices,
        headers_index,
        value_desc,
        compression_type,
        progress_group_id,
        transactional_id,
    }))
}

pub fn describe_create_index(
    _: &StatementContext,
    _: CreateIndexStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_index(
    scx: &StatementContext,
    mut stmt: CreateIndexStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateIndexStatement {
        name,
        on_name,
        in_cluster,
        key_parts,
        with_options,
        if_not_exists,
    } = &mut stmt;
    let on = scx.get_item_by_resolved_name(on_name)?;
    let on_item_type = on.item_type();

    if !matches!(
        on_item_type,
        CatalogItemType::View
            | CatalogItemType::MaterializedView
            | CatalogItemType::Source
            | CatalogItemType::Table
    ) {
        sql_bail!(
            "index cannot be created on {} because it is a {}",
            on_name.full_name_str(),
            on.item_type()
        )
    }

    let on_desc = on.desc(&scx.catalog.resolve_full_name(on.name()))?;

    let filled_key_parts = match key_parts {
        Some(kp) => kp.to_vec(),
        None => {
            // `key_parts` is None if we're creating a "default" index.
            on.desc(&scx.catalog.resolve_full_name(on.name()))?
                .typ()
                .default_key()
                .iter()
                .map(|i| match on_desc.get_unambiguous_name(*i) {
                    Some(n) => Expr::Identifier(vec![n.clone().into()]),
                    _ => Expr::Value(Value::Number((i + 1).to_string())),
                })
                .collect()
        }
    };
    let keys = query::plan_index_exprs(scx, &on_desc, filled_key_parts.clone())?;

    let index_name = if let Some(name) = name {
        QualifiedItemName {
            qualifiers: on.name().qualifiers.clone(),
            item: normalize::ident(name.clone()),
        }
    } else {
        let mut idx_name = QualifiedItemName {
            qualifiers: on.name().qualifiers.clone(),
            item: on.name().item.clone(),
        };
        if key_parts.is_none() {
            // We're trying to create the "default" index.
            idx_name.item += "_primary_idx";
        } else {
            // Use PG schema for automatically naming indexes:
            // `<table>_<_-separated indexed expressions>_idx`
            let index_name_col_suffix = keys
                .iter()
                .map(|k| match k {
                    mz_expr::MirScalarExpr::Column(i) => match on_desc.get_unambiguous_name(*i) {
                        Some(col_name) => col_name.to_string(),
                        None => format!("{}", i + 1),
                    },
                    _ => "expr".to_string(),
                })
                .join("_");
            write!(idx_name.item, "_{index_name_col_suffix}_idx")
                .expect("write on strings cannot fail");
            idx_name.item = normalize::ident(Ident::new(&idx_name.item)?)
        }

        if !*if_not_exists {
            scx.catalog.find_available_name(idx_name)
        } else {
            idx_name
        }
    };

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&index_name);
    let partial_name = PartialItemName::from(full_name.clone());
    // For PostgreSQL compatibility, we need to prevent creating indexes when
    // there is an existing object *or* type of the same name.
    //
    // Technically, we only need to prevent coexistence of indexes and types
    // that have an associated relation (record types but not list/map types).
    // Enforcing that would be more complicated, though. It's backwards
    // compatible to weaken this restriction in the future.
    if let (Ok(item), false, false) = (
        scx.catalog.resolve_item_or_type(&partial_name),
        *if_not_exists,
        scx.pcx().map_or(false, |pcx| pcx.ignore_if_exists_errors),
    ) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    let options = plan_index_options(scx, with_options.clone())?;
    let cluster_id = match in_cluster {
        None => scx.resolve_cluster(None)?.id(),
        Some(in_cluster) => in_cluster.id,
    };

    *in_cluster = Some(ResolvedClusterName {
        id: cluster_id,
        print_name: None,
    });

    // Normalize `stmt`.
    *name = Some(Ident::new(index_name.item.clone())?);
    *key_parts = Some(filled_key_parts);
    let if_not_exists = *if_not_exists;
    if let ResolvedItemName::Item { print_id, .. } = &mut stmt.on_name {
        *print_id = false;
    }
    let create_sql = normalize::create_statement(scx, Statement::CreateIndex(stmt))?;
    let compaction_window = options.iter().find_map(|o| {
        #[allow(irrefutable_let_patterns)]
        if let crate::plan::IndexOption::RetainHistory(lcw) = o {
            Some(lcw.clone())
        } else {
            None
        }
    });

    Ok(Plan::CreateIndex(CreateIndexPlan {
        name: index_name,
        index: Index {
            create_sql,
            on: on.id(),
            keys,
            cluster_id,
            compaction_window,
        },
        if_not_exists,
    }))
}

pub fn describe_create_type(
    _: &StatementContext,
    _: CreateTypeStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_type(
    scx: &StatementContext,
    stmt: CreateTypeStatement<Aug>,
) -> Result<Plan, PlanError> {
    let create_sql = normalize::create_statement(scx, Statement::CreateType(stmt.clone()))?;
    let CreateTypeStatement { name, as_type, .. } = stmt;

    fn validate_data_type(
        scx: &StatementContext,
        data_type: ResolvedDataType,
        as_type: &str,
        key: &str,
    ) -> Result<(GlobalId, Vec<i64>), PlanError> {
        let (id, modifiers) = match data_type {
            ResolvedDataType::Named { id, modifiers, .. } => (id, modifiers),
            _ => sql_bail!(
                "CREATE TYPE ... AS {}option {} can only use named data types, but \
                        found unnamed data type {}. Use CREATE TYPE to create a named type first",
                as_type,
                key,
                data_type.human_readable_name(),
            ),
        };

        let item = scx.catalog.get_item(&id);
        match item.type_details() {
            None => sql_bail!(
                "{} must be of class type, but received {} which is of class {}",
                key,
                scx.catalog.resolve_full_name(item.name()),
                item.item_type()
            ),
            Some(CatalogTypeDetails {
                typ: CatalogType::Char,
                ..
            }) => {
                bail_unsupported!("embedding char type in a list or map")
            }
            _ => {
                // Validate that the modifiers are actually valid.
                scalar_type_from_catalog(scx.catalog, id, &modifiers)?;

                Ok((id, modifiers))
            }
        }
    }

    let inner = match as_type {
        CreateTypeAs::List { options } => {
            let CreateTypeListOptionExtracted {
                element_type,
                seen: _,
            } = CreateTypeListOptionExtracted::try_from(options)?;
            let element_type =
                element_type.ok_or_else(|| sql_err!("ELEMENT TYPE option is required"))?;
            let (id, modifiers) = validate_data_type(scx, element_type, "LIST ", "ELEMENT TYPE")?;
            CatalogType::List {
                element_reference: id,
                element_modifiers: modifiers,
            }
        }
        CreateTypeAs::Map { options } => {
            let CreateTypeMapOptionExtracted {
                key_type,
                value_type,
                seen: _,
            } = CreateTypeMapOptionExtracted::try_from(options)?;
            let key_type = key_type.ok_or_else(|| sql_err!("KEY TYPE option is required"))?;
            let value_type = value_type.ok_or_else(|| sql_err!("VALUE TYPE option is required"))?;
            let (key_id, key_modifiers) = validate_data_type(scx, key_type, "MAP ", "KEY TYPE")?;
            let (value_id, value_modifiers) =
                validate_data_type(scx, value_type, "MAP ", "VALUE TYPE")?;
            CatalogType::Map {
                key_reference: key_id,
                key_modifiers,
                value_reference: value_id,
                value_modifiers,
            }
        }
        CreateTypeAs::Record { column_defs } => {
            let mut fields = vec![];
            for column_def in column_defs {
                let data_type = column_def.data_type;
                let key = ident(column_def.name.clone());
                let (id, modifiers) = validate_data_type(scx, data_type, "", &key)?;
                fields.push(CatalogRecordField {
                    name: ColumnName::from(key.clone()),
                    type_reference: id,
                    type_modifiers: modifiers,
                });
            }
            CatalogType::Record { fields }
        }
    };

    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name)?)?;

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    // For PostgreSQL compatibility, we need to prevent creating types when
    // there is an existing object *or* type of the same name.
    if let Ok(item) = scx.catalog.resolve_item_or_type(&partial_name) {
        if item.item_type().conflicts_with_type() {
            return Err(PlanError::ItemAlreadyExists {
                name: full_name.to_string(),
                item_type: item.item_type(),
            });
        }
    }

    Ok(Plan::CreateType(CreateTypePlan {
        name,
        typ: Type { create_sql, inner },
    }))
}

generate_extracted_config!(CreateTypeListOption, (ElementType, ResolvedDataType));

generate_extracted_config!(
    CreateTypeMapOption,
    (KeyType, ResolvedDataType),
    (ValueType, ResolvedDataType)
);

#[derive(Debug)]
pub enum PlannedAlterRoleOption {
    Attributes(PlannedRoleAttributes),
    Variable(PlannedRoleVariable),
}

#[derive(Debug)]
pub struct PlannedRoleAttributes {
    pub inherit: Option<bool>,
}

fn plan_role_attributes(options: Vec<RoleAttribute>) -> Result<PlannedRoleAttributes, PlanError> {
    let mut planned_attributes = PlannedRoleAttributes { inherit: None };

    for option in options {
        match option {
            RoleAttribute::Login | RoleAttribute::NoLogin => {
                bail_never_supported!("LOGIN attribute", "sql/create-role/#details");
            }
            RoleAttribute::SuperUser | RoleAttribute::NoSuperUser => {
                bail_never_supported!("SUPERUSER attribute", "sql/create-role/#details");
            }
            RoleAttribute::Inherit | RoleAttribute::NoInherit
                if planned_attributes.inherit.is_some() =>
            {
                sql_bail!("conflicting or redundant options");
            }
            RoleAttribute::CreateCluster | RoleAttribute::NoCreateCluster => {
                bail_never_supported!(
                    "CREATECLUSTER attribute",
                    "sql/create-role/#details",
                    "Use system privileges instead."
                );
            }
            RoleAttribute::CreateDB | RoleAttribute::NoCreateDB => {
                bail_never_supported!(
                    "CREATEDB attribute",
                    "sql/create-role/#details",
                    "Use system privileges instead."
                );
            }
            RoleAttribute::CreateRole | RoleAttribute::NoCreateRole => {
                bail_never_supported!(
                    "CREATEROLE attribute",
                    "sql/create-role/#details",
                    "Use system privileges instead."
                );
            }

            RoleAttribute::Inherit => planned_attributes.inherit = Some(true),
            RoleAttribute::NoInherit => planned_attributes.inherit = Some(false),
        }
    }
    if planned_attributes.inherit == Some(false) {
        bail_unsupported!("non inherit roles");
    }

    Ok(planned_attributes)
}

#[derive(Debug)]
pub enum PlannedRoleVariable {
    Set { name: String, value: VariableValue },
    Reset { name: String },
}

impl PlannedRoleVariable {
    pub fn name(&self) -> &str {
        match self {
            PlannedRoleVariable::Set { name, .. } => name,
            PlannedRoleVariable::Reset { name } => name,
        }
    }
}

fn plan_role_variable(variable: SetRoleVar) -> Result<PlannedRoleVariable, PlanError> {
    let plan = match variable {
        SetRoleVar::Set { name, value } => PlannedRoleVariable::Set {
            name: name.to_string(),
            value: scl::plan_set_variable_to(value)?,
        },
        SetRoleVar::Reset { name } => PlannedRoleVariable::Reset {
            name: name.to_string(),
        },
    };
    Ok(plan)
}

pub fn describe_create_role(
    _: &StatementContext,
    _: CreateRoleStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_role(
    _: &StatementContext,
    CreateRoleStatement { name, options }: CreateRoleStatement,
) -> Result<Plan, PlanError> {
    let attributes = plan_role_attributes(options)?;
    Ok(Plan::CreateRole(CreateRolePlan {
        name: normalize::ident(name),
        attributes: attributes.into(),
    }))
}

pub fn describe_create_cluster(
    _: &StatementContext,
    _: CreateClusterStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

// WARNING:
// DO NOT set any `Default` value here using the built-in mechanism of `generate_extracted_config`!
// These options are also used in ALTER CLUSTER, where not giving an option means that the value of
// that option stays the same. If you were to give a default value here, then not giving that option
// to ALTER CLUSTER would always reset the value of that option to the default.
generate_extracted_config!(
    ClusterOption,
    (AvailabilityZones, Vec<String>),
    (Disk, bool),
    (IntrospectionDebugging, bool),
    (IntrospectionInterval, OptionalDuration),
    (Managed, bool),
    (Replicas, Vec<ReplicaDefinition<Aug>>),
    (ReplicationFactor, u32),
    (Size, String),
    (Schedule, ClusterScheduleOptionValue)
);

generate_extracted_config!(
    ClusterFeature,
    (ReoptimizeImportedViews, Option<bool>, Default(None)),
    (EnableEagerDeltaJoins, Option<bool>, Default(None)),
    (EnableNewOuterJoinLowering, Option<bool>, Default(None)),
    (EnableVariadicLeftJoinLowering, Option<bool>, Default(None)),
    (EnableLetrecFixpointAnalysis, Option<bool>, Default(None))
);

pub fn plan_create_cluster(
    scx: &StatementContext,
    CreateClusterStatement {
        name,
        options,
        features,
    }: CreateClusterStatement<Aug>,
) -> Result<Plan, PlanError> {
    let ClusterOptionExtracted {
        availability_zones,
        introspection_debugging,
        introspection_interval,
        managed,
        replicas,
        replication_factor,
        seen: _,
        size,
        disk: disk_in,
        schedule,
    }: ClusterOptionExtracted = options.try_into()?;

    let managed = managed.unwrap_or_else(|| replicas.is_none());

    if !scx.catalog.active_role_id().is_system() {
        if !features.is_empty() {
            sql_bail!("FEATURES not supported for non-system users");
        }
    }

    let schedule = schedule.unwrap_or(ClusterScheduleOptionValue::Manual);

    if managed {
        if replicas.is_some() {
            sql_bail!("REPLICAS not supported for managed clusters");
        }
        let Some(size) = size else {
            sql_bail!("SIZE must be specified for managed clusters");
        };
        if disk_in.is_some() {
            scx.require_feature_flag(&vars::ENABLE_DISK_CLUSTER_REPLICAS)?;
        }

        let compute = plan_compute_replica_config(
            introspection_interval,
            introspection_debugging.unwrap_or(false),
        )?;

        let replication_factor = if matches!(schedule, ClusterScheduleOptionValue::Manual) {
            replication_factor.unwrap_or(1)
        } else {
            scx.require_feature_flag(&ENABLE_CLUSTER_SCHEDULE_REFRESH)?;
            if replication_factor.is_some() {
                sql_bail!("REPLICATION FACTOR cannot be given together with any SCHEDULE other than MANUAL");
            }
            // If we have a non-trivial schedule, then let's not have any replicas initially,
            // to avoid quickly going back and forth if the schedule doesn't want a replica
            // initially.
            0
        };
        let availability_zones = availability_zones.unwrap_or_default();

        if !availability_zones.is_empty() {
            scx.require_feature_flag(&vars::ENABLE_MANAGED_CLUSTER_AVAILABILITY_ZONES)?;
        }

        let disk_default = scx.catalog.system_vars().disk_cluster_replicas_default();
        let mut disk = disk_in.unwrap_or(disk_default);

        // HACK(benesch): disk is always enabled for v2 cluster sizes, and it
        // is an error to specify `DISK = FALSE` or `DISK = TRUE` explicitly.
        //
        // The long term plan is to phase out the v1 cluster sizes, at which
        // point we'll be able to remove the `DISK` option entirely and simply
        // always enable disk.
        if is_cluster_size_v2(&size) {
            if disk_in.is_some() {
                sql_bail!("DISK option not supported for cluster sizes ending in cc or C because disk is always enabled");
            }
            disk = true;
        }

        // Plan OptimizerFeatureOverrides.
        let ClusterFeatureExtracted {
            reoptimize_imported_views,
            enable_eager_delta_joins,
            enable_new_outer_join_lowering,
            enable_variadic_left_join_lowering,
            enable_letrec_fixpoint_analysis,
            seen: _,
        } = ClusterFeatureExtracted::try_from(features)?;
        let optimizer_feature_overrides = OptimizerFeatureOverrides {
            reoptimize_imported_views,
            enable_eager_delta_joins,
            enable_new_outer_join_lowering,
            enable_variadic_left_join_lowering,
            enable_letrec_fixpoint_analysis,
            ..Default::default()
        };

        let schedule = plan_cluster_schedule(schedule)?;

        Ok(Plan::CreateCluster(CreateClusterPlan {
            name: normalize::ident(name),
            variant: CreateClusterVariant::Managed(CreateClusterManagedPlan {
                replication_factor,
                size,
                availability_zones,
                compute,
                disk,
                optimizer_feature_overrides,
                schedule,
            }),
        }))
    } else {
        let Some(replica_defs) = replicas else {
            sql_bail!("REPLICAS must be specified for unmanaged clusters");
        };
        if availability_zones.is_some() {
            sql_bail!("AVAILABILITY ZONES not supported for unmanaged clusters");
        }
        if replication_factor.is_some() {
            sql_bail!("REPLICATION FACTOR not supported for unmanaged clusters");
        }
        if introspection_debugging.is_some() {
            sql_bail!("INTROSPECTION DEBUGGING not supported for unmanaged clusters");
        }
        if introspection_interval.is_some() {
            sql_bail!("INTROSPECTION INTERVAL not supported for unmanaged clusters");
        }
        if size.is_some() {
            sql_bail!("SIZE not supported for unmanaged clusters");
        }
        if disk_in.is_some() {
            sql_bail!("DISK not supported for unmanaged clusters");
        }
        if !features.is_empty() {
            sql_bail!("FEATURES not supported for unmanaged clusters");
        }
        if !matches!(schedule, ClusterScheduleOptionValue::Manual) {
            sql_bail!(
                "cluster schedules other than MANUAL are not supported for unmanaged clusters"
            );
        }

        let mut replicas = vec![];
        for ReplicaDefinition { name, options } in replica_defs {
            replicas.push((normalize::ident(name), plan_replica_config(scx, options)?));
        }

        Ok(Plan::CreateCluster(CreateClusterPlan {
            name: normalize::ident(name),
            variant: CreateClusterVariant::Unmanaged(CreateClusterUnmanagedPlan { replicas }),
        }))
    }
}

generate_extracted_config!(
    ReplicaOption,
    (AvailabilityZone, String),
    (BilledAs, String),
    (ComputeAddresses, Vec<String>),
    (ComputectlAddresses, Vec<String>),
    (Disk, bool),
    (Internal, bool, Default(false)),
    (IntrospectionDebugging, bool, Default(false)),
    (IntrospectionInterval, OptionalDuration),
    (Size, String),
    (StorageAddresses, Vec<String>),
    (StoragectlAddresses, Vec<String>),
    (Workers, u16)
);

fn plan_replica_config(
    scx: &StatementContext,
    options: Vec<ReplicaOption<Aug>>,
) -> Result<ReplicaConfig, PlanError> {
    let ReplicaOptionExtracted {
        availability_zone,
        billed_as,
        compute_addresses,
        computectl_addresses,
        disk: disk_in,
        internal,
        introspection_debugging,
        introspection_interval,
        size,
        storage_addresses,
        storagectl_addresses,
        workers,
        ..
    }: ReplicaOptionExtracted = options.try_into()?;

    let compute = plan_compute_replica_config(introspection_interval, introspection_debugging)?;

    if disk_in.is_some() {
        scx.require_feature_flag(&vars::ENABLE_DISK_CLUSTER_REPLICAS)?;
    }

    match (
        size,
        availability_zone,
        billed_as,
        storagectl_addresses,
        storage_addresses,
        computectl_addresses,
        compute_addresses,
        workers,
    ) {
        // Common cases we expect end users to hit.
        (None, _, None, None, None, None, None, None) => {
            // We don't mention the unmanaged options in the error message
            // because they are only available in unsafe mode.
            sql_bail!("SIZE option must be specified");
        }
        (Some(size), availability_zone, billed_as, None, None, None, None, None) => {
            let disk_default = scx.catalog.system_vars().disk_cluster_replicas_default();
            let mut disk = disk_in.unwrap_or(disk_default);

            // HACK(benesch): disk is always enabled for v2 cluster sizes, and
            // it is an error to specify `DISK = FALSE` or `DISK = TRUE`
            // explicitly.
            //
            // The long term plan is to phase out the v1 cluster sizes, at which
            // point we'll be able to remove the `DISK` option entirely and
            // simply always enable disk.
            if is_cluster_size_v2(&size) {
                if disk_in.is_some() {
                    sql_bail!("DISK option not supported for cluster sizes ending in cc or C because disk is always enabled");
                }
                disk = true;
            }

            Ok(ReplicaConfig::Orchestrated {
                size,
                availability_zone,
                compute,
                disk,
                billed_as,
                internal,
            })
        }

        (
            None,
            None,
            None,
            storagectl_addresses,
            storage_addresses,
            computectl_addresses,
            compute_addresses,
            workers,
        ) => {
            scx.require_feature_flag(&vars::ENABLE_UNORCHESTRATED_CLUSTER_REPLICAS)?;

            // When manually testing Materialize in unsafe mode, it's easy to
            // accidentally omit one of these options, so we try to produce
            // helpful error messages.
            let Some(storagectl_addrs) = storagectl_addresses else {
                sql_bail!("missing STORAGECTL ADDRESSES option");
            };
            let Some(storage_addrs) = storage_addresses else {
                sql_bail!("missing STORAGE ADDRESSES option");
            };
            let Some(computectl_addrs) = computectl_addresses else {
                sql_bail!("missing COMPUTECTL ADDRESSES option");
            };
            let Some(compute_addrs) = compute_addresses else {
                sql_bail!("missing COMPUTE ADDRESSES option");
            };
            let workers = workers.unwrap_or(1);

            if computectl_addrs.len() != compute_addrs.len() {
                sql_bail!("COMPUTECTL ADDRESSES and COMPUTE ADDRESSES must have the same length");
            }
            if storagectl_addrs.len() != storage_addrs.len() {
                sql_bail!("STORAGECTL ADDRESSES and STORAGE ADDRESSES must have the same length");
            }
            if storagectl_addrs.len() != computectl_addrs.len() {
                sql_bail!(
                    "COMPUTECTL ADDRESSES and STORAGECTL ADDRESSES must have the same length"
                );
            }

            if workers == 0 {
                sql_bail!("WORKERS must be greater than 0");
            }

            if disk_in.is_some() {
                sql_bail!("DISK can't be specified for unorchestrated clusters");
            }

            Ok(ReplicaConfig::Unorchestrated {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers: workers.into(),
                compute,
            })
        }
        _ => {
            // We don't bother trying to produce a more helpful error message
            // here because no user is likely to hit this path.
            sql_bail!("invalid mixture of orchestrated and unorchestrated replica options");
        }
    }
}

fn plan_compute_replica_config(
    introspection_interval: Option<OptionalDuration>,
    introspection_debugging: bool,
) -> Result<ComputeReplicaConfig, PlanError> {
    let introspection_interval = introspection_interval
        .map(|OptionalDuration(i)| i)
        .unwrap_or(Some(DEFAULT_REPLICA_LOGGING_INTERVAL));
    let introspection = match introspection_interval {
        Some(interval) => Some(ComputeReplicaIntrospectionConfig {
            interval,
            debugging: introspection_debugging,
        }),
        None if introspection_debugging => {
            sql_bail!("INTROSPECTION DEBUGGING cannot be specified without INTROSPECTION INTERVAL")
        }
        None => None,
    };
    let compute = ComputeReplicaConfig { introspection };
    Ok(compute)
}

fn plan_cluster_schedule(
    schedule: ClusterScheduleOptionValue,
) -> Result<ClusterSchedule, PlanError> {
    Ok(match schedule {
        ClusterScheduleOptionValue::Manual => ClusterSchedule::Manual,
        // If `REHYDRATION TIME ESTIMATE` is not explicitly given, we default to 0.
        ClusterScheduleOptionValue::Refresh {
            rehydration_time_estimate: None,
        } => ClusterSchedule::Refresh {
            rehydration_time_estimate: Duration::from_millis(0),
        },
        // Otherwise we convert the `IntervalValue` to a `Duration`.
        ClusterScheduleOptionValue::Refresh {
            rehydration_time_estimate: Some(interval_value),
        } => {
            let interval = Interval::try_from_value(Value::Interval(interval_value))?;
            if interval.as_microseconds() < 0 {
                sql_bail!(
                    "REHYDRATION TIME ESTIMATE must be non-negative; got: {}",
                    interval
                );
            }
            if interval.months != 0 {
                // This limitation is because we want this interval to be cleanly convertable
                // to a unix epoch timestamp difference. When the interval involves months, then
                // this is not true anymore, because months have variable lengths.
                sql_bail!("REHYDRATION TIME ESTIMATE must not involve units larger than days");
            }
            let duration = interval.duration()?;
            if u64::try_from(duration.as_millis()).is_err() {
                sql_bail!("REHYDRATION TIME ESTIMATE too large");
            }
            ClusterSchedule::Refresh {
                rehydration_time_estimate: duration,
            }
        }
    })
}

pub fn describe_create_cluster_replica(
    _: &StatementContext,
    _: CreateClusterReplicaStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_cluster_replica(
    scx: &StatementContext,
    CreateClusterReplicaStatement {
        definition: ReplicaDefinition { name, options },
        of_cluster,
    }: CreateClusterReplicaStatement<Aug>,
) -> Result<Plan, PlanError> {
    let cluster = scx
        .catalog
        .resolve_cluster(Some(&normalize::ident(of_cluster)))?;
    let current_replica_count = cluster.replica_ids().iter().count();
    if contains_storage_objects(scx, cluster) && current_replica_count > 0 {
        let internal_replica_count = cluster.replicas().iter().filter(|r| r.internal()).count();
        return Err(PlanError::CreateReplicaFailStorageObjects {
            current_replica_count,
            internal_replica_count,
            hypothetical_replica_count: current_replica_count + 1,
        });
    }

    let config = plan_replica_config(scx, options)?;

    if let ReplicaConfig::Orchestrated { internal: true, .. } = &config {
        if MANAGED_REPLICA_PATTERN.is_match(name.as_str()) {
            return Err(PlanError::MangedReplicaName(name.into_string()));
        }
    } else {
        ensure_cluster_is_not_managed(scx, cluster.id())?;
    }

    Ok(Plan::CreateClusterReplica(CreateClusterReplicaPlan {
        name: normalize::ident(name),
        cluster_id: cluster.id(),
        config,
    }))
}

pub fn describe_create_secret(
    _: &StatementContext,
    _: CreateSecretStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_create_secret(
    scx: &StatementContext,
    stmt: CreateSecretStatement<Aug>,
) -> Result<Plan, PlanError> {
    let CreateSecretStatement {
        name,
        if_not_exists,
        value,
    } = &stmt;

    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name.to_owned())?)?;
    let mut create_sql_statement = stmt.clone();
    create_sql_statement.value = Expr::Value(Value::String("********".to_string()));
    let create_sql =
        normalize::create_statement(scx, Statement::CreateSecret(create_sql_statement))?;
    let secret_as = query::plan_secret_as(scx, value.clone())?;

    let secret = Secret {
        create_sql,
        secret_as,
    };

    Ok(Plan::CreateSecret(CreateSecretPlan {
        name,
        secret,
        if_not_exists: *if_not_exists,
    }))
}

pub fn describe_create_connection(
    _: &StatementContext,
    _: CreateConnectionStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(CreateConnectionOption, (Validate, bool));

pub fn plan_create_connection(
    scx: &StatementContext,
    stmt: CreateConnectionStatement<Aug>,
) -> Result<Plan, PlanError> {
    let create_sql = normalize::create_statement(scx, Statement::CreateConnection(stmt.clone()))?;
    let CreateConnectionStatement {
        name,
        connection_type,
        values,
        if_not_exists,
        with_options,
    } = stmt;

    let connection_options_extracted = connection::ConnectionOptionExtracted::try_from(values)?;
    let connection = connection_options_extracted.try_into_connection(scx, connection_type)?;
    if let Connection::Aws(_) = &connection {
        scx.require_feature_flag(&vars::ENABLE_AWS_CONNECTION)?;
    } else if let Connection::MySql(_) = &connection {
        scx.require_feature_flag(&vars::ENABLE_MYSQL_SOURCE)?;
    }
    let name = scx.allocate_qualified_name(normalize::unresolved_item_name(name)?)?;

    let options = CreateConnectionOptionExtracted::try_from(with_options)?;
    if options.validate.is_some() {
        scx.require_feature_flag(&vars::ENABLE_CONNECTION_VALIDATION_SYNTAX)?;
    }
    let validate = match options.validate {
        Some(val) => val,
        None => {
            scx.catalog
                .system_vars()
                .enable_default_connection_validation()
                && connection.validate_by_default()
        }
    };

    // Check for an object in the catalog with this same name
    let full_name = scx.catalog.resolve_full_name(&name);
    let partial_name = PartialItemName::from(full_name.clone());
    if let (false, Ok(item)) = (if_not_exists, scx.catalog.resolve_item(&partial_name)) {
        return Err(PlanError::ItemAlreadyExists {
            name: full_name.to_string(),
            item_type: item.item_type(),
        });
    }

    let plan = CreateConnectionPlan {
        name,
        if_not_exists,
        connection: crate::plan::Connection {
            create_sql,
            connection,
        },
        validate,
    };
    Ok(Plan::CreateConnection(plan))
}

fn plan_drop_database(
    scx: &StatementContext,
    if_exists: bool,
    name: &UnresolvedDatabaseName,
    cascade: bool,
) -> Result<Option<DatabaseId>, PlanError> {
    Ok(match resolve_database(scx, name, if_exists)? {
        Some(database) => {
            if !cascade && database.has_schemas() {
                sql_bail!(
                    "database '{}' cannot be dropped with RESTRICT while it contains schemas",
                    name,
                );
            }
            Some(database.id())
        }
        None => None,
    })
}

pub fn describe_drop_objects(
    _: &StatementContext,
    _: DropObjectsStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_objects(
    scx: &mut StatementContext,
    DropObjectsStatement {
        object_type,
        if_exists,
        names,
        cascade,
    }: DropObjectsStatement,
) -> Result<Plan, PlanError> {
    assert_ne!(
        object_type,
        mz_sql_parser::ast::ObjectType::Func,
        "rejected in parser"
    );
    let object_type = object_type.into();

    let mut referenced_ids = Vec::new();
    for name in names {
        let id = match &name {
            UnresolvedObjectName::Cluster(name) => {
                plan_drop_cluster(scx, if_exists, name, cascade)?.map(ObjectId::Cluster)
            }
            UnresolvedObjectName::ClusterReplica(name) => {
                plan_drop_cluster_replica(scx, if_exists, name)?.map(ObjectId::ClusterReplica)
            }
            UnresolvedObjectName::Database(name) => {
                plan_drop_database(scx, if_exists, name, cascade)?.map(ObjectId::Database)
            }
            UnresolvedObjectName::Schema(name) => {
                plan_drop_schema(scx, if_exists, name, cascade)?.map(ObjectId::Schema)
            }
            UnresolvedObjectName::Role(name) => {
                plan_drop_role(scx, if_exists, name)?.map(ObjectId::Role)
            }
            UnresolvedObjectName::Item(name) => {
                plan_drop_item(scx, object_type, if_exists, name.clone(), cascade)?
                    .map(ObjectId::Item)
            }
        };
        match id {
            Some(id) => referenced_ids.push(id),
            None => scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            }),
        }
    }
    let drop_ids = scx.catalog.object_dependents(&referenced_ids);

    Ok(Plan::DropObjects(DropObjectsPlan {
        referenced_ids,
        drop_ids,
        object_type,
    }))
}

fn plan_drop_schema(
    scx: &StatementContext,
    if_exists: bool,
    name: &UnresolvedSchemaName,
    cascade: bool,
) -> Result<Option<(ResolvedDatabaseSpecifier, SchemaSpecifier)>, PlanError> {
    Ok(match resolve_schema(scx, name.clone(), if_exists)? {
        Some((database_spec, schema_spec)) => {
            if let ResolvedDatabaseSpecifier::Ambient = database_spec {
                sql_bail!(
                    "cannot drop schema {name} because it is required by the database system",
                );
            }
            if let SchemaSpecifier::Temporary = schema_spec {
                sql_bail!("cannot drop schema {name} because it is a temporary schema",)
            }
            let schema = scx.get_schema(&database_spec, &schema_spec);
            if !cascade && schema.has_items() {
                let full_schema_name = scx.catalog.resolve_full_schema_name(schema.name());
                sql_bail!(
                    "schema '{}' cannot be dropped without CASCADE while it contains objects",
                    full_schema_name
                );
            }
            Some((database_spec, schema_spec))
        }
        None => None,
    })
}

fn plan_drop_role(
    scx: &StatementContext,
    if_exists: bool,
    name: &Ident,
) -> Result<Option<RoleId>, PlanError> {
    match scx.catalog.resolve_role(name.as_str()) {
        Ok(role) => {
            let id = role.id();
            if &id == scx.catalog.active_role_id() {
                sql_bail!("current role cannot be dropped");
            }
            for role in scx.catalog.get_roles() {
                for (member_id, grantor_id) in role.membership() {
                    if &id == grantor_id {
                        let member_role = scx.catalog.get_role(member_id);
                        sql_bail!(
                            "cannot drop role {}: still depended up by membership of role {} in role {}",
                            name.as_str(), role.name(), member_role.name()
                        );
                    }
                }
            }
            Ok(Some(role.id()))
        }
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e.into()),
    }
}

fn plan_drop_cluster(
    scx: &StatementContext,
    if_exists: bool,
    name: &Ident,
    cascade: bool,
) -> Result<Option<ClusterId>, PlanError> {
    Ok(match resolve_cluster(scx, name, if_exists)? {
        Some(cluster) => {
            if !cascade && !cluster.bound_objects().is_empty() {
                return Err(PlanError::DependentObjectsStillExist {
                    object_type: "cluster".to_string(),
                    object_name: cluster.name().to_string(),
                    dependents: Vec::new(),
                });
            }
            Some(cluster.id())
        }
        None => None,
    })
}

/// Returns `true` if the cluster has any storage object. Return `false` if the cluster has no
/// objects.
fn contains_storage_objects(scx: &StatementContext, cluster: &dyn CatalogCluster) -> bool {
    cluster.bound_objects().iter().any(|id| {
        matches!(
            scx.catalog.get_item(id).item_type(),
            CatalogItemType::Source | CatalogItemType::Sink
        )
    })
}

fn plan_drop_cluster_replica(
    scx: &StatementContext,
    if_exists: bool,
    name: &QualifiedReplica,
) -> Result<Option<(ClusterId, ReplicaId)>, PlanError> {
    let cluster = resolve_cluster_replica(scx, name, if_exists)?;
    Ok(cluster.map(|(cluster, replica_id)| (cluster.id(), replica_id)))
}

fn plan_drop_item(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    name: UnresolvedItemName,
    cascade: bool,
) -> Result<Option<GlobalId>, PlanError> {
    let resolved = match resolve_item_or_type(scx, object_type, name, if_exists) {
        Ok(r) => r,
        // Return a more helpful error on `DROP VIEW <materialized-view>`.
        Err(PlanError::MismatchedObjectType {
            name,
            is_type: ObjectType::MaterializedView,
            expected_type: ObjectType::View,
        }) => {
            return Err(PlanError::DropViewOnMaterializedView(name.to_string()));
        }
        e => e?,
    };

    Ok(match resolved {
        Some(catalog_item) => {
            if catalog_item.id().is_system() {
                sql_bail!(
                    "cannot drop {} {} because it is required by the database system",
                    catalog_item.item_type(),
                    scx.catalog.minimal_qualification(catalog_item.name()),
                );
            }

            if !cascade {
                for id in catalog_item.used_by() {
                    let dep = scx.catalog.get_item(id);
                    if dependency_prevents_drop(object_type, dep) {
                        return Err(PlanError::DependentObjectsStillExist {
                            object_type: catalog_item.item_type().to_string(),
                            object_name: scx
                                .catalog
                                .minimal_qualification(catalog_item.name())
                                .to_string(),
                            dependents: vec![(
                                dep.item_type().to_string(),
                                scx.catalog.minimal_qualification(dep.name()).to_string(),
                            )],
                        });
                    }
                }
                // TODO(jkosh44) It would be nice to also check if any active subscribe or pending peek
                //  relies on entry. Unfortunately, we don't have that information readily available.
            }
            Some(catalog_item.id())
        }
        None => None,
    })
}

/// Does the dependency `dep` prevent a drop of a non-cascade query?
fn dependency_prevents_drop(object_type: ObjectType, dep: &dyn CatalogItem) -> bool {
    match object_type {
        ObjectType::Type => true,
        _ => match dep.item_type() {
            CatalogItemType::Func
            | CatalogItemType::Table
            | CatalogItemType::Source
            | CatalogItemType::View
            | CatalogItemType::MaterializedView
            | CatalogItemType::Sink
            | CatalogItemType::Type
            | CatalogItemType::Secret
            | CatalogItemType::Connection => true,
            CatalogItemType::Index => false,
        },
    }
}

pub fn describe_alter_index_options(
    _: &StatementContext,
    _: AlterIndexStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn describe_drop_owned(
    _: &StatementContext,
    _: DropOwnedStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_drop_owned(
    scx: &StatementContext,
    DropOwnedStatement {
        role_names,
        cascade,
    }: DropOwnedStatement<Aug>,
) -> Result<Plan, PlanError> {
    let role_ids: BTreeSet<_> = role_names.into_iter().map(|role| role.id).collect();
    let mut drop_ids = Vec::new();
    let mut privilege_revokes = Vec::new();
    let mut default_privilege_revokes = Vec::new();

    fn update_privilege_revokes(
        object_id: SystemObjectId,
        privileges: &PrivilegeMap,
        role_ids: &BTreeSet<RoleId>,
        privilege_revokes: &mut Vec<(SystemObjectId, MzAclItem)>,
    ) {
        privilege_revokes.extend(iter::zip(
            iter::repeat(object_id),
            privileges
                .all_values()
                .filter(|privilege| role_ids.contains(&privilege.grantee))
                .cloned(),
        ));
    }

    // Replicas
    for replica in scx.catalog.get_cluster_replicas() {
        if role_ids.contains(&replica.owner_id()) {
            drop_ids.push((replica.cluster_id(), replica.replica_id()).into());
        }
    }

    // Clusters
    for cluster in scx.catalog.get_clusters() {
        if role_ids.contains(&cluster.owner_id()) {
            // Note: CASCADE is not required for replicas.
            if !cascade {
                let non_owned_bound_objects: Vec<_> = cluster
                    .bound_objects()
                    .into_iter()
                    .map(|global_id| scx.catalog.get_item(global_id))
                    .filter(|item| !role_ids.contains(&item.owner_id()))
                    .collect();
                if !non_owned_bound_objects.is_empty() {
                    let names: Vec<_> = non_owned_bound_objects
                        .into_iter()
                        .map(|item| {
                            (
                                item.item_type().to_string(),
                                scx.catalog.resolve_full_name(item.name()).to_string(),
                            )
                        })
                        .collect();
                    return Err(PlanError::DependentObjectsStillExist {
                        object_type: "cluster".to_string(),
                        object_name: cluster.name().to_string(),
                        dependents: names,
                    });
                }
            }
            drop_ids.push(cluster.id().into());
        }
        update_privilege_revokes(
            SystemObjectId::Object(cluster.id().into()),
            cluster.privileges(),
            &role_ids,
            &mut privilege_revokes,
        );
    }

    // Items
    for item in scx.catalog.get_items() {
        if role_ids.contains(&item.owner_id()) {
            if !cascade {
                // When this item gets dropped it will also drop its progress source, so we need to
                // check the users of those.
                for sub_item in item
                    .progress_id()
                    .iter()
                    .map(|id| scx.catalog.get_item(id))
                    .chain(iter::once(item))
                {
                    let non_owned_dependencies: Vec<_> = sub_item
                        .used_by()
                        .into_iter()
                        .map(|global_id| scx.catalog.get_item(global_id))
                        .filter(|item| dependency_prevents_drop(item.item_type().into(), *item))
                        .filter(|item| !role_ids.contains(&item.owner_id()))
                        .collect();
                    if !non_owned_dependencies.is_empty() {
                        let names: Vec<_> = non_owned_dependencies
                            .into_iter()
                            .map(|item| {
                                (
                                    item.item_type().to_string(),
                                    scx.catalog.resolve_full_name(item.name()).to_string(),
                                )
                            })
                            .collect();
                        return Err(PlanError::DependentObjectsStillExist {
                            object_type: item.item_type().to_string(),
                            object_name: scx
                                .catalog
                                .resolve_full_name(item.name())
                                .to_string()
                                .to_string(),
                            dependents: names,
                        });
                    }
                }
            }
            drop_ids.push(item.id().into());
        }
        update_privilege_revokes(
            SystemObjectId::Object(item.id().into()),
            item.privileges(),
            &role_ids,
            &mut privilege_revokes,
        );
    }

    // Schemas
    for schema in scx.catalog.get_schemas() {
        if !schema.id().is_temporary() {
            if role_ids.contains(&schema.owner_id()) {
                if !cascade {
                    let non_owned_dependencies: Vec<_> = schema
                        .item_ids()
                        .map(|global_id| scx.catalog.get_item(&global_id))
                        .filter(|item| dependency_prevents_drop(item.item_type().into(), *item))
                        .filter(|item| !role_ids.contains(&item.owner_id()))
                        .collect();
                    if !non_owned_dependencies.is_empty() {
                        let full_schema_name = scx.catalog.resolve_full_schema_name(schema.name());
                        sql_bail!(
                            "schema {} cannot be dropped without CASCADE while it contains non-owned objects",
                            full_schema_name.to_string().quoted()
                        );
                    }
                }
                drop_ids.push((*schema.database(), *schema.id()).into())
            }
            update_privilege_revokes(
                SystemObjectId::Object((*schema.database(), *schema.id()).into()),
                schema.privileges(),
                &role_ids,
                &mut privilege_revokes,
            );
        }
    }

    // Databases
    for database in scx.catalog.get_databases() {
        if role_ids.contains(&database.owner_id()) {
            if !cascade {
                let non_owned_schemas: Vec<_> = database
                    .schemas()
                    .into_iter()
                    .filter(|schema| !role_ids.contains(&schema.owner_id()))
                    .collect();
                if !non_owned_schemas.is_empty() {
                    sql_bail!(
                        "database {} cannot be dropped without CASCADE while it contains non-owned schemas",
                        database.name().quoted(),
                    );
                }
            }
            drop_ids.push(database.id().into());
        }
        update_privilege_revokes(
            SystemObjectId::Object(database.id().into()),
            database.privileges(),
            &role_ids,
            &mut privilege_revokes,
        );
    }

    // System
    update_privilege_revokes(
        SystemObjectId::System,
        scx.catalog.get_system_privileges(),
        &role_ids,
        &mut privilege_revokes,
    );

    for (default_privilege_object, default_privilege_acl_items) in
        scx.catalog.get_default_privileges()
    {
        for default_privilege_acl_item in default_privilege_acl_items {
            if role_ids.contains(&default_privilege_object.role_id)
                || role_ids.contains(&default_privilege_acl_item.grantee)
            {
                default_privilege_revokes.push((
                    default_privilege_object.clone(),
                    default_privilege_acl_item.clone(),
                ));
            }
        }
    }

    let drop_ids = scx.catalog.object_dependents(&drop_ids);

    let system_ids: Vec<_> = drop_ids.iter().filter(|id| id.is_system()).collect();
    if !system_ids.is_empty() {
        let mut owners = system_ids
            .into_iter()
            .filter_map(|object_id| scx.catalog.get_owner_id(object_id))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|role_id| scx.catalog.get_role(&role_id).name().quoted());
        sql_bail!(
            "cannot drop objects owned by role {} because they are required by the database system",
            owners.join(", "),
        );
    }

    Ok(Plan::DropOwned(DropOwnedPlan {
        role_ids: role_ids.into_iter().collect(),
        drop_ids,
        privilege_revokes,
        default_privilege_revokes,
    }))
}

fn plan_retain_history_option(
    scx: &StatementContext,
    retain_history: Option<OptionalDuration>,
) -> Result<Option<CompactionWindow>, PlanError> {
    if let Some(OptionalDuration(lcw)) = retain_history {
        Ok(Some(plan_retain_history(scx, lcw)?))
    } else {
        Ok(None)
    }
}

// Convert a specified RETAIN HISTORY option into a compaction window. `None` corresponds to
// `DisableCompaction`. A zero duration will error. This is because the `OptionalDuration` type
// already converts the zero duration into `None`. This function must not be called in the `RESET
// (RETAIN HISTORY)` path, which should be handled by the outer `Option<OptionalDuration>` being
// `None`.
fn plan_retain_history(
    scx: &StatementContext,
    lcw: Option<Duration>,
) -> Result<CompactionWindow, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_LOGICAL_COMPACTION_WINDOW)?;
    match lcw {
        // A zero duration has already been converted to `None` by `OptionalDuration` (and means
        // disable compaction), and should never occur here. Furthermore, some things actually do
        // break when this is set to real zero:
        // https://github.com/MaterializeInc/materialize/issues/13221.
        Some(Duration::ZERO) => Err(PlanError::InvalidOptionValue {
            option_name: "RETAIN HISTORY".to_string(),
            err: Box::new(PlanError::Unstructured(
                "internal error: unexpectedly zero".to_string(),
            )),
        }),
        Some(duration) => Ok(duration.try_into()?),
        None => Ok(CompactionWindow::DisableCompaction),
    }
}

generate_extracted_config!(IndexOption, (RetainHistory, OptionalDuration));

fn plan_index_options(
    scx: &StatementContext,
    with_opts: Vec<IndexOption<Aug>>,
) -> Result<Vec<crate::plan::IndexOption>, PlanError> {
    if !with_opts.is_empty() {
        // Index options are not durable.
        scx.require_feature_flag(&vars::ENABLE_INDEX_OPTIONS)?;
    }

    let IndexOptionExtracted { retain_history, .. }: IndexOptionExtracted = with_opts.try_into()?;

    let mut out = Vec::with_capacity(1);
    if let Some(cw) = plan_retain_history_option(scx, retain_history)? {
        out.push(crate::plan::IndexOption::RetainHistory(cw));
    }
    Ok(out)
}

generate_extracted_config!(
    TableOption,
    (RetainHistory, OptionalDuration),
    (RedactedTest, String)
);

fn plan_table_options(
    scx: &StatementContext,
    with_opts: Vec<TableOption<Aug>>,
) -> Result<Vec<crate::plan::TableOption>, PlanError> {
    let TableOptionExtracted {
        retain_history,
        redacted_test,
        ..
    }: TableOptionExtracted = with_opts.try_into()?;

    if redacted_test.is_some() {
        scx.require_feature_flag(&vars::ENABLE_REDACTED_TEST_OPTION)?;
    }

    let mut out = Vec::with_capacity(1);
    if let Some(cw) = plan_retain_history_option(scx, retain_history)? {
        out.push(crate::plan::TableOption::RetainHistory(cw));
    }
    Ok(out)
}

pub fn plan_alter_index_options(
    scx: &mut StatementContext,
    AlterIndexStatement {
        index_name,
        if_exists,
        action,
    }: AlterIndexStatement<Aug>,
) -> Result<Plan, PlanError> {
    let object_type = ObjectType::Index;
    match action {
        AlterIndexAction::ResetOptions(options) => {
            let mut options = options.into_iter();
            if let Some(opt) = options.next() {
                match opt {
                    IndexOptionName::RetainHistory => {
                        if options.next().is_some() {
                            sql_bail!("RETAIN HISTORY must be only option");
                        }
                        return alter_retain_history(
                            scx,
                            object_type,
                            if_exists,
                            UnresolvedObjectName::Item(index_name),
                            None,
                        );
                    }
                }
            }
            sql_bail!("expected option");
        }
        AlterIndexAction::SetOptions(options) => {
            let mut options = options.into_iter();
            if let Some(opt) = options.next() {
                match opt.name {
                    IndexOptionName::RetainHistory => {
                        if options.next().is_some() {
                            sql_bail!("RETAIN HISTORY must be only option");
                        }
                        return alter_retain_history(
                            scx,
                            object_type,
                            if_exists,
                            UnresolvedObjectName::Item(index_name),
                            opt.value,
                        );
                    }
                }
            }
            sql_bail!("expected option");
        }
    }
}

pub fn describe_alter_cluster_set_options(
    _: &StatementContext,
    _: AlterClusterStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_cluster(
    scx: &mut StatementContext,
    AlterClusterStatement {
        name,
        action,
        if_exists,
    }: AlterClusterStatement<Aug>,
) -> Result<Plan, PlanError> {
    let cluster = match resolve_cluster(scx, &name, if_exists)? {
        Some(entry) => entry,
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type: ObjectType::Cluster,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Cluster,
            }));
        }
    };

    let mut options: PlanClusterOption = Default::default();

    match action {
        AlterClusterAction::SetOptions(set_options) => {
            let ClusterOptionExtracted {
                availability_zones,
                introspection_debugging,
                introspection_interval,
                managed,
                replicas: replica_defs,
                replication_factor,
                seen: _,
                size,
                disk,
                schedule,
            }: ClusterOptionExtracted = set_options.try_into()?;

            match managed.unwrap_or_else(|| cluster.is_managed()) {
                true => {
                    if replica_defs.is_some() {
                        sql_bail!("REPLICAS not supported for managed clusters");
                    }
                    if schedule.is_some()
                        && !matches!(schedule, Some(ClusterScheduleOptionValue::Manual))
                    {
                        scx.require_feature_flag(&ENABLE_CLUSTER_SCHEDULE_REFRESH)?;
                    }

                    if let Some(replication_factor) = replication_factor {
                        if schedule.is_some()
                            && !matches!(schedule, Some(ClusterScheduleOptionValue::Manual))
                        {
                            sql_bail!("REPLICATION FACTOR cannot be given together with any SCHEDULE other than MANUAL");
                        }
                        if let Some(current_schedule) = cluster.schedule() {
                            if !matches!(current_schedule, ClusterSchedule::Manual) {
                                sql_bail!("REPLICATION FACTOR cannot be set if the cluster SCHEDULE is anything other than MANUAL");
                            }
                        }

                        let internal_replica_count =
                            cluster.replicas().iter().filter(|r| r.internal()).count();
                        let hypothetical_replica_count =
                            internal_replica_count + usize::cast_from(replication_factor);

                        // Total number of replicas running is internal replicas
                        // + replication factor.
                        if contains_storage_objects(scx, cluster) && hypothetical_replica_count > 1
                        {
                            return Err(PlanError::CreateReplicaFailStorageObjects {
                                current_replica_count: cluster.replica_ids().iter().count(),
                                internal_replica_count,
                                hypothetical_replica_count,
                            });
                        }
                    }
                }
                false => {
                    if availability_zones.is_some() {
                        sql_bail!("AVAILABILITY ZONES not supported for unmanaged clusters");
                    }
                    if replication_factor.is_some() {
                        sql_bail!("REPLICATION FACTOR not supported for unmanaged clusters");
                    }
                    if introspection_debugging.is_some() {
                        sql_bail!("INTROSPECTION DEBUGGING not supported for unmanaged clusters");
                    }
                    if introspection_interval.is_some() {
                        sql_bail!("INTROSPECTION INTERVAL not supported for unmanaged clusters");
                    }
                    if size.is_some() {
                        sql_bail!("SIZE not supported for unmanaged clusters");
                    }
                    if disk.is_some() {
                        sql_bail!("DISK not supported for unmanaged clusters");
                    }
                    if schedule.is_some()
                        && !matches!(schedule, Some(ClusterScheduleOptionValue::Manual))
                    {
                        sql_bail!("cluster schedules other than MANUAL are not supported for unmanaged clusters");
                    }
                    if let Some(current_schedule) = cluster.schedule() {
                        if !matches!(current_schedule, ClusterSchedule::Manual)
                            && schedule.is_none()
                        {
                            sql_bail!(
                                "when switching a cluster to unmanaged, if the managed \
                                cluster's SCHEDULE is anything other than MANUAL, you have to \
                                explicitly set the SCHEDULE to MANUAL"
                            );
                        }
                    }
                }
            }

            let mut replicas = vec![];
            for ReplicaDefinition { name, options } in
                replica_defs.into_iter().flat_map(Vec::into_iter)
            {
                replicas.push((normalize::ident(name), plan_replica_config(scx, options)?));
            }

            if let Some(managed) = managed {
                options.managed = AlterOptionParameter::Set(managed);
            }
            if let Some(replication_factor) = replication_factor {
                options.replication_factor = AlterOptionParameter::Set(replication_factor);
            }
            if let Some(size) = &size {
                // HACK(benesch): disk is always enabled for v2 cluster sizes,
                // and it is an error to specify `DISK = FALSE` or `DISK = TRUE`
                // explicitly.
                //
                // The long term plan is to phase out the v1 cluster sizes, at
                // which point we'll be able to remove the `DISK` option
                // entirely and simply always enable disk.
                if is_cluster_size_v2(size) {
                    if disk.is_some() {
                        sql_bail!("DISK option not supported for cluster sizes ending in cc or C because disk is always enabled");
                    } else {
                        options.disk = AlterOptionParameter::Set(true);
                    }
                }
                options.size = AlterOptionParameter::Set(size.clone());
            }
            if let Some(availability_zones) = availability_zones {
                options.availability_zones = AlterOptionParameter::Set(availability_zones);
            }
            if let Some(introspection_debugging) = introspection_debugging {
                options.introspection_debugging =
                    AlterOptionParameter::Set(introspection_debugging);
            }
            if let Some(introspection_interval) = introspection_interval {
                options.introspection_interval = AlterOptionParameter::Set(introspection_interval);
            }
            if let Some(disk) = disk {
                // HACK(benesch): disk is always enabled for v2 cluster sizes,
                // and it is an error to specify `DISK = FALSE` or `DISK = TRUE`
                // explicitly.
                //
                // The long term plan is to phase out the v1 cluster sizes, at
                // which point we'll be able to remove the `DISK` option
                // entirely and simply always enable disk.
                let size = size.as_deref().unwrap_or_else(|| {
                    cluster.managed_size().expect("cluster known to be managed")
                });
                if is_cluster_size_v2(size) {
                    sql_bail!("DISK option not supported for cluster sizes ending in cc or C because disk is always enabled");
                }

                if disk {
                    scx.require_feature_flag(&vars::ENABLE_DISK_CLUSTER_REPLICAS)?;
                }
                options.disk = AlterOptionParameter::Set(disk);
            }
            if !replicas.is_empty() {
                options.replicas = AlterOptionParameter::Set(replicas);
            }
            if let Some(schedule) = schedule {
                options.schedule = AlterOptionParameter::Set(plan_cluster_schedule(schedule)?);
            }
        }
        AlterClusterAction::ResetOptions(reset_options) => {
            use AlterOptionParameter::Reset;
            use ClusterOptionName::*;
            for option in reset_options {
                match option {
                    AvailabilityZones => options.availability_zones = Reset,
                    Disk => options.disk = Reset,
                    IntrospectionInterval => options.introspection_interval = Reset,
                    IntrospectionDebugging => options.introspection_debugging = Reset,
                    Managed => options.managed = Reset,
                    Replicas => options.replicas = Reset,
                    ReplicationFactor => options.replication_factor = Reset,
                    Size => options.size = Reset,
                    Schedule => options.schedule = Reset,
                }
            }
        }
    }
    Ok(Plan::AlterCluster(AlterClusterPlan {
        id: cluster.id(),
        name: cluster.name().to_string(),
        options,
    }))
}

pub fn describe_alter_set_cluster(
    _: &StatementContext,
    _: AlterSetClusterStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_item_set_cluster(
    scx: &StatementContext,
    AlterSetClusterStatement {
        if_exists,
        set_cluster: in_cluster_name,
        name,
        object_type,
    }: AlterSetClusterStatement<Aug>,
) -> Result<Plan, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_ALTER_SET_CLUSTER)?;

    let object_type = object_type.into();

    // Prevent access to `SET CLUSTER` for unsupported objects.
    match object_type {
        ObjectType::MaterializedView => {}
        ObjectType::Index | ObjectType::Sink | ObjectType::Source => {
            bail_unsupported!(20841, format!("ALTER {object_type} SET CLUSTER"))
        }
        _ => {
            bail_never_supported!(
                format!("ALTER {object_type} SET CLUSTER"),
                "sql/alter-set-cluster/",
                format!("{object_type} has no associated cluster")
            )
        }
    }

    let in_cluster = scx.catalog.get_cluster(in_cluster_name.id);

    match resolve_item_or_type(scx, object_type, name.clone(), if_exists)? {
        Some(entry) => {
            let current_cluster = entry.cluster_id();
            let Some(current_cluster) = current_cluster else {
                sql_bail!("No cluster associated with {name}");
            };

            if current_cluster == in_cluster.id() {
                Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
            } else {
                Ok(Plan::AlterSetCluster(AlterSetClusterPlan {
                    id: entry.id(),
                    set_cluster: in_cluster.id(),
                }))
            }
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
    }
}

pub fn describe_alter_object_rename(
    _: &StatementContext,
    _: AlterObjectRenameStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_object_rename(
    scx: &mut StatementContext,
    AlterObjectRenameStatement {
        name,
        object_type,
        to_item_name,
        if_exists,
    }: AlterObjectRenameStatement,
) -> Result<Plan, PlanError> {
    let object_type = object_type.into();
    match (object_type, name) {
        (
            ObjectType::View
            | ObjectType::MaterializedView
            | ObjectType::Table
            | ObjectType::Source
            | ObjectType::Index
            | ObjectType::Sink
            | ObjectType::Secret
            | ObjectType::Connection,
            UnresolvedObjectName::Item(name),
        ) => plan_alter_item_rename(scx, object_type, name, to_item_name, if_exists),
        (ObjectType::Cluster, UnresolvedObjectName::Cluster(name)) => {
            plan_alter_cluster_rename(scx, object_type, name, to_item_name, if_exists)
        }
        (ObjectType::ClusterReplica, UnresolvedObjectName::ClusterReplica(name)) => {
            plan_alter_cluster_replica_rename(scx, object_type, name, to_item_name, if_exists)
        }
        (ObjectType::Schema, UnresolvedObjectName::Schema(name)) => {
            plan_alter_schema_rename(scx, name, to_item_name, if_exists)
        }
        (object_type, name) => {
            unreachable!("parser set the wrong object type '{object_type:?}' for name {name:?}")
        }
    }
}

pub fn plan_alter_schema_rename(
    scx: &mut StatementContext,
    name: UnresolvedSchemaName,
    to_schema_name: Ident,
    if_exists: bool,
) -> Result<Plan, PlanError> {
    let Some((db_spec, schema_spec)) = resolve_schema(scx, name.clone(), if_exists)? else {
        let object_type = ObjectType::Schema;
        scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
            name: name.to_ast_string(),
            object_type,
        });
        return Ok(Plan::AlterNoop(AlterNoopPlan { object_type }));
    };

    // Make sure the name is unique.
    if scx
        .resolve_schema_in_database(&db_spec, &to_schema_name)
        .is_ok()
    {
        return Err(PlanError::Catalog(CatalogError::SchemaAlreadyExists(
            to_schema_name.clone().into_string(),
        )));
    }

    // Prevent users from renaming system related schemas.
    let schema = scx.catalog.get_schema(&db_spec, &schema_spec);
    if schema.id().is_system() {
        bail_never_supported!(format!("renaming the {} schema", schema.name().schema))
    }

    Ok(Plan::AlterSchemaRename(AlterSchemaRenamePlan {
        cur_schema_spec: (db_spec, schema_spec),
        new_schema_name: to_schema_name.into_string(),
    }))
}

pub fn plan_alter_schema_swap<F>(
    scx: &mut StatementContext,
    name_a: UnresolvedSchemaName,
    name_b: Ident,
    gen_temp_suffix: F,
) -> Result<Plan, PlanError>
where
    F: Fn(&dyn Fn(&str) -> bool) -> Result<String, PlanError>,
{
    let schema_a = scx.resolve_schema(name_a.clone())?;

    let db_spec = schema_a.database().clone();
    if matches!(db_spec, ResolvedDatabaseSpecifier::Ambient) {
        sql_bail!("cannot swap schemas that are in the ambient database");
    };
    let schema_b = scx.resolve_schema_in_database(&db_spec, &name_b)?;

    // We cannot swap system schemas.
    if schema_a.id().is_system() || schema_b.id().is_system() {
        bail_never_supported!("swapping a system schema".to_string())
    }

    // Generate a temporary name we can swap schema_a to.
    //
    // 'check' returns if the temp schema name would be valid.
    let check = |temp_suffix: &str| {
        let mut temp_name = ident!("mz_schema_swap_");
        temp_name.append_lossy(temp_suffix);
        scx.resolve_schema_in_database(&db_spec, &temp_name)
            .is_err()
    };
    let temp_suffix = gen_temp_suffix(&check)?;
    let name_temp = format!("mz_schema_swap_{temp_suffix}");

    Ok(Plan::AlterSchemaSwap(AlterSchemaSwapPlan {
        schema_a_spec: (*schema_a.database(), *schema_a.id()),
        schema_a_name: schema_a.name().schema.to_string(),
        schema_b_spec: (*schema_b.database(), *schema_b.id()),
        schema_b_name: schema_b.name().schema.to_string(),
        name_temp,
    }))
}

pub fn plan_alter_item_rename(
    scx: &mut StatementContext,
    object_type: ObjectType,
    name: UnresolvedItemName,
    to_item_name: Ident,
    if_exists: bool,
) -> Result<Plan, PlanError> {
    let resolved = match resolve_item_or_type(scx, object_type, name.clone(), if_exists) {
        Ok(r) => r,
        // Return a more helpful error on `DROP VIEW <materialized-view>`.
        Err(PlanError::MismatchedObjectType {
            name,
            is_type: ObjectType::MaterializedView,
            expected_type: ObjectType::View,
        }) => {
            return Err(PlanError::AlterViewOnMaterializedView(name.to_string()));
        }
        e => e?,
    };

    match resolved {
        Some(entry) => {
            let full_name = scx.catalog.resolve_full_name(entry.name());
            let item_type = entry.item_type();

            let proposed_name = QualifiedItemName {
                qualifiers: entry.name().qualifiers.clone(),
                item: to_item_name.clone().into_string(),
            };

            // For PostgreSQL compatibility, items and types cannot have
            // overlapping names in a variety of situations. See the comment on
            // `CatalogItemType::conflicts_with_type` for details.
            let conflicting_type_exists;
            let conflicting_item_exists;
            if item_type == CatalogItemType::Type {
                conflicting_type_exists = scx.catalog.get_type_by_name(&proposed_name).is_some();
                conflicting_item_exists = scx
                    .catalog
                    .get_item_by_name(&proposed_name)
                    .map(|item| item.item_type().conflicts_with_type())
                    .unwrap_or(false);
            } else {
                conflicting_type_exists = item_type.conflicts_with_type()
                    && scx.catalog.get_type_by_name(&proposed_name).is_some();
                conflicting_item_exists = scx.catalog.get_item_by_name(&proposed_name).is_some();
            };
            if conflicting_type_exists || conflicting_item_exists {
                sql_bail!("catalog item '{}' already exists", to_item_name);
            }

            Ok(Plan::AlterItemRename(AlterItemRenamePlan {
                id: entry.id(),
                current_full_name: full_name,
                to_name: normalize::ident(to_item_name),
                object_type,
            }))
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
    }
}

pub fn plan_alter_cluster_rename(
    scx: &mut StatementContext,
    object_type: ObjectType,
    name: Ident,
    to_name: Ident,
    if_exists: bool,
) -> Result<Plan, PlanError> {
    match resolve_cluster(scx, &name, if_exists)? {
        Some(entry) => Ok(Plan::AlterClusterRename(AlterClusterRenamePlan {
            id: entry.id(),
            name: entry.name().to_string(),
            to_name: ident(to_name),
        })),
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
    }
}

pub fn plan_alter_cluster_swap<F>(
    scx: &mut StatementContext,
    name_a: Ident,
    name_b: Ident,
    gen_temp_suffix: F,
) -> Result<Plan, PlanError>
where
    F: Fn(&dyn Fn(&str) -> bool) -> Result<String, PlanError>,
{
    let cluster_a = scx.resolve_cluster(Some(&name_a))?;
    let cluster_b = scx.resolve_cluster(Some(&name_b))?;

    let check = |temp_suffix: &str| {
        let mut temp_name = ident!("mz_schema_swap_");
        temp_name.append_lossy(temp_suffix);
        match scx.catalog.resolve_cluster(Some(temp_name.as_str())) {
            // Temp name does not exist, so we can use it.
            Err(CatalogError::UnknownCluster(_)) => true,
            // Temp name already exists!
            Ok(_) | Err(_) => false,
        }
    };
    let temp_suffix = gen_temp_suffix(&check)?;
    let name_temp = format!("mz_cluster_swap_{temp_suffix}");

    Ok(Plan::AlterClusterSwap(AlterClusterSwapPlan {
        id_a: cluster_a.id(),
        id_b: cluster_b.id(),
        name_a: name_a.into_string(),
        name_b: name_b.into_string(),
        name_temp,
    }))
}

pub fn plan_alter_cluster_replica_rename(
    scx: &mut StatementContext,
    object_type: ObjectType,
    name: QualifiedReplica,
    to_item_name: Ident,
    if_exists: bool,
) -> Result<Plan, PlanError> {
    match resolve_cluster_replica(scx, &name, if_exists)? {
        Some((cluster, replica)) => {
            ensure_cluster_is_not_managed(scx, cluster.id())?;
            Ok(Plan::AlterClusterReplicaRename(
                AlterClusterReplicaRenamePlan {
                    cluster_id: cluster.id(),
                    replica_id: replica,
                    name: QualifiedReplica {
                        cluster: Ident::new(cluster.name())?,
                        replica: name.replica,
                    },
                    to_name: normalize::ident(to_item_name),
                },
            ))
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
    }
}

pub fn describe_alter_object_swap(
    _: &StatementContext,
    _: AlterObjectSwapStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_object_swap(
    scx: &mut StatementContext,
    stmt: AlterObjectSwapStatement,
) -> Result<Plan, PlanError> {
    scx.require_feature_flag(&vars::ENABLE_ALTER_SWAP)?;

    let AlterObjectSwapStatement {
        object_type,
        name_a,
        name_b,
    } = stmt;
    let object_type = object_type.into();

    // We'll try 10 times to generate a temporary suffix.
    let gen_temp_suffix = |check_fn: &dyn Fn(&str) -> bool| {
        let mut attempts = 0;
        let name_temp = loop {
            attempts += 1;
            if attempts > 10 {
                tracing::warn!("Unable to generate temp id for swapping");
                sql_bail!("unable to swap!");
            }

            // Call the provided closure to make sure this name is unique!
            let short_id = mz_ore::id_gen::temp_id();
            if check_fn(&short_id) {
                break short_id;
            }
        };

        Ok(name_temp)
    };

    match (object_type, name_a, name_b) {
        (ObjectType::Schema, UnresolvedObjectName::Schema(name_a), name_b) => {
            plan_alter_schema_swap(scx, name_a, name_b, gen_temp_suffix)
        }
        (ObjectType::Cluster, UnresolvedObjectName::Cluster(name_a), name_b) => {
            plan_alter_cluster_swap(scx, name_a, name_b, gen_temp_suffix)
        }
        (object_type, _, _) => Err(PlanError::Unsupported {
            feature: format!("ALTER {object_type} .. SWAP WITH ..."),
            issue_no: Some(12972),
        }),
    }
}

pub fn describe_alter_retain_history(
    _: &StatementContext,
    _: AlterRetainHistoryStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_retain_history(
    scx: &StatementContext,
    AlterRetainHistoryStatement {
        object_type,
        if_exists,
        name,
        history,
    }: AlterRetainHistoryStatement<Aug>,
) -> Result<Plan, PlanError> {
    alter_retain_history(scx, object_type.into(), if_exists, name, history)
}

fn alter_retain_history(
    scx: &StatementContext,
    object_type: ObjectType,
    if_exists: bool,
    name: UnresolvedObjectName,
    history: Option<WithOptionValue<Aug>>,
) -> Result<Plan, PlanError> {
    let name = match (object_type, name) {
        (
            // View gets a special error below.
            ObjectType::View
            | ObjectType::MaterializedView
            | ObjectType::Table
            | ObjectType::Source
            | ObjectType::Index,
            UnresolvedObjectName::Item(name),
        ) => name,
        (object_type, _) => {
            sql_bail!("{object_type} does not support RETAIN HISTORY")
        }
    };
    match resolve_item_or_type(scx, object_type, name.clone(), if_exists)? {
        Some(entry) => {
            let full_name = scx.catalog.resolve_full_name(entry.name());
            let item_type = entry.item_type();

            // Return a more helpful error on `ALTER VIEW <materialized-view>`.
            if object_type == ObjectType::View && item_type == CatalogItemType::MaterializedView {
                return Err(PlanError::AlterViewOnMaterializedView(
                    full_name.to_string(),
                ));
            } else if object_type == ObjectType::View {
                sql_bail!("{object_type} does not support RETAIN HISTORY")
            } else if object_type != item_type {
                sql_bail!(
                    "\"{}\" is a {} not a {}",
                    full_name,
                    entry.item_type(),
                    format!("{object_type}").to_lowercase()
                )
            }

            // Save the original value so we can write it back down in the create_sql catalog item.
            let (value, lcw) = match &history {
                Some(WithOptionValue::RetainHistoryFor(value)) => {
                    let window = OptionalDuration::try_from_value(value.clone())?;
                    (Some(value.clone()), window.0)
                }
                // None is RESET, so use the default CW.
                None => (None, Some(DEFAULT_LOGICAL_COMPACTION_WINDOW_DURATION)),
                _ => sql_bail!("unexpected value type for RETAIN HISTORY"),
            };
            let window = plan_retain_history(scx, lcw)?;

            Ok(Plan::AlterRetainHistory(AlterRetainHistoryPlan {
                id: entry.id(),
                value,
                window,
                object_type,
            }))
        }
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_ast_string(),
                object_type,
            });

            Ok(Plan::AlterNoop(AlterNoopPlan { object_type }))
        }
    }
}

pub fn describe_alter_secret_options(
    _: &StatementContext,
    _: AlterSecretStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_secret(
    scx: &mut StatementContext,
    stmt: AlterSecretStatement<Aug>,
) -> Result<Plan, PlanError> {
    let AlterSecretStatement {
        name,
        if_exists,
        value,
    } = stmt;
    let object_type = ObjectType::Secret;
    let id = match resolve_item_or_type(scx, object_type, name.clone(), if_exists)? {
        Some(entry) => entry.id(),
        None => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: name.to_string(),
                object_type,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan { object_type }));
        }
    };

    let secret_as = query::plan_secret_as(scx, value)?;

    Ok(Plan::AlterSecret(AlterSecretPlan { id, secret_as }))
}

pub fn describe_alter_connection(
    _: &StatementContext,
    _: AlterConnectionStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(AlterConnectionOption, (Validate, bool));

pub fn plan_alter_connection(
    scx: &StatementContext,
    stmt: AlterConnectionStatement<Aug>,
) -> Result<Plan, PlanError> {
    let AlterConnectionStatement {
        name,
        if_exists,
        actions,
        with_options,
    } = stmt;
    let conn_name = normalize::unresolved_item_name(name)?;
    let entry = match scx.catalog.resolve_item(&conn_name) {
        Ok(entry) => entry,
        Err(_) if if_exists => {
            scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
                name: conn_name.to_string(),
                object_type: ObjectType::Sink,
            });

            return Ok(Plan::AlterNoop(AlterNoopPlan {
                object_type: ObjectType::Connection,
            }));
        }
        Err(e) => return Err(e.into()),
    };

    let connection = entry.connection()?;

    if actions
        .iter()
        .any(|action| matches!(action, AlterConnectionAction::RotateKeys))
    {
        if actions.len() > 1 {
            sql_bail!("cannot specify any other actions alongside ALTER CONNECTION...ROTATE KEYS");
        }

        if !with_options.is_empty() {
            sql_bail!(
                "ALTER CONNECTION...ROTATE KEYS does not support WITH ({})",
                with_options.iter().map(|o| o.to_ast_string()).join(", ")
            );
        }

        if !matches!(connection, Connection::Ssh(_)) {
            sql_bail!(
                "{} is not an SSH connection",
                scx.catalog.resolve_full_name(entry.name())
            )
        }

        return Ok(Plan::AlterConnection(AlterConnectionPlan {
            id: entry.id(),
            action: crate::plan::AlterConnectionAction::RotateKeys,
        }));
    }

    let options = AlterConnectionOptionExtracted::try_from(with_options)?;
    if options.validate.is_some() {
        scx.require_feature_flag(&vars::ENABLE_CONNECTION_VALIDATION_SYNTAX)?;
    }

    let validate = match options.validate {
        Some(val) => val,
        None => {
            scx.catalog
                .system_vars()
                .enable_default_connection_validation()
                && connection.validate_by_default()
        }
    };

    let connection_type = match connection {
        Connection::Aws(_) => CreateConnectionType::Aws,
        Connection::AwsPrivatelink(_) => CreateConnectionType::AwsPrivatelink,
        Connection::Kafka(_) => CreateConnectionType::Kafka,
        Connection::Csr(_) => CreateConnectionType::Csr,
        Connection::Postgres(_) => CreateConnectionType::Postgres,
        Connection::Ssh(_) => CreateConnectionType::Ssh,
        Connection::MySql(_) => CreateConnectionType::MySql,
    };

    // Collect all options irrespective of action taken on them.
    let specified_options: BTreeSet<_> = actions
        .iter()
        .map(|action: &AlterConnectionAction<Aug>| match action {
            AlterConnectionAction::SetOption(option) => option.name.clone(),
            AlterConnectionAction::DropOption(name) => name.clone(),
            AlterConnectionAction::RotateKeys => unreachable!(),
        })
        .collect();

    for invalid in INALTERABLE_OPTIONS {
        if specified_options.contains(invalid) {
            sql_bail!("cannot ALTER {} option {}", connection_type, invalid);
        }
    }

    connection::validate_options_per_connection_type(connection_type, specified_options)?;

    // Partition operations into set and drop
    let (set_options_vec, mut drop_options): (Vec<_>, BTreeSet<_>) =
        actions.into_iter().partition_map(|action| match action {
            AlterConnectionAction::SetOption(option) => Either::Left(option),
            AlterConnectionAction::DropOption(name) => Either::Right(name),
            AlterConnectionAction::RotateKeys => unreachable!(),
        });

    let set_options: BTreeMap<_, _> = set_options_vec
        .clone()
        .into_iter()
        .map(|option| (option.name, option.value))
        .collect();

    // Type check values + avoid duplicates; we don't want to e.g. let users
    // drop and set the same option in the same statement, so treating drops as
    // sets here is fine.
    let connection_options_extracted =
        connection::ConnectionOptionExtracted::try_from(set_options_vec)?;

    let duplicates: Vec<_> = connection_options_extracted
        .seen
        .intersection(&drop_options)
        .collect();

    if !duplicates.is_empty() {
        sql_bail!(
            "cannot both SET and DROP/RESET options {}",
            duplicates
                .iter()
                .map(|option| option.to_string())
                .join(", ")
        )
    }

    for mutually_exclusive_options in MUTUALLY_EXCLUSIVE_SETS {
        let set_options_count = mutually_exclusive_options
            .iter()
            .filter(|o| set_options.contains_key(o))
            .count();
        let drop_options_count = mutually_exclusive_options
            .iter()
            .filter(|o| drop_options.contains(o))
            .count();

        // Disallow setting _and_ resetting mutually exclusive options
        if set_options_count > 0 && drop_options_count > 0 {
            sql_bail!(
                "cannot both SET and DROP/RESET mutually exclusive {} options {}",
                connection_type,
                mutually_exclusive_options
                    .iter()
                    .map(|option| option.to_string())
                    .join(", ")
            )
        }

        // If any option is either set or dropped, ensure all mutually exclusive
        // options are dropped. We do this "behind the scenes", even though we
        // disallow users from performing the same action because this is the
        // mechanism by which we overwrite values elsewhere in the code.
        if set_options_count > 0 || drop_options_count > 0 {
            drop_options.extend(mutually_exclusive_options.iter().cloned());
        }

        // n.b. if mutually exclusive options are set, those will error when we
        // try to replan the connection.
    }

    Ok(Plan::AlterConnection(AlterConnectionPlan {
        id: entry.id(),
        action: crate::plan::AlterConnectionAction::AlterOptions {
            set_options,
            drop_options,
            validate,
        },
    }))
}

pub fn describe_alter_sink(
    _: &StatementContext,
    _: AlterSinkStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_sink(
    scx: &mut StatementContext,
    stmt: AlterSinkStatement<Aug>,
) -> Result<Plan, PlanError> {
    let AlterSinkStatement {
        sink_name,
        if_exists,
        action: _,
    } = stmt;

    let object_type = ObjectType::Sink;
    let _ = resolve_item_or_type(scx, object_type, sink_name, if_exists)?;

    bail_unsupported!("ALTER SINK");
}

pub fn describe_alter_source(
    _: &StatementContext,
    _: AlterSourceStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    // TODO: put the options here, right?
    Ok(StatementDesc::new(None))
}

generate_extracted_config!(
    AlterSourceAddSubsourceOption,
    (TextColumns, Vec::<UnresolvedItemName>, Default(vec![])),
    (Details, String)
);

pub fn plan_alter_source(
    scx: &mut StatementContext,
    stmt: AlterSourceStatement<Aug>,
) -> Result<Plan, PlanError> {
    let AlterSourceStatement {
        source_name,
        if_exists,
        action,
    } = stmt;
    let object_type = ObjectType::Source;

    if resolve_item_or_type(scx, object_type, source_name.clone(), if_exists)?.is_none() {
        scx.catalog.add_notice(PlanNotice::ObjectDoesNotExist {
            name: source_name.to_string(),
            object_type,
        });

        return Ok(Plan::AlterNoop(AlterNoopPlan { object_type }));
    }

    match action {
        AlterSourceAction::SetOptions(options) => {
            let mut options = options.into_iter();
            let option = options.next().unwrap();
            if option.name == CreateSourceOptionName::RetainHistory {
                if options.next().is_some() {
                    sql_bail!("RETAIN HISTORY must be only option");
                }
                return alter_retain_history(
                    scx,
                    object_type,
                    if_exists,
                    UnresolvedObjectName::Item(source_name),
                    option.value,
                );
            }
            // n.b we use this statement in purification in a way that cannot be
            // planned directly.
            sql_bail!(
                "Cannot modify the {} of a SOURCE.",
                option.name.to_ast_string()
            );
        }
        AlterSourceAction::ResetOptions(reset) => {
            let mut options = reset.into_iter();
            let option = options.next().unwrap();
            if option == CreateSourceOptionName::RetainHistory {
                if options.next().is_some() {
                    sql_bail!("RETAIN HISTORY must be only option");
                }
                return alter_retain_history(
                    scx,
                    object_type,
                    if_exists,
                    UnresolvedObjectName::Item(source_name),
                    None,
                );
            }
            sql_bail!("Cannot modify the {} of a SOURCE.", option.to_ast_string());
        }
        AlterSourceAction::DropSubsources { .. } => {
            sql_bail!("ALTER SOURCE...DROP SUBSOURCE no longer supported; use DROP SOURCE")
        }
        AlterSourceAction::AddSubsources { .. } => {
            unreachable!("ALTER SOURCE...ADD SUBSOURCE must be purified")
        }
    };
}

pub fn describe_alter_system_set(
    _: &StatementContext,
    _: AlterSystemSetStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_system_set(
    _: &StatementContext,
    AlterSystemSetStatement { name, to }: AlterSystemSetStatement,
) -> Result<Plan, PlanError> {
    let name = name.to_string();
    Ok(Plan::AlterSystemSet(AlterSystemSetPlan {
        name,
        value: scl::plan_set_variable_to(to)?,
    }))
}

pub fn describe_alter_system_reset(
    _: &StatementContext,
    _: AlterSystemResetStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_system_reset(
    _: &StatementContext,
    AlterSystemResetStatement { name }: AlterSystemResetStatement,
) -> Result<Plan, PlanError> {
    let name = name.to_string();
    Ok(Plan::AlterSystemReset(AlterSystemResetPlan { name }))
}

pub fn describe_alter_system_reset_all(
    _: &StatementContext,
    _: AlterSystemResetAllStatement,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_system_reset_all(
    _: &StatementContext,
    _: AlterSystemResetAllStatement,
) -> Result<Plan, PlanError> {
    Ok(Plan::AlterSystemResetAll(AlterSystemResetAllPlan {}))
}

pub fn describe_alter_role(
    _: &StatementContext,
    _: AlterRoleStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_alter_role(
    _scx: &StatementContext,
    AlterRoleStatement { name, option }: AlterRoleStatement<Aug>,
) -> Result<Plan, PlanError> {
    let option = match option {
        AlterRoleOption::Attributes(attrs) => {
            let attrs = plan_role_attributes(attrs)?;
            PlannedAlterRoleOption::Attributes(attrs)
        }
        AlterRoleOption::Variable(variable) => {
            let var = plan_role_variable(variable)?;
            PlannedAlterRoleOption::Variable(var)
        }
    };

    Ok(Plan::AlterRole(AlterRolePlan {
        id: name.id,
        name: name.name,
        option,
    }))
}

pub fn describe_comment(
    _: &StatementContext,
    _: CommentStatement<Aug>,
) -> Result<StatementDesc, PlanError> {
    Ok(StatementDesc::new(None))
}

pub fn plan_comment(
    scx: &mut StatementContext,
    stmt: CommentStatement<Aug>,
) -> Result<Plan, PlanError> {
    const MAX_COMMENT_LENGTH: usize = 1024;

    scx.require_feature_flag(&vars::ENABLE_COMMENT)?;

    let CommentStatement { object, comment } = stmt;

    // TODO(parkmycar): Make max comment length configurable.
    if let Some(c) = &comment {
        if c.len() > 1024 {
            return Err(PlanError::CommentTooLong {
                length: c.len(),
                max_size: MAX_COMMENT_LENGTH,
            });
        }
    }

    let (object_id, column_pos) = match &object {
        com_ty @ CommentObjectType::Table { name }
        | com_ty @ CommentObjectType::View { name }
        | com_ty @ CommentObjectType::MaterializedView { name }
        | com_ty @ CommentObjectType::Index { name }
        | com_ty @ CommentObjectType::Func { name }
        | com_ty @ CommentObjectType::Connection { name }
        | com_ty @ CommentObjectType::Source { name }
        | com_ty @ CommentObjectType::Sink { name }
        | com_ty @ CommentObjectType::Secret { name } => {
            let item = scx.get_item_by_resolved_name(name)?;
            match (com_ty, item.item_type()) {
                (CommentObjectType::Table { .. }, CatalogItemType::Table) => {
                    (CommentObjectId::Table(item.id()), None)
                }
                (CommentObjectType::View { .. }, CatalogItemType::View) => {
                    (CommentObjectId::View(item.id()), None)
                }
                (CommentObjectType::MaterializedView { .. }, CatalogItemType::MaterializedView) => {
                    (CommentObjectId::MaterializedView(item.id()), None)
                }
                (CommentObjectType::Index { .. }, CatalogItemType::Index) => {
                    (CommentObjectId::Index(item.id()), None)
                }
                (CommentObjectType::Func { .. }, CatalogItemType::Func) => {
                    (CommentObjectId::Func(item.id()), None)
                }
                (CommentObjectType::Connection { .. }, CatalogItemType::Connection) => {
                    (CommentObjectId::Connection(item.id()), None)
                }
                (CommentObjectType::Source { .. }, CatalogItemType::Source) => {
                    (CommentObjectId::Source(item.id()), None)
                }
                (CommentObjectType::Sink { .. }, CatalogItemType::Sink) => {
                    (CommentObjectId::Sink(item.id()), None)
                }
                (CommentObjectType::Secret { .. }, CatalogItemType::Secret) => {
                    (CommentObjectId::Secret(item.id()), None)
                }
                (com_ty, cat_ty) => {
                    let expected_type = match com_ty {
                        CommentObjectType::Table { .. } => ObjectType::Table,
                        CommentObjectType::View { .. } => ObjectType::View,
                        CommentObjectType::MaterializedView { .. } => ObjectType::MaterializedView,
                        CommentObjectType::Index { .. } => ObjectType::Index,
                        CommentObjectType::Func { .. } => ObjectType::Func,
                        CommentObjectType::Connection { .. } => ObjectType::Connection,
                        CommentObjectType::Source { .. } => ObjectType::Source,
                        CommentObjectType::Sink { .. } => ObjectType::Sink,
                        CommentObjectType::Secret { .. } => ObjectType::Secret,
                        _ => unreachable!("these are the only types we match on"),
                    };

                    return Err(PlanError::InvalidObjectType {
                        expected_type: SystemObjectType::Object(expected_type),
                        actual_type: SystemObjectType::Object(cat_ty.into()),
                        object_name: item.name().item.clone(),
                    });
                }
            }
        }
        CommentObjectType::Type { ty } => match ty {
            ResolvedDataType::AnonymousList(_) | ResolvedDataType::AnonymousMap { .. } => {
                sql_bail!("cannot comment on anonymous list or map type");
            }
            ResolvedDataType::Named { id, modifiers, .. } => {
                if !modifiers.is_empty() {
                    sql_bail!("cannot comment on type with modifiers");
                }
                (CommentObjectId::Type(*id), None)
            }
            ResolvedDataType::Error => unreachable!("should have been caught in name resolution"),
        },
        CommentObjectType::Column { name } => {
            let (item, pos) = scx.get_column_by_resolved_name(name)?;
            match item.item_type() {
                CatalogItemType::Table => (CommentObjectId::Table(item.id()), Some(pos + 1)),
                CatalogItemType::Source => (CommentObjectId::Source(item.id()), Some(pos + 1)),
                CatalogItemType::View => (CommentObjectId::View(item.id()), Some(pos + 1)),
                CatalogItemType::MaterializedView => {
                    (CommentObjectId::MaterializedView(item.id()), Some(pos + 1))
                }
                CatalogItemType::Type => (CommentObjectId::Type(item.id()), Some(pos + 1)),
                r => {
                    return Err(PlanError::Unsupported {
                        feature: format!("Specifying comments on a column of {r}"),
                        issue_no: Some(21465),
                    });
                }
            }
        }
        CommentObjectType::Role { name } => (CommentObjectId::Role(name.id), None),
        CommentObjectType::Database { name } => {
            (CommentObjectId::Database(*name.database_id()), None)
        }
        CommentObjectType::Schema { name } => (
            CommentObjectId::Schema((*name.database_spec(), *name.schema_spec())),
            None,
        ),
        CommentObjectType::Cluster { name } => (CommentObjectId::Cluster(name.id), None),
        CommentObjectType::ClusterReplica { name } => {
            let replica = scx.catalog.resolve_cluster_replica(name)?;
            (
                CommentObjectId::ClusterReplica((replica.cluster_id(), replica.replica_id())),
                None,
            )
        }
    };

    // Note: the `mz_comments` table uses an `Int4` for the column position, but in the catalog storage we
    // store a `usize` which would be a `Uint8`. We guard against a safe conversion here because
    // it's the easiest place to raise an error.
    //
    // TODO(parkmycar): https://github.com/MaterializeInc/materialize/issues/22246.
    if let Some(p) = column_pos {
        i32::try_from(p).map_err(|_| PlanError::TooManyColumns {
            max_num_columns: MAX_NUM_COLUMNS,
            req_num_columns: p,
        })?;
    }

    Ok(Plan::Comment(CommentPlan {
        object_id,
        sub_component: column_pos,
        comment,
    }))
}

pub(crate) fn resolve_cluster<'a>(
    scx: &'a StatementContext,
    name: &'a Ident,
    if_exists: bool,
) -> Result<Option<&'a dyn CatalogCluster<'a>>, PlanError> {
    match scx.resolve_cluster(Some(name)) {
        Ok(cluster) => Ok(Some(cluster)),
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn resolve_cluster_replica<'a>(
    scx: &'a StatementContext,
    name: &QualifiedReplica,
    if_exists: bool,
) -> Result<Option<(&'a dyn CatalogCluster<'a>, ReplicaId)>, PlanError> {
    match scx.resolve_cluster(Some(&name.cluster)) {
        Ok(cluster) => match cluster.replica_ids().get(name.replica.as_str()) {
            Some(replica_id) => Ok(Some((cluster, *replica_id))),
            None if if_exists => Ok(None),
            None => Err(sql_err!(
                "CLUSTER {} has no CLUSTER REPLICA named {}",
                cluster.name(),
                name.replica.as_str().quoted(),
            )),
        },
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn resolve_database<'a>(
    scx: &'a StatementContext,
    name: &'a UnresolvedDatabaseName,
    if_exists: bool,
) -> Result<Option<&'a dyn CatalogDatabase>, PlanError> {
    match scx.resolve_database(name) {
        Ok(database) => Ok(Some(database)),
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn resolve_schema<'a>(
    scx: &'a StatementContext,
    name: UnresolvedSchemaName,
    if_exists: bool,
) -> Result<Option<(ResolvedDatabaseSpecifier, SchemaSpecifier)>, PlanError> {
    match scx.resolve_schema(name) {
        Ok(schema) => Ok(Some((schema.database().clone(), schema.id().clone()))),
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e),
    }
}

pub(crate) fn resolve_item_or_type<'a>(
    scx: &'a StatementContext,
    object_type: ObjectType,
    name: UnresolvedItemName,
    if_exists: bool,
) -> Result<Option<&'a dyn CatalogItem>, PlanError> {
    let name = normalize::unresolved_item_name(name)?;
    let catalog_item = match object_type {
        ObjectType::Type => scx.catalog.resolve_type(&name),
        _ => scx.catalog.resolve_item(&name),
    };

    match catalog_item {
        Ok(item) => {
            let is_type = ObjectType::from(item.item_type());
            if object_type == is_type {
                Ok(Some(item))
            } else {
                Err(PlanError::MismatchedObjectType {
                    name: scx.catalog.minimal_qualification(item.name()),
                    is_type,
                    expected_type: object_type,
                })
            }
        }
        Err(_) if if_exists => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Returns an error if the given cluster is a managed cluster
fn ensure_cluster_is_not_managed(
    scx: &StatementContext,
    cluster_id: ClusterId,
) -> Result<(), PlanError> {
    let cluster = scx.catalog.get_cluster(cluster_id);
    if cluster.is_managed() {
        Err(PlanError::ManagedCluster {
            cluster_name: cluster.name().to_string(),
        })
    } else {
        Ok(())
    }
}
