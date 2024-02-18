// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;
use std::error::Error;
use std::num::{ParseIntError, TryFromIntError};
use std::sync::Arc;
use std::time::Duration;
use std::{fmt, io};

use itertools::Itertools;
use mz_expr::EvalError;
use mz_mysql_util::MySqlError;
use mz_ore::error::ErrorExt;
use mz_ore::stack::RecursionLimitError;
use mz_ore::str::{separated, StrExt};
use mz_postgres_util::PostgresError;
use mz_repr::adt::char::InvalidCharLengthError;
use mz_repr::adt::mz_acl_item::AclMode;
use mz_repr::adt::numeric::InvalidNumericMaxScaleError;
use mz_repr::adt::timestamp::InvalidTimestampPrecisionError;
use mz_repr::adt::varchar::InvalidVarCharMaxLengthError;
use mz_repr::{strconv, ColumnName, GlobalId};
use mz_sql_parser::ast::display::AstDisplay;
use mz_sql_parser::ast::{IdentError, UnresolvedItemName};
use mz_sql_parser::parser::{ParserError, ParserStatementError};

use crate::catalog::{
    CatalogError, CatalogItemType, ErrorMessageObjectDescription, SystemObjectType,
};
use crate::names::{PartialItemName, ResolvedItemName};
use crate::plan::plan_utils::JoinSide;
use crate::plan::scope::ScopeItem;
use crate::pure::error::{
    CsrPurificationError, KafkaSinkPurificationError, KafkaSourcePurificationError,
    LoadGeneratorSourcePurificationError, MySqlSourcePurificationError, PgSourcePurificationError,
};
use crate::session::vars::VarError;

#[derive(Clone, Debug)]
pub enum PlanError {
    /// This feature is not yet supported, but may be supported at some point in the future.
    Unsupported {
        feature: String,
        issue_no: Option<usize>,
    },
    /// This feature is not supported, and will likely never be supported.
    NeverSupported {
        feature: String,
        documentation_link: Option<String>,
        details: Option<String>,
    },
    UnknownColumn {
        table: Option<PartialItemName>,
        column: ColumnName,
        similar: Box<[ColumnName]>,
    },
    UngroupedColumn {
        table: Option<PartialItemName>,
        column: ColumnName,
    },
    WrongJoinTypeForLateralColumn {
        table: Option<PartialItemName>,
        column: ColumnName,
    },
    AmbiguousColumn(ColumnName),
    TooManyColumns {
        max_num_columns: usize,
        req_num_columns: usize,
    },
    AmbiguousTable(PartialItemName),
    UnknownColumnInUsingClause {
        column: ColumnName,
        join_side: JoinSide,
    },
    AmbiguousColumnInUsingClause {
        column: ColumnName,
        join_side: JoinSide,
    },
    MisqualifiedName(String),
    OverqualifiedDatabaseName(String),
    OverqualifiedSchemaName(String),
    UnderqualifiedColumnName(String),
    SubqueriesDisallowed {
        context: String,
    },
    UnknownParameter(usize),
    RecursionLimit(RecursionLimitError),
    StrconvParse(strconv::ParseError),
    Catalog(CatalogError),
    UpsertSinkWithoutKey,
    UpsertSinkWithInvalidKey {
        name: String,
        desired_key: Vec<String>,
        valid_keys: Vec<Vec<String>>,
    },
    InvalidWmrRecursionLimit(String),
    InvalidNumericMaxScale(InvalidNumericMaxScaleError),
    InvalidCharLength(InvalidCharLengthError),
    InvalidId(GlobalId),
    InvalidIdent(IdentError),
    InvalidObject(Box<ResolvedItemName>),
    InvalidObjectType {
        expected_type: SystemObjectType,
        actual_type: SystemObjectType,
        object_name: String,
    },
    InvalidPrivilegeTypes {
        invalid_privileges: AclMode,
        object_description: ErrorMessageObjectDescription,
    },
    InvalidVarCharMaxLength(InvalidVarCharMaxLengthError),
    InvalidTimestampPrecision(InvalidTimestampPrecisionError),
    InvalidSecret(Box<ResolvedItemName>),
    InvalidTemporarySchema,
    MangedReplicaName(String),
    ParserStatement(ParserStatementError),
    Parser(ParserError),
    DropViewOnMaterializedView(String),
    DropSubsource {
        subsource: String,
        source: String,
    },
    DropProgressCollection {
        progress_collection: String,
        source: String,
    },
    DropNonSubsource {
        non_subsource: String,
        source: String,
    },
    DropLastSubsource {
        source: String,
    },
    DependentObjectsStillExist {
        object_type: String,
        object_name: String,
        // (dependent type, name)
        dependents: Vec<(String, String)>,
    },
    AlterViewOnMaterializedView(String),
    ShowCreateViewOnMaterializedView(String),
    ExplainViewOnMaterializedView(String),
    UnacceptableTimelineName(String),
    FetchingCsrSchemaFailed {
        schema_lookup: String,
        cause: Arc<dyn Error + Send + Sync>,
    },
    PostgresConnectionErr {
        cause: Arc<mz_postgres_util::PostgresError>,
    },
    MySqlConnectionErr {
        cause: Arc<MySqlError>,
    },
    SubsourceNameConflict {
        name: UnresolvedItemName,
        upstream_references: Vec<UnresolvedItemName>,
    },
    SubsourceDuplicateReference {
        name: UnresolvedItemName,
        target_names: Vec<UnresolvedItemName>,
    },
    /// This is the ALTER SOURCE version of [`Self::SubsourceDuplicateReference`].
    SubsourceAlreadyReferredTo {
        name: UnresolvedItemName,
    },
    InvalidProtobufSchema {
        cause: protobuf_native::OperationFailedError,
    },
    InvalidOptionValue {
        // Expected to be generated from the `to_ast_string` value on the option
        // name.
        option_name: String,
        err: Box<PlanError>,
    },
    UnexpectedDuplicateReference {
        name: UnresolvedItemName,
    },
    /// Declaration of a recursive type did not match the inferred type.
    RecursiveTypeMismatch(String, Vec<String>, Vec<String>),
    UnknownFunction {
        name: String,
        arg_types: Vec<String>,
    },
    IndistinctFunction {
        name: String,
        arg_types: Vec<String>,
    },
    UnknownOperator {
        name: String,
        arg_types: Vec<String>,
    },
    IndistinctOperator {
        name: String,
        arg_types: Vec<String>,
    },
    InvalidPrivatelinkAvailabilityZone {
        name: String,
        supported_azs: BTreeSet<String>,
    },
    InvalidSchemaName,
    ItemAlreadyExists {
        name: String,
        item_type: CatalogItemType,
    },
    ManagedCluster {
        cluster_name: String,
    },
    InvalidKeysInSubscribeEnvelopeUpsert,
    InvalidKeysInSubscribeEnvelopeDebezium,
    InvalidOrderByInSubscribeWithinTimestampOrderBy,
    FromValueRequiresParen,
    VarError(VarError),
    UnsolvablePolymorphicFunctionInput,
    ShowCommandInView,
    WebhookValidationDoesNotUseColumns,
    WebhookValidationNonDeterministic,
    InternalFunctionCall,
    CommentTooLong {
        length: usize,
        max_size: usize,
    },
    InvalidTimestampInterval {
        min: Duration,
        max: Duration,
        requested: Duration,
    },
    InvalidGroupSizeHints,
    PgSourcePurification(PgSourcePurificationError),
    KafkaSourcePurification(KafkaSourcePurificationError),
    KafkaSinkPurification(KafkaSinkPurificationError),
    LoadGeneratorSourcePurification(LoadGeneratorSourcePurificationError),
    CsrPurification(CsrPurificationError),
    MySqlSourcePurification(MySqlSourcePurificationError),
    MissingName(CatalogItemType),
    InvalidRefreshAt,
    InvalidRefreshEveryAlignedTo,
    CreateReplicaFailStorageObjects {
        /// The current number of replicas on the cluster
        current_replica_count: usize,
        /// THe number of internal replicas on the cluster
        internal_replica_count: usize,
        /// The number of replicas that executing this command would have
        /// created
        hypothetical_replica_count: usize,
    },
    // TODO(benesch): eventually all errors should be structured.
    Unstructured(String),
}

impl PlanError {
    pub(crate) fn ungrouped_column(item: &ScopeItem) -> PlanError {
        PlanError::UngroupedColumn {
            table: item.table_name.clone(),
            column: item.column_name.clone(),
        }
    }

    pub fn detail(&self) -> Option<String> {
        match self {
            Self::NeverSupported { details, .. } => details.clone(),
            Self::FetchingCsrSchemaFailed { cause, .. } => Some(cause.to_string_with_causes()),
            Self::PostgresConnectionErr { cause } => Some(cause.to_string_with_causes()),
            Self::InvalidProtobufSchema { cause } => Some(cause.to_string_with_causes()),
            Self::InvalidOptionValue { err, .. } => err.detail(),
            Self::UpsertSinkWithInvalidKey {
                name,
                desired_key,
                valid_keys,
            } => {
                let valid_keys = if valid_keys.is_empty() {
                    "There are no known valid unique keys for the underlying relation.".into()
                } else {
                    format!(
                        "The following keys are known to be unique for the underlying relation:\n{}",
                        valid_keys
                            .iter()
                            .map(|k|
                                format!("  ({})", k.iter().map(|c| c.as_str().quoted()).join(", "))
                            )
                            .join("\n"),
                    )
                };
                Some(format!(
                    "Materialize could not prove that the specified upsert envelope key ({}) \
                    was a unique key of the underlying relation {}. {valid_keys}",
                    separated(", ", desired_key.iter().map(|c| c.as_str().quoted())),
                    name.quoted()
                ))
            }
            Self::VarError(e) => e.detail(),
            Self::InternalFunctionCall => Some("This function is for the internal use of the database system and cannot be called directly.".into()),
            Self::PgSourcePurification(e) => e.detail(),
            Self::KafkaSourcePurification(e) => e.detail(),
            Self::LoadGeneratorSourcePurification(e) => e.detail(),
            Self::CsrPurification(e) => e.detail(),
            Self::KafkaSinkPurification(e) => e.detail(),
            Self::CreateReplicaFailStorageObjects { current_replica_count: current, internal_replica_count: internal, hypothetical_replica_count: target } => {
                Some(format!(
                    "Currently have {} replica{}{}; command would result in {}",
                    current,
                    if *current != 1 { "s" } else { "" },
                    if *internal > 0 {
                        format!(" ({} internal)", internal)
                    } else {
                        "".to_string()
                    },
                    target
                ))
            },
            Self::SubsourceNameConflict {
                name: _,
                upstream_references,
            } => Some(format!(
                "referenced tables with duplicate name: {}",
                itertools::join(upstream_references, ", ")
            )),
            Self::SubsourceDuplicateReference {
                name: _,
                target_names,
            } => Some(format!(
                "subsources referencing table: {}",
                itertools::join(target_names, ", ")
            )),
            _ => None,
        }
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            Self::DropViewOnMaterializedView(_) => {
                Some("Use DROP MATERIALIZED VIEW to remove a materialized view.".into())
            }
            Self::DropSubsource { source, subsource } => Some(format!(
                "Use ALTER SOURCE {source} DROP SUBSOURCE {subsource}"
            )),
            Self::DropLastSubsource { source } | Self::DropProgressCollection { source, .. } => Some(format!(
                "Use DROP SOURCE {source} to drop the primary source along with all subsources"
            )),
            Self::DependentObjectsStillExist {..} => Some("Use DROP ... CASCADE to drop the dependent objects too.".into()),
            Self::AlterViewOnMaterializedView(_) => {
                Some("Use ALTER MATERIALIZED VIEW to rename a materialized view.".into())
            }
            Self::ShowCreateViewOnMaterializedView(_) => {
                Some("Use SHOW CREATE MATERIALIZED VIEW to show a materialized view.".into())
            }
            Self::ExplainViewOnMaterializedView(_) => {
                Some("Use EXPLAIN [...] MATERIALIZED VIEW to explain a materialized view.".into())
            }
            Self::UnacceptableTimelineName(_) => {
                Some("The prefix \"mz_\" is reserved for system timelines.".into())
            }
            Self::PostgresConnectionErr { cause } => {
                if let Some(cause) = cause.source() {
                    if let Some(cause) = cause.downcast_ref::<io::Error>() {
                        if cause.kind() == io::ErrorKind::TimedOut {
                            return Some(
                                "Do you have a firewall or security group that is \
                                preventing Materialize from conecting to your PostgreSQL server?"
                                    .into(),
                            );
                        }
                    }
                }
                None
            }
            Self::InvalidOptionValue { err, .. } => err.hint(),
            Self::UnknownFunction { ..} => Some("No function matches the given name and argument types. You might need to add explicit type casts.".into()),
            Self::IndistinctFunction {..} => {
                Some("Could not choose a best candidate function. You might need to add explicit type casts.".into())
            }
            Self::UnknownOperator {..} => {
                Some("No operator matches the given name and argument types. You might need to add explicit type casts.".into())
            }
            Self::IndistinctOperator {..} => {
                Some("Could not choose a best candidate operator. You might need to add explicit type casts.".into())
            },
            Self::InvalidPrivatelinkAvailabilityZone { supported_azs, ..} => {
                let supported_azs_str = supported_azs.iter().join("\n  ");
                Some(format!("Did you supply an availability zone name instead of an ID? Known availability zone IDs:\n  {}", supported_azs_str))
            }
            Self::InvalidKeysInSubscribeEnvelopeUpsert => {
                Some("All keys must be columns on the underlying relation.".into())
            }
            Self::InvalidKeysInSubscribeEnvelopeDebezium => {
                Some("All keys must be columns on the underlying relation.".into())
            }
            Self::InvalidOrderByInSubscribeWithinTimestampOrderBy => {
                Some("All order bys must be output columns.".into())
            }
            Self::UpsertSinkWithInvalidKey { .. } | Self::UpsertSinkWithoutKey => {
                Some("See: https://materialize.com/s/sink-key-selection".into())
            }
            Self::Catalog(e) => e.hint(),
            Self::VarError(e) => e.hint(),
            Self::PgSourcePurification(e) => e.hint(),
            Self::KafkaSourcePurification(e) => e.hint(),
            Self::LoadGeneratorSourcePurification(e) => e.hint(),
            Self::CsrPurification(e) => e.hint(),
            Self::KafkaSinkPurification(e) => e.hint(),
            Self::UnknownColumn { table, similar, .. } => {
                let suffix = "Make sure to surround case sensitive names in double quotes.";
                match &similar[..] {
                    [] => None,
                    [column] => Some(format!("The similarly named column {} does exist. {suffix}", ColumnDisplay { table, column })),
                    names => {
                        let similar = names.into_iter().map(|column| ColumnDisplay { table, column }).join(", ");
                        Some(format!("There are similarly named columns that do exist: {similar}. {suffix}"))
                    }
                }
            }
            Self::RecursiveTypeMismatch(..) => {
                Some("You will need to rewrite or cast the query's expressions.".into())
            },
            Self::InvalidRefreshAt
            | Self::InvalidRefreshEveryAlignedTo => {
                Some("Calling `mz_now()` is allowed.".into())
            },
            Self::SubsourceNameConflict { .. } | Self::SubsourceAlreadyReferredTo { .. } => {
                Some("Specify target table names using FOR TABLES (foo AS bar), or limit the upstream tables using FOR SCHEMAS (foo)".into())
            },
            _ => None,
        }
    }
}

impl fmt::Display for PlanError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Unsupported { feature, issue_no } => {
                write!(f, "{} not yet supported", feature)?;
                if let Some(issue_no) = issue_no {
                    write!(f, ", see https://github.com/MaterializeInc/materialize/issues/{} for more details", issue_no)?;
                }
                Ok(())
            }
            Self::NeverSupported { feature, documentation_link: documentation_path,.. } => {
                write!(f, "{feature} is not supported",)?;
                if let Some(documentation_path) = documentation_path {
                    write!(f, ", for more information consult the documentation at https://materialize.com/docs/{documentation_path}")?;
                }
                Ok(())
            }
            Self::UnknownColumn { table, column, similar: _ } => write!(
                f,
                "column {} does not exist",
                ColumnDisplay { table, column }
            ),
            Self::UngroupedColumn { table, column } => write!(
                f,
                "column {} must appear in the GROUP BY clause or be used in an aggregate function",
                ColumnDisplay { table, column },
            ),
            Self::WrongJoinTypeForLateralColumn { table, column } => write!(
                f,
                "column {} cannot be referenced from this part of the query: \
                the combining JOIN type must be INNER or LEFT for a LATERAL reference",
                ColumnDisplay { table, column },
            ),
            Self::AmbiguousColumn(column) => write!(
                f,
                "column reference {} is ambiguous",
                column.as_str().quoted()
            ),
            Self::TooManyColumns { max_num_columns, req_num_columns } => write!(
                f,
                "attempt to create relation with too many columns, {} max: {}",
                req_num_columns, max_num_columns
            ),
            Self::AmbiguousTable(table) => write!(
                f,
                "table reference {} is ambiguous",
                table.item.as_str().quoted()
            ),
            Self::UnknownColumnInUsingClause { column, join_side } => write!(
                f,
                "column {} specified in USING clause does not exist in {} table",
                column.as_str().quoted(),
                join_side,
            ),
            Self::AmbiguousColumnInUsingClause { column, join_side } => write!(
                f,
                "common column name {} appears more than once in {} table",
                column.as_str().quoted(),
                join_side,
            ),
            Self::MisqualifiedName(name) => write!(
                f,
                "qualified name did not have between 1 and 3 components: {}",
                name
            ),
            Self::OverqualifiedDatabaseName(name) => write!(
                f,
                "database name '{}' does not have exactly one component",
                name
            ),
            Self::OverqualifiedSchemaName(name) => write!(
                f,
                "schema name '{}' cannot have more than two components",
                name
            ),
            Self::UnderqualifiedColumnName(name) => write!(
                f,
                "column name '{}' must have at least a table qualification",
                name
            ),
            Self::UnacceptableTimelineName(name) => {
                write!(f, "unacceptable timeline name {}", name.quoted(),)
            }
            Self::SubqueriesDisallowed { context } => {
                write!(f, "{} does not allow subqueries", context)
            }
            Self::UnknownParameter(n) => write!(f, "there is no parameter ${}", n),
            Self::RecursionLimit(e) => write!(f, "{}", e),
            Self::StrconvParse(e) => write!(f, "{}", e),
            Self::Catalog(e) => write!(f, "{}", e),
            Self::UpsertSinkWithoutKey => write!(f, "upsert sinks must specify a key"),
            Self::UpsertSinkWithInvalidKey { .. } => {
                write!(f, "upsert key could not be validated as unique")
            }
            Self::InvalidWmrRecursionLimit(msg) => write!(f, "Invalid WITH MUTUALLY RECURSIVE recursion limit. {}", msg),
            Self::InvalidNumericMaxScale(e) => e.fmt(f),
            Self::InvalidCharLength(e) => e.fmt(f),
            Self::InvalidVarCharMaxLength(e) => e.fmt(f),
            Self::InvalidTimestampPrecision(e) => e.fmt(f),
            Self::Parser(e) => e.fmt(f),
            Self::ParserStatement(e) => e.fmt(f),
            Self::Unstructured(e) => write!(f, "{}", e),
            Self::InvalidId(id) => write!(f, "invalid id {}", id),
            Self::InvalidIdent(err) => write!(f, "invalid identifier, {err}"),
            Self::InvalidObject(i) => write!(f, "{} is not a database object", i.full_name_str()),
            Self::InvalidObjectType{expected_type, actual_type, object_name} => write!(f, "{actual_type} {object_name} is not a {expected_type}"),
            Self::InvalidPrivilegeTypes{ invalid_privileges, object_description, } => {
                write!(f, "invalid privilege types {} for {}", invalid_privileges.to_error_string(), object_description)
            },
            Self::InvalidSecret(i) => write!(f, "{} is not a secret", i.full_name_str()),
            Self::InvalidTemporarySchema => {
                write!(f, "cannot create temporary item in non-temporary schema")
            }
            Self::DropViewOnMaterializedView(name)
            | Self::AlterViewOnMaterializedView(name)
            | Self::ShowCreateViewOnMaterializedView(name)
            | Self::ExplainViewOnMaterializedView(name) => write!(f, "{name} is not a view"),
            Self::FetchingCsrSchemaFailed { schema_lookup, .. } => {
                write!(f, "failed to fetch schema {schema_lookup} from schema registry")
            }
            Self::PostgresConnectionErr { .. } => {
                write!(f, "failed to connect to PostgreSQL database")
            }
            Self::MySqlConnectionErr { cause } => {
                write!(f, "failed to connect to MySQL database: {}", cause)
            }
            Self::SubsourceNameConflict {
                name , upstream_references: _,
            } => {
                write!(f, "multiple subsources would be named {}", name)
            },
            Self::SubsourceDuplicateReference {
                name,
                target_names: _,
            } => {
                write!(f, "multiple subsources refer to table {}", name)
            },
            Self::SubsourceAlreadyReferredTo { name } => {
                write!(f, "another subsource already refers to {}", name)
            }
            Self::InvalidProtobufSchema { .. } => {
                write!(f, "invalid protobuf schema")
            }
            Self::DropSubsource { subsource, source: _} => write!(f, "SOURCE {} is a subsource and must be dropped with ALTER SOURCE...DROP SUBSOURCE", subsource.quoted()),
            Self::DropLastSubsource { source } => write!(f, "SOURCE {} must retain at least one non-progress subsource", source.quoted()),
            Self::DropProgressCollection { progress_collection, source: _} => write!(f, "SOURCE {} is a progress collection and cannot be dropped independently of its primary source", progress_collection.quoted()),
            Self::DropNonSubsource { non_subsource, source} => write!(f, "SOURCE {} is a not a subsource of {}", non_subsource.quoted(), source.quoted()),
            Self::DependentObjectsStillExist {object_type, object_name, dependents} => {
                let reason = match &dependents[..] {
                    [] => " because other objects depend on it".to_string(),
                    dependents => {
                        let dependents = dependents.iter().map(|(dependent_type, dependent_name)| format!("{} {}", dependent_type, dependent_name.quoted())).join(", ");
                        format!(": still depended upon by {dependents}")
                    },
                };
                let object_name = object_name.quoted();
                write!(f, "cannot drop {object_type} {object_name}{reason}")
            }
            Self::InvalidOptionValue { option_name, err } => write!(f, "invalid {} option value: {}", option_name, err),
            Self::UnexpectedDuplicateReference { name } => write!(f, "unexpected multiple references to {}", name.to_ast_string()),
            Self::RecursiveTypeMismatch(name, declared, inferred) => {
                let declared = separated(", ", declared);
                let inferred = separated(", ", inferred);
                let name = name.quoted();
                write!(f, "WITH MUTUALLY RECURSIVE query {name} declared types ({declared}), but query returns types ({inferred})")
            },
            Self::UnknownFunction {name, arg_types, ..} => {
                write!(f, "function {}({}) does not exist", name, arg_types.join(", "))
            },
            Self::IndistinctFunction {name, arg_types, ..} => {
                write!(f, "function {}({}) is not unique", name, arg_types.join(", "))
            },
            Self::UnknownOperator {name, arg_types, ..} => {
                write!(f, "operator does not exist: {}", match arg_types.as_slice(){
                    [typ] => format!("{} {}", name, typ),
                    [ltyp, rtyp] => {
                        format!("{} {} {}", ltyp, name, rtyp)
                    }
                    _ => unreachable!("non-unary non-binary operator"),
                })
            },
            Self::IndistinctOperator {name, arg_types, ..} => {
                write!(f, "operator is not unique: {}", match arg_types.as_slice(){
                    [typ] => format!("{} {}", name, typ),
                    [ltyp, rtyp] => {
                        format!("{} {} {}", ltyp, name, rtyp)
                    }
                    _ => unreachable!("non-unary non-binary operator"),
                })
            },
            Self::InvalidPrivatelinkAvailabilityZone { name, ..} => write!(f, "invalid AWS PrivateLink availability zone {}", name.quoted()),
            Self::InvalidSchemaName => write!(f, "no schema has been selected to create in"),
            Self::ItemAlreadyExists { name, item_type } => write!(f, "{item_type} {} already exists", name.quoted()),
            Self::ManagedCluster {cluster_name} => write!(f, "cannot modify managed cluster {cluster_name}"),
            Self::InvalidKeysInSubscribeEnvelopeUpsert => {
                write!(f, "invalid keys in SUBSCRIBE ENVELOPE UPSERT (KEY (..))")
            }
            Self::InvalidKeysInSubscribeEnvelopeDebezium => {
                write!(f, "invalid keys in SUBSCRIBE ENVELOPE DEBEZIUM (KEY (..))")
            }
            Self::InvalidOrderByInSubscribeWithinTimestampOrderBy => {
                write!(f, "invalid ORDER BY in SUBSCRIBE WITHIN TIMESTAMP ORDER BY")
            }
            Self::FromValueRequiresParen => f.write_str(
                "VALUES expression in FROM clause must be surrounded by parentheses"
            ),
            Self::VarError(e) => e.fmt(f),
            Self::UnsolvablePolymorphicFunctionInput => f.write_str(
                "could not determine polymorphic type because input has type unknown"
            ),
            Self::ShowCommandInView => f.write_str("SHOW commands are not allowed in views"),
            Self::WebhookValidationDoesNotUseColumns => f.write_str(
                "expression provided in CHECK does not reference any columns"
            ),
            Self::WebhookValidationNonDeterministic => f.write_str(
                "expression provided in CHECK is not deterministic"
            ),
            Self::InternalFunctionCall => f.write_str("cannot call function with arguments of type internal"),
            Self::CommentTooLong { length, max_size } => {
                write!(f, "provided comment was {length} bytes long, max size is {max_size} bytes")
            }
            Self::InvalidTimestampInterval { min, max, requested } => {
                write!(f, "invalid timestamp interval of {}ms, must be in the range [{}ms, {}ms]", requested.as_millis(), min.as_millis(), max.as_millis())
            }
            Self::InvalidGroupSizeHints => f.write_str("EXPECTED GROUP SIZE cannot be provided \
                simultaneously with any of AGGREGATE INPUT GROUP SIZE, DISTINCT ON INPUT GROUP SIZE, \
                or LIMIT INPUT GROUP SIZE"),
            Self::PgSourcePurification(e) => write!(f, "POSTGRES source validation: {}", e),
            Self::KafkaSourcePurification(e) => write!(f, "KAFKA source validation: {}", e),
            Self::LoadGeneratorSourcePurification(e) => write!(f, "LOAD GENERATOR source validation: {}", e),
            Self::KafkaSinkPurification(e) => write!(f, "KAFKA sink validation: {}", e),
            Self::CsrPurification(e) => write!(f, "CONFLUENT SCHEMA REGISTRY validation: {}", e),
            Self::MySqlSourcePurification(e) => write!(f, "MYSQL source validation: {}", e),
            Self::MangedReplicaName(name) => {
                write!(f, "{name} is reserved for replicas of managed clusters")
            }
            Self::MissingName(item_type) => {
                write!(f, "unspecified name for {item_type}")
            }
            Self::InvalidRefreshAt => {
                write!(f, "REFRESH AT argument must be an expression that can be simplified \
                           and/or cast to a constant whose type is mz_timestamp")
            }
            Self::InvalidRefreshEveryAlignedTo => {
                write!(f, "REFRESH EVERY ... ALIGNED TO argument must be an expression that can be simplified \
                           and/or cast to a constant whose type is mz_timestamp")
            }
            Self::CreateReplicaFailStorageObjects {..} => {
                write!(f, "cannot create more than one replica of a cluster containing sources or sinks")
            },
        }
    }
}

impl Error for PlanError {}

impl From<CatalogError> for PlanError {
    fn from(e: CatalogError) -> PlanError {
        PlanError::Catalog(e)
    }
}

impl From<strconv::ParseError> for PlanError {
    fn from(e: strconv::ParseError) -> PlanError {
        PlanError::StrconvParse(e)
    }
}

impl From<RecursionLimitError> for PlanError {
    fn from(e: RecursionLimitError) -> PlanError {
        PlanError::RecursionLimit(e)
    }
}

impl From<InvalidNumericMaxScaleError> for PlanError {
    fn from(e: InvalidNumericMaxScaleError) -> PlanError {
        PlanError::InvalidNumericMaxScale(e)
    }
}

impl From<InvalidCharLengthError> for PlanError {
    fn from(e: InvalidCharLengthError) -> PlanError {
        PlanError::InvalidCharLength(e)
    }
}

impl From<InvalidVarCharMaxLengthError> for PlanError {
    fn from(e: InvalidVarCharMaxLengthError) -> PlanError {
        PlanError::InvalidVarCharMaxLength(e)
    }
}

impl From<InvalidTimestampPrecisionError> for PlanError {
    fn from(e: InvalidTimestampPrecisionError) -> PlanError {
        PlanError::InvalidTimestampPrecision(e)
    }
}

impl From<anyhow::Error> for PlanError {
    fn from(e: anyhow::Error) -> PlanError {
        // WIP: Do we maybe want to keep the alternate selector for these?
        sql_err!("{}", e.display_with_causes())
    }
}

impl From<TryFromIntError> for PlanError {
    fn from(e: TryFromIntError) -> PlanError {
        sql_err!("{}", e.display_with_causes())
    }
}

impl From<ParseIntError> for PlanError {
    fn from(e: ParseIntError) -> PlanError {
        sql_err!("{}", e.display_with_causes())
    }
}

impl From<EvalError> for PlanError {
    fn from(e: EvalError) -> PlanError {
        sql_err!("{}", e.display_with_causes())
    }
}

impl From<ParserError> for PlanError {
    fn from(e: ParserError) -> PlanError {
        PlanError::Parser(e)
    }
}

impl From<ParserStatementError> for PlanError {
    fn from(e: ParserStatementError) -> PlanError {
        PlanError::ParserStatement(e)
    }
}

impl From<PostgresError> for PlanError {
    fn from(e: PostgresError) -> PlanError {
        PlanError::PostgresConnectionErr { cause: Arc::new(e) }
    }
}

impl From<MySqlError> for PlanError {
    fn from(e: MySqlError) -> PlanError {
        PlanError::MySqlConnectionErr { cause: Arc::new(e) }
    }
}

impl From<VarError> for PlanError {
    fn from(e: VarError) -> Self {
        PlanError::VarError(e)
    }
}

impl From<PgSourcePurificationError> for PlanError {
    fn from(e: PgSourcePurificationError) -> Self {
        PlanError::PgSourcePurification(e)
    }
}

impl From<KafkaSourcePurificationError> for PlanError {
    fn from(e: KafkaSourcePurificationError) -> Self {
        PlanError::KafkaSourcePurification(e)
    }
}

impl From<KafkaSinkPurificationError> for PlanError {
    fn from(e: KafkaSinkPurificationError) -> Self {
        PlanError::KafkaSinkPurification(e)
    }
}

impl From<CsrPurificationError> for PlanError {
    fn from(e: CsrPurificationError) -> Self {
        PlanError::CsrPurification(e)
    }
}

impl From<LoadGeneratorSourcePurificationError> for PlanError {
    fn from(e: LoadGeneratorSourcePurificationError) -> Self {
        PlanError::LoadGeneratorSourcePurification(e)
    }
}

impl From<MySqlSourcePurificationError> for PlanError {
    fn from(e: MySqlSourcePurificationError) -> Self {
        PlanError::MySqlSourcePurification(e)
    }
}

impl From<IdentError> for PlanError {
    fn from(e: IdentError) -> Self {
        PlanError::InvalidIdent(e)
    }
}

struct ColumnDisplay<'a> {
    table: &'a Option<PartialItemName>,
    column: &'a ColumnName,
}

impl<'a> fmt::Display for ColumnDisplay<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if let Some(table) = &self.table {
            format!("{}.{}", table.item, self.column).quoted().fmt(f)
        } else {
            self.column.as_str().quoted().fmt(f)
        }
    }
}
