// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use itertools::Itertools;
use postgres::error::SqlState;

use mz_adapter::session::TransactionCode;
use mz_adapter::{AdapterError, AdapterNotice, StartupMessage};
use mz_expr::EvalError;
use mz_repr::{ColumnName, NotNullViolation, RelationDesc};
use mz_sql::ast::NoticeSeverity;
use mz_sql::plan::PlanError;
use mz_sql::session::vars::{ClientSeverity as AdapterClientSeverity, VarError};

// Pgwire protocol versions are represented as 32-bit integers, where the
// high 16 bits represent the major version and the low 16 bits represent the
// minor version.
//
// There have only been three released protocol versions, v1.0, v2.0, and v3.0.
// The protocol changes very infrequently: the most recent protocol version,
// v3.0, was released with Postgres v7.4 in 2003.
//
// Somewhat unfortunately, the protocol overloads the version field to indicate
// special types of connections, namely, SSL connections and cancellation
// connections. These pseudo-versions were constructed to avoid ever matching
// a true protocol version.

pub const VERSION_1: i32 = 0x10000;
pub const VERSION_2: i32 = 0x20000;
pub const VERSION_3: i32 = 0x30000;
pub const VERSION_CANCEL: i32 = (1234 << 16) + 5678;
pub const VERSION_SSL: i32 = (1234 << 16) + 5679;
pub const VERSION_GSSENC: i32 = (1234 << 16) + 5680;

pub const VERSIONS: &[i32] = &[
    VERSION_1,
    VERSION_2,
    VERSION_3,
    VERSION_CANCEL,
    VERSION_SSL,
    VERSION_GSSENC,
];

/// Like [`FrontendMessage`], but only the messages that can occur during
/// startup protocol negotiation.
#[derive(Debug)]
pub enum FrontendStartupMessage {
    /// Begin a connection.
    Startup {
        version: i32,
        params: BTreeMap<String, String>,
    },

    /// Request SSL encryption for the connection.
    SslRequest,

    /// Request GSSAPI encryption for the connection.
    GssEncRequest,

    /// Cancel a query that is running on another connection.
    CancelRequest {
        /// The target connection ID.
        conn_id: u32,
        /// The secret key for the target connection.
        secret_key: u32,
    },
}

/// A decoded frontend pgwire [message], representing instructions for the
/// backend.
///
/// [message]: https://www.postgresql.org/docs/11/protocol-message-formats.html
#[derive(Debug)]
pub enum FrontendMessage {
    /// Execute the specified SQL.
    ///
    /// This is issued as part of the simple query flow.
    Query {
        /// The SQL to execute.
        sql: String,
    },

    /// Parse the specified SQL into a prepared statement.
    ///
    /// This starts the extended query flow.
    Parse {
        /// The name of the prepared statement to create. An empty string
        /// specifies the unnamed prepared statement.
        name: String,
        /// The SQL to parse.
        sql: String,
        /// The OID of each parameter data type for which the client wants to
        /// prespecify types. A zero OID is equivalent to leaving the type
        /// unspecified.
        ///
        /// The number of specified parameter data types can be less than the
        /// number of parameters specified in the query.
        param_types: Vec<u32>,
    },

    /// Describe an existing prepared statement.
    ///
    /// This command is part of the extended query flow.
    DescribeStatement {
        /// The name of the prepared statement to describe.
        name: String,
    },

    /// Describe an existing portal.
    ///
    /// This command is part of the extended query flow.
    DescribePortal {
        /// The name of the portal to describe.
        name: String,
    },

    /// Bind an existing prepared statement to a portal.
    ///
    /// Note that we can't actually bind parameters yet (issue#609), but that is
    /// an important part of this command.
    ///
    /// This command is part of the extended query flow.
    Bind {
        /// The destination portal. An empty string selects the unnamed
        /// portal. The portal can later be executed with the `Execute` command.
        portal_name: String,
        /// The source prepared statement. An empty string selects the unnamed
        /// prepared statement.
        statement_name: String,
        /// The formats used to encode the parameters in `raw_parameters`.
        param_formats: Vec<mz_pgrepr::Format>,
        /// The value of each parameter, encoded using the formats described
        /// by `parameter_formats`.
        raw_params: Vec<Option<Vec<u8>>>,
        /// The desired formats for the columns in the result set.
        result_formats: Vec<mz_pgrepr::Format>,
    },

    /// Execute a bound portal.
    ///
    /// This command is part of the extended query flow.
    Execute {
        /// The name of the portal to execute.
        portal_name: String,
        /// The maximum number number of rows to return before suspending.
        ///
        /// 0 or negative means infinite.
        max_rows: i32,
    },

    /// Flush any pending output.
    ///
    /// This command is part of the extended query flow.
    Flush,

    /// Finish an extended query.
    ///
    /// This command is part of the extended query flow.
    Sync,

    /// Close the named statement.
    ///
    /// This command is part of the extended query flow.
    CloseStatement {
        name: String,
    },

    /// Close the named portal.
    ///
    // This command is part of the extended query flow.
    ClosePortal {
        name: String,
    },

    /// Terminate a connection.
    Terminate,

    CopyData(Vec<u8>),

    CopyDone,

    CopyFail(String),

    Password {
        password: String,
    },
}

impl FrontendMessage {
    pub fn name(&self) -> &'static str {
        match self {
            FrontendMessage::Query { .. } => "query",
            FrontendMessage::Parse { .. } => "parse",
            FrontendMessage::DescribeStatement { .. } => "describe_statement",
            FrontendMessage::DescribePortal { .. } => "describe_portal",
            FrontendMessage::Bind { .. } => "bind",
            FrontendMessage::Execute { .. } => "execute",
            FrontendMessage::Flush => "flush",
            FrontendMessage::Sync => "sync",
            FrontendMessage::CloseStatement { .. } => "close_statement",
            FrontendMessage::ClosePortal { .. } => "close_portal",
            FrontendMessage::Terminate => "terminate",
            FrontendMessage::CopyData(_) => "copy_data",
            FrontendMessage::CopyDone => "copy_done",
            FrontendMessage::CopyFail(_) => "copy_fail",
            FrontendMessage::Password { .. } => "password",
        }
    }
}

/// Internal representation of a backend [message]
///
/// [message]: https://www.postgresql.org/docs/11/protocol-message-formats.html
#[derive(Debug)]
pub enum BackendMessage {
    AuthenticationOk,
    AuthenticationCleartextPassword,
    CommandComplete {
        tag: String,
    },
    EmptyQueryResponse,
    ReadyForQuery(TransactionCode),
    RowDescription(Vec<FieldDescription>),
    DataRow(Vec<Option<mz_pgrepr::Value>>),
    ParameterStatus(&'static str, String),
    BackendKeyData {
        conn_id: u32,
        secret_key: u32,
    },
    ParameterDescription(Vec<mz_pgrepr::Type>),
    PortalSuspended,
    NoData,
    ParseComplete,
    BindComplete,
    CloseComplete,
    ErrorResponse(ErrorResponse),
    CopyInResponse {
        overall_format: mz_pgrepr::Format,
        column_formats: Vec<mz_pgrepr::Format>,
    },
    CopyOutResponse {
        overall_format: mz_pgrepr::Format,
        column_formats: Vec<mz_pgrepr::Format>,
    },
    CopyData(Vec<u8>),
    CopyDone,
}

impl From<ErrorResponse> for BackendMessage {
    fn from(err: ErrorResponse) -> BackendMessage {
        BackendMessage::ErrorResponse(err)
    }
}

#[derive(Debug)]
pub struct ErrorResponse {
    pub severity: Severity,
    pub code: SqlState,
    pub message: String,
    pub detail: Option<String>,
    pub hint: Option<String>,
    pub position: Option<usize>,
}

impl ErrorResponse {
    pub fn fatal<S>(code: SqlState, message: S) -> ErrorResponse
    where
        S: Into<String>,
    {
        ErrorResponse::new(Severity::Fatal, code, message)
    }

    pub fn error<S>(code: SqlState, message: S) -> ErrorResponse
    where
        S: Into<String>,
    {
        ErrorResponse::new(Severity::Error, code, message)
    }

    pub fn notice<S>(code: SqlState, message: S) -> ErrorResponse
    where
        S: Into<String>,
    {
        ErrorResponse::new(Severity::Notice, code, message)
    }

    fn new<S>(severity: Severity, code: SqlState, message: S) -> ErrorResponse
    where
        S: Into<String>,
    {
        ErrorResponse {
            severity,
            code,
            message: message.into(),
            detail: None,
            hint: None,
            position: None,
        }
    }

    pub fn from_adapter_error(severity: Severity, e: AdapterError) -> ErrorResponse {
        // TODO(benesch): we should only use `SqlState::INTERNAL_ERROR` for
        // those errors that are truly internal errors. At the moment we have
        // a various classes of uncategorized errors that use this error code
        // inappropriately.
        let code = match &e {
            // DATA_EXCEPTION to match what Postgres returns for degenerate
            // range bounds
            AdapterError::AbsurdSubscribeBounds { .. } => SqlState::DATA_EXCEPTION,
            AdapterError::AmbiguousSystemColumnReference => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::BadItemInStorageCluster { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::Catalog(_) => SqlState::INTERNAL_ERROR,
            AdapterError::ChangedPlan => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::ModifyLinkedCluster { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::DuplicateCursor(_) => SqlState::DUPLICATE_CURSOR,
            AdapterError::Eval(EvalError::CharacterNotValidForEncoding(_)) => {
                SqlState::PROGRAM_LIMIT_EXCEEDED
            }
            AdapterError::Eval(EvalError::CharacterTooLargeForEncoding(_)) => {
                SqlState::PROGRAM_LIMIT_EXCEEDED
            }
            AdapterError::Eval(EvalError::NullCharacterNotPermitted) => {
                SqlState::PROGRAM_LIMIT_EXCEEDED
            }
            AdapterError::Eval(_) => SqlState::INTERNAL_ERROR,
            AdapterError::Explain(_) => SqlState::INTERNAL_ERROR,
            AdapterError::IdExhaustionError => SqlState::INTERNAL_ERROR,
            AdapterError::Internal(_) => SqlState::INTERNAL_ERROR,
            AdapterError::IntrospectionDisabled { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::InvalidLogDependency { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::InvalidClusterReplicaAz { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::InvalidClusterReplicaSize { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::InvalidStorageClusterSize { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::SourceOrSinkSizeRequired { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::InvalidTableMutationSelection => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::ConstraintViolation(NotNullViolation(_)) => SqlState::NOT_NULL_VIOLATION,
            AdapterError::NoClusterReplicasAvailable(_) => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::OperationProhibitsTransaction(_) => SqlState::ACTIVE_SQL_TRANSACTION,
            AdapterError::OperationRequiresTransaction(_) => SqlState::NO_ACTIVE_SQL_TRANSACTION,
            AdapterError::ParseError(_) => SqlState::SYNTAX_ERROR,
            AdapterError::PlanError(PlanError::InvalidSchemaName) => SqlState::INVALID_SCHEMA_NAME,
            AdapterError::PlanError(_) => SqlState::INTERNAL_ERROR,
            AdapterError::PreparedStatementExists(_) => SqlState::DUPLICATE_PSTATEMENT,
            AdapterError::ReadOnlyTransaction => SqlState::READ_ONLY_SQL_TRANSACTION,
            AdapterError::ReadWriteUnavailable => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::StatementTimeout => SqlState::QUERY_CANCELED,
            AdapterError::IdleInTransactionSessionTimeout => {
                SqlState::IDLE_IN_TRANSACTION_SESSION_TIMEOUT
            }
            AdapterError::RecursionLimit(_) => SqlState::INTERNAL_ERROR,
            AdapterError::RelationOutsideTimeDomain { .. } => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::ResourceExhaustion { .. } => SqlState::INSUFFICIENT_RESOURCES,
            AdapterError::ResultSize(_) => SqlState::OUT_OF_MEMORY,
            AdapterError::SafeModeViolation(_) => SqlState::INTERNAL_ERROR,
            AdapterError::SqlCatalog(_) => SqlState::INTERNAL_ERROR,
            AdapterError::SubscribeOnlyTransaction => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::Transform(_) => SqlState::INTERNAL_ERROR,
            AdapterError::Unauthorized(_) => SqlState::INSUFFICIENT_PRIVILEGE,
            AdapterError::UncallableFunction { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::UnknownCursor(_) => SqlState::INVALID_CURSOR_NAME,
            AdapterError::UnknownPreparedStatement(_) => SqlState::UNDEFINED_PSTATEMENT,
            AdapterError::UnknownLoginRole(_) => SqlState::INVALID_AUTHORIZATION_SPECIFICATION,
            AdapterError::UnknownClusterReplica { .. } => SqlState::UNDEFINED_OBJECT,
            AdapterError::UnmaterializableFunction(_) => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::UnrecognizedConfigurationParam(_) => SqlState::UNDEFINED_OBJECT,
            AdapterError::UnstableDependency { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::Unsupported(..) => SqlState::FEATURE_NOT_SUPPORTED,
            AdapterError::Unstructured(_) => SqlState::INTERNAL_ERROR,
            AdapterError::UntargetedLogRead { .. } => SqlState::FEATURE_NOT_SUPPORTED,
            // It's not immediately clear which error code to use here because a
            // "write-only transaction" and "single table write transaction" are
            // not things in Postgres. This error code is the generic "bad txn thing"
            // code, so it's probably the best choice.
            AdapterError::WriteOnlyTransaction => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::MultiTableWriteTransaction => SqlState::INVALID_TRANSACTION_STATE,
            AdapterError::Storage(_) | AdapterError::Compute(_) | AdapterError::Orchestrator(_) => {
                SqlState::INTERNAL_ERROR
            }
            AdapterError::ConcurrentRoleDrop(_) => SqlState::UNDEFINED_OBJECT,
            AdapterError::VarError(e) => match e {
                VarError::ConstrainedParameter { .. } => SqlState::INVALID_PARAMETER_VALUE,
                VarError::FixedValueParameter(_) => SqlState::INVALID_PARAMETER_VALUE,
                VarError::InvalidParameterType(_) => SqlState::INVALID_PARAMETER_VALUE,
                VarError::InvalidParameterValue { .. } => SqlState::INVALID_PARAMETER_VALUE,
                VarError::ReadOnlyParameter(_) => SqlState::CANT_CHANGE_RUNTIME_PARAM,
                VarError::UnknownParameter(_) => SqlState::UNDEFINED_OBJECT,
            },
        };
        ErrorResponse {
            severity,
            code,
            message: e.to_string(),
            detail: e.detail(),
            hint: e.hint(),
            position: None,
        }
    }

    pub fn from_adapter_notice(notice: AdapterNotice) -> ErrorResponse {
        let code = match &notice {
            AdapterNotice::DatabaseAlreadyExists { .. } => SqlState::DUPLICATE_DATABASE,
            AdapterNotice::SchemaAlreadyExists { .. } => SqlState::DUPLICATE_SCHEMA,
            AdapterNotice::TableAlreadyExists { .. } => SqlState::DUPLICATE_TABLE,
            AdapterNotice::ObjectAlreadyExists { .. } => SqlState::DUPLICATE_OBJECT,
            AdapterNotice::DatabaseDoesNotExist { .. } => SqlState::WARNING,
            AdapterNotice::ClusterDoesNotExist { .. } => SqlState::WARNING,
            AdapterNotice::ExistingTransactionInProgress => SqlState::ACTIVE_SQL_TRANSACTION,
            AdapterNotice::ExplicitTransactionControlInImplicitTransaction => {
                SqlState::NO_ACTIVE_SQL_TRANSACTION
            }
            AdapterNotice::UserRequested { .. } => SqlState::WARNING,
            AdapterNotice::ClusterReplicaStatusChanged { .. } => SqlState::WARNING,
            AdapterNotice::DroppedActiveDatabase { .. } => SqlState::WARNING,
            AdapterNotice::DroppedActiveCluster { .. } => SqlState::WARNING,
            AdapterNotice::QueryTimestamp { .. } => SqlState::WARNING,
            AdapterNotice::EqualSubscribeBounds { .. } => SqlState::WARNING,
            AdapterNotice::QueryTrace { .. } => SqlState::WARNING,
            AdapterNotice::UnimplementedIsolationLevel { .. } => SqlState::WARNING,
            AdapterNotice::DroppedSubscribe { .. } => SqlState::WARNING,
            AdapterNotice::BadStartupSetting { .. } => SqlState::WARNING,
            AdapterNotice::RbacDisabled => SqlState::WARNING,
            AdapterNotice::RoleMembershipAlreadyExists { .. } => SqlState::WARNING,
            AdapterNotice::RoleMembershipDoesNotExists { .. } => SqlState::WARNING,
        };
        ErrorResponse {
            severity: Severity::for_adapter_notice(&notice),
            code,
            message: notice.to_string(),
            detail: notice.detail(),
            hint: notice.hint(),
            position: None,
        }
    }

    pub fn from_startup_message(message: StartupMessage) -> ErrorResponse {
        ErrorResponse {
            severity: Severity::Notice,
            code: SqlState::SUCCESSFUL_COMPLETION,
            message: message.to_string(),
            detail: message.detail(),
            hint: message.hint(),
            position: None,
        }
    }

    pub fn with_position(mut self, position: usize) -> ErrorResponse {
        self.position = Some(position);
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Severity {
    Panic,
    Fatal,
    Error,
    Warning,
    Notice,
    Debug,
    Info,
    Log,
}

impl Severity {
    pub fn is_error(&self) -> bool {
        matches!(self, Severity::Panic | Severity::Fatal | Severity::Error)
    }

    pub fn is_fatal(&self) -> bool {
        matches!(self, Severity::Fatal)
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Severity::Error => "ERROR",
            Severity::Fatal => "FATAL",
            Severity::Panic => "PANIC",
            Severity::Warning => "WARNING",
            Severity::Notice => "NOTICE",
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Log => "LOG",
        }
    }

    /// Checks if a message of a given severity level should be sent to a client.
    ///
    /// The ordering of severity levels used for client-level filtering differs from the
    /// one used for server-side logging in two aspects: INFO messages are always sent,
    /// and the LOG severity is considered as below NOTICE, while it is above ERROR for
    /// server-side logs.
    ///
    /// Postgres only considers the session setting after the client authentication
    /// handshake is completed. Since this function is only called after client authentication
    /// is done, we are not treating this case right now, but be aware if refactoring it.
    pub fn should_output_to_client(&self, minimum_client_severity: &AdapterClientSeverity) -> bool {
        match (minimum_client_severity, self) {
            // INFO messages are always sent
            (_, Severity::Info) => true,
            (AdapterClientSeverity::Error, Severity::Error | Severity::Fatal | Severity::Panic) => {
                true
            }
            (
                AdapterClientSeverity::Warning,
                Severity::Error | Severity::Fatal | Severity::Panic | Severity::Warning,
            ) => true,
            (
                AdapterClientSeverity::Notice,
                Severity::Error
                | Severity::Fatal
                | Severity::Panic
                | Severity::Warning
                | Severity::Notice,
            ) => true,
            (
                AdapterClientSeverity::Info,
                Severity::Error
                | Severity::Fatal
                | Severity::Panic
                | Severity::Warning
                | Severity::Notice,
            ) => true,
            (
                AdapterClientSeverity::Log,
                Severity::Error
                | Severity::Fatal
                | Severity::Panic
                | Severity::Warning
                | Severity::Notice
                | Severity::Log,
            ) => true,
            (
                AdapterClientSeverity::Debug1
                | AdapterClientSeverity::Debug2
                | AdapterClientSeverity::Debug3
                | AdapterClientSeverity::Debug4
                | AdapterClientSeverity::Debug5,
                _,
            ) => true,

            (
                AdapterClientSeverity::Error,
                Severity::Warning | Severity::Notice | Severity::Log | Severity::Debug,
            ) => false,
            (
                AdapterClientSeverity::Warning,
                Severity::Notice | Severity::Log | Severity::Debug,
            ) => false,
            (AdapterClientSeverity::Notice, Severity::Log | Severity::Debug) => false,
            (AdapterClientSeverity::Info, Severity::Log | Severity::Debug) => false,
            (AdapterClientSeverity::Log, Severity::Debug) => false,
        }
    }

    pub fn for_adapter_notice(notice: &AdapterNotice) -> Severity {
        match notice {
            AdapterNotice::DatabaseAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::SchemaAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::TableAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::ObjectAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::DatabaseDoesNotExist { .. } => Severity::Notice,
            AdapterNotice::ClusterDoesNotExist { .. } => Severity::Notice,
            AdapterNotice::ExistingTransactionInProgress => Severity::Warning,
            AdapterNotice::ExplicitTransactionControlInImplicitTransaction => Severity::Warning,
            AdapterNotice::UserRequested { severity } => match severity {
                NoticeSeverity::Debug => Severity::Debug,
                NoticeSeverity::Info => Severity::Info,
                NoticeSeverity::Log => Severity::Log,
                NoticeSeverity::Notice => Severity::Notice,
                NoticeSeverity::Warning => Severity::Warning,
            },
            AdapterNotice::ClusterReplicaStatusChanged { .. } => Severity::Notice,
            AdapterNotice::DroppedActiveDatabase { .. } => Severity::Notice,
            AdapterNotice::DroppedActiveCluster { .. } => Severity::Notice,
            AdapterNotice::QueryTimestamp { .. } => Severity::Notice,
            AdapterNotice::EqualSubscribeBounds { .. } => Severity::Notice,
            AdapterNotice::QueryTrace { .. } => Severity::Notice,
            AdapterNotice::UnimplementedIsolationLevel { .. } => Severity::Notice,
            AdapterNotice::DroppedSubscribe { .. } => Severity::Notice,
            AdapterNotice::BadStartupSetting { .. } => Severity::Notice,
            AdapterNotice::RbacDisabled => Severity::Notice,
            AdapterNotice::RoleMembershipAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::RoleMembershipDoesNotExists { .. } => Severity::Warning,
        }
    }
}

#[derive(Debug)]
pub struct FieldDescription {
    pub name: ColumnName,
    pub table_id: u32,
    pub column_id: u16,
    pub type_oid: u32,
    pub type_len: i16,
    pub type_mod: i32,
    pub format: mz_pgrepr::Format,
}

pub fn encode_row_description(
    desc: &RelationDesc,
    formats: &[mz_pgrepr::Format],
) -> Vec<FieldDescription> {
    desc.iter()
        .zip_eq(formats)
        .map(|((name, typ), format)| {
            let pg_type = mz_pgrepr::Type::from(&typ.scalar_type);
            FieldDescription {
                name: name.clone(),
                table_id: 0,
                column_id: 0,
                type_oid: pg_type.oid(),
                type_len: pg_type.typlen(),
                type_mod: pg_type.typmod(),
                format: *format,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_should_output_to_client() {
        #[rustfmt::skip]
        let test_cases = [
            (AdapterClientSeverity::Debug1, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (AdapterClientSeverity::Debug2, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (AdapterClientSeverity::Debug3, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (AdapterClientSeverity::Debug4, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (AdapterClientSeverity::Debug5, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (AdapterClientSeverity::Log, vec![Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (AdapterClientSeverity::Log, vec![Severity::Debug], false),
            (AdapterClientSeverity::Info, vec![Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (AdapterClientSeverity::Info, vec![Severity::Debug, Severity::Log], false),
            (AdapterClientSeverity::Notice, vec![Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (AdapterClientSeverity::Notice, vec![Severity::Debug, Severity::Log], false),
            (AdapterClientSeverity::Warning, vec![Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (AdapterClientSeverity::Warning, vec![Severity::Debug, Severity::Log, Severity::Notice], false),
            (AdapterClientSeverity::Error, vec![Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (AdapterClientSeverity::Error, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning], false),
        ];

        for test_case in test_cases {
            run_test(test_case)
        }

        fn run_test(test_case: (AdapterClientSeverity, Vec<Severity>, bool)) {
            let client_min_messages_setting = test_case.0;
            let expected = test_case.2;
            for message_severity in test_case.1 {
                assert!(
                    message_severity.should_output_to_client(&client_min_messages_setting)
                        == expected
                )
            }
        }
    }
}
