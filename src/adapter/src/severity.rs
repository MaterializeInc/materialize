// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_sql::ast::NoticeSeverity;
use mz_sql::plan::PlanNotice;
use mz_sql::session::vars::ClientSeverity;

use crate::AdapterNotice;

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
    pub fn should_output_to_client(&self, minimum_client_severity: &ClientSeverity) -> bool {
        match (minimum_client_severity, self) {
            // INFO messages are always sent
            (_, Severity::Info) => true,
            (ClientSeverity::Error, Severity::Error | Severity::Fatal | Severity::Panic) => true,
            (
                ClientSeverity::Warning,
                Severity::Error | Severity::Fatal | Severity::Panic | Severity::Warning,
            ) => true,
            (
                ClientSeverity::Notice,
                Severity::Error
                | Severity::Fatal
                | Severity::Panic
                | Severity::Warning
                | Severity::Notice,
            ) => true,
            (
                ClientSeverity::Info,
                Severity::Error
                | Severity::Fatal
                | Severity::Panic
                | Severity::Warning
                | Severity::Notice,
            ) => true,
            (
                ClientSeverity::Log,
                Severity::Error
                | Severity::Fatal
                | Severity::Panic
                | Severity::Warning
                | Severity::Notice
                | Severity::Log,
            ) => true,
            (
                ClientSeverity::Debug1
                | ClientSeverity::Debug2
                | ClientSeverity::Debug3
                | ClientSeverity::Debug4
                | ClientSeverity::Debug5,
                _,
            ) => true,

            (
                ClientSeverity::Error,
                Severity::Warning | Severity::Notice | Severity::Log | Severity::Debug,
            ) => false,
            (ClientSeverity::Warning, Severity::Notice | Severity::Log | Severity::Debug) => false,
            (ClientSeverity::Notice, Severity::Log | Severity::Debug) => false,
            (ClientSeverity::Info, Severity::Log | Severity::Debug) => false,
            (ClientSeverity::Log, Severity::Debug) => false,
        }
    }

    /// Returns the severity for a notice.
    pub fn for_adapter_notice(notice: &AdapterNotice) -> Severity {
        match notice {
            AdapterNotice::DatabaseAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::SchemaAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::TableAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::ObjectAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::DatabaseDoesNotExist { .. } => Severity::Notice,
            AdapterNotice::ClusterDoesNotExist { .. } => Severity::Notice,
            AdapterNotice::NoResolvableSearchPathSchema { .. } => Severity::Notice,
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
            AdapterNotice::RbacSystemDisabled => Severity::Notice,
            AdapterNotice::RbacUserDisabled => Severity::Notice,
            AdapterNotice::RoleMembershipAlreadyExists { .. } => Severity::Notice,
            AdapterNotice::RoleMembershipDoesNotExists { .. } => Severity::Warning,
            AdapterNotice::AutoRunOnIntrospectionCluster => Severity::Debug,
            AdapterNotice::AlterIndexOwner { .. } => Severity::Warning,
            AdapterNotice::CannotRevoke { .. } => Severity::Warning,
            AdapterNotice::NonApplicablePrivilegeTypes { .. } => Severity::Notice,
            AdapterNotice::PlanNotice(notice) => match notice {
                PlanNotice::ObjectDoesNotExist { .. } => Severity::Notice,
                PlanNotice::UpsertSinkKeyNotEnforced { .. } => Severity::Warning,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn test_should_output_to_client() {
        #[rustfmt::skip]
        let test_cases = [
            (ClientSeverity::Debug1, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Debug2, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Debug3, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Debug4, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Debug5, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Log, vec![Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Log, vec![Severity::Debug], false),
            (ClientSeverity::Info, vec![Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Info, vec![Severity::Debug, Severity::Log], false),
            (ClientSeverity::Notice, vec![Severity::Notice, Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Notice, vec![Severity::Debug, Severity::Log], false),
            (ClientSeverity::Warning, vec![Severity::Warning, Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Warning, vec![Severity::Debug, Severity::Log, Severity::Notice], false),
            (ClientSeverity::Error, vec![Severity::Error, Severity::Fatal, Severity:: Panic, Severity::Info], true),
            (ClientSeverity::Error, vec![Severity::Debug, Severity::Log, Severity::Notice, Severity::Warning], false),
        ];

        for test_case in test_cases {
            run_test(test_case)
        }

        fn run_test(test_case: (ClientSeverity, Vec<Severity>, bool)) {
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
