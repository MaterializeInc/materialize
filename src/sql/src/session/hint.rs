// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal::Private;

/// Prometheus label for [`ApplicationNameHint::Unspecified`].
const UNSPECIFIED_LABEL: &str = "unspecified";
/// Prometheus label for [`ApplicationNameHint::Unrecognized`].
const UNRECOGNIZED_LABEL: &str = "unrecognized";
/// Prometheus label for [`ApplicationNameHint::Psql`].
const PSQL_LABEL: &str = "psql";
/// Prometheus label for [`ApplicationNameHint::Dbt`].
const DBT_LABEL: &str = "dbt";
/// Prometheus label for [`ApplicationNameHint::WebConsole`].
const WEB_CONSOLE_LABEL: &str = "web_console";
/// Promehteus label for [`ApplicationNameHint::WebConsoleShell`].
const WEB_CONSOLE_SHELL_LABEL: &str = "web_console_shell";
/// Prometheus label for [`ApplicationNameHint::MzPsql`].
const MZ_PSQL_LABEL: &str = "mz_psql";
/// Prometheus label for [`ApplicationNameHint::MaterializeFivetranDestination`].
const MATERIALIZE_FIVETRAN_DESTINATION_LABEL: &str = "materialize_fivetran_destination";
/// Prometheus label for [`ApplicationNameHint::TerraformProviderMaterialize`].
const TERRAFORM_PROVIDER_MATERIALIZE_LABEL: &str = "terraform_provider_materialize";
/// Prometheus label for [`ApplicationNameHint::TablePlus`].
const TABLE_PLUS_LABEL: &str = "table_plus";
/// Prometheus label for [`ApplicationNameHint::DataGrip`].
const DATA_GRIP_LABEL: &str = "data_grip";
/// Prometheus label for [`ApplicationNameHint::DBeaver`].
const D_BEAVER_LABEL: &str = "dbeaver";
/// Prometheus label for [`ApplicationNameHint::MzVscode`].
const MZ_VSCODE_LABEL: &str = "mz_vscode";
/// Prometheus label for [`ApplicationNameHint::MzGrafanaIntegration`].
const MZ_GRAFANA_LABEL: &str = "mz_grafana";

/// A hint for what application is making a request to the adapter.
///
/// Note: [`ApplicationNameHint`] gets logged as a label in our Prometheus metrics, and for
/// labels we need to be conscious of the cardinality, so please be careful with how many
/// variants we add to this enum.
///
/// Note: each enum variant contains an `internal::Private` to prevent creating this enum
/// directly. To create an instance of [`ApplicationNameHint`] please see
/// [`ApplicationNameHint::from_str`].
#[derive(Debug, Copy, Clone)]
pub enum ApplicationNameHint {
    /// No `application_name` was set.
    Unspecified(Private),
    /// An `application_name` was set, but it's not one we recognize.
    Unrecognized(Private),
    /// Request came from `psql`.
    Psql(Private),
    /// Request came from `dbt`.
    Dbt(Private),
    /// Request came from our web console.
    WebConsole(Private),
    /// Request came from the SQL shell in our web console.
    WebConsoleShell(Private),
    /// Request came from the `psql` shell spawned by `mz`.
    MzPsql(Private),
    /// Request came from our Fivetran Destination,
    MaterializeFivetranDestination(Private),
    /// Request came from a version of our Terraform provider.
    TerraformProviderMaterialize(Private),
    /// Request came from TablePlus.
    TablePlus(Private),
    /// Request came from a version of DataGrip.
    DataGrip(Private),
    /// Request came from a version of DBeaver.
    DBeaver(Private),
    /// Request came from our Visual Studio Code integration.
    MzVscode(Private),
    /// Request came from our Grafana integration.
    MzGrafanaIntegration(Private),
}

impl ApplicationNameHint {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "psql" => ApplicationNameHint::Psql(Private),
            "dbt" => ApplicationNameHint::Dbt(Private),
            "web_console" => ApplicationNameHint::WebConsole(Private),
            "web_console_shell" => ApplicationNameHint::WebConsoleShell(Private),
            "mz_psql" => ApplicationNameHint::MzPsql(Private),
            // Note: Make sure this is kept in sync with the `fivetran-destination` crate.
            "materialize_fivetran_destination" => {
                ApplicationNameHint::MaterializeFivetranDestination(Private)
            }
            "tableplus" => ApplicationNameHint::TablePlus(Private),
            "mz_vscode" => ApplicationNameHint::MzVscode(Private),
            "mz_grafana_integration" => ApplicationNameHint::MzGrafanaIntegration(Private),
            // Terraform provides the version as a suffix.
            x if x.starts_with("terraform-provider-materialize") => {
                ApplicationNameHint::TerraformProviderMaterialize(Private)
            }
            // DataGrip provides the version as a suffix.
            x if x.starts_with("datagrip") => ApplicationNameHint::DataGrip(Private),
            // DBeaver provides the version as a suffix.
            x if x.starts_with("dbeaver") => ApplicationNameHint::DBeaver(Private),
            "" => ApplicationNameHint::Unspecified(Private),
            // TODO(parkertimmerman): We should keep some record of these "unrecognized"
            // names, and possibly support more popular ones in the future.
            _ => ApplicationNameHint::Unrecognized(Private),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            ApplicationNameHint::Unspecified(_) => UNSPECIFIED_LABEL,
            ApplicationNameHint::Unrecognized(_) => UNRECOGNIZED_LABEL,
            ApplicationNameHint::Psql(_) => PSQL_LABEL,
            ApplicationNameHint::Dbt(_) => DBT_LABEL,
            ApplicationNameHint::WebConsole(_) => WEB_CONSOLE_LABEL,
            ApplicationNameHint::WebConsoleShell(_) => WEB_CONSOLE_SHELL_LABEL,
            ApplicationNameHint::MzPsql(_) => MZ_PSQL_LABEL,
            ApplicationNameHint::MaterializeFivetranDestination(_) => {
                MATERIALIZE_FIVETRAN_DESTINATION_LABEL
            }
            ApplicationNameHint::TablePlus(_) => TABLE_PLUS_LABEL,
            ApplicationNameHint::MzVscode(_) => MZ_VSCODE_LABEL,
            ApplicationNameHint::MzGrafanaIntegration(_) => MZ_GRAFANA_LABEL,
            ApplicationNameHint::TerraformProviderMaterialize(_) => {
                TERRAFORM_PROVIDER_MATERIALIZE_LABEL
            }
            ApplicationNameHint::DataGrip(_) => DATA_GRIP_LABEL,
            ApplicationNameHint::DBeaver(_) => D_BEAVER_LABEL,
        }
    }

    /// Returns whether or not we should trace errors for this requests with this application name.
    pub fn should_trace_errors(&self) -> bool {
        // Note(parkmycar): For now we only trace errors for the web console since we contol all of
        // those queries and in general they should never fail. As opposed to user queries which
        // are arbitrary.
        matches!(self, ApplicationNameHint::WebConsole(_))
    }
}

mod internal {
    #[derive(Debug, Copy, Clone)]
    pub struct Private;
}
