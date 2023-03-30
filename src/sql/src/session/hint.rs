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
}

impl ApplicationNameHint {
    pub fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "psql" => ApplicationNameHint::Psql(Private),
            "dbt" => ApplicationNameHint::Dbt(Private),
            "web_console" => ApplicationNameHint::WebConsole(Private),
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
        }
    }
}

mod internal {
    #[derive(Debug, Copy, Clone)]
    pub struct Private;
}
