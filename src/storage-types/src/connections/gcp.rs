// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! GCP configuration for sources and sinks.

use gcp_auth::{CustomServiceAccount, TokenProvider};
use mz_ore::error::ErrorExt;
use mz_repr::{CatalogItemId, GlobalId};
use serde::{Deserialize, Serialize};

use crate::AlterCompatible;
use crate::configuration::StorageConfiguration;
use crate::controller::AlterError;

/// Scope used when probing the credentials during validation. Picked because
/// every service-account key is allowed to mint tokens for it, so a successful
/// response confirms the key is well-formed and accepted by Google's token
/// endpoint without requiring any specific IAM grants on the service account.
const VALIDATION_SCOPE: &str = "https://www.googleapis.com/auth/cloud-platform";

/// GCP connection configuration.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct GcpConnection {
    /// Secret containing a GCP service-account key in JSON format,
    /// as produced by `gcloud iam service-accounts keys create`.
    pub credentials_json: CatalogItemId,
}

impl AlterCompatible for GcpConnection {
    fn alter_compatible(&self, _id: GlobalId, _other: &Self) -> Result<(), AlterError> {
        // Every element of the GCP connection is configurable.
        Ok(())
    }
}

impl GcpConnection {
    /// Validates this connection by reading the service-account key out of the
    /// secrets store, parsing it, and exchanging it for an OAuth2 access token
    /// at Google's token endpoint.
    pub(crate) async fn validate(
        &self,
        _id: CatalogItemId,
        storage_configuration: &StorageConfiguration,
    ) -> Result<(), GcpConnectionValidationError> {
        let json = storage_configuration
            .connection_context
            .secrets_reader
            .read_string(self.credentials_json)
            .await
            .map_err(GcpConnectionValidationError::SecretRead)?;
        let service_account = CustomServiceAccount::from_json(&json)
            .map_err(GcpConnectionValidationError::ParseKey)?;
        service_account
            .token(&[VALIDATION_SCOPE])
            .await
            .map_err(GcpConnectionValidationError::FetchToken)?;
        Ok(())
    }

    pub(crate) fn validate_by_default(&self) -> bool {
        false
    }
}

/// An error returned by `GcpConnection::validate`.
#[derive(thiserror::Error, Debug)]
pub enum GcpConnectionValidationError {
    #[error("failed to read service-account key from secret store: {}", .0.display_with_causes())]
    SecretRead(#[source] anyhow::Error),
    #[error("failed to parse service-account key JSON: {}", .0.display_with_causes())]
    ParseKey(#[source] gcp_auth::Error),
    #[error("failed to obtain access token from Google: {}", .0.display_with_causes())]
    FetchToken(#[source] gcp_auth::Error),
}

impl GcpConnectionValidationError {
    pub fn detail(&self) -> Option<String> {
        None
    }

    pub fn hint(&self) -> Option<String> {
        match self {
            GcpConnectionValidationError::ParseKey(_) => Some(
                "The secret must hold the JSON output of `gcloud iam service-accounts keys create`."
                    .into(),
            ),
            _ => None,
        }
    }
}
