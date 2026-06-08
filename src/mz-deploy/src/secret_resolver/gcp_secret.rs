// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Google Cloud Secret Manager secret provider.
//!
//! Resolves secret values by reading from GCP Secret Manager. The provider
//! accepts either a bare secret name (resolved under the configured
//! `gcp_project` at version `latest`) or a full GCP resource path of the
//! form `projects/PROJECT/secrets/NAME[/versions/VERSION]`, which lets
//! callers override the configured project or pin a specific version.
//!
//! `gcp_project` is optional: full-path calls work without it.
//!
//! Credentials are resolved by gcloud-sdk's Application Default Credentials
//! chain (env-var service account, `~/.config/gcloud`, workload identity).

use super::json_field::extract_json_field;
use super::{SecretProvider, SecretResolveError};
use async_trait::async_trait;
use gcloud_sdk::GoogleApi;
use gcloud_sdk::google::cloud::secretmanager::v1::{
    AccessSecretVersionRequest, secret_manager_service_client::SecretManagerServiceClient,
};
use std::ops::RangeInclusive;
use tokio::sync::OnceCell;

const PROVIDER_NAME: &str = "gcp_secret";
const GCP_SECRETMANAGER_URL: &str = "https://secretmanager.googleapis.com";

type GcpClient = GoogleApi<SecretManagerServiceClient<gcloud_sdk::GoogleAuthMiddleware>>;

/// Resolves secrets from Google Cloud Secret Manager.
///
/// `project` is the optional default project ID used for bare-name calls;
/// full-path calls (`projects/.../secrets/...`) work regardless.
///
/// The gcloud-sdk client (including credential resolution) is loaded lazily
/// on the first `resolve()` call, so projects that never invoke
/// `gcp_secret()` pay no startup cost.
pub(super) struct GcpSecretProvider {
    project: Option<String>,
    client: OnceCell<GcpClient>,
}

impl GcpSecretProvider {
    pub(super) fn new(project: Option<&str>) -> Self {
        Self {
            project: project.map(str::to_string),
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Result<&GcpClient, SecretResolveError> {
        self.client
            .get_or_try_init(|| async {
                GoogleApi::from_function(
                    SecretManagerServiceClient::new,
                    GCP_SECRETMANAGER_URL,
                    None,
                )
                .await
                .map_err(|e| SecretResolveError::ResolutionFailed {
                    name: PROVIDER_NAME.to_string(),
                    reason: format!("failed to initialize GCP client: {}", e),
                })
            })
            .await
    }
}

#[async_trait]
impl SecretProvider for GcpSecretProvider {
    fn name(&self) -> &str {
        PROVIDER_NAME
    }

    fn accepted_args(&self) -> RangeInclusive<usize> {
        1..=2
    }

    async fn resolve(&self, args: &[String]) -> Result<String, SecretResolveError> {
        let secret_arg = &args[0];
        let resource_path = resolve_resource_path(secret_arg, self.project.as_deref())?;

        let api = self.client().await?;
        let mut client = api.get();
        let response = client
            .access_secret_version(AccessSecretVersionRequest {
                name: resource_path.clone(),
            })
            .await
            .map_err(|e| SecretResolveError::ResolutionFailed {
                name: PROVIDER_NAME.to_string(),
                reason: format!("failed to fetch secret '{}': {}", secret_arg, e),
            })?;

        let payload =
            response
                .into_inner()
                .payload
                .ok_or_else(|| SecretResolveError::ResolutionFailed {
                    name: PROVIDER_NAME.to_string(),
                    reason: format!("secret '{}' returned an empty payload", secret_arg),
                })?;

        let secret_string = payload.data.sensitive_value_to_str().map_err(|_| {
            SecretResolveError::ResolutionFailed {
                name: PROVIDER_NAME.to_string(),
                reason: format!(
                    "secret '{}' is a binary secret; only text secrets are supported",
                    secret_arg
                ),
            }
        })?;

        match args.get(1) {
            None => Ok(secret_string.to_string()),
            Some(json_key) => {
                extract_json_field(secret_string, json_key, secret_arg).map_err(|reason| {
                    SecretResolveError::ResolutionFailed {
                        name: PROVIDER_NAME.to_string(),
                        reason,
                    }
                })
            }
        }
    }
}

/// Expand a user-provided arg into a fully qualified GCP Secret Manager
/// resource path.
///
/// - `projects/.../secrets/...` (full path): used verbatim, with
///   `/versions/latest` appended if no version is specified. The configured
///   project is ignored.
/// - Bare name: requires `configured_project`; expands to
///   `projects/<p>/secrets/<n>/versions/latest`.
/// - Bare name with no configured project: returns a `ResolutionFailed`
///   error directing the user at either setting `gcp_project` or passing a
///   full resource path.
fn resolve_resource_path(
    arg: &str,
    configured_project: Option<&str>,
) -> Result<String, SecretResolveError> {
    if arg.starts_with("projects/") {
        if arg.contains("/versions/") {
            Ok(arg.to_string())
        } else {
            Ok(format!("{}/versions/latest", arg))
        }
    } else {
        match configured_project {
            Some(project) => Ok(format!(
                "projects/{}/secrets/{}/versions/latest",
                project, arg
            )),
            None => Err(SecretResolveError::ResolutionFailed {
                name: PROVIDER_NAME.to_string(),
                reason: format!(
                    "no 'gcp_project' configured; either set it under \
                     [<profile>.security] in project.toml or pass a full \
                     'projects/.../secrets/{}' resource path",
                    arg
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_resource_path_bare_name_with_project() {
        let path = resolve_resource_path("db-password", Some("my-proj")).unwrap();
        assert_eq!(path, "projects/my-proj/secrets/db-password/versions/latest");
    }

    #[test]
    fn resolve_resource_path_bare_name_without_project_errors() {
        let err = resolve_resource_path("db-password", None).unwrap_err();
        match err {
            SecretResolveError::ResolutionFailed { name, reason } => {
                assert_eq!(name, PROVIDER_NAME);
                assert!(reason.contains("gcp_project"));
                assert!(reason.contains("db-password"));
                assert!(reason.contains("projects/"));
            }
            other => panic!("expected ResolutionFailed, got: {:?}", other),
        }
    }

    #[test]
    fn resolve_resource_path_full_path_appends_latest() {
        let path =
            resolve_resource_path("projects/other-proj/secrets/api-key", Some("ignored")).unwrap();
        assert_eq!(path, "projects/other-proj/secrets/api-key/versions/latest");
    }

    #[test]
    fn resolve_resource_path_full_path_with_version_passthrough() {
        let path = resolve_resource_path("projects/p/secrets/db-pw/versions/3", None).unwrap();
        assert_eq!(path, "projects/p/secrets/db-pw/versions/3");
    }

    #[test]
    fn resolve_resource_path_full_path_ignores_configured_project() {
        let path = resolve_resource_path(
            "projects/override/secrets/foo/versions/latest",
            Some("default-proj"),
        )
        .unwrap();
        assert_eq!(path, "projects/override/secrets/foo/versions/latest");
    }
}
