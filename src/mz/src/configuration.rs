// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    collections::{BTreeMap, HashMap},
    fmt::Display,
    fs,
    path::PathBuf,
};

use anyhow::{bail, Context, Ok, Result};
use dirs::home_dir;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE, USER_AGENT};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::region::CloudProviderRegion;

#[derive(Serialize, Deserialize, Debug)]
struct Profile0 {
    email: String,
    #[serde(rename(serialize = "app-password", deserialize = "app-password"))]
    app_password: String,
    region: Option<CloudProviderRegion>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct Configuration {
    #[serde(skip)]
    modified: bool,
    current_profile: String,
    profiles: BTreeMap<String, Profile0>,
}

pub(crate) struct Profile<'a> {
    _modified: &'a mut bool,
    profile: &'a mut Profile0,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FronteggAuth {
    pub access_token: String,
}

#[derive(Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub(crate) struct FronteggAPIToken {
    pub(crate) client_id: String,
    pub(crate) secret: String,
}

pub(crate) struct ValidProfile<'a> {
    pub(crate) profile: &'a Profile<'a>,
    pub(crate) frontegg_auth: FronteggAuth,
}

#[allow(dead_code)]
impl Configuration {
    const PROFILES_DIR_NAME: &str = ".config/mz";
    const PROFILES_FILE_NAME: &str = "profiles.toml";
    const DEFAULT_PROFILE: &str = "default";

    pub(crate) fn load() -> Result<Configuration> {
        fn path_exists(path: &PathBuf) -> bool {
            fs::metadata(path).is_ok()
        }

        let mut config_path = get_config_path()?;

        if !path_exists(&config_path) {
            fs::create_dir_all(config_path.as_path())
                .context("failed to create directory for configuration file")?;
        };

        config_path.push(Self::PROFILES_FILE_NAME);

        path_exists(&config_path)
            .then(|| {
                let contents = fs::read_to_string(&config_path)
                    .context("failed to read configuration file")?;

                let config = toml::from_str::<Configuration>(&contents).with_context(|| {
                    format!(
                        "failed to read profiles from configuration file {}",
                        config_path.into_os_string().to_string_lossy()
                    )
                })?;

                Ok(config)
            })
            .unwrap_or_else(|| Ok(Configuration::default()))
    }

    pub(crate) fn current_profile(&self, profile: Option<String>) -> String {
        profile.unwrap_or_else(|| self.current_profile.clone())
    }

    pub(crate) fn get_profile(&mut self, profile: Option<String>) -> Result<Profile> {
        let profile = self.current_profile(profile);
        self.profiles
            .get_mut(&profile)
            .map(|p| Profile {
                _modified: &mut self.modified,
                profile: p,
            })
            .context("Profile not found. Please, add one or login using `mz login`.")
    }

    pub(crate) fn get_profiles(&self, profile: Option<String>) -> Vec<String> {
        let mut keys = self
            .profiles
            .keys()
            .cloned()
            .chain(profile.into_iter())
            .collect::<Vec<_>>();

        keys.push(self.current_profile.clone());
        keys.sort();
        keys.dedup();
        keys
    }

    pub(crate) fn update_default_profile(&mut self, profile: String) {
        self.modified = true;
        self.current_profile = profile;
    }

    pub(crate) fn create_or_update_profile(
        &mut self,
        name: String,
        email: String,
        api_token: FronteggAPIToken,
    ) {
        self.modified = true;
        self.profiles.insert(
            name,
            Profile0 {
                email,
                app_password: api_token.to_string(),
                region: None,
            },
        );
    }

    pub(crate) fn close(self) -> Result<()> {
        if !self.modified {
            return Ok(());
        }

        let mut config_path = get_config_path()?;
        config_path.push(Self::PROFILES_FILE_NAME);

        let contents =
            toml::to_string_pretty(&self).context("failed to write out updated configuration")?;

        fs::write(config_path, contents).context("failed to write out updated configuration")
    }
}

fn get_config_path() -> Result<PathBuf> {
    home_dir()
        .map(|mut path| {
            path.push(Configuration::PROFILES_DIR_NAME);
            path
        })
        .context("failed to find $HOME directory")
}

impl Default for Configuration {
    fn default() -> Self {
        Self {
            modified: false,
            current_profile: Self::DEFAULT_PROFILE.to_string(),
            profiles: Default::default(),
        }
    }
}

#[allow(dead_code)]
impl Profile<'_> {
    pub(crate) fn get_email(&self) -> &str {
        &self.profile.email
    }

    pub(crate) fn get_app_password(&self) -> &str {
        &self.profile.app_password
    }

    pub(crate) fn get_default_region(&self) -> Option<CloudProviderRegion> {
        self.profile.region
    }

    pub(crate) fn set_email(&mut self, email: String) {
        *self._modified = true;
        self.profile.email = email;
    }

    pub(crate) fn set_password(&mut self, password: String) {
        *self._modified = true;
        self.profile.app_password = password;
    }

    pub(crate) fn set_default_region(&mut self, region: CloudProviderRegion) {
        *self._modified = true;
        self.profile.region = Some(region)
    }

    pub(crate) async fn validate(&self, client: &Client) -> Result<ValidProfile<'_>> {
        let api_token: FronteggAPIToken = self.profile.app_password.as_str().try_into()?;
        let mut access_token_request_body = HashMap::new();
        access_token_request_body.insert("clientId", api_token.client_id.as_str());
        access_token_request_body.insert("secret", api_token.secret.as_str());

        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("reqwest"));
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

        let authentication_result = client
            .post(crate::MACHINE_AUTH_URL)
            .headers(headers)
            .json(&access_token_request_body)
            .send()
            .await
            .context("failed to connect to server")?;

        if authentication_result.status() == 401 {
            bail!("Unauthorized. Please, check the credentials.");
        }

        let auth = authentication_result
            .json::<FronteggAuth>()
            .await
            .context("failed to parse results from server")?;

        Ok(ValidProfile {
            profile: self,
            frontegg_auth: auth,
        })
    }
}

impl TryFrom<&str> for FronteggAPIToken {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self> {
        if value.len() != 68 || !value.starts_with("mzp_") {
            bail!("api tokens must be exactly 68 characters and begin with mzp_")
        }

        let client_id =
            Uuid::parse_str(&value[4..36]).context("failed to parse client_id from api_token")?;

        let secret =
            Uuid::parse_str(&value[36..68]).context("failed to parse secret from api_token")?;

        Ok(FronteggAPIToken {
            client_id: client_id.to_string(),
            secret: secret.to_string(),
        })
    }
}

impl Display for FronteggAPIToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts = format!("mzp_{}{}", self.client_id, self.secret).replace('-', "");
        write!(f, "{}", parts)
    }
}
