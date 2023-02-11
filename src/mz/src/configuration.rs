// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::str::FromStr;
use std::{collections::BTreeMap, fmt::Display, fs, path::PathBuf};

use anyhow::{bail, Context};
use dirs::home_dir;
use once_cell::sync::Lazy;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::api::CloudProviderRegion;
use crate::vault::Token;

pub const WEB_DOCS_URL: &str = "https://www.materialize.com/docs";

pub static DEFAULT_ENDPOINT: Lazy<Endpoint> =
    Lazy::new(|| "https://cloud.materialize.com".parse().unwrap());

/// A Materialize Cloud API endpoint.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(transparent)]
pub struct Endpoint {
    url: Url,
}

impl Endpoint {
    /// Returns the URL for the cloud regions.
    pub fn cloud_regions_url(&self) -> Url {
        self.with_path(&["_metadata", "cloud-regions.json"])
    }

    /// Returns the URL for the OAuth token exchange.
    pub fn web_login_url(&self, profile_name: &str, port: u16) -> Url {
        let mut url = self.with_path(&["account", "login"]);
        let mut query_pairs = url.query_pairs_mut();
        query_pairs.append_pair(
            "redirectUrl",
            &format!("/access/cli?redirectUri=http://localhost:{port}"),
        );
        query_pairs.append_pair("profile", profile_name);
        drop(query_pairs);
        url
    }

    /// Returns the URL for reading API tokens.
    pub fn api_token_url(&self) -> Url {
        self.admin_with_path(&["identity", "resources", "users", "api-tokens", "v1"])
    }

    /// Returns the URL for authenticating with an email and password.
    pub fn user_auth_url(&self) -> Url {
        self.admin_with_path(&["identity", "resources", "auth", "v1", "user"])
    }

    /// Returns the URL for authenticating with an API token.
    pub fn api_token_auth_url(&self) -> Url {
        self.admin_with_path(&["identity", "resources", "auth", "v1", "api-token"])
    }

    /// Reports whether this is the default API endpoint.
    pub fn is_default(&self) -> bool {
        *self == *DEFAULT_ENDPOINT
    }

    fn with_path(&self, path: &[&str]) -> Url {
        let mut url = self.url.clone();
        url.path_segments_mut()
            .expect("constructor validated URL can be a base")
            .extend(path);
        url
    }

    fn admin_with_path(&self, path: &[&str]) -> Url {
        let mut url = self.with_path(path);
        let host = url.host().expect("constructor validated URL has host");
        url.set_host(Some(&format!("admin.{host}"))).unwrap();
        url
    }
}

impl Default for Endpoint {
    fn default() -> Endpoint {
        DEFAULT_ENDPOINT.clone()
    }
}

impl FromStr for Endpoint {
    type Err = url::ParseError;

    fn from_str(s: &str) -> Result<Endpoint, url::ParseError> {
        let url: Url = s.parse()?;
        if !url.has_host() {
            Err(url::ParseError::EmptyHost)
        } else {
            Ok(Endpoint { url })
        }
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.url.fmt(f)
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Profile0 {
    email: String,
    #[serde(rename(serialize = "app-password", deserialize = "app-password"))]
    #[serde(default, skip_serializing_if = "Token::is_default")]
    app_password: Token,
    region: Option<CloudProviderRegion>,
    #[serde(default, skip_serializing_if = "Endpoint::is_default")]
    endpoint: Endpoint,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Configuration {
    #[serde(skip)]
    modified: bool,
    current_profile: String,
    profiles: BTreeMap<String, Profile0>,
}

pub struct Profile<'a> {
    _modified: &'a mut bool,
    profile: &'a mut Profile0,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FronteggAuth {
    pub access_token: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FronteggAPIToken {
    pub client_id: String,
    pub secret: String,
}

pub struct ValidProfile<'a> {
    pub profile: &'a Profile<'a>,
    pub frontegg_auth: FronteggAuth,
    pub app_password: String,
}

#[allow(dead_code)]
impl Configuration {
    const PROFILES_DIR_NAME: &str = ".config/mz";
    const PROFILES_FILE_NAME: &str = "profiles.toml";
    const DEFAULT_PROFILE: &str = "default";

    pub fn load(profile: Option<&str>) -> Result<Configuration, anyhow::Error> {
        fn path_exists(path: &PathBuf) -> bool {
            fs::metadata(path).is_ok()
        }

        let mut config_path = get_config_path()?;

        if !path_exists(&config_path) {
            fs::create_dir_all(config_path.as_path())
                .context("failed to create directory for configuration file")?;
        };

        config_path.push(Self::PROFILES_FILE_NAME);

        let mut config = path_exists(&config_path)
            .then(|| {
                let contents = fs::read_to_string(&config_path)
                    .context("failed to read configuration file")?;

                let config = toml::from_str::<Configuration>(&contents).with_context(|| {
                    format!(
                        "failed to read profiles from configuration file {}",
                        config_path.into_os_string().to_string_lossy()
                    )
                })?;

                Ok::<_, anyhow::Error>(config)
            })
            .unwrap_or_else(|| Ok(Configuration::default()))?;

        if let Some(profile) = profile {
            config.current_profile = profile.into();
        }

        Ok(config)
    }

    pub fn current_profile(&self) -> String {
        self.current_profile.to_string()
    }

    pub fn get_profile(&mut self) -> Result<Profile, anyhow::Error> {
        let profile = &self.current_profile;
        self.profiles
            .get_mut(profile)
            .map(|p| Profile {
                _modified: &mut self.modified,
                profile: p,
            })
            .context("Profile not found. Please, add one or login using `mz login`.")
    }

    pub fn get_profiles(&self, profile: Option<String>) -> Vec<String> {
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

    pub fn update_current_profile(&mut self, profile: String) {
        self.modified = true;
        self.current_profile = profile;
    }

    pub fn create_or_update_profile(
        &mut self,
        endpoint: Endpoint,
        name: String,
        email: String,
        token: Token,
    ) {
        self.modified = true;
        let region = self.profiles.get(&name).and_then(|p| p.region);
        self.profiles.insert(
            name,
            Profile0 {
                email,
                app_password: token,
                region,
                endpoint,
            },
        );
    }

    pub fn close(self) -> Result<(), anyhow::Error> {
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

fn get_config_path() -> Result<PathBuf, anyhow::Error> {
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

impl Profile<'_> {
    pub fn endpoint(&self) -> &Endpoint {
        &self.profile.endpoint
    }

    pub fn get_email(&self) -> &str {
        &self.profile.email
    }

    pub fn get_default_region(&self) -> Option<CloudProviderRegion> {
        self.profile.region
    }

    pub fn set_default_region(&mut self, region: CloudProviderRegion) {
        *self._modified = true;
        self.profile.region = Some(region)
    }

    pub fn get_frontegg_api_token(&self, name: &str) -> Result<FronteggAPIToken, anyhow::Error> {
        self.profile
            .app_password
            .retrieve(name, &self.profile.email)?
            .as_str()
            .try_into()
    }

    pub async fn validate(
        &self,
        name: &str,
        client: &Client,
    ) -> Result<ValidProfile<'_>, anyhow::Error> {
        let api_token: FronteggAPIToken = self.get_frontegg_api_token(name)?;

        let authentication_result = client
            .post(self.endpoint().api_token_auth_url())
            .json(&api_token)
            .send()
            .await
            .context("failed to connect to server")?;

        if authentication_result.status() == 401 {
            bail!("failed to validate profile. reauthorize using mz login --force [profile]");
        }

        let auth = authentication_result
            .json::<FronteggAuth>()
            .await
            .context("failed to parse results from server")?;

        Ok(ValidProfile {
            profile: self,
            frontegg_auth: auth,
            app_password: api_token.to_string(),
        })
    }
}

impl TryFrom<&str> for FronteggAPIToken {
    type Error = anyhow::Error;

    fn try_from(value: &str) -> Result<Self, anyhow::Error> {
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
