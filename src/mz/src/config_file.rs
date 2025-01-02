// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Configuration file management.

use std::fs::OpenOptions;
use std::io::Read;
use std::path::PathBuf;
use std::sync::LazyLock;
use std::{collections::BTreeMap, str::FromStr};

use maplit::btreemap;
use mz_ore::str::StrExt;
use serde::{Deserialize, Serialize};
use tokio::fs;
use toml_edit::{value, Document};

#[cfg(target_os = "macos")]
use security_framework::passwords::{get_generic_password, set_generic_password};

use crate::error::Error;

/// Service name displayed to the user when using the keychain.
/// If you ever have to change this value, make sure to update,
/// the keychain service name in the VS Code extension.
#[cfg(target_os = "macos")]
static KEYCHAIN_SERVICE_NAME: &str = "Materialize";

/// Old keychain name keeped for compatibility.
/// TODO: Should be removed after > 0.2.6
#[cfg(target_os = "macos")]
static OLD_KEYCHAIN_SERVICE_NAME: &str = "Materialize mz CLI";

#[cfg(target_os = "macos")]
static DEFAULT_VAULT_VALUE: LazyLock<Option<&str>> =
    LazyLock::new(|| Some(Vault::Keychain.as_str()));

#[cfg(not(target_os = "macos"))]
static DEFAULT_VAULT_VALUE: LazyLock<Option<&str>> = LazyLock::new(|| Some(Vault::Inline.as_str()));

static GLOBAL_PARAMS: LazyLock<BTreeMap<&'static str, GlobalParam>> = LazyLock::new(|| {
    btreemap! {
        "profile" => GlobalParam {
            get: |config_file| {
                config_file.profile.as_deref().or(Some("default"))
            },
        },
        "vault" => GlobalParam {
            get: |config_file| {
                config_file.vault.as_deref().or(*DEFAULT_VAULT_VALUE)
            },
        }
    }
});

/// Represents an on-disk configuration file for `mz`.
#[derive(Clone)]
pub struct ConfigFile {
    path: PathBuf,
    parsed: TomlConfigFile,
    editable: Document,
}

impl ConfigFile {
    /// Computes the default path for the configuration file.
    pub fn default_path() -> Result<PathBuf, Error> {
        let Some(mut path) = dirs::home_dir() else {
            panic!("unable to discover home directory")
        };
        path.push(".config/materialize/mz.toml");
        Ok(path)
    }

    /// Loads a configuration file from the specified path.
    pub async fn load(path: PathBuf) -> Result<ConfigFile, Error> {
        // Create the parent directory if it doesn't exist
        if let Some(parent) = path.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).await?;
            }
        }

        // Create the file if it doesn't exist
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut buffer = String::new();
        file.read_to_string(&mut buffer)?;

        let parsed = toml_edit::de::from_str(&buffer)?;
        let editable = buffer.parse()?;

        Ok(ConfigFile {
            path,
            parsed,
            editable,
        })
    }

    /// Loads a profile from the configuration file.
    /// Panics if the profile is not found.
    pub fn load_profile<'a>(&'a self, name: &'a str) -> Result<Profile<'a>, Error> {
        match &self.parsed.profiles {
            Some(profiles) => match profiles.get(name) {
                None => Err(Error::ProfileMissing(name.to_string())),
                Some(parsed_profile) => Ok(Profile {
                    name,
                    parsed: parsed_profile,
                }),
            },
            None => Err(Error::ProfilesMissing),
        }
    }

    /// Adds a new profile to the config file.
    pub async fn add_profile(&self, name: String, profile: TomlProfile) -> Result<(), Error> {
        let mut editable = self.editable.clone();

        let profiles = editable.entry("profiles").or_insert(toml_edit::table());
        let mut new_profile = toml_edit::Table::new();

        self.add_app_password(&mut new_profile, &name, profile.clone())?;
        new_profile["region"] = value(profile.region.unwrap_or("aws/us-east-1".to_string()));

        if let Some(admin_endpoint) = profile.admin_endpoint {
            new_profile["admin-endpoint"] = value(admin_endpoint);
        }

        if let Some(cloud_endpoint) = profile.cloud_endpoint {
            new_profile["cloud-endpoint"] = value(cloud_endpoint);
        }

        if let Some(vault) = profile.vault {
            new_profile["vault"] = value(vault.to_string());
        }

        profiles[name.clone()] = toml_edit::Item::Table(new_profile);
        editable["profiles"] = profiles.clone();

        // If there is no profile assigned in the global config assign one.
        editable["profile"] = editable.entry("profile").or_insert(value(name)).clone();

        // TODO: I don't know why it creates an empty [profiles] table
        fs::write(&self.path, editable.to_string()).await?;

        Ok(())
    }

    /// Adds an app-password to the configuration file or
    /// to the keychain if the vault is enabled.
    #[cfg(target_os = "macos")]
    pub fn add_app_password(
        &self,
        new_profile: &mut toml_edit::Table,
        name: &str,
        profile: TomlProfile,
    ) -> Result<(), Error> {
        if Vault::Keychain == self.vault() {
            let app_password = profile.app_password.ok_or(Error::AppPasswordMissing)?;
            set_generic_password(KEYCHAIN_SERVICE_NAME, name, app_password.as_bytes())
                .map_err(|e| Error::MacOsSecurityError(e.to_string()))?;
        } else {
            new_profile["app-password"] =
                value(profile.app_password.ok_or(Error::AppPasswordMissing)?);
        }

        Ok(())
    }

    /// Adds an app-password to the configuration file.
    #[cfg(not(target_os = "macos"))]
    pub fn add_app_password(
        &self,
        new_profile: &mut toml_edit::Table,
        // Compatibility param.
        _name: &str,
        profile: TomlProfile,
    ) -> Result<(), Error> {
        new_profile["app-password"] = value(profile.app_password.ok_or(Error::AppPasswordMissing)?);

        Ok(())
    }

    /// Removes a profile from the configuration file.
    pub async fn remove_profile<'a>(&self, name: &str) -> Result<(), Error> {
        let mut editable = self.editable.clone();
        let profiles = editable["profiles"]
            .as_table_mut()
            .ok_or(Error::ProfilesMissing)?;
        profiles.remove(name);

        fs::write(&self.path, editable.to_string()).await?;

        Ok(())
    }

    /// Retrieves the default profile
    pub fn profile(&self) -> &str {
        (GLOBAL_PARAMS["profile"].get)(&self.parsed).unwrap()
    }

    /// Retrieves the default vault value
    pub fn vault(&self) -> &str {
        (GLOBAL_PARAMS["vault"].get)(&self.parsed).unwrap()
    }

    /// Retrieves all the available profiles
    pub fn profiles(&self) -> Option<BTreeMap<String, TomlProfile>> {
        self.parsed.profiles.clone()
    }

    /// Returns a list of all the possible profile configuration values
    pub fn list_profile_params(
        &self,
        profile_name: &str,
    ) -> Result<Vec<(&str, Option<String>)>, Error> {
        // Use the parsed profile rather than reading from the editable.
        // If there is a missing field it is more difficult to detect.
        let profile = self
            .parsed
            .profiles
            .clone()
            .ok_or(Error::ProfilesMissing)?
            .get(profile_name)
            .ok_or(Error::ProfileMissing(self.profile().to_string()))?
            .clone();

        let out = vec![
            ("admin-endpoint", profile.admin_endpoint),
            ("app-password", profile.app_password),
            ("cloud-endpoint", profile.cloud_endpoint),
            ("region", profile.region),
            ("vault", profile.vault.map(|x| x.to_string())),
        ];

        Ok(out)
    }

    /// Gets the value of a profile's configuration parameter.
    pub fn get_profile_param<'a>(
        &'a self,
        name: &str,
        profile: &'a str,
    ) -> Result<Option<&'a str>, Error> {
        let profile = self.load_profile(profile)?;
        let value = (PROFILE_PARAMS[name].get)(profile.parsed);

        Ok(value)
    }

    /// Sets the value of a profile's configuration parameter.
    pub async fn set_profile_param(
        &self,
        profile_name: &str,
        name: &str,
        value: Option<&str>,
    ) -> Result<(), Error> {
        let mut editable = self.editable.clone();

        // Update the value
        match value {
            None => {
                let profile = editable["profiles"][profile_name]
                    .as_table_mut()
                    .ok_or(Error::ProfileMissing(name.to_string()))?;
                if profile.contains_key(name) {
                    profile.remove(name);
                }
            }
            Some(value) => editable["profiles"][profile_name][name] = toml_edit::value(value),
        }

        fs::write(&self.path, editable.to_string()).await?;

        Ok(())
    }

    /// Gets the value of a configuration parameter.
    pub fn get_param(&self, name: &str) -> Result<Option<&str>, Error> {
        match GLOBAL_PARAMS.get(name) {
            Some(param) => Ok((param.get)(&self.parsed)),
            None => panic!("unknown configuration parameter {}", name.quoted()),
        }
    }

    /// Lists the all configuration parameters.
    pub fn list_params(&self) -> Vec<(&str, Option<&str>)> {
        let mut out = vec![];
        for (name, param) in &*GLOBAL_PARAMS {
            out.push((*name, (param.get)(&self.parsed)));
        }
        out
    }

    /// Sets the value of a configuration parameter.
    pub async fn set_param(&self, name: &str, value: Option<&str>) -> Result<(), Error> {
        if !GLOBAL_PARAMS.contains_key(name) {
            panic!("unknown configuration parameter {}", name.quoted());
        }
        let mut editable = self.editable.clone();
        match value {
            None => {
                editable.remove(name);
            }
            Some(value) => editable[name] = toml_edit::value(value),
        }
        fs::write(&self.path, editable.to_string()).await?;
        Ok(())
    }
}

static PROFILE_PARAMS: LazyLock<BTreeMap<&'static str, ProfileParam>> = LazyLock::new(|| {
    btreemap! {
        "app-password" => ProfileParam {
            get: |t| t.app_password.as_deref(),
        },
        "region" => ProfileParam {
            get: |t| t.region.as_deref(),
        },
        "vault" => ProfileParam {
            get: |t| t.vault.clone().map(|x| x.as_str()),
        },
        "admin-endpoint" => ProfileParam {
            get: |t| t.admin_endpoint.as_deref(),
        },
        "cloud-endpoint" => ProfileParam {
            get: |t| t.cloud_endpoint.as_deref(),
        },
    }
});

/// Defines the profile structure inside the configuration file.
///
/// It is divided into two fields:
/// * name: represents the profile name.
/// * parsed: represents the configuration values of the profile.
pub struct Profile<'a> {
    name: &'a str,
    parsed: &'a TomlProfile,
}

impl Profile<'_> {
    /// Returns the name of the profile.
    pub fn name(&self) -> &str {
        self.name
    }

    /// Returns the app password in the profile configuration.
    #[cfg(target_os = "macos")]
    pub fn app_password(&self, global_vault: &str) -> Result<String, Error> {
        if let Some(vault) = self.vault().or(Some(global_vault)) {
            if vault == Vault::Keychain {
                let password = get_generic_password(KEYCHAIN_SERVICE_NAME, self.name);

                match password {
                    Ok(generic_password) => {
                        let parsed_password = String::from_utf8(generic_password.to_vec());
                        match parsed_password {
                            Ok(app_password) => return Ok(app_password),
                            Err(err) => return Err(Error::MacOsSecurityError(err.to_string())),
                        }
                    }
                    Err(err) => {
                        // Not found error code. Check if it belongs to the old service.
                        if err.code() == -25300 {
                            let password =
                                get_generic_password(OLD_KEYCHAIN_SERVICE_NAME, self.name);
                            if let Ok(generic_password) = password {
                                let parsed_password = String::from_utf8(generic_password.to_vec());

                                // If there is a match, migrate the password from the old service name to the one one.
                                match parsed_password {
                                    Ok(app_password) => {
                                        set_generic_password(
                                            KEYCHAIN_SERVICE_NAME,
                                            self.name,
                                            app_password.as_bytes(),
                                        )
                                        .map_err(|e| Error::MacOsSecurityError(e.to_string()))?;
                                        return Ok(app_password);
                                    }
                                    Err(err) => {
                                        return Err(Error::MacOsSecurityError(err.to_string()))
                                    }
                                }
                            }
                        }

                        return Err(Error::MacOsSecurityError(err.to_string()));
                    }
                }
            }
        }

        (PROFILE_PARAMS["app-password"].get)(self.parsed)
            .map(|x| x.to_string())
            .ok_or(Error::AppPasswordMissing)
    }

    /// Returns the app password in the profile configuration.
    #[cfg(not(target_os = "macos"))]
    pub fn app_password(&self, _global_vault: &str) -> Result<String, Error> {
        (PROFILE_PARAMS["app-password"].get)(self.parsed)
            .map(|x| x.to_string())
            .ok_or(Error::AppPasswordMissing)
    }

    /// Returns the region in the profile configuration.
    pub fn region(&self) -> Option<&str> {
        (PROFILE_PARAMS["region"].get)(self.parsed)
    }

    /// Returns the vault value in the profile configuration.
    pub fn vault(&self) -> Option<&str> {
        (PROFILE_PARAMS["vault"].get)(self.parsed)
    }

    /// Returns the admin endpoint in the profile configuration.
    pub fn admin_endpoint(&self) -> Option<&str> {
        (PROFILE_PARAMS["admin-endpoint"].get)(self.parsed)
    }

    /// Returns the cloud endpoint in the profile configuration.
    pub fn cloud_endpoint(&self) -> Option<&str> {
        (PROFILE_PARAMS["cloud-endpoint"].get)(self.parsed)
    }
}

struct ConfigParam<T> {
    get: fn(&T) -> Option<&str>,
}

type GlobalParam = ConfigParam<TomlConfigFile>;
type ProfileParam = ConfigParam<TomlProfile>;

/// This structure represents the two possible
/// values for the vault field.
#[derive(Clone, Deserialize, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Vault {
    /// Default for macOS. Stores passwords in the macOS keychain.
    Keychain,
    /// Default for Linux. Stores passwords in the config file.
    Inline,
}

impl ToString for Vault {
    fn to_string(&self) -> String {
        match self {
            Vault::Keychain => "keychain".to_string(),
            Vault::Inline => "inline".to_string(),
        }
    }
}

impl Vault {
    fn as_str(&self) -> &'static str {
        match self {
            Vault::Keychain => "keychain",
            Vault::Inline => "inline",
        }
    }
}

impl FromStr for Vault {
    type Err = crate::error::Error;
    fn from_str(s: &str) -> Result<Self, crate::error::Error> {
        match s.to_ascii_lowercase().as_str() {
            "keychain" => Ok(Vault::Keychain),
            "inline" => Ok(Vault::Inline),
            _ => Err(Error::InvalidVaultError),
        }
    }
}

impl PartialEq<&str> for Vault {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<Vault> for &str {
    fn eq(&self, other: &Vault) -> bool {
        self == &other.as_str()
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
struct TomlConfigFile {
    profile: Option<String>,
    vault: Option<String>,
    profiles: Option<BTreeMap<String, TomlProfile>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
/// Describes the structure fields for a profile in the configuration file.
pub struct TomlProfile {
    /// The profile's unique app-password
    pub app_password: Option<String>,
    /// The profile's region to use by default.
    pub region: Option<String>,
    /// The vault value to use in MacOS.
    pub vault: Option<Vault>,
    /// A custom admin endpoint used for development.
    pub admin_endpoint: Option<String>,
    /// A custom cloud endpoint used for development.
    pub cloud_endpoint: Option<String>,
}
