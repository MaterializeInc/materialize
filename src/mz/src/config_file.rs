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

use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Read;
use std::path::PathBuf;

use maplit::btreemap;
use mz_ore::str::StrExt;
use once_cell::sync::Lazy;
use serde::{Deserialize, Serialize};
use tokio::fs;
use toml_edit::{value, Document};

use crate::error::Error;

static GLOBAL_PARAMS: Lazy<BTreeMap<&'static str, GlobalParam>> = Lazy::new(|| {
    btreemap! {
        "profile" => GlobalParam {
            get: |config_file| {
                config_file.profile.as_deref().or(Some("default"))
            },
        },
        "vault" => GlobalParam {
            get: |config_file| {
                Some("TODO")
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
        // Best feature store features we need to call our selves
        // Features store development kit.
        // Streamlit way

        // Create the file if it doesn't exist
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
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
    pub fn load_profile<'a>(&'a self, name: &'a str) -> Result<Profile, Error> {
        match &self.parsed.profiles {
            Some(profiles) => match profiles.get(name) {
                None => panic!("unknown profile {}", name.quoted()),
                Some(parsed_profile) => Ok(Profile {
                    name,
                    parsed: parsed_profile,
                    config_file: self,
                }),
            },
            None => panic!("unknown profile {}", name.quoted()),
        }
    }

    /// Adds a new profile to the config file.
    pub async fn add_profile(&self, name: String, profile: TomlProfile) -> Result<(), Error> {
        let mut editable = self.editable.clone();

        let profiles = editable.entry("profiles").or_insert(toml_edit::table());
        let mut new_profile = toml_edit::Table::new();
        // TODO: Replace unwrap for error.
        new_profile["app-password"] = value(profile.app_password.unwrap());
        new_profile["region"] = value(profile.region.unwrap_or("aws/us-east-1".to_string()));

        if let Some(admin_endpoint) = profile.admin_endpoint {
            new_profile["admin-endpoint"] = value(admin_endpoint);
        }

        if let Some(cloud_endpoint) = profile.cloud_endpoint {
            new_profile["cloud-endpoint"] = value(cloud_endpoint);
        }

        if let Some(vault) = profile.vault {
            new_profile["vault"] = value(vault);
        }

        profiles[name] = toml_edit::Item::Table(new_profile);
        editable["profiles"] = profiles.clone();

        // TODO: I don't know why it creates an empty [profiles] table
        fs::write(&self.path, editable.to_string()).await?;

        Ok(())
    }

    /// Removes a profile from the configuration file.
    pub async fn remove_profile<'a>(&self, name: &str) -> Result<(), Error> {
        let mut editable = self.editable.clone();
        let profiles = editable["profiles"].as_table_mut().unwrap();
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

    pub fn list_profile_params(&self) -> Vec<(&str, Option<String>)> {
        // Use the parsed profile rather than reading from the editable.
        // If there is a missing field it is more difficult to detect.
        // TODO: Turn unwrap into errors. And use PROFILE_PARAMS
        let profile = self
            .parsed
            .profiles
            .clone()
            .unwrap()
            .get(self.profile())
            .unwrap()
            .clone();

        let out = vec![
            ("admin_endpoint", profile.admin_endpoint),
            ("app-password", profile.app_password),
            ("cloud_endpoint", profile.cloud_endpoint),
            ("region", profile.region),
            ("vault", profile.vault),
        ];

        out
    }

    /// Gets the value of a profile's configuration parameter.
    pub fn get_profile_param(&self, name: &str) -> Result<Option<&str>, Error> {
        let profile = self.load_profile(self.profile())?;
        let value = (PROFILE_PARAMS[name].get)(profile.parsed);

        Ok(value)
    }

    /// Sets the value of a profile's configuration parameter.
    pub async fn set_profile_param(&self, name: &str, value: Option<&str>) -> Result<(), Error> {
        let mut editable = self.editable.clone();

        // Update the value
        match value {
            None => {
                let profile = editable["profiles"][self.profile()].as_table_mut().unwrap();
                if profile.contains_key(&name.to_string()) {
                    profile.remove(&name.to_string());
                }
            }
            Some(value) => editable["profiles"][self.profile()][name] = toml_edit::value(value),
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

static PROFILE_PARAMS: Lazy<BTreeMap<&'static str, ProfileParam>> = Lazy::new(|| {
    btreemap! {
        "app-password" => ProfileParam {
            get: |t| t.app_password.as_deref(),
        },
        "region" => ProfileParam {
            get: |t| t.region.as_deref(),
        },
        "vault" => ProfileParam {
            get: |t| t.vault.as_deref(),
        },
        "admin-endpoint" => ProfileParam {
            get: |t| t.admin_endpoint.as_deref(),
        },
        "cloud-endpoint" => ProfileParam {
            get: |t| t.cloud_endpoint.as_deref(),
        },
    }
});

pub struct Profile<'a> {
    name: &'a str,
    parsed: &'a TomlProfile,
    config_file: &'a ConfigFile,
}

impl Profile<'_> {
    pub fn name(&self) -> &str {
        self.name
    }

    pub fn app_password(&self) -> &str {
        (PROFILE_PARAMS["app-password"].get)(self.parsed).unwrap()
    }

    pub fn region(&self) -> Option<&str> {
        (PROFILE_PARAMS["region"].get)(self.parsed)
    }

    pub fn vault(&self) -> Option<&str> {
        (PROFILE_PARAMS["vault"].get)(self.parsed)
    }

    pub fn admin_endpoint(&self) -> Option<&str> {
        // TODO: return default admin endpoint if unset.
        (PROFILE_PARAMS["admin-endpoint"].get)(self.parsed)
    }

    pub fn cloud_endpoint(&self) -> Option<&str> {
        // TODO: return default cloud endpoint if unset.
        (PROFILE_PARAMS["cloud-endpoint"].get)(self.parsed)
    }
}

struct ConfigParam<T> {
    get: fn(&T) -> Option<&str>,
}

type GlobalParam = ConfigParam<TomlConfigFile>;
type ProfileParam = ConfigParam<TomlProfile>;

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
pub struct TomlProfile {
    pub app_password: Option<String>,
    pub region: Option<String>,
    pub vault: Option<String>,
    pub admin_endpoint: Option<String>,
    pub cloud_endpoint: Option<String>,
}
