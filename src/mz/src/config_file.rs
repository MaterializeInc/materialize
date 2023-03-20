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
use std::path::PathBuf;

use anyhow::bail;
use maplit::btreemap;
use once_cell::sync::Lazy;
use serde::Deserialize;
use tokio::fs;
use toml_edit::Document;

use mz_ore::str::StrExt;

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
pub struct ConfigFile {
    path: PathBuf,
    parsed: TomlConfigFile,
    editable: Document,
}

impl ConfigFile {
    /// Computes the default path for the configuration file.
    pub fn default_path() -> Result<PathBuf, anyhow::Error> {
        let Some(mut path) = dirs::home_dir() else {
            bail!("unable to discover home directory")
        };
        path.push(".config/materialize/mz.toml");
        Ok(path)
    }

    /// Loads a configuration file from the specified path.
    pub async fn load(path: PathBuf) -> Result<ConfigFile, anyhow::Error> {
        let raw = fs::read_to_string(&path).await?;
        let parsed = toml_edit::de::from_str(&raw)?;
        let editable = raw.parse()?;
        Ok(ConfigFile {
            path,
            parsed,
            editable,
        })
    }

    pub fn load_profile<'a>(&'a self, name: &'a str) -> Result<Profile, anyhow::Error> {
        match self.parsed.profiles.get(name) {
            None => bail!("unknown profile {}", name.quoted()),
            Some(parsed) => Ok(Profile {
                name,
                parsed,
                config_file: self,
            }),
        }
    }

    pub fn profile(&self) -> &str {
        (GLOBAL_PARAMS["profile"].get)(&self.parsed).unwrap()
    }

    pub fn vault(&self) -> &str {
        (GLOBAL_PARAMS["profile"].get)(&self.parsed).unwrap()
    }

    /// Gets the value of a configuration parameter.
    pub fn get_param(&self, name: &str) -> Result<Option<&str>, anyhow::Error> {
        match GLOBAL_PARAMS.get(name) {
            Some(param) => Ok((param.get)(&self.parsed)),
            None => bail!("unknown configuration parameter {}", name.quoted()),
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
    pub async fn set_param(&self, name: &str, value: Option<&str>) -> Result<(), anyhow::Error> {
        if !GLOBAL_PARAMS.contains_key(name) {
            bail!("unknown configuration parameter {}", name.quoted());
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
    pub fn app_password(&self) -> &str {
        (PROFILE_PARAMS["app-password"].get)(self.parsed).unwrap()
    }

    pub fn region(&self) -> Option<&str> {
        (PROFILE_PARAMS["region"].get)(self.parsed)
    }

    pub fn vault(&self) -> &str {
        (PROFILE_PARAMS["vault"].get)(self.parsed).unwrap()
    }

    pub fn admin_endpoint(&self) -> &str {
        // TODO: return default admin endpoint if unset.
        (PROFILE_PARAMS["admin-endpoint"].get)(self.parsed).unwrap()
    }

    pub fn cloud_endpoint(&self) -> &str {
        // TODO: return default cloud endpoint if unset.
        (PROFILE_PARAMS["cloud-endpoint"].get)(self.parsed).unwrap()
    }
}

struct ConfigParam<T> {
    get: fn(&T) -> Option<&str>,
}

type GlobalParam = ConfigParam<TomlConfigFile>;
type ProfileParam = ConfigParam<TomlProfile>;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
struct TomlConfigFile {
    profile: Option<String>,
    vault: Option<String>,
    profiles: BTreeMap<String, TomlProfile>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
#[serde(deny_unknown_fields)]
struct TomlProfile {
    app_password: Option<String>,
    region: Option<String>,
    vault: Option<String>,
    admin_endpoint: Option<String>,
    cloud_endpoint: Option<String>,
}
