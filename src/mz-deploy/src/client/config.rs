use serde::Deserialize;
use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error(
        "profiles configuration file not found. Searched:\n  - {project_path}\n  - {global_path}\n\nCreate a profiles.toml file in one of these locations with connection details."
    )]
    ProfilesNotFound {
        project_path: String,
        global_path: String,
    },
    #[error("failed to read profiles configuration from {path}: {source}")]
    ReadError {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse profiles configuration from {path}: {source}")]
    ParseError {
        path: String,
        source: toml::de::Error,
    },
    #[error("no default profile found. Add a profile named 'default' to your profiles.toml")]
    NoDefaultProfile,
    #[error("profile '{name}' not found in configuration")]
    ProfileNotFound { name: String },
    #[error("environment variable '{var}' not found for profile '{profile}'")]
    EnvVarNotFound { var: String, profile: String },
}

#[derive(Debug, Clone)]
pub struct Profile {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
struct ProfileData {
    pub host: String,
    #[serde(default = "default_port")]
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
}

fn default_port() -> u16 {
    6875
}

#[derive(Debug)]
pub struct ProfilesConfig {
    profiles: HashMap<String, Profile>,
    source_path: PathBuf,
}

impl ProfilesConfig {
    /// Load profiles configuration, checking project directory first, then global directory
    pub fn load() -> Result<Self, ConfigError> {
        let project_path = PathBuf::from(".mz/profiles.toml");

        let global_path = dirs::home_dir()
            .map(|home| home.join(".mz/profiles.toml"))
            .unwrap_or_else(|| PathBuf::from("~/.mz/profiles.toml"));

        // Try project directory first
        let (path, content) = if project_path.exists() {
            let content =
                fs::read_to_string(&project_path).map_err(|source| ConfigError::ReadError {
                    path: project_path.display().to_string(),
                    source,
                })?;
            (project_path, content)
        } else if global_path.exists() {
            let content =
                fs::read_to_string(&global_path).map_err(|source| ConfigError::ReadError {
                    path: global_path.display().to_string(),
                    source,
                })?;
            (global_path, content)
        } else {
            return Err(ConfigError::ProfilesNotFound {
                project_path: project_path.display().to_string(),
                global_path: global_path.display().to_string(),
            });
        };

        let profiles_data: HashMap<String, ProfileData> =
            toml::from_str(&content).map_err(|source| ConfigError::ParseError {
                path: path.display().to_string(),
                source,
            })?;

        // Convert ProfileData to Profile by adding the name field
        let mut profiles = HashMap::new();
        for (name, data) in profiles_data {
            profiles.insert(
                name.clone(),
                Profile {
                    name: name.clone(),
                    host: data.host,
                    port: data.port,
                    username: data.username,
                    password: data.password,
                },
            );
        }

        // Validate that a default profile exists
        if !profiles.contains_key("default") {
            return Err(ConfigError::NoDefaultProfile);
        }

        Ok(ProfilesConfig {
            profiles,
            source_path: path,
        })
    }

    /// Get a profile by name
    pub fn get_profile(&self, name: &str) -> Result<Profile, ConfigError> {
        self.profiles
            .get(name)
            .cloned()
            .ok_or_else(|| ConfigError::ProfileNotFound {
                name: name.to_string(),
            })
    }

    /// Get the default profile
    pub fn get_default_profile(&self) -> Result<Profile, ConfigError> {
        self.get_profile("default")
    }

    /// Expand environment variables in a profile's password field
    /// Supports ${VAR_NAME} syntax
    pub fn expand_env_vars(&self, mut profile: Profile) -> Result<Profile, ConfigError> {
        if let Some(password) = &profile.password
            && password.starts_with("${")
            && password.ends_with("}")
        {
            let var_name = &password[2..password.len() - 1];
            let env_value = std::env::var(var_name).map_err(|_| ConfigError::EnvVarNotFound {
                var: var_name.to_string(),
                profile: profile.name.clone(),
            })?;
            profile.password = Some(env_value);
        }

        // Also check for environment variable override
        // Format: MZ_PROFILE_{PROFILE_NAME}_PASSWORD
        let env_var_name = format!("MZ_PROFILE_{}_PASSWORD", profile.name.to_uppercase());
        if let Ok(password) = std::env::var(&env_var_name) {
            profile.password = Some(password);
        }

        Ok(profile)
    }

    pub fn source_path(&self) -> &PathBuf {
        &self.source_path
    }
}
