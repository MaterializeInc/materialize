//! Configuration loading for profiles and project settings.
//!
//! Loads connection profiles from `profiles.toml` (resolved via `--profiles-dir`,
//! `MZ_DEPLOY_PROFILES_DIR`, or `~/.mz/` default) and project settings
//! from `project.toml`. Key types:
//!
//! - [`Profile`] — Resolved connection details (host, port, credentials).
//! - [`ProfilesConfig`] — All profiles loaded from a single `profiles.toml`.
//! - [`ProjectSettings`] — Per-project config: active profile name and optional
//!   Materialize version / Docker image override.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const DEFAULT_DOCKER_IMAGE: &str = "materialize/materialized:latest";

#[derive(Debug, Deserialize, Clone, Default)]
pub struct SecurityConfig {
    /// AWS profile name for loading secrets from AWS Secrets Manager.
    aws_profile: Option<String>,
}

impl SecurityConfig {
    pub fn aws_profile(&self) -> Option<&str> {
        self.aws_profile.as_deref()
    }
}

#[derive(Debug, Deserialize, Clone, Default)]
pub struct ProfileConfig {
    /// Optional suffix to append to database and cluster names for this profile.
    /// For example, `profile_suffix = "_staging"` would rename `materialize` to
    /// `materialize_staging` and `analytics` to `analytics_staging`.
    /// The suffix includes the delimiter (user provides `"_staging"`, not `"staging"`).
    pub profile_suffix: Option<String>,
    /// Security-related configuration (e.g., AWS profile for secret resolution).
    #[serde(default)]
    pub security: SecurityConfig,
    /// psql-style variables resolved in SQL files before parsing.
    /// Defined per profile as `[profiles.<name>.variables]` in `project.toml`.
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProjectSettings {
    pub profile: String,
    pub mz_version: Option<String>,
    pub profiles: Option<BTreeMap<String, ProfileConfig>>,
}

impl ProjectSettings {
    pub fn load(project_directory: &Path) -> Result<Self, ConfigError> {
        let path = project_directory.join("project.toml");
        match fs::read_to_string(&path) {
            Ok(content) => toml::from_str(&content).map_err(|source| ConfigError::ParseError {
                path: path.display().to_string(),
                source,
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                Err(ConfigError::ProjectSettingsNotFound {
                    path: path.display().to_string(),
                })
            }
            Err(source) => Err(ConfigError::ReadError {
                path: path.display().to_string(),
                source,
            }),
        }
    }

    /// Returns the profile config for the given profile name.
    /// Falls back to `ProfileConfig::default()` if no entry exists.
    pub fn config_for_profile(&self, profile_name: &str) -> ProfileConfig {
        self.profiles
            .as_ref()
            .and_then(|p| p.get(profile_name))
            .cloned()
            .unwrap_or_default()
    }

    /// Returns the profile suffix for the given profile name, if configured.
    pub fn suffix_for_profile(&self, profile_name: &str) -> Option<&str> {
        self.profiles
            .as_ref()
            .and_then(|p| p.get(profile_name))
            .and_then(|c| c.profile_suffix.as_deref())
    }

    pub fn docker_image(&self) -> String {
        match self.mz_version.as_deref() {
            None | Some("cloud") => DEFAULT_DOCKER_IMAGE.to_string(),
            Some(tag) => format!("materialize/materialized:{}", tag),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error(
        "profiles configuration file not found at {path}\n\nSee mz-deploy help profiles for more information"
    )]
    ProfilesNotFound { path: String },
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
    #[error("project.toml not found at {path}")]
    ProjectSettingsNotFound { path: String },
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
    #[serde(alias = "user")]
    pub username: Option<String>,
    pub password: Option<String>,
}

fn default_port() -> u16 {
    6875
}

#[derive(Debug)]
pub struct ProfilesConfig {
    profiles: BTreeMap<String, Profile>,
    source_path: PathBuf,
}

impl ProfilesConfig {
    /// Load profiles configuration from a directory.
    ///
    /// # Arguments
    /// * `profiles_dir` - Optional directory containing `profiles.toml`.
    ///                     If None, defaults to `~/.mz`.
    pub fn load(profiles_dir: Option<&Path>) -> Result<Self, ConfigError> {
        let dir = match profiles_dir {
            Some(d) => d.to_path_buf(),
            None => dirs::home_dir()
                .map(|home| home.join(".mz"))
                .unwrap_or_else(|| PathBuf::from("~/.mz")),
        };

        let path = dir.join("profiles.toml");

        let content = match fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Err(ConfigError::ProfilesNotFound {
                    path: path.display().to_string(),
                });
            }
            Err(source) => {
                return Err(ConfigError::ReadError {
                    path: path.display().to_string(),
                    source,
                });
            }
        };

        let profiles_data: BTreeMap<String, ProfileData> =
            toml::from_str(&content).map_err(|source| ConfigError::ParseError {
                path: path.display().to_string(),
                source,
            })?;

        // Convert ProfileData to Profile by adding the name field
        let mut profiles = BTreeMap::new();
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

    pub fn profile_names(&self) -> Vec<&str> {
        self.profiles.keys().map(|s| s.as_str()).collect()
    }

    pub fn source_path(&self) -> &PathBuf {
        &self.source_path
    }

    /// Convenience method to load profiles and get a specific profile in one call
    ///
    /// # Arguments
    /// * `profiles_dir` - Optional directory containing `profiles.toml`
    /// * `cli_profile` - Optional profile name from CLI flag override
    /// * `default_profile` - Default profile name from project.toml
    pub fn load_profile(
        profiles_dir: Option<&Path>,
        cli_profile: Option<&str>,
        default_profile: &str,
    ) -> Result<Profile, ConfigError> {
        let config = Self::load(profiles_dir)?;
        let name = cli_profile.unwrap_or(default_profile);
        let profile = config.get_profile(name)?;
        config.expand_env_vars(profile)
    }
}

/// Resolved settings for an mz-deploy execution.
///
/// Constructed once in `main.rs` from CLI args + `project.toml` + `profiles.toml`,
/// then passed to every command. Commands extract what they need (`directory`,
/// `profile_name`, `profile_suffix`, `docker_image`, `connection()`, `profile_config`).
#[derive(Debug, Clone)]
pub struct Settings {
    /// Project root directory (from --directory, default ".").
    pub directory: PathBuf,
    /// Resolved profile name (CLI --profile overrides project.toml default).
    pub profile_name: String,
    /// Resolved Docker image for type checking and tests.
    pub docker_image: String,
    /// Per-profile config (security, profile_suffix) — used for SecretResolver.
    pub profile_config: ProfileConfig,
    /// Database connection profile. None for commands that don't connect (compile, test).
    connection: Option<Profile>,
}

impl Settings {
    /// Load settings from CLI args, project.toml, and profiles.toml.
    ///
    /// `needs_connection` controls whether a connection profile is loaded from
    /// `profiles.toml`. Commands like `compile` and `test` don't need one.
    pub fn load(
        directory: PathBuf,
        cli_profile: Option<&str>,
        docker_image_override: Option<&str>,
        needs_connection: bool,
        profiles_dir: Option<&Path>,
    ) -> Result<Self, ConfigError> {
        let project_settings = ProjectSettings::load(&directory)?;
        let profile_name = cli_profile.unwrap_or(&project_settings.profile).to_string();

        let profile_config = project_settings.config_for_profile(&profile_name);

        let docker_image = match docker_image_override {
            Some(image) => image.to_string(),
            None => project_settings.docker_image(),
        };

        let connection = if needs_connection {
            Some(ProfilesConfig::load_profile(
                profiles_dir,
                cli_profile,
                &project_settings.profile,
            )?)
        } else {
            None
        };

        Ok(Settings {
            directory,
            profile_name,
            docker_image,
            profile_config,
            connection,
        })
    }

    /// Profile suffix applied to both database and cluster names (e.g., `"_staging"`).
    pub fn profile_suffix(&self) -> Option<&str> {
        self.profile_config.profile_suffix.as_deref()
    }

    /// psql-style variables for this profile.
    pub fn variables(&self) -> &BTreeMap<String, String> {
        &self.profile_config.variables
    }

    /// Returns the database connection profile.
    ///
    /// # Panics
    /// Panics if `Settings` was loaded with `needs_connection: false`.
    /// Calling this on a non-connected `Settings` is a programmer error.
    pub fn connection(&self) -> &Profile {
        self.connection
            .as_ref()
            .expect("Settings::connection() called but needs_connection was false")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_profile_config_deserializes_profile_suffix() {
        let toml = r#"
            profile = "default"

            [profiles.staging]
            profile_suffix = "_staging"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("staging");
        assert_eq!(config.profile_suffix.as_deref(), Some("_staging"));
    }

    #[test]
    fn test_profile_config_profile_suffix_optional() {
        let toml = r#"
            profile = "default"

            [profiles.prod.security]
            aws_profile = "prod-aws"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("prod");
        assert!(config.profile_suffix.is_none());
        assert_eq!(config.security.aws_profile(), Some("prod-aws"));
    }

    #[test]
    fn test_suffix_for_profile_returns_suffix() {
        let toml = r#"
            profile = "default"

            [profiles.staging]
            profile_suffix = "_staging"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        assert_eq!(settings.suffix_for_profile("staging"), Some("_staging"));
    }

    #[test]
    fn test_suffix_for_profile_missing_profile() {
        let toml = r#"
            profile = "default"

            [profiles.staging]
            profile_suffix = "_staging"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        assert_eq!(settings.suffix_for_profile("nonexistent"), None);
    }

    #[test]
    fn test_suffix_for_profile_no_profiles_section() {
        let toml = r#"
            profile = "default"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        assert_eq!(settings.suffix_for_profile("staging"), None);
    }

    #[test]
    fn test_config_for_profile_without_security_section() {
        let toml = r#"
            profile = "default"

            [profiles.prod]
            profile_suffix = "_prod"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("prod");
        assert_eq!(config.profile_suffix.as_deref(), Some("_prod"));
        assert_eq!(config.security.aws_profile(), None);
    }

    #[test]
    fn test_profile_config_deserializes_variables() {
        let toml = r#"
            profile = "default"

            [profiles.staging.variables]
            cluster = "staging_cluster"
            pg_host = "staging-replica.internal"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("staging");
        assert_eq!(
            config.variables.get("cluster").map(|s| s.as_str()),
            Some("staging_cluster")
        );
        assert_eq!(
            config.variables.get("pg_host").map(|s| s.as_str()),
            Some("staging-replica.internal")
        );
    }

    #[test]
    fn test_profile_config_variables_default_empty() {
        let toml = r#"
            profile = "default"

            [profiles.prod]
            profile_suffix = "_prod"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("prod");
        assert!(config.variables.is_empty());
    }

    #[test]
    fn test_profile_config_variables_missing_profile() {
        let toml = r#"
            profile = "default"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("nonexistent");
        assert!(config.variables.is_empty());
    }
}
