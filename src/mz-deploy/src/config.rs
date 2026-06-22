// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Configuration loading for profiles and project settings.
//!
//! Loads connection profiles from `profiles.toml` (resolved via `--profiles-dir`,
//! `MZ_DEPLOY_PROFILES_DIR`, or `~/.mz/` default) and project settings
//! from `project.toml`. Key types:
//!
//! - [`Profile`] — Resolved connection details (host, port, credentials).
//! - [`ProfilesConfig`] — All profiles loaded from a single `profiles.toml`.
//! - [`ProjectSettings`] — Per-project config: active profile name, optional
//!   Materialize version / Docker image override, and an optional `dependencies`
//!   array of fully qualified `database.schema.object` names that this project
//!   reads from but does not own.
//!
//! Passwords can be pulled from the environment via an inline `${VAR}` in the
//! password field, or via `MZ_PROFILE_<NAME>_PASSWORD` (see
//! [`ProfilesConfig::expand_env_vars`] for details).

use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use thiserror::Error;

use crate::project::ir::object_id::ObjectId;

/// Repository path for the Materialize Docker image, without a tag.
const DOCKER_IMAGE_BASE: &str = "materialize/materialized";

/// The Docker image used when no `mz_version` is configured.
pub fn default_docker_image() -> String {
    format!("{DOCKER_IMAGE_BASE}:latest")
}

/// Security-related settings for a profile (e.g., AWS credentials for secret resolution).
#[derive(Debug, Deserialize, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct SecurityConfig {
    /// AWS profile name for loading secrets from AWS Secrets Manager.
    aws_profile: Option<String>,
}

impl SecurityConfig {
    pub fn aws_profile(&self) -> Option<&str> {
        self.aws_profile.as_deref()
    }
}

/// Per-profile configuration from `project.toml`.
///
/// Each profile can specify a name suffix, security settings, and psql-style
/// variables that are resolved in SQL files before parsing.
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
    /// Defined per profile as `[<profile>.variables]` in `project.toml`.
    #[serde(default)]
    pub variables: BTreeMap<String, String>,
    /// When true, treat the target as if RBAC is disabled: skip all role
    /// creation, grants, and role-membership checks. Intended for the
    /// single-user Materialize emulator.
    #[serde(default)]
    pub emulator: bool,
}

/// Parsed contents of `project.toml`.
///
/// Specifies optional Materialize version override, per-profile configuration
/// sections, and an optional list of external dependency object names (fully
/// qualified `database.schema.object` strings).
///
/// The active profile is **not** stored here — it's resolved from `--profile`,
/// `MZ_DEPLOY_PROFILE`, or the per-project `.mzprofile` file. See
/// [`read_mzprofile`].
#[derive(Debug, Deserialize, Clone)]
pub struct ProjectSettings {
    pub mz_version: Option<String>,

    #[serde(flatten)]
    pub profiles: BTreeMap<String, ProfileConfig>,
    /// Raw dependency strings from the `dependencies` array in `project.toml`.
    /// Each entry must be a fully qualified `database.schema.object` name.
    #[serde(default, rename = "dependencies")]
    raw_dependencies: Vec<String>,
}

/// Filename of the per-project default-profile pointer.
///
/// Lives at the project root (alongside `project.toml`), holds a single
/// profile name as plain text. Analogous to kubectl's
/// `current-context`. Machine-local — should be listed in `.gitignore` so
/// developers on the same project can each set their own default.
pub const MZPROFILE_FILENAME: &str = ".mzprofile";

/// Read the default profile name from `<project_directory>/.mzprofile`.
///
/// Returns `Ok(None)` if the file doesn't exist. Blank lines and lines
/// beginning with `#` are ignored; the first remaining trimmed line is the
/// profile name.
pub fn read_mzprofile(project_directory: &Path) -> Result<Option<String>, ConfigError> {
    let path = project_directory.join(MZPROFILE_FILENAME);
    match fs::read_to_string(&path) {
        Ok(content) => {
            for line in content.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.starts_with('#') {
                    continue;
                }
                return Ok(Some(trimmed.to_string()));
            }
            Ok(None)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(source) => Err(ConfigError::ReadError {
            path: path.display().to_string(),
            source,
        }),
    }
}

/// Write `profile_name` to `<project_directory>/.mzprofile`, replacing any
/// existing content.
pub fn write_mzprofile(project_directory: &Path, profile_name: &str) -> Result<(), ConfigError> {
    let path = project_directory.join(MZPROFILE_FILENAME);
    fs::write(&path, format!("{}\n", profile_name)).map_err(|source| ConfigError::WriteError {
        path: path.display().to_string(),
        source,
    })
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
    ///
    /// Falls back to `ProfileConfig::default()` if no entry exists, except for
    /// the built-in [`EMULATOR_PROFILE_NAME`] profile, which defaults to
    /// `emulator = true`. A user-defined entry of the same name wins.
    pub fn config_for_profile(&self, profile_name: &str) -> ProfileConfig {
        if let Some(config) = self.profiles.get(profile_name) {
            return config.clone();
        }
        if profile_name == EMULATOR_PROFILE_NAME {
            return ProfileConfig {
                emulator: true,
                ..Default::default()
            };
        }
        ProfileConfig::default()
    }

    /// Returns the profile suffix for the given profile name, if configured.
    pub fn suffix_for_profile(&self, profile_name: &str) -> Option<&str> {
        self.profiles
            .get(profile_name)
            .and_then(|c| c.profile_suffix.as_deref())
    }

    pub fn docker_image(&self) -> String {
        match self.mz_version.as_deref() {
            None | Some("cloud") => default_docker_image(),
            Some(tag) => format!("{DOCKER_IMAGE_BASE}:{tag}"),
        }
    }

    /// Parse and validate the `dependencies` array from `project.toml`.
    ///
    /// Each entry must be a fully qualified `database.schema.object` name.
    /// Returns `ConfigError::InvalidDependency` for entries that are not
    /// three-dot-separated parts, and `ConfigError::DuplicateDependency` if
    /// the same object appears more than once.
    pub fn validate_dependencies(&self) -> Result<BTreeSet<ObjectId>, ConfigError> {
        let mut seen = BTreeSet::new();
        for entry in &self.raw_dependencies {
            let id = entry
                .parse::<ObjectId>()
                .map_err(|_| ConfigError::InvalidDependency {
                    entry: entry.clone(),
                })?;
            if !seen.insert(id) {
                return Err(ConfigError::DuplicateDependency {
                    entry: entry.clone(),
                });
            }
        }
        Ok(seen)
    }
}

/// Errors that can occur when loading or resolving configuration from
/// `profiles.toml` and `project.toml`.
#[derive(Debug, Error)]
pub enum ConfigError {
    #[error(
        "profiles configuration file not found at {path}\n\nSee mz-deploy help profiles for more information"
    )]
    ProfilesNotFound { path: String },
    #[error("failed to read {path}: {source}")]
    ReadError {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to write {path}: {source}")]
    WriteError {
        path: String,
        source: std::io::Error,
    },
    #[error("failed to parse {path}: {source}")]
    ParseError {
        path: String,
        source: toml::de::Error,
    },
    #[error(
        "no profile selected: pass --profile, set MZ_DEPLOY_PROFILE, or run `mz-deploy profile set <name>`"
    )]
    NoProfileConfigured,
    #[error("could not determine home directory; set $HOME or pass --profiles-dir")]
    HomeDirNotFound,
    #[error("project.toml not found at {path}")]
    ProjectSettingsNotFound { path: String },
    #[error("profile '{name}' not found in configuration")]
    ProfileNotFound { name: String },
    #[error("environment variable '{var}' not found for profile '{profile}'")]
    EnvVarNotFound { var: String, profile: String },
    #[error(
        "invalid option key '{key}' in profile '{profile}': option keys must be \
         valid identifiers (alphanumeric and underscore, starting with a letter \
         or underscore)"
    )]
    InvalidOptionKey { key: String, profile: String },
    #[error(
        "invalid dependency '{entry}': expected a fully qualified 'database.schema.object' name"
    )]
    InvalidDependency { entry: String },
    #[error(
        "duplicate dependency '{entry}': each object may appear at most once in 'dependencies'"
    )]
    DuplicateDependency { entry: String },
    #[error(
        "profile '{profile}' has no host configured: set 'host' (SQL pgwire) \
         or 'http_host' (HTTP API) in profiles.toml"
    )]
    ProfileMissingAnyHost { profile: String },
    #[error(
        "profile '{profile}' has no SQL host configured: this command requires \
         a SQL connection. Set 'host' in profiles.toml."
    )]
    ProfileMissingSqlHost { profile: String },
    #[error(
        "profile '{profile}' has no HTTP host configured: 'mz-deploy mcp' \
         requires the HTTP API hostname. Set 'http_host' in profiles.toml."
    )]
    ProfileMissingHttpHost { profile: String },
}

/// TLS mode selection for a profile, matching libpq's `sslmode` vocabulary
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
    VerifyCa,
    VerifyFull,
}

impl SslMode {
    /// Wire name accepted by libpq (`PGSSLMODE`, `sslmode=` in conninfo).
    pub(crate) const fn libpq_name(self) -> &'static str {
        match self {
            SslMode::Disable => "disable",
            SslMode::Prefer => "prefer",
            SslMode::Require => "require",
            SslMode::VerifyCa => "verify-ca",
            SslMode::VerifyFull => "verify-full",
        }
    }
}

/// Resolved connection details for a Materialize region.
///
/// Constructed from a `profiles.toml` entry after environment variable expansion.
///
/// Either `host` or `http_host` must be set. SQL commands require `host`;
/// `mz-deploy mcp` requires `http_host`. A profile that only sets `http_host`
/// is valid for MCP-only use.
#[derive(Debug, Clone)]
pub struct Profile {
    pub name: String,
    /// SQL pgwire host. Required for any command that opens a SQL connection.
    pub host: Option<String>,
    pub port: u16,
    pub username: String,
    pub password: Option<String>,
    /// Session variables to set via libpq's `options` parameter (`-c key=value` flags).
    pub options: BTreeMap<String, String>,
    /// TLS mode override. `None` means "pick a default based on host":
    /// `Prefer` for loopback, `Require` otherwise.
    pub sslmode: Option<SslMode>,
    /// Explicit CA bundle path for `verify-ca` / `verify-full`. `None` falls
    /// through to the platform CA hunt.
    pub sslrootcert: Option<PathBuf>,
    /// Hostname of the Materialize HTTP API.
    pub http_host: Option<String>,
}

impl Profile {
    /// Borrow the SQL host, returning a clear error if the profile only
    /// configures the HTTP API.
    pub fn require_host(&self) -> Result<&str, ConfigError> {
        self.host
            .as_deref()
            .ok_or_else(|| ConfigError::ProfileMissingSqlHost {
                profile: self.name.clone(),
            })
    }

    /// Borrow the HTTP host, returning a clear error if the profile only
    /// configures the SQL pgwire endpoint.
    pub fn require_http_host(&self) -> Result<&str, ConfigError> {
        self.http_host
            .as_deref()
            .ok_or_else(|| ConfigError::ProfileMissingHttpHost {
                profile: self.name.clone(),
            })
    }
}

#[derive(Debug, Deserialize, Clone)]
struct ProfileData {
    #[serde(default)]
    pub host: Option<String>,
    #[serde(default = "default_port")]
    pub port: u16,
    pub username: String,
    pub password: Option<String>,
    #[serde(default)]
    pub options: BTreeMap<String, String>,
    #[serde(default)]
    pub sslmode: Option<SslMode>,
    #[serde(default)]
    pub sslrootcert: Option<PathBuf>,
    #[serde(default)]
    pub http_host: Option<String>,
}

fn default_port() -> u16 {
    6875
}

/// Name of the built-in profile that targets a local Materialize emulator.
///
/// Available without any `profiles.toml` or `project.toml` entry: it resolves
/// to `localhost:6875` as user `materialize` with [`ProfileConfig::emulator`]
/// set, so a fresh checkout can deploy against a local emulator with zero
/// setup. A user-defined profile of the same name takes precedence.
pub const EMULATOR_PROFILE_NAME: &str = "emulator";

/// The built-in connection profile for the local Materialize emulator.
fn emulator_profile() -> Profile {
    Profile {
        name: EMULATOR_PROFILE_NAME.to_string(),
        host: Some("localhost".to_string()),
        port: default_port(),
        username: "materialize".to_string(),
        password: None,
        options: BTreeMap::new(),
        sslmode: None,
        sslrootcert: None,
        http_host: None,
    }
}

/// Uppercase a profile name and replace any non-alphanumeric character with
/// `_` so it can be embedded in a shell-legal env var identifier like
/// `MZ_PROFILE_MY_PROD_PASSWORD`.
fn sanitize_profile_for_env(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_uppercase()
            } else {
                '_'
            }
        })
        .collect()
}

/// Check whether `key` is a valid identifier for a profile `[options]` entry.
///
/// Keys must start with an ASCII letter or underscore, followed by any number
/// of ASCII letters, digits, or underscores. Empty keys are rejected.
fn is_valid_option_key(key: &str) -> bool {
    let mut chars = key.chars();
    match chars.next() {
        None => false,
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }
        _ => false,
    }
}

/// All connection profiles loaded from a `profiles.toml` file.
///
/// Provides lookup by profile name and environment variable expansion
/// for password fields.
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
                .ok_or(ConfigError::HomeDirNotFound)?
                .join(".mz"),
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
            for key in data.options.keys() {
                if !is_valid_option_key(key) {
                    return Err(ConfigError::InvalidOptionKey {
                        profile: name.clone(),
                        key: key.clone(),
                    });
                }
            }
            if data.host.is_none() && data.http_host.is_none() {
                return Err(ConfigError::ProfileMissingAnyHost {
                    profile: name.clone(),
                });
            }
            profiles.insert(
                name.clone(),
                Profile {
                    name: name.clone(),
                    host: data.host,
                    port: data.port,
                    username: data.username,
                    password: data.password,
                    options: data.options,
                    sslmode: data.sslmode,
                    sslrootcert: data.sslrootcert,
                    http_host: data.http_host,
                },
            );
        }

        // The emulator profile is built in. A user-defined entry of the same
        // name wins, so only insert it when absent.
        profiles
            .entry(EMULATOR_PROFILE_NAME.to_string())
            .or_insert_with(emulator_profile);

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

    /// Resolve a profile's password, applying environment variable overrides.
    ///
    /// Two override mechanisms, applied in order:
    ///
    /// 1. **Inline `${VAR_NAME}`** in `profiles.toml` — when the `password` field
    ///    is exactly `"${SOMETHING}"` (no surrounding text), the referenced env
    ///    var is read and substituted. Missing vars produce an error.
    /// 2. **`MZ_PROFILE_<NAME>_PASSWORD`** — always checked; if set, overrides
    ///    whatever the file (or step 1) produced. The profile name is
    ///    uppercased and non-alphanumeric characters are replaced with `_` to
    ///    form a valid shell identifier (so `my-prod` → `MY_PROD`). Profile
    ///    names that differ only in such characters will collide on the same
    ///    env var.
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

        let env_var_name = format!(
            "MZ_PROFILE_{}_PASSWORD",
            sanitize_profile_for_env(&profile.name)
        );
        if let Ok(password) = std::env::var(&env_var_name) {
            profile.password = Some(password);
        }

        Ok(profile)
    }

    pub fn profile_names(&self) -> Vec<&str> {
        self.profiles.keys().map(|s| s.as_str()).collect()
    }

    pub fn source_path(&self) -> &Path {
        &self.source_path
    }

    /// Convenience method to resolve a profile by name from the configured
    /// profiles file and expand its env-var references.
    pub fn resolve_profile(
        profiles_dir: Option<&Path>,
        name: &str,
    ) -> Result<Profile, ConfigError> {
        match Self::load(profiles_dir) {
            Ok(config) => {
                let profile = config.get_profile(name)?;
                config.expand_env_vars(profile)
            }
            // The built-in emulator profile must resolve even with no
            // `profiles.toml` present, so a fresh checkout can target a local
            // emulator with zero setup.
            Err(ConfigError::ProfilesNotFound { .. }) if name == EMULATOR_PROFILE_NAME => {
                Ok(emulator_profile())
            }
            Err(e) => Err(e),
        }
    }
}

/// Resolved settings for an mz-deploy execution.
///
/// Constructed once in `main.rs` from CLI args + `project.toml` + `profiles.toml`,
/// then passed to every command. Commands extract what they need (`directory`,
/// `profile_name`, `profile_suffix`, `docker_image`, `connection()`, `profile_config`,
/// `dependencies`).
///
/// `profile_name` is `None` for commands that don't require a connection
/// (`compile`, `test`, `explain`) when no profile is set. In that mode
/// `profile_config` is the default (no variables, no suffix), and any SQL
/// referencing an unresolved variable will fail with a hint to set a profile.
#[derive(Debug, Clone)]
pub struct Settings {
    /// Project root directory (from --directory, default ".").
    pub directory: PathBuf,
    /// Resolved profile name (CLI --profile overrides project.toml default).
    /// `None` when no profile is configured and the command doesn't require one.
    pub profile_name: Option<String>,
    /// Resolved Docker image for the ephemeral container used by `test` and `explain`.
    pub docker_image: String,
    /// Per-profile config (security, profile_suffix) — used for SecretResolver.
    /// Default-constructed (empty variables, no suffix) when `profile_name` is `None`.
    pub profile_config: ProfileConfig,
    /// Validated external dependencies declared in `project.toml`.
    pub dependencies: BTreeSet<ObjectId>,
    /// Database connection profile. None for commands that don't connect (compile, test).
    connection: Option<Profile>,
}

impl Settings {
    /// Load settings from CLI args, project.toml, and profiles.toml.
    ///
    /// `needs_connection` controls whether a connection profile is loaded from
    /// `profiles.toml`. Commands like `compile` and `test` don't need one and
    /// will succeed with no profile selected; in that case `profile_name`
    /// is `None` and `profile_config` is default.
    pub fn load(
        directory: PathBuf,
        cli_profile: Option<&str>,
        docker_image_override: Option<&str>,
        needs_connection: bool,
        profiles_dir: Option<&Path>,
    ) -> Result<Self, ConfigError> {
        let project_settings = ProjectSettings::load(&directory)?;

        // Resolution order: --profile flag / MZ_DEPLOY_PROFILE env var (both
        // arrive via `cli_profile`), then the project-root pointer written
        // by `mz-deploy profile set`. When all three are missing:
        //   - commands that connect (`needs_connection: true`) hard-error
        //   - commands that don't connect proceed with `profile_name: None`
        let profile_name: Option<String> = match cli_profile {
            Some(p) => Some(p.to_string()),
            None => match read_mzprofile(&directory)? {
                Some(p) => Some(p),
                None if needs_connection => return Err(ConfigError::NoProfileConfigured),
                None => None,
            },
        };

        let profile_config = match &profile_name {
            Some(name) => project_settings.config_for_profile(name),
            None => ProfileConfig::default(),
        };

        let docker_image = match docker_image_override {
            Some(image) => image.to_string(),
            None => project_settings.docker_image(),
        };

        let dependencies = project_settings.validate_dependencies()?;

        let connection = if needs_connection {
            // Safe to unwrap: if we got here with needs_connection, profile_name is Some.
            let name = profile_name
                .as_deref()
                .expect("needs_connection requires a profile name");
            Some(ProfilesConfig::resolve_profile(profiles_dir, name)?)
        } else {
            None
        };

        Ok(Settings {
            directory,
            profile_name,
            docker_image,
            profile_config,
            dependencies,
            connection,
        })
    }

    /// The active profile name, if one is set.
    pub fn profile_name(&self) -> Option<&str> {
        self.profile_name.as_deref()
    }

    /// Profile suffix applied to both database and cluster names (e.g., `"_staging"`).
    pub fn profile_suffix(&self) -> Option<&str> {
        self.profile_config.profile_suffix.as_deref()
    }

    /// psql-style variables for this profile.
    pub fn variables(&self) -> &BTreeMap<String, String> {
        &self.profile_config.variables
    }

    /// Whether this profile targets the single-user emulator, in which case the
    /// RBAC role/grant machinery is treated as disabled.
    pub fn emulator(&self) -> bool {
        self.profile_config.emulator
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

    #[mz_ore::test]
    fn test_profile_config_deserializes_profile_suffix() {
        let toml = r#"

            [staging]
            profile_suffix = "_staging"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("staging");
        assert_eq!(config.profile_suffix.as_deref(), Some("_staging"));
    }

    #[mz_ore::test]
    fn test_profile_config_deserializes_emulator() {
        let toml = r#"

            [local]
            emulator = true
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("local");
        assert!(config.emulator);
    }

    #[mz_ore::test]
    fn test_profile_config_emulator_defaults_false() {
        let toml = r#"

            [staging]
            profile_suffix = "_staging"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("staging");
        assert!(!config.emulator);
    }

    #[mz_ore::test]
    fn test_builtin_emulator_config_defaults_emulator_true() {
        let settings: ProjectSettings = toml::from_str("").unwrap();
        let config = settings.config_for_profile(EMULATOR_PROFILE_NAME);
        assert!(config.emulator);
    }

    #[mz_ore::test]
    fn test_user_defined_emulator_config_overrides_builtin() {
        let toml = r#"

            [emulator]
            profile_suffix = "_local"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile(EMULATOR_PROFILE_NAME);
        // The user's entry wins, so `emulator` is whatever they set (default false).
        assert!(!config.emulator);
        assert_eq!(config.profile_suffix.as_deref(), Some("_local"));
    }

    #[mz_ore::test]
    fn test_profile_config_profile_suffix_optional() {
        let toml = r#"

            [prod.security]
            aws_profile = "prod-aws"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("prod");
        assert!(config.profile_suffix.is_none());
        assert_eq!(config.security.aws_profile(), Some("prod-aws"));
    }

    #[mz_ore::test]
    fn test_suffix_for_profile_returns_suffix() {
        let toml = r#"

            [staging]
            profile_suffix = "_staging"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        assert_eq!(settings.suffix_for_profile("staging"), Some("_staging"));
    }

    #[mz_ore::test]
    fn test_suffix_for_profile_missing_profile() {
        let toml = r#"

            [staging]
            profile_suffix = "_staging"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        assert_eq!(settings.suffix_for_profile("nonexistent"), None);
    }

    #[mz_ore::test]
    fn test_suffix_for_profile_no_profiles_section() {
        let toml = r#"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        assert_eq!(settings.suffix_for_profile("staging"), None);
    }

    #[mz_ore::test]
    fn test_config_for_profile_without_security_section() {
        let toml = r#"

            [prod]
            profile_suffix = "_prod"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("prod");
        assert_eq!(config.profile_suffix.as_deref(), Some("_prod"));
        assert_eq!(config.security.aws_profile(), None);
    }

    #[mz_ore::test]
    fn test_profile_config_deserializes_variables() {
        let toml = r#"

            [staging.variables]
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

    #[mz_ore::test]
    fn test_profile_config_variables_default_empty() {
        let toml = r#"

            [prod]
            profile_suffix = "_prod"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("prod");
        assert!(config.variables.is_empty());
    }

    #[mz_ore::test]
    fn test_profile_config_variables_missing_profile() {
        let toml = r#"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let config = settings.config_for_profile("nonexistent");
        assert!(config.variables.is_empty());
    }

    #[mz_ore::test]
    fn test_dependencies_parses_valid_entries() {
        let toml = r#"

            dependencies = [
                "ontology.public.customers",
                "ontology.public.orders",
            ]
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let deps = settings.validate_dependencies().unwrap();
        assert_eq!(deps.len(), 2);
        assert!(deps.iter().any(|d| d.object() == "customers"));
        assert!(deps.iter().any(|d| d.object() == "orders"));
    }

    #[mz_ore::test]
    fn test_dependencies_defaults_to_empty() {
        let toml = r#"
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        assert!(settings.validate_dependencies().unwrap().is_empty());
    }

    #[mz_ore::test]
    fn test_dependencies_rejects_two_part_name() {
        let toml = r#"
            dependencies = ["public.orders"]
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let err = settings.validate_dependencies().unwrap_err();
        assert!(matches!(err, ConfigError::InvalidDependency { .. }));
    }

    #[mz_ore::test]
    fn test_dependencies_rejects_duplicates() {
        let toml = r#"
            dependencies = [
                "ontology.public.customers",
                "ontology.public.customers",
            ]
        "#;
        let settings: ProjectSettings = toml::from_str(toml).unwrap();
        let err = settings.validate_dependencies().unwrap_err();
        assert!(matches!(err, ConfigError::DuplicateDependency { .. }));
    }

    #[mz_ore::test]
    fn test_profile_deserializes_options() {
        let toml = r#"
            [staging]
            host = "staging.example.com"
            username = "deploy_bot"

            [staging.options]
            cluster = "staging_cluster"
            search_path = "public,reporting"
        "#;
        let data: BTreeMap<String, ProfileData> = toml::from_str(toml).unwrap();
        let staging = data.get("staging").unwrap();
        assert_eq!(
            staging.options.get("cluster").map(String::as_str),
            Some("staging_cluster")
        );
        assert_eq!(
            staging.options.get("search_path").map(String::as_str),
            Some("public,reporting")
        );
    }

    #[mz_ore::test]
    fn test_profile_options_default_empty() {
        let toml = r#"
            [prod]
            host = "prod.example.com"
            username = "deploy_bot"
        "#;
        let data: BTreeMap<String, ProfileData> = toml::from_str(toml).unwrap();
        assert!(data.get("prod").unwrap().options.is_empty());
    }

    #[mz_ore::test]
    fn test_is_valid_option_key_accepts_identifiers() {
        assert!(is_valid_option_key("cluster"));
        assert!(is_valid_option_key("search_path"));
        assert!(is_valid_option_key("_mz_internal"));
        assert!(is_valid_option_key("a1"));
        assert!(is_valid_option_key("A"));
    }

    #[mz_ore::test]
    fn test_is_valid_option_key_rejects_invalid() {
        assert!(!is_valid_option_key(""));
        assert!(!is_valid_option_key("search path"));
        assert!(!is_valid_option_key("1cluster"));
        assert!(!is_valid_option_key("cluster-name"));
        assert!(!is_valid_option_key("cluster.name"));
        assert!(!is_valid_option_key("'"));
        assert!(!is_valid_option_key("café"));
    }

    #[mz_ore::test]
    fn test_profiles_config_rejects_invalid_option_key() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("profiles.toml");
        fs::write(
            &path,
            r#"
                [staging]
                host = "staging.example.com"
                username = "deploy_bot"

                [staging.options]
                "search path" = "public"
            "#,
        )
        .unwrap();
        let err = ProfilesConfig::load(Some(dir.path())).unwrap_err();
        match err {
            ConfigError::InvalidOptionKey { profile, key } => {
                assert_eq!(profile, "staging");
                assert_eq!(key, "search path");
            }
            other => panic!("expected InvalidOptionKey, got {other:?}"),
        }
    }

    #[mz_ore::test]
    fn test_builtin_emulator_profile_present_in_loaded_config() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("profiles.toml"),
            r#"
                [staging]
                host = "staging.example.com"
                username = "deploy_bot"
            "#,
        )
        .unwrap();
        let config = ProfilesConfig::load(Some(dir.path())).unwrap();
        assert!(config.profile_names().contains(&EMULATOR_PROFILE_NAME));
        let profile = config.get_profile(EMULATOR_PROFILE_NAME).unwrap();
        assert_eq!(profile.host.as_deref(), Some("localhost"));
        assert_eq!(profile.port, 6875);
        assert_eq!(profile.username, "materialize");
    }

    #[mz_ore::test]
    fn test_builtin_emulator_profile_resolves_without_profiles_file() {
        let dir = tempfile::tempdir().unwrap();
        // No profiles.toml written.
        let profile =
            ProfilesConfig::resolve_profile(Some(dir.path()), EMULATOR_PROFILE_NAME).unwrap();
        assert_eq!(profile.host.as_deref(), Some("localhost"));
        assert_eq!(profile.port, 6875);
        assert_eq!(profile.username, "materialize");
    }

    #[mz_ore::test]
    fn test_user_defined_emulator_profile_overrides_builtin() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join("profiles.toml"),
            r#"
                [emulator]
                host = "custom.example.com"
                username = "alice"
            "#,
        )
        .unwrap();
        let config = ProfilesConfig::load(Some(dir.path())).unwrap();
        let profile = config.get_profile(EMULATOR_PROFILE_NAME).unwrap();
        assert_eq!(profile.host.as_deref(), Some("custom.example.com"));
        assert_eq!(profile.username, "alice");
    }

    #[mz_ore::test]
    fn mzprofile_absent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_mzprofile(dir.path()).unwrap(), None);
    }

    #[mz_ore::test]
    fn mzprofile_reads_single_line() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join(MZPROFILE_FILENAME), "staging\n").unwrap();
        assert_eq!(
            read_mzprofile(dir.path()).unwrap().as_deref(),
            Some("staging")
        );
    }

    #[mz_ore::test]
    fn mzprofile_skips_comments_and_blanks() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(
            dir.path().join(MZPROFILE_FILENAME),
            "# set by `mz-deploy profile set`\n\n  dev\n",
        )
        .unwrap();
        assert_eq!(read_mzprofile(dir.path()).unwrap().as_deref(), Some("dev"));
    }

    #[mz_ore::test]
    fn mzprofile_write_then_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        write_mzprofile(dir.path(), "prod").unwrap();
        assert_eq!(read_mzprofile(dir.path()).unwrap().as_deref(), Some("prod"));
    }

    #[mz_ore::test]
    fn project_toml_rejects_string_under_profile_key() {
        let err = toml::from_str::<ProjectSettings>(r#"profile = "default""#).unwrap_err();
        assert!(
            err.to_string().contains("expected struct ProfileConfig"),
            "got: {err}"
        );
    }

    #[mz_ore::test]
    fn validate_dependencies_accepts_user_three_part() {
        let settings: ProjectSettings =
            toml::from_str(r#"dependencies = ["materialize.public.foo"]"#).unwrap();
        let deps = settings.validate_dependencies().unwrap();
        assert_eq!(deps.len(), 1);
        let dep = deps.iter().next().unwrap();
        assert_eq!(dep.database(), Some("materialize"));
        assert_eq!(dep.schema(), "public");
        assert_eq!(dep.object(), "foo");
    }

    #[mz_ore::test]
    fn validate_dependencies_accepts_system_two_part() {
        let settings: ProjectSettings =
            toml::from_str(r#"dependencies = ["mz_catalog.mz_objects", "pg_catalog.pg_class"]"#)
                .unwrap();
        let deps = settings.validate_dependencies().unwrap();
        assert_eq!(deps.len(), 2);
        for dep in &deps {
            assert_eq!(dep.database(), None);
        }
    }

    #[mz_ore::test]
    fn validate_dependencies_rejects_non_system_two_part() {
        let settings: ProjectSettings = toml::from_str(r#"dependencies = ["public.foo"]"#).unwrap();
        let err = settings.validate_dependencies().unwrap_err();
        assert!(matches!(err, ConfigError::InvalidDependency { .. }));
    }

    #[mz_ore::test]
    fn validate_dependencies_rejects_one_part() {
        let settings: ProjectSettings = toml::from_str(r#"dependencies = ["foo"]"#).unwrap();
        let err = settings.validate_dependencies().unwrap_err();
        assert!(matches!(err, ConfigError::InvalidDependency { .. }));
    }

    #[mz_ore::test]
    fn validate_dependencies_rejects_duplicates() {
        let settings: ProjectSettings =
            toml::from_str(r#"dependencies = ["mz_catalog.mz_objects", "mz_catalog.mz_objects"]"#)
                .unwrap();
        let err = settings.validate_dependencies().unwrap_err();
        assert!(matches!(err, ConfigError::DuplicateDependency { .. }));
    }
}
