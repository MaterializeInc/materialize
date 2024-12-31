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

//! Command-line parsing utilities.

use std::ffi::OsString;
use std::fmt::{self, Display};
use std::str::FromStr;

use clap::Parser;

/// A help template for use with clap that does not include the name of the
/// binary or the version in the help output.
const NO_VERSION_HELP_TEMPLATE: &str = "{about}

USAGE:
    {usage}

{all-args}";

/// Configures command-line parsing via [`parse_args`].
#[derive(Debug, Default, Clone)]
pub struct CliConfig<'a> {
    /// An optional prefix to apply to the environment variable name for  all
    /// arguments with an environment variable fallback.
    //
    // TODO(benesch): switch to the clap-native `env_prefix` option if that
    // gets implemented: https://github.com/clap-rs/clap/issues/3221.
    pub env_prefix: Option<&'a str>,
    /// Enable clap's built-in `--version` flag.
    ///
    /// We disable this by default because most of our binaries are not
    /// meaningfully versioned.
    pub enable_version_flag: bool,
}

/// Parses command-line arguments according to a clap `Parser` after
/// applying Materialize-specific customizations.
pub fn parse_args<O>(config: CliConfig) -> O
where
    O: Parser,
{
    // Construct the prefixed environment variable names for all
    // environment-enabled arguments, if requested. We have to construct these
    // names before constructing `clap` below to get the lifetimes to work out.
    let arg_envs: Vec<_> = O::command()
        .get_arguments()
        .filter_map(|arg| match (config.env_prefix, arg.get_env()) {
            (Some(prefix), Some(env)) => {
                let mut prefixed_env = OsString::from(prefix);
                prefixed_env.push(env);
                Some((arg.get_id(), prefixed_env))
            }
            _ => None,
        })
        .collect();

    let mut clap = O::command().args_override_self(true);

    if !config.enable_version_flag {
        clap = clap.disable_version_flag(true);
        clap = clap.help_template(NO_VERSION_HELP_TEMPLATE);
    }

    for (arg, env) in &arg_envs {
        clap = clap.mut_arg(*arg, |arg| arg.env_os(env));
    }

    O::from_arg_matches(&clap.get_matches()).unwrap()
}

/// A command-line argument of the form `KEY=VALUE`.
#[derive(Debug, Clone)]
pub struct KeyValueArg<K, V> {
    /// The key of the command-line argument.
    pub key: K,
    /// The value of the command-line argument.
    pub value: V,
}

impl<K, V> FromStr for KeyValueArg<K, V>
where
    K: FromStr,
    K::Err: Display,
    V: FromStr,
    V::Err: Display,
{
    type Err = String;

    fn from_str(s: &str) -> Result<KeyValueArg<K, V>, String> {
        let mut parts = s.splitn(2, '=');
        let key = parts.next().expect("always one part");
        let value = parts
            .next()
            .ok_or_else(|| "must have format KEY=VALUE".to_string())?;
        Ok(KeyValueArg {
            key: key.parse().map_err(|e| format!("parsing key: {}", e))?,
            value: value.parse().map_err(|e| format!("parsing value: {}", e))?,
        })
    }
}

/// A command-line argument that defaults to `true`
/// that can be falsified with `--flag=false`
// TODO: replace with `SetTrue` when available in clap.
#[derive(Debug, Clone)]
pub struct DefaultTrue {
    /// The value for this flag
    pub value: bool,
}

impl Default for DefaultTrue {
    fn default() -> Self {
        Self { value: true }
    }
}

impl fmt::Display for DefaultTrue {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl FromStr for DefaultTrue {
    type Err = std::str::ParseBoolError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(Self { value: s.parse()? })
    }
}
