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

use anyhow::{anyhow, bail, Result};
use clap::Parser;
use keyring::Entry;
use serde::{de::Visitor, Deserialize, Serialize};

use crate::configuration::FronteggAPIToken;

const INLINE_PREFIX: &str = "mzp_";

/// A vault stores sensitive information and provides
/// back a token than can be used for later retrieval.
/// By default, credentials are stored in the OS specific
/// keychain if it exists.
#[derive(Parser, Debug)]
#[clap(group = clap::ArgGroup::new("vault").multiple(false))]
pub struct Vault {
    /// Inline the credentials in the CLIs
    /// configuration file.
    #[clap(long, short, group = "vault")]
    inline: bool,
}

impl Vault {
    pub fn store(&self, profile: &str, email: &str, api_token: FronteggAPIToken) -> Result<Token> {
        let password = api_token.to_string();
        if self.inline {
            return Ok(Token::Inline(password));
        }

        match Entry::new(&service_name(profile), email).set_password(&api_token.to_string()) {
            Ok(_) => Ok(Token::Keychain),
            Err(keyring::Error::NoEntry) => Ok(Token::Inline(password)),
            Err(e) => bail!(e),
        }
    }
}

#[derive(PartialEq, Eq)]
pub enum Token {
    Inline(String),
    Keychain,
}

impl Default for Token {
    fn default() -> Self {
        Token::Keychain
    }
}

impl Token {
    pub fn is_default(&self) -> bool {
        *self == Token::Keychain
    }

    pub fn retrieve(&self, profile: &str, email: &str) -> Result<String> {
        match self {
            Token::Inline(app_password) => Ok(app_password.to_string()),
            Token::Keychain => {
                let entry = Entry::new(&service_name(profile), email);
                entry.get_password().map_err(|e| match e {
                    keyring::Error::NoEntry => anyhow!(
                        "No credentials found in local keychain. Reauthenticate with mz login."
                    ),
                    _ => anyhow!(e),
                })
            }
        }
    }
}

fn service_name(profile: &str) -> String {
    format!("Materialize CLI - Profile[{profile}]")
}

/// Custom debug implementation
/// to avoid leaking sensitive data.
impl std::fmt::Debug for Token {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Inline(_) => write!(f, "Inline(_)"),
            Self::Keychain => write!(f, "Keychain"),
        }
    }
}

impl Serialize for Token {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Token::Inline(app_password) => serializer.serialize_str(app_password),
            Token::Keychain => unreachable!("keychain passwords should never be serialized"),
        }
    }
}

struct TokenVisitor;

impl<'de> Visitor<'de> for TokenVisitor {
    type Value = Token;

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        if v.starts_with(INLINE_PREFIX) {
            return Ok(Token::Inline(v.to_string()));
        }

        Err(E::custom("unknown app password token"))
    }

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("app password token")
    }
}

impl<'de> Deserialize<'de> for Token {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_str(TokenVisitor)
    }
}
