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

use anyhow::Result;
use clap::Parser;
use serde::de::Visitor;
use serde::{Deserialize, Serialize};

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
    #[clap(long, group = "vault")]
    inline: bool,
}

impl Vault {
    pub fn store(&self, profile: &str, email: &str, api_token: FronteggAPIToken) -> Result<Token> {
        if self.inline {
            let password = api_token.to_string();
            return Ok(Token::Inline(password));
        }

        keychain::set_app_password(profile, email, api_token)
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
            Token::Keychain => keychain::get_app_password(profile, email),
        }
    }
}

#[cfg(not(target_os = "macos"))]
mod keychain {
    use anyhow::{bail, Result};

    use crate::configuration::FronteggAPIToken;

    use super::Token;

    pub fn get_app_password(_: &str, _: &str) -> Result<String> {
        bail!("no credentials set. authenticate using mz login")
    }

    pub fn set_app_password(_: &str, _: &str, api_token: FronteggAPIToken) -> Result<Token> {
        // The platform does not support local keychains.
        // Fall back to inline credential management.
        let password = api_token.to_string();
        Ok(Token::Inline(password))
    }
}

#[cfg(target_os = "macos")]
mod keychain {
    use anyhow::{anyhow, Context, Result};
    use security_framework::base::Error;
    use security_framework::passwords::{get_generic_password, set_generic_password};

    use crate::configuration::FronteggAPIToken;

    use super::Token;

    pub fn get_app_password(profile: &str, email: &str) -> Result<String> {
        let service = service_name(profile);
        let password = get_generic_password(&service, email).map_err(error_handler)?;
        String::from_utf8(password.to_vec()).context("failed to decode app password")
    }

    pub fn set_app_password(
        profile: &str,
        email: &str,
        api_token: FronteggAPIToken,
    ) -> Result<Token> {
        let service = service_name(profile);
        set_generic_password(&service, email, api_token.to_string().as_bytes())
            .map_err(error_handler)
            .map(|_| Token::Keychain)
    }

    /// The MacOS error codes used here are from:
    /// <https://opensource.apple.com/source/libsecurity_keychain/libsecurity_keychain-78/lib/SecBase.h.auto.html>
    fn error_handler(error: Error) -> anyhow::Error {
        if error.code() == -25300 {
            anyhow!("no credentials found in local keychain. reauthenticate with mz login.")
        } else {
            anyhow::Error::new(error).context("failed to access keychain storage")
        }
    }

    fn service_name(profile: &str) -> String {
        format!("Materialize CLI - Profile[{profile}]")
    }
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
