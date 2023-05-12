// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Display;
use std::fs::read_to_string;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use mz::api::{get_provider_region_environment, CloudProviderRegion};
use mz::configuration::ValidProfile;
use postgres_protocol::escape::{escape_identifier, escape_literal};
use reqwest::Client;
use serde::{Deserialize, Serialize};

pub use self::clap_clippy_hack::*;

// Do not add anything but structs/enums with Clap derives in this module!
//
// Clap v3 sometimes triggers this warning with subcommands,
// and its unclear if it will be fixed in v3, and not just
// in v4. This can't be overridden at the macro level, and instead must be overridden
// at the module level.
//
// TODO(guswynn): remove this when we are using Clap v4.
#[allow(clippy::almost_swapped)]
mod clap_clippy_hack {
    use super::*;
    #[derive(Debug, Subcommand)]
    pub enum SecretCommand {
        /// Create a new secret
        Create {
            /// The name of the secret to create
            name: String,

            #[clap(flatten)]
            contents: Contents,
        },

        /// Delete a secret
        Delete {
            /// The name of the secret to create
            name: String,
        },

        // List all secrets
        List,

        /// Update a secret
        Update {
            /// The name of the secret to update
            name: String,

            #[clap(flatten)]
            contents: Contents,
        },
    }
}

impl SecretCommand {
    pub(crate) async fn execute(
        &self,
        profile: ValidProfile<'_>,
        region: CloudProviderRegion,
        client: Client,
    ) -> Result<()> {
        match self {
            SecretCommand::Create { name, contents } => {
                let name = escape_identifier(name);
                let contents = contents.get()?;
                let contents = escape_literal(&contents);
                let sql = Sql {
                    query: format!("create secret {name} as {contents};"),
                };

                execute_query(&client, &profile, region, sql)
                    .await
                    .context("failed to create secret")
            }
            SecretCommand::Delete { name } => {
                let name = escape_identifier(name);
                let sql = Sql {
                    query: format!("drop secret {name};"),
                };

                execute_query(&client, &profile, region, sql)
                    .await
                    .context("failed to delete secret")
            }
            SecretCommand::List => {
                let sql = Sql {
                    query: "set cluster = mz_introspection; show secrets;".to_string(),
                };

                execute_query(&client, &profile, region, sql)
                    .await
                    .context("failed to list secrets")
            }
            SecretCommand::Update { name, contents } => {
                let name = escape_identifier(name);
                let contents = contents.get()?;
                let contents = escape_literal(&contents);
                let sql = Sql {
                    query: format!("alter secret {name} as {contents};"),
                };

                execute_query(&client, &profile, region, sql)
                    .await
                    .context("failed to update secret")
            }
        }
    }
}

#[derive(Parser, Debug)]
#[clap(group = clap::ArgGroup::new("input").multiple(false))]
pub struct Contents {
    /// The value of a secret. This cannot be
    /// specified if --file is also set.
    #[clap(group = "input")]
    value: Option<String>,

    /// A file containing the value of a secret. This flag
    /// cannot be specified if a literal value is provided.
    #[clap(short, long, group = "input")]
    file: Option<String>,
}

impl Contents {
    fn get(&self) -> Result<String> {
        if let Some(ref value) = self.value {
            return Ok(value.clone());
        }

        if let Some(ref path) = self.file {
            return read_to_string(path)
                .with_context(|| format!("failed to read contents from file {}", path));
        }

        rpassword::prompt_password("secret>").context("failed to read secret")
    }
}

#[derive(Serialize, Debug)]
struct Sql {
    query: String,
}

#[derive(Deserialize, Debug)]
struct Notice {
    severity: String,
    message: String,
}

impl Display for Notice {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.severity, self.message)
    }
}

#[derive(Deserialize, Debug)]
#[serde(untagged, deny_unknown_fields)]
#[allow(dead_code)]
enum SqlResult {
    Rows {
        rows: Vec<Vec<String>>,
        col_names: Vec<String>,
        notices: Vec<Notice>,
    },
    Error {
        error: String,
        notices: Vec<Notice>,
    },
    Ok {
        ok: String,
        notices: Vec<Notice>,
    },
}

#[derive(Deserialize, Debug)]
struct Results {
    results: Vec<SqlResult>,
}

async fn execute_query(
    client: &Client,
    valid_profile: &ValidProfile<'_>,
    cloud_provider_region: CloudProviderRegion,
    sql: Sql,
) -> Result<(), anyhow::Error> {
    let environment =
        get_provider_region_environment(client, valid_profile, &cloud_provider_region)
            .await
            .context("retrieving cloud provider region")?;

    let endpoint = &environment.environmentd_https_address
        [0..environment.environmentd_https_address.len() - 4];
    let url = format!("https://{endpoint}/api/sql");

    let results = client
        .post(url)
        .basic_auth(
            valid_profile.profile.get_email(),
            Some(valid_profile.app_password.to_string()),
        )
        .json(&sql)
        .send()
        .await
        .context("failed to issue query")?
        .json::<Results>()
        .await
        .context("failed to extract results")?;

    for result in results.results {
        match result {
            SqlResult::Rows { rows, notices, .. } => {
                for row in rows {
                    println!("{}", row.join("\t"));
                }

                for notice in notices {
                    eprintln!("{notice}")
                }
            }
            SqlResult::Ok { notices, .. } => {
                for notice in notices {
                    eprintln!("{notice}")
                }
            }
            SqlResult::Error { error, notices } => {
                for notice in notices {
                    eprintln!("{notice}")
                }
                eprintln!("{error}")
            }
        }
    }

    Ok(())
}
