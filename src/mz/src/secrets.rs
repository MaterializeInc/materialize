// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{fmt::Display, fs::read_to_string};

use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::{
    configuration::ValidProfile,
    region::{get_provider_region_environment, CloudProviderRegion},
};

#[derive(Debug, Subcommand)]
#[clap(group = clap::ArgGroup::new("input").multiple(false))]
pub enum SecretCommand {
    /// Alter a secret
    Alter {
        /// The name of the secret to create
        #[clap(short, long)]
        name: String,

        #[clap(flatten)]
        contents: Contents,
    },

    /// Create a new secret
    Create {
        /// The name of the secret to create
        #[clap(short, long)]
        name: String,

        #[clap(flatten)]
        contents: Contents,
    },

    /// Delete a secret
    Delete {
        /// The name of the secret to create
        #[clap(short, long)]
        name: String,
    },

    // List all secrets
    List,
}

impl SecretCommand {
    pub(crate) async fn execute(
        &self,
        profile: ValidProfile<'_>,
        region: CloudProviderRegion,
        client: Client,
    ) -> Result<()> {
        match self {
            SecretCommand::Alter { name, contents } => {
                let contents = contents.get()?;
                let sql = Sql {
                    query: format!("alter secret {name} as '{contents}';"),
                };

                execute_query(&client, &profile, region, sql)
                    .await
                    .context("failed to create secret")
            }
            SecretCommand::Create { name, contents } => {
                let contents = contents.get()?;
                let sql = Sql {
                    query: format!("create secret {name} as '{contents}';"),
                };

                execute_query(&client, &profile, region, sql)
                    .await
                    .context("failed to create secret")
            }
            SecretCommand::Delete { name } => {
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
        }
    }
}

#[derive(Parser, Debug)]
#[clap(group = clap::ArgGroup::new("input").multiple(false))]
pub struct Contents {
    /// The value of a secret. This flag cannot be
    /// specified if --file is also set.
    #[clap(short, long, group = "input")]
    value: Option<String>,

    /// A file containing the value of a secret. This flag
    /// cannot be specified if --value is also set.
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

        bail!("must specify either --value or --file")
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
            Some(valid_profile.profile.get_app_password()),
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
