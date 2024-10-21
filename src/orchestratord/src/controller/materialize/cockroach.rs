// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use k8s_openapi::api::core::v1::Secret;
use kube::Api;
use rand::{distributions::Alphanumeric, thread_rng, Rng};
use tracing::{debug, trace};
use urlencoding::encode;

use crate::k8s::{apply_resource, get_resource};
use mz_cloud_resources::crd::materialize::v1alpha1::Materialize;
use mz_ore::instrument;

// TODO: we should not be passing in cockroach connection info through cli
// flags - this will work for testing in kind for now, but we'll want to move
// this to a kubernetes secret or something like that
#[derive(clap::Parser)]
pub struct CockroachInfo {
    #[clap(long)]
    pub cockroach_endpoint: String,
    #[clap(long)]
    pub cockroach_port: u16,
    #[clap(long)]
    pub cockroach_ca_cert: String,
    #[clap(long)]
    pub cockroach_username: String,
    #[clap(long)]
    pub cockroach_password: String,
}

pub async fn create_database(
    cockroach_info: &CockroachInfo,
    mz: &Materialize,
) -> Result<(), anyhow::Error> {
    let connection = make_connection(cockroach_info, "postgres").await?;

    trace!("creating role");
    connection
        .execute(
            &format!(
                r#"CREATE ROLE IF NOT EXISTS "{}" WITH NOLOGIN"#,
                mz.cockroach_role_name(),
            ),
            &[],
        )
        .await?;

    trace!("creating database");
    connection
        .execute(
            &format!(
                r#"CREATE DATABASE IF NOT EXISTS "{}" WITH OWNER "{}""#,
                mz.cockroach_database_name(),
                mz.cockroach_role_name(),
            ),
            &[],
        )
        .await?;

    trace!("done creating database");

    Ok(())
}

pub async fn manage_role_password(
    cockroach_info: &CockroachInfo,
    client: kube::Client,
    mz: &Materialize,
) -> Result<(), anyhow::Error> {
    let secret_api: Api<Secret> = Api::namespaced(client, &mz.namespace());

    let connection = make_connection(cockroach_info, "postgres").await?;

    // Fetch or set the password for the role. If we already have a
    // Kubernetes secret on hand, we can pull out the password from the
    // secret. Otherwise, we generate a new password and run `ALTER ROLE` to
    // install the password.
    //
    // In the past, we blindly ran `ALTER ROLE` on every reconciliation, but
    // this was expensive. So we're now careful to only run `ALTER ROLE` if
    // we don't already know the password.
    let password = match get_resource(&secret_api, &Materialize::cockroach_secret_name())
        .await?
        .and_then(|secret| secret.data)
        .as_ref()
        .and_then(|data| data.get("PASSWORD"))
    {
        Some(secret) => {
            debug!("using existing cockroach secret");
            String::from_utf8(secret.0.clone()).unwrap()
        }
        None => {
            debug!("generating new cockroach secret");
            let password: String = thread_rng()
                .sample_iter(&Alphanumeric)
                .take(32)
                .map(char::from)
                .collect();
            alter_role(mz, connection, &password).await?;
            password
        }
    };

    // Unconditionally update the Kubernetes secret, even if we just pulled
    // out the current password from the secret. This is important because
    // `DATABASE_CONFIGS` may change, and we want to update the secret to
    // include new database configs and remove old database configs.
    //
    // NOTE(benesch): things would be clearer if we had *two* secrets, one
    // that contained only the Cockroach password, and one that contained the
    // database configurations, but that's a delicate change to roll out.
    let secret = create_secret(cockroach_info, mz, &password);
    trace!("applying cockroach role password k8s secret");
    apply_resource(&secret_api, &secret).await?;

    Ok(())
}

async fn make_connection(
    cockroach_info: &CockroachInfo,
    database_name: &str,
) -> Result<tokio_postgres::Client, anyhow::Error> {
    let mut config = tokio_postgres::config::Config::new();
    config
        .application_name("orchestratord")
        .host(&cockroach_info.cockroach_endpoint)
        .port(cockroach_info.cockroach_port)
        .user(&cockroach_info.cockroach_username)
        .password(&cockroach_info.cockroach_password)
        .dbname(database_name)
        .ssl_mode(tokio_postgres::config::SslMode::VerifyCa)
        .ssl_root_cert(cockroach_info.cockroach_ca_cert.as_bytes());
    let tls = mz_tls_util::make_tls(&config)?;

    let (client, connection) = if let Some(timeout) = config.get_connect_timeout() {
        tokio::time::timeout(timeout.clone(), config.connect(tls)).await??
    } else {
        config.connect(tls).await?
    };

    mz_ore::task::spawn(|| "cockroach connection", async move {
        if let Err(e) = connection.await {
            panic!("Error in connection to MZ instance: {e}");
        }
    });

    Ok(client)
}

fn create_secret(cockroach_info: &CockroachInfo, mz: &Materialize, pg_password: &str) -> Secret {
    let pg_database_url = format!(
        "postgres://{}:{}@{}:{}/{}?sslmode=verify-full&sslrootcert_inline={}",
        mz.cockroach_role_name(),
        encode(pg_password),
        cockroach_info.cockroach_endpoint,
        cockroach_info.cockroach_port,
        mz.cockroach_database_name(),
        encode(&cockroach_info.cockroach_ca_cert),
    );

    let mut string_data = BTreeMap::new();
    string_data.insert("MZ_METADATA_BACKEND_URL".to_string(), pg_database_url);
    string_data.insert("PASSWORD".to_string(), pg_password.to_string());

    Secret {
        metadata: mz.managed_resource_meta(Materialize::cockroach_secret_name()),
        string_data: Some(string_data),
        ..Default::default()
    }
}

#[instrument(fields(
    role_name=%mz.cockroach_role_name(),
    database_name=%mz.cockroach_database_name(),
))]
async fn alter_role(
    mz: &Materialize,
    connection: tokio_postgres::Client,
    pg_password: &str,
) -> Result<(), anyhow::Error> {
    trace!("altering role to set password");
    connection
        .execute(
            &format!(
                r#"ALTER ROLE "{}" WITH LOGIN PASSWORD '{}'"#,
                &mz.cockroach_role_name(),
                &pg_password,
            ),
            &[],
        )
        .await?;

    Ok(())
}
