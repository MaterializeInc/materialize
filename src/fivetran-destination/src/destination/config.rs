// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use openssl::ssl::{SslConnector, SslMethod};
use openssl::x509::store::X509StoreBuilder;
use openssl::x509::X509;
use postgres_openssl::MakeTlsConnector;
use std::collections::BTreeMap;

use crate::error::{Context, OpError, OpErrorKind};
use crate::fivetran_sdk::form_field::Type;
use crate::fivetran_sdk::{
    ConfigurationFormResponse, ConfigurationTest, FormField, TestRequest, TextField,
};
use crate::utils;

pub const FIVETRAN_DESTINATION_APPLICATION_NAME: &str = "materialize_fivetran_destination";

/// Returns a [`ConfigurationFormResponse`] which defines what configuration fields are available
/// for users of the destination to specify.
pub fn handle_configuration_form_request() -> ConfigurationFormResponse {
    ConfigurationFormResponse {
        schema_selection_supported: true,
        table_selection_supported: true,
        fields: vec![
            FormField {
                name: "host".into(),
                label: "Host".into(),
                description: Some("The hostname of your Materialize region".into()),
                required: true,
                r#type: Some(Type::TextField(TextField::PlainText.into())),
            },
            FormField {
                name: "user".into(),
                label: "User".into(),
                description: Some("The user to connect as".into()),
                required: true,
                r#type: Some(Type::TextField(TextField::PlainText.into())),
            },
            FormField {
                name: "app_password".into(),
                label: "App password".into(),
                description: Some("The app password to authenticate with".into()),
                required: true,
                r#type: Some(Type::TextField(TextField::Password.into())),
            },
            FormField {
                name: "dbname".into(),
                label: "Database".into(),
                description: Some("The name of the database to connect to".into()),
                required: true,
                r#type: Some(Type::TextField(TextField::PlainText.into())),
            },
            FormField {
                name: "cluster".into(),
                label: "Cluster".into(),
                description: Some("The cluster to run operations on".into()),
                required: false,
                r#type: Some(Type::TextField(TextField::PlainText.into())),
            },
        ],
        tests: vec![
            ConfigurationTest {
                name: "connect".into(),
                label: "Connecting to Materialize region".into(),
            },
            ConfigurationTest {
                name: "permissions".into(),
                label: "Checking permissions".into(),
            },
        ],
    }
}

pub async fn handle_test_request(request: TestRequest) -> Result<(), OpError> {
    match request.name.as_str() {
        "connect" => test_connect(request.configuration)
            .await
            .context("test_connect"),
        "permissions" => test_permissions(request.configuration)
            .await
            .context("test_permissions"),
        "ping" => Ok(()),
        name => {
            let error = OpErrorKind::UnknownRequest(name.to_string());
            Err(error.into())
        }
    }
}

async fn test_connect(config: BTreeMap<String, String>) -> Result<(), OpError> {
    let _ = connect(config).await?;
    Ok(())
}

async fn test_permissions(config: BTreeMap<String, String>) -> Result<(), OpError> {
    let (dbname, client) = connect(config).await?;
    let row = client
        .query_one(
            "SELECT has_database_privilege($1, 'CREATE') OR mz_is_superuser() AS has_create",
            &[&dbname],
        )
        .await
        .context("querying privileges")?;
    let has_create: bool = row.get("has_create");

    if !has_create {
        let err = OpErrorKind::MissingPrivilege {
            privilege: "CREATE",
            object: dbname,
        }
        .into();
        return Err(err);
    }
    Ok(())
}

pub async fn connect(
    mut config: BTreeMap<String, String>,
) -> Result<(String, tokio_postgres::Client), OpError> {
    tracing::info!(?config, "Connecting to Materialize");
    let host = config
        .remove("host")
        .ok_or(OpErrorKind::FieldMissing("host"))?;
    let user = config
        .remove("user")
        .ok_or(OpErrorKind::FieldMissing("user"))?;
    let app_password = config
        .remove("app_password")
        .ok_or(OpErrorKind::FieldMissing("app_password"))?;
    let dbname = config
        .remove("dbname")
        .ok_or(OpErrorKind::FieldMissing("dbname"))?;
    let cluster = config
        .remove("cluster")
        // Fivetran provides an empty string for optional parameters.
        .and_then(|val| if val.is_empty() { None } else { Some(val) });

    // Compile in the CA certificate bundle downloaded by the build script, and
    // configure the TLS connector to reference that compiled-in CA bundle,
    // rather than attempting to use the system's CA bundle. This supports
    // running in Fivetran's environment, where the CA bundle will not be
    // available. This does introduce a small amount of risk, as the CA bundle
    // will not be updated until we issue a new release of the Fivetran
    // destination.
    //
    // TODO: depend on the system's certificate bundle instead, once Fivetran
    // supports running destinations in a containerized environment.
    let ca_bundle = include_bytes!(concat!(env!("OUT_DIR"), "/ca-certificate.crt"));
    let ca_certs = X509::stack_from_pem(ca_bundle)?;
    let mut cert_store = X509StoreBuilder::new()?;
    for cert in ca_certs {
        cert_store.add_cert(cert)?;
    }
    let mut builder = SslConnector::builder(SslMethod::tls_client())?;
    builder.set_verify_cert_store(cert_store.build())?;

    let options = if let Some(cluster) = cluster.as_ref() {
        format!("--cluster={}", utils::escape_options(cluster))
    } else {
        String::new()
    };

    let tls_connector = MakeTlsConnector::new(builder.build());
    let (client, conn) = tokio_postgres::Config::new()
        .host(&host)
        .user(&user)
        .port(6875)
        .password(app_password)
        .dbname(&dbname)
        .application_name(FIVETRAN_DESTINATION_APPLICATION_NAME)
        .options(&options)
        .connect(tls_connector)
        .await?;

    mz_ore::task::spawn(|| "postgres_connection", async move {
        if let Err(e) = conn.await {
            panic!("tokio-postgres connection error: {}", e);
        }
    });

    Ok((dbname, client))
}
