// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use std::collections::BTreeMap;

use crate::error::{Context, OpError, OpErrorKind};
use crate::fivetran_sdk::form_field::Type;
use crate::fivetran_sdk::{
    ConfigurationFormResponse, ConfigurationTest, FormField, TestRequest, TextField,
};

pub const FIVETRAN_DESTINATION_APPLICATION_NAME: &str = "materialize_fivetran_destination";

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
            "SELECT has_database_privilege($1, 'CREATE') AS has_create",
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

    let builder = SslConnector::builder(SslMethod::tls_client())?;
    let tls_connector = MakeTlsConnector::new(builder.build());
    let (client, conn) = tokio_postgres::Config::new()
        .host(&host)
        .user(&user)
        .port(6875)
        .password(app_password)
        .dbname(&dbname)
        .application_name(FIVETRAN_DESTINATION_APPLICATION_NAME)
        .connect(tls_connector)
        .await?;

    mz_ore::task::spawn(|| "postgres_connection", async move {
        if let Err(e) = conn.await {
            panic!("tokio-postgres connection error: {}", e);
        }
    });

    Ok((dbname, client))
}
