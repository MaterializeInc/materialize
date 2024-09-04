// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// can't call foreign function `OPENSSL_init_ssl` on OS `linux`
#![cfg(not(miri))]

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::time::Duration;

use chrono::Utc;
use jsonwebtoken::{DecodingKey, EncodingKey};
use mz_frontegg_mock::{
    models::ApiToken, models::UserConfig, models::UserRole, FronteggMockServer,
};
use mz_ore::now::SYSTEM_TIME;
use openssl::rsa::Rsa;
use reqwest::Client;
use serde_json::json;
use uuid::Uuid;

#[allow(dead_code)]
struct TestContext {
    server: FronteggMockServer,
    client: Client,
    base_url: String,
    user_email: String,
    user_password: String,
    client_id: Uuid,
    secret: Uuid,
    tenant_id: Uuid,
    roles: Vec<UserRole>,
}

fn generate_rsa_keys() -> (Vec<u8>, Vec<u8>) {
    let rsa = Rsa::generate(2048).unwrap();
    let private_key = rsa.private_key_to_pem().unwrap();
    let public_key = rsa.public_key_to_pem().unwrap();
    (private_key, public_key)
}

async fn setup_test_context() -> TestContext {
    let (private_key, public_key) = generate_rsa_keys();

    let tenant_id = Uuid::new_v4();
    let email = "user@example.com".to_string();
    let password = Uuid::new_v4().to_string();
    let client_id = Uuid::new_v4();
    let secret = Uuid::new_v4();
    let initial_api_tokens = vec![ApiToken {
        client_id,
        secret,
        description: None,
        created_at: Utc::now(),
    }];
    let roles = vec!["user".to_string()];
    let users = BTreeMap::from([(
        email.clone(),
        UserConfig {
            id: Uuid::new_v4(),
            email: email.clone(),
            password: password.clone(),
            tenant_id,
            initial_api_tokens,
            roles,
            auth_provider: None,
            verified: Some(true),
            metadata: None,
        },
    )]);

    let issuer = "frontegg-mock".to_owned();
    let encoding_key = EncodingKey::from_rsa_pem(&private_key).unwrap();
    let decoding_key = DecodingKey::from_rsa_pem(&public_key).unwrap();

    let roles: Vec<UserRole> = vec![
        UserRole {
            id: uuid::Uuid::new_v4().to_string(),
            name: "Organization Admin".to_string(),
            key: "MaterializePlatformAdmin".to_string(),
        },
        UserRole {
            id: uuid::Uuid::new_v4().to_string(),
            name: "Organization Member".to_string(),
            key: "MaterializePlatform".to_string(),
        },
    ];

    const EXPIRES_IN_SECS: i64 = 50;
    let server = FronteggMockServer::start(
        Some(&SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
        issuer,
        encoding_key,
        decoding_key,
        users,
        BTreeMap::new(),
        None,
        SYSTEM_TIME.clone(),
        EXPIRES_IN_SECS,
        Some(Duration::from_millis(100)),
        Some(roles.clone()),
    )
    .await
    .unwrap();

    let client = Client::new();
    let base_url = server.base_url.clone();

    TestContext {
        server,
        client,
        base_url,
        user_email: email,
        user_password: password,
        client_id,
        secret,
        tenant_id,
        roles,
    }
}

async fn authenticate(ctx: &TestContext) -> Result<String, Box<dyn std::error::Error>> {
    let auth_response = ctx
        .client
        .post(format!("{}/identity/resources/auth/v1/user", ctx.base_url))
        .json(&json!({
            "email": ctx.user_email,
            "password": ctx.user_password
        }))
        .send()
        .await?;

    if auth_response.status() != 200 {
        return Err("Authentication failed".into());
    }

    let auth_body: serde_json::Value = auth_response.json().await?;
    Ok(auth_body["accessToken"].as_str().unwrap().to_string())
}

// Authentication Tests

#[mz_ore::test(tokio::test)]
async fn test_auth_api_token() {
    let ctx = setup_test_context().await;

    let response = ctx
        .client
        .post(format!(
            "{}/identity/resources/auth/v1/api-token",
            ctx.base_url
        ))
        .json(&json!({
            "clientId": ctx.client_id.to_string(),
            "secret": ctx.secret.to_string()
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.get("accessToken").is_some());
    assert!(body.get("refreshToken").is_some());
    assert!(body.get("expiresIn").is_some());
}

#[mz_ore::test(tokio::test)]
async fn test_invalid_authentication() {
    let ctx = setup_test_context().await;

    let response = ctx
        .client
        .post(format!("{}/identity/resources/auth/v1/user", ctx.base_url))
        .json(&json!({
            "email": "invalid@example.com",
            "password": "wrongpassword"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}

#[mz_ore::test(tokio::test)]
async fn test_token_refresh() {
    let ctx = setup_test_context().await;

    let initial_response = ctx
        .client
        .post(format!(
            "{}/identity/resources/auth/v1/api-token",
            ctx.base_url
        ))
        .json(&json!({
            "clientId": ctx.client_id.to_string(),
            "secret": ctx.secret.to_string()
        }))
        .send()
        .await
        .unwrap();

    let initial_body: serde_json::Value = initial_response.json().await.unwrap();
    let refresh_token = initial_body["refreshToken"].as_str().unwrap();

    let refresh_response = ctx
        .client
        .post(format!(
            "{}/identity/resources/auth/v1/api-token/token/refresh",
            ctx.base_url
        ))
        .json(&json!({
            "refreshToken": refresh_token
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(refresh_response.status(), 200);
    let refresh_body: serde_json::Value = refresh_response.json().await.unwrap();
    assert!(refresh_body.get("accessToken").is_some());
}

#[mz_ore::test(tokio::test)]
async fn test_invalid_api_token_authentication() {
    let ctx = setup_test_context().await;

    let response = ctx
        .client
        .post(format!(
            "{}/identity/resources/auth/v1/api-token",
            ctx.base_url
        ))
        .json(&json!({
            "clientId": Uuid::new_v4().to_string(),
            "secret": Uuid::new_v4().to_string()
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}

// User Profile Tests

#[mz_ore::test(tokio::test)]
async fn test_get_user_profile() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let profile_response = ctx
        .client
        .get(format!("{}/identity/resources/users/v2/me", ctx.base_url))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(profile_response.status(), 200);
    let profile_body: serde_json::Value = profile_response.json().await.unwrap();
    assert!(profile_body.get("tenantId").is_some());
}

#[mz_ore::test(tokio::test)]
async fn test_get_user_profile_without_token() {
    let ctx = setup_test_context().await;

    let profile_response = ctx
        .client
        .get(format!("{}/identity/resources/users/v2/me", ctx.base_url))
        .send()
        .await
        .unwrap();

    assert_eq!(profile_response.status(), 400);
}

// User API Token Tests

#[mz_ore::test(tokio::test)]
async fn test_create_user_api_token() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .post(format!(
            "{}/identity/resources/users/api-tokens/v1",
            ctx.base_url
        ))
        .bearer_auth(access_token)
        .json(&json!({
            "description": "Test API Token"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.get("clientId").is_some());
    assert!(body.get("secret").is_some());
}

#[mz_ore::test(tokio::test)]
async fn test_list_user_api_tokens() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .get(format!(
            "{}/identity/resources/users/api-tokens/v1",
            ctx.base_url
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.as_array().is_some());
}

#[mz_ore::test(tokio::test)]
async fn test_delete_user_api_token() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a token
    let create_response = ctx
        .client
        .post(format!(
            "{}/identity/resources/users/api-tokens/v1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "description": "Token to be deleted"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let token_id = create_body["clientId"].as_str().unwrap();

    // Now, delete the token
    let delete_response = ctx
        .client
        .delete(format!(
            "{}/identity/resources/users/api-tokens/v1/{}",
            ctx.base_url, token_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(delete_response.status(), 200);
}

// Tenant API Token Tests

#[mz_ore::test(tokio::test)]
async fn test_create_tenant_api_token() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .post(format!(
            "{}/identity/resources/tenants/api-tokens/v1",
            ctx.base_url
        ))
        .bearer_auth(access_token)
        .json(&json!({
            "description": "Test Tenant API Token",
            "roleIds": ["user"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.get("clientId").is_some());
    assert!(body.get("secret").is_some());
}

#[mz_ore::test(tokio::test)]
async fn test_list_tenant_api_tokens() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .get(format!(
            "{}/identity/resources/tenants/api-tokens/v1",
            ctx.base_url
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.as_array().is_some());
}

#[mz_ore::test(tokio::test)]
async fn test_delete_tenant_api_token() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a token
    let create_response = ctx
        .client
        .post(format!(
            "{}/identity/resources/tenants/api-tokens/v1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "description": "Tenant Token to be deleted",
            "roleIds": ["user"]
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let token_id = create_body["clientId"].as_str().unwrap();

    // Now, delete the token
    let delete_response = ctx
        .client
        .delete(format!(
            "{}/identity/resources/tenants/api-tokens/v1/{}",
            ctx.base_url, token_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(delete_response.status(), 200);
}

// User Management Tests

#[mz_ore::test(tokio::test)]
async fn test_create_user() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let new_user_email = "newuser@example.com";
    let response = ctx
        .client
        .post(format!("{}/identity/resources/users/v2", ctx.base_url))
        .bearer_auth(access_token)
        .json(&json!({
            "email": new_user_email
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["email"], new_user_email);
}

#[mz_ore::test(tokio::test)]
async fn test_get_user() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a new user
    let new_user_email = "user_to_get@example.com";
    let create_response = ctx
        .client
        .post(format!("{}/identity/resources/users/v2", ctx.base_url))
        .bearer_auth(&access_token)
        .json(&json!({
            "email": new_user_email,
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let user_id = create_body["id"].as_str().unwrap();

    // Now, get the user
    let get_response = ctx
        .client
        .get(format!(
            "{}/identity/resources/users/v1/{}",
            ctx.base_url, user_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(get_response.status(), 200);
    let body: serde_json::Value = get_response.json().await.unwrap();
    assert_eq!(body["email"], new_user_email);
}

#[mz_ore::test(tokio::test)]
async fn test_delete_user() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a new user
    let new_user_email = "user_to_delete@example.com";
    let create_response = ctx
        .client
        .post(format!("{}/identity/resources/users/v2", ctx.base_url))
        .bearer_auth(&access_token)
        .json(&json!({
            "email": new_user_email
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let user_id = create_body["id"].as_str().unwrap();

    // Now, delete the user
    let delete_response = ctx
        .client
        .delete(format!(
            "{}/identity/resources/users/v1/{}",
            ctx.base_url, user_id
        ))
        .bearer_auth(access_token.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(delete_response.status(), 200);

    // Verify that the user is deleted
    let get_response = ctx
        .client
        .get(format!(
            "{}/identity/resources/users/v1/{}",
            ctx.base_url, user_id
        ))
        .bearer_auth(access_token.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(get_response.status(), 404);
}

#[mz_ore::test(tokio::test)]
async fn test_update_user_roles() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a new user
    let new_user_email = "user_to_update@example.com";
    let create_response = ctx
        .client
        .post(format!("{}/identity/resources/users/v2", ctx.base_url))
        .bearer_auth(&access_token)
        .json(&json!({
            "email": new_user_email
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(create_response.status(), 201);

    // Now, update the user's roles
    let update_response = ctx
        .client
        .put(format!(
            "{}/frontegg/team/resources/members/v1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "email": new_user_email,
            "roleIds": ["user", "admin"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(update_response.status(), 200);
    let body: serde_json::Value = update_response.json().await.unwrap();
    assert_eq!(body["email"], new_user_email);
    assert!(body["roles"].as_array().unwrap().len() == 2);
}

#[mz_ore::test(tokio::test)]
async fn test_create_user_with_invalid_role() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .post(format!("{}/identity/resources/users/v2", ctx.base_url))
        .bearer_auth(access_token)
        .json(&json!({
            "email": "newuser@example.com",
            "roleIds": ["invalid_role_id"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 400);
}

#[mz_ore::test(tokio::test)]
async fn test_create_duplicate_user() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let email = "duplicate@example.com";

    // Create the first user
    ctx.client
        .post(format!("{}/identity/resources/users/v2", ctx.base_url))
        .bearer_auth(&access_token)
        .json(&json!({ "email": email }))
        .send()
        .await
        .unwrap();

    // Try to create a duplicate user
    let response = ctx
        .client
        .post(format!("{}/identity/resources/users/v2", ctx.base_url))
        .bearer_auth(access_token)
        .json(&json!({ "email": email }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 409);
}

#[mz_ore::test(tokio::test)]
async fn test_update_nonexistent_user_roles() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .put(format!(
            "{}/frontegg/team/resources/members/v1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "email": "nonexistent@example.com",
            "roleIds": ["user"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

#[mz_ore::test(tokio::test)]
async fn test_delete_nonexistent_user() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .delete(format!(
            "{}/identity/resources/users/v1/{}",
            ctx.base_url,
            Uuid::new_v4()
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

#[mz_ore::test(tokio::test)]
async fn test_get_user_password() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // Create a new user
    let new_user_email = "password_test_user@example.com";
    let create_response = ctx
        .client
        .post(format!("{}/identity/resources/users/v2", ctx.base_url))
        .bearer_auth(&access_token)
        .json(&json!({
            "email": new_user_email,
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(create_response.status(), 201);

    // Get the user's password
    let get_password_response = ctx
        .client
        .post(format!("{}/api/internal-mock/user-password", ctx.base_url))
        .bearer_auth(access_token)
        .json(&json!({
            "email": new_user_email
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(get_password_response.status(), 200);
    let user_data: serde_json::Value = get_password_response.json().await.unwrap();
    assert_eq!(user_data["email"], new_user_email);
    assert!(!user_data["password"].as_str().unwrap().is_empty());

    // Verify that the password works by authenticating with it
    let auth_response = ctx
        .client
        .post(format!("{}/identity/resources/auth/v1/user", ctx.base_url))
        .json(&json!({
            "email": user_data["email"],
            "password": user_data["password"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(auth_response.status(), 200);
}

// SSO Configuration Tests

#[mz_ore::test(tokio::test)]
async fn test_list_sso_configs() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // Create a couple of SSO configurations
    for i in 1..=2 {
        ctx.client
            .post(format!(
                "{}/frontegg/team/resources/sso/v1/configurations",
                ctx.base_url
            ))
            .bearer_auth(&access_token)
            .json(&json!({
                "enabled": true,
                "ssoEndpoint": format!("https://example{}.com/sso", i),
                "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
                "signRequest": true,
                "acsUrl": format!("https://example{}.com/acs", i),
                "spEntityId": format!("example-sp-{}", i),
                "type": "saml"
            }))
            .send()
            .await
            .unwrap();
    }

    // List all SSO configurations
    let response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let configs = body.as_array().unwrap();
    assert!(configs.len() >= 2);

    // Check that each configuration has the expected fields
    for config in configs {
        assert!(config.get("id").is_some());
        assert!(config.get("enabled").is_some());
        assert!(config.get("ssoEndpoint").is_some());
        assert!(config.get("type").is_some());
    }
}

#[mz_ore::test(tokio::test)]
async fn test_create_sso_config() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.get("id").is_some());
}

#[mz_ore::test(tokio::test)]
async fn test_get_sso_config() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration
    let create_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let config_id = create_body["id"].as_str().unwrap();

    // Now, get the SSO configuration
    let get_response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(get_response.status(), 200);
    let body: serde_json::Value = get_response.json().await.unwrap();
    assert_eq!(body["id"], config_id);
}

#[mz_ore::test(tokio::test)]
async fn test_update_sso_config() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration
    let create_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let config_id = create_body["id"].as_str().unwrap();

    // Now, update the SSO configuration
    let update_response = ctx
        .client
        .patch(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token)
        .json(&json!({
            "enabled": false,
            "ssoEndpoint": "https://example.com/new-sso"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(update_response.status(), 200);
    let body: serde_json::Value = update_response.json().await.unwrap();
    assert_eq!(body["enabled"], false);
    assert_eq!(body["ssoEndpoint"], "https://example.com/new-sso");
}

#[mz_ore::test(tokio::test)]
async fn test_delete_sso_config() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration
    let create_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let config_id = create_body["id"].as_str().unwrap();

    // Now, delete the SSO configuration
    let delete_response = ctx
        .client
        .delete(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(delete_response.status(), 200);

    // Verify that the configuration is deleted
    let get_response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(get_response.status(), 404);
}

#[mz_ore::test(tokio::test)]
async fn test_get_nonexistent_sso_config() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}",
            ctx.base_url,
            Uuid::new_v4()
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 404);
}

#[mz_ore::test(tokio::test)]
async fn test_sso_group_mapping_crud() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // Create SSO configuration
    let create_sso_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_sso_body: serde_json::Value = create_sso_response.json().await.unwrap();
    let config_id = create_sso_body["id"].as_str().unwrap();

    // Create group mapping
    let create_mapping_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups",
            ctx.base_url, config_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "group": "TestGroup",
            "roleIds": ["user"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(create_mapping_response.status(), 200);
    let create_mapping_body: serde_json::Value = create_mapping_response.json().await.unwrap();
    let mapping_id = create_mapping_body["id"].as_str().unwrap();

    // Read group mapping
    let get_mapping_response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups/{}",
            ctx.base_url, config_id, mapping_id
        ))
        .bearer_auth(&access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(get_mapping_response.status(), 200);

    // Update group mapping
    let update_mapping_response = ctx
        .client
        .patch(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups/{}",
            ctx.base_url, config_id, mapping_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "group": "UpdatedTestGroup",
            "roleIds": ["user", "admin"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(update_mapping_response.status(), 200);

    // Delete group mapping
    let delete_mapping_response = ctx
        .client
        .delete(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups/{}",
            ctx.base_url, config_id, mapping_id
        ))
        .bearer_auth(&access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(delete_mapping_response.status(), 200);

    // Verify deletion
    let get_deleted_mapping_response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups/{}",
            ctx.base_url, config_id, mapping_id
        ))
        .bearer_auth(&access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(get_deleted_mapping_response.status(), 404);
}

#[mz_ore::test(tokio::test)]
async fn test_create_sso_domain() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration
    let create_sso_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_sso_body: serde_json::Value = create_sso_response.json().await.unwrap();
    let config_id = create_sso_body["id"].as_str().unwrap();

    // Now, create a domain for the SSO configuration
    let create_domain_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/domains",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token)
        .json(&json!({
            "domain": "example.com"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(create_domain_response.status(), 200);
    let body: serde_json::Value = create_domain_response.json().await.unwrap();
    assert_eq!(body["domain"], "example.com");
}

#[mz_ore::test(tokio::test)]
async fn test_get_sso_domains() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration and a domain
    let create_sso_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_sso_body: serde_json::Value = create_sso_response.json().await.unwrap();
    let config_id = create_sso_body["id"].as_str().unwrap();

    ctx.client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/domains",
            ctx.base_url, config_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "domain": "example.com"
        }))
        .send()
        .await
        .unwrap();

    // Now, get the domains for the SSO configuration
    let get_domains_response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/domains",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(get_domains_response.status(), 200);
    let body: serde_json::Value = get_domains_response.json().await.unwrap();
    assert!(body.as_array().unwrap().len() > 0);
}

#[mz_ore::test(tokio::test)]
async fn test_update_sso_domain() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration and a domain
    let create_sso_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_sso_body: serde_json::Value = create_sso_response.json().await.unwrap();
    let config_id = create_sso_body["id"].as_str().unwrap();

    let create_domain_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/domains",
            ctx.base_url, config_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "domain": "example.com"
        }))
        .send()
        .await
        .unwrap();

    let create_domain_body: serde_json::Value = create_domain_response.json().await.unwrap();
    let domain_id = create_domain_body["id"].as_str().unwrap();

    // Now, update the domain
    let update_domain_response = ctx
        .client
        .patch(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/domains/{}",
            ctx.base_url, config_id, domain_id
        ))
        .bearer_auth(access_token)
        .json(&json!({
            "domain": "updated-example.com",
            "validated": true
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(update_domain_response.status(), 200);
    let body: serde_json::Value = update_domain_response.json().await.unwrap();
    assert_eq!(body["domain"], "updated-example.com");
    assert_eq!(body["validated"], true);
}

#[mz_ore::test(tokio::test)]
async fn test_delete_sso_domain() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration and a domain
    let create_sso_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_sso_body: serde_json::Value = create_sso_response.json().await.unwrap();
    let config_id = create_sso_body["id"].as_str().unwrap();

    let create_domain_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/domains",
            ctx.base_url, config_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "domain": "example.com"
        }))
        .send()
        .await
        .unwrap();

    let create_domain_body: serde_json::Value = create_domain_response.json().await.unwrap();
    let domain_id = create_domain_body["id"].as_str().unwrap();

    // Now, delete the domain
    let delete_domain_response = ctx
        .client
        .delete(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/domains/{}",
            ctx.base_url, config_id, domain_id
        ))
        .bearer_auth(access_token.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(delete_domain_response.status(), 200);

    // Verify that the domain is deleted
    let get_domains_response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/domains",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = get_domains_response.json().await.unwrap();
    assert!(!body
        .as_array()
        .unwrap()
        .iter()
        .any(|domain| domain["id"] == domain_id));
}

#[mz_ore::test(tokio::test)]
async fn test_create_sso_group_mapping() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration
    let create_sso_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_sso_body: serde_json::Value = create_sso_response.json().await.unwrap();
    let config_id = create_sso_body["id"].as_str().unwrap();

    // Now, create a group mapping for the SSO configuration
    let create_group_mapping_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token)
        .json(&json!({
            "group": "TestGroup",
            "roleIds": ["user"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(create_group_mapping_response.status(), 200);
    let body: serde_json::Value = create_group_mapping_response.json().await.unwrap();
    assert_eq!(body["group"], "TestGroup");
}

#[mz_ore::test(tokio::test)]
async fn test_get_sso_group_mappings() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration and a group mapping
    let create_sso_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_sso_body: serde_json::Value = create_sso_response.json().await.unwrap();
    let config_id = create_sso_body["id"].as_str().unwrap();

    ctx.client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups",
            ctx.base_url, config_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "group": "TestGroup",
            "roleIds": ["user"]
        }))
        .send()
        .await
        .unwrap();

    // Now, get the group mappings for the SSO configuration
    let get_group_mappings_response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(get_group_mappings_response.status(), 200);
    let body: serde_json::Value = get_group_mappings_response.json().await.unwrap();
    assert!(body.as_array().unwrap().len() > 0);
}

#[mz_ore::test(tokio::test)]
async fn test_update_sso_group_mapping() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration and a group mapping
    let create_sso_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_sso_body: serde_json::Value = create_sso_response.json().await.unwrap();
    let config_id = create_sso_body["id"].as_str().unwrap();

    let create_group_mapping_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups",
            ctx.base_url, config_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "group": "TestGroup",
            "roleIds": ["user"]
        }))
        .send()
        .await
        .unwrap();

    let create_group_mapping_body: serde_json::Value =
        create_group_mapping_response.json().await.unwrap();
    let group_id = create_group_mapping_body["id"].as_str().unwrap();

    // Now, update the group mapping
    let update_group_mapping_response = ctx
        .client
        .patch(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups/{}",
            ctx.base_url, config_id, group_id
        ))
        .bearer_auth(access_token)
        .json(&json!({
            "group": "UpdatedTestGroup",
            "roleIds": ["user", "admin"],
            "enabled": true
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(update_group_mapping_response.status(), 200);
    let body: serde_json::Value = update_group_mapping_response.json().await.unwrap();
    assert_eq!(body["group"], "UpdatedTestGroup");
    assert_eq!(body["roleIds"].as_array().unwrap().len(), 2);
    assert_eq!(body["enabled"], true);
}

#[mz_ore::test(tokio::test)]
async fn test_delete_sso_group_mapping() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration and a group mapping
    let create_sso_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_sso_body: serde_json::Value = create_sso_response.json().await.unwrap();
    let config_id = create_sso_body["id"].as_str().unwrap();

    let create_group_mapping_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups",
            ctx.base_url, config_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "group": "TestGroup",
            "roleIds": ["user"]
        }))
        .send()
        .await
        .unwrap();

    let create_group_mapping_body: serde_json::Value =
        create_group_mapping_response.json().await.unwrap();
    let group_id = create_group_mapping_body["id"].as_str().unwrap();

    // Now, delete the group mapping
    let delete_group_mapping_response = ctx
        .client
        .delete(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups/{}",
            ctx.base_url, config_id, group_id
        ))
        .bearer_auth(access_token.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(delete_group_mapping_response.status(), 200);

    // Verify that the group mapping is deleted
    let get_group_mappings_response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/groups",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = get_group_mappings_response.json().await.unwrap();
    assert!(!body
        .as_array()
        .unwrap()
        .iter()
        .any(|group| group["id"] == group_id));
}

#[mz_ore::test(tokio::test)]
async fn test_get_sso_default_roles() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration
    let create_sso_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_sso_body: serde_json::Value = create_sso_response.json().await.unwrap();
    let config_id = create_sso_body["id"].as_str().unwrap();

    // Now, get the default roles for the SSO configuration
    let get_default_roles_response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/roles",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(get_default_roles_response.status(), 200);
    let body: serde_json::Value = get_default_roles_response.json().await.unwrap();
    assert!(body.get("roleIds").is_some());
}

#[mz_ore::test(tokio::test)]
async fn test_set_sso_default_roles() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create an SSO configuration
    let create_sso_response = ctx
        .client
        .post(format!(
            "{}/frontegg/team/resources/sso/v1/configurations",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "enabled": true,
            "ssoEndpoint": "https://example.com/sso",
            "publicCertificate": "-----BEGIN CERTIFICATE-----\nMIIC...",
            "signRequest": true,
            "acsUrl": "https://example.com/acs",
            "spEntityId": "example-sp",
            "type": "saml"
        }))
        .send()
        .await
        .unwrap();

    let create_sso_body: serde_json::Value = create_sso_response.json().await.unwrap();
    let config_id = create_sso_body["id"].as_str().unwrap();

    // Now, set the default roles for the SSO configuration
    let set_default_roles_response = ctx
        .client
        .put(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/roles",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token.clone())
        .json(&json!({
            "roleIds": ["user", "admin"]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(set_default_roles_response.status(), 201);
    let body: serde_json::Value = set_default_roles_response.json().await.unwrap();
    assert_eq!(body["roleIds"].as_array().unwrap().len(), 2);

    // Verify that the default roles were set correctly
    let get_default_roles_response = ctx
        .client
        .get(format!(
            "{}/frontegg/team/resources/sso/v1/configurations/{}/roles",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    let get_body: serde_json::Value = get_default_roles_response.json().await.unwrap();
    assert_eq!(get_body["roleIds"].as_array().unwrap().len(), 2);
}

// Group Tests

#[mz_ore::test(tokio::test)]
async fn test_add_users_to_group() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // Create a group
    let create_group_response = ctx
        .client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "name": "Test Group",
            "description": "A test group"
        }))
        .send()
        .await
        .unwrap();

    let create_group_body: serde_json::Value = create_group_response.json().await.unwrap();
    let group_id = create_group_body["id"].as_str().unwrap();

    // Create users
    let mut user_ids = Vec::new();
    for email in &["user1@example.com", "user2@example.com"] {
        let create_user_response = ctx
            .client
            .post(format!("{}/identity/resources/users/v2", ctx.base_url))
            .bearer_auth(&access_token)
            .json(&json!({
                "email": email
            }))
            .send()
            .await
            .unwrap();

        let create_user_body: serde_json::Value = create_user_response.json().await.unwrap();
        user_ids.push(create_user_body["id"].as_str().unwrap().to_string());
    }

    // Add users to the group
    let add_users_response = ctx
        .client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1/{}/users",
            ctx.base_url, group_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({ "userIds": user_ids }))
        .send()
        .await
        .unwrap();

    assert_eq!(add_users_response.status(), 201);

    // Verify that the users were added to the group
    let get_group_response = ctx
        .client
        .get(format!(
            "{}/frontegg/identity/resources/groups/v1/{}",
            ctx.base_url, group_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    let get_group_body: serde_json::Value = get_group_response.json().await.unwrap();
    let group_users = get_group_body["users"].as_array().unwrap();
    assert_eq!(group_users.len(), 2);
}

#[mz_ore::test(tokio::test)]
async fn test_list_groups() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // Create a couple of groups
    for i in 1..=2 {
        ctx.client
            .post(format!(
                "{}/frontegg/identity/resources/groups/v1",
                ctx.base_url
            ))
            .bearer_auth(&access_token)
            .json(&json!({
                "name": format!("Test Group {}", i),
                "description": format!("A test group {}", i)
            }))
            .send()
            .await
            .unwrap();
    }

    // List all groups
    let response = ctx
        .client
        .get(format!(
            "{}/frontegg/identity/resources/groups/v1",
            ctx.base_url
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    let groups = body["groups"].as_array().unwrap();
    assert!(groups.len() >= 2);

    // Check that each group has the expected fields
    for group in groups {
        assert!(group.get("id").is_some());
        assert!(group.get("name").is_some());
        assert!(group.get("description").is_some());
    }
}

#[mz_ore::test(tokio::test)]
async fn test_create_group() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1",
            ctx.base_url
        ))
        .bearer_auth(access_token)
        .json(&json!({
            "name": "Test Group",
            "description": "A test group"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 201);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["name"], "Test Group");
}

#[mz_ore::test(tokio::test)]
async fn test_get_group() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a group
    let create_response = ctx
        .client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "name": "Test Group",
            "description": "A test group"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let group_id = create_body["id"].as_str().unwrap();

    // Now, get the group
    let get_response = ctx
        .client
        .get(format!(
            "{}/frontegg/identity/resources/groups/v1/{}",
            ctx.base_url, group_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(get_response.status(), 200);
    let body: serde_json::Value = get_response.json().await.unwrap();
    assert_eq!(body["name"], "Test Group");
}

#[mz_ore::test(tokio::test)]
async fn test_update_group() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a group
    let create_response = ctx
        .client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "name": "Test Group",
            "description": "A test group"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let group_id = create_body["id"].as_str().unwrap();

    // Now, update the group
    let update_response = ctx
        .client
        .patch(format!(
            "{}/frontegg/identity/resources/groups/v1/{}",
            ctx.base_url, group_id
        ))
        .bearer_auth(access_token)
        .json(&json!({
            "name": "Updated Test Group",
            "description": "An updated test group"
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(update_response.status(), 200);
    let body: serde_json::Value = update_response.json().await.unwrap();
    assert_eq!(body["name"], "Updated Test Group");
    assert_eq!(body["description"], "An updated test group");
}

#[mz_ore::test(tokio::test)]
async fn test_delete_group() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a group
    let create_response = ctx
        .client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "name": "Test Group",
            "description": "A test group"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let group_id = create_body["id"].as_str().unwrap();

    // Now, delete the group
    let delete_response = ctx
        .client
        .delete(format!(
            "{}/frontegg/identity/resources/groups/v1/{}",
            ctx.base_url, group_id
        ))
        .bearer_auth(access_token.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(delete_response.status(), 200);

    // Verify that the group is deleted
    let get_response = ctx
        .client
        .get(format!(
            "{}/frontegg/identity/resources/groups/v1/{}",
            ctx.base_url, group_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(get_response.status(), 404);
}

#[mz_ore::test(tokio::test)]
async fn test_add_roles_to_group() {
    let ctx = setup_test_context().await;
    let default_roles = ctx.roles.clone();
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a group
    let create_response = ctx
        .client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "name": "Test Group",
            "description": "A test group"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let group_id = create_body["id"].as_str().unwrap();

    assert_eq!(group_id.len(), 36);
    assert_eq!(group_id.chars().filter(|&c| c == '-').count(), 4);

    // Now, add roles to the group
    let add_roles_response = ctx
        .client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1/{}/roles",
            ctx.base_url, group_id
        ))
        .bearer_auth(access_token.clone())
        .json(&json!({
            "roleIds": [default_roles[0].id.clone(), default_roles[1].id.clone()]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(add_roles_response.status(), 201);

    // Verify that the roles were added
    let get_response = ctx
        .client
        .get(format!(
            "{}/frontegg/identity/resources/groups/v1/{}",
            ctx.base_url, group_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = get_response.json().await.unwrap();
    assert!(body["roles"].as_array().unwrap().len() >= 2);
}

#[mz_ore::test(tokio::test)]
async fn test_remove_roles_from_group() {
    let ctx = setup_test_context().await;
    let default_roles = ctx.roles.clone();
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a group and add roles
    let create_response = ctx
        .client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "name": "Test Group",
            "description": "A test group"
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let group_id = create_body["id"].as_str().unwrap();

    // Add roles to the group
    let add_roles_response = ctx
        .client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1/{}/roles",
            ctx.base_url, group_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "roleIds": [default_roles[0].id.clone(), default_roles[1].id.clone()]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        add_roles_response.status(),
        201,
        "Failed to add roles to group"
    );

    // Now, remove roles from the group
    let remove_roles_response = ctx
        .client
        .delete(format!(
            "{}/frontegg/identity/resources/groups/v1/{}/roles",
            ctx.base_url, group_id
        ))
        .bearer_auth(access_token.clone())
        .json(&json!({
            "roleIds": [default_roles[1].id.clone()]
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(
        remove_roles_response.status(),
        200,
        "Failed to remove roles from group"
    );

    // Verify that the role was removed
    let get_response = ctx
        .client
        .get(format!(
            "{}/frontegg/identity/resources/groups/v1/{}",
            ctx.base_url, group_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = get_response.json().await.unwrap();
    let roles = body["roles"].as_array().unwrap();
    assert_eq!(roles.len(), 1, "Unexpected number of roles after removal");
    assert_eq!(roles[0]["id"], default_roles[0].id);
}

// SCIM Configuration Tests

#[mz_ore::test(tokio::test)]
async fn test_create_scim_configuration() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .post(format!(
            "{}/frontegg/directory/resources/v1/configurations/scim2",
            ctx.base_url
        ))
        .bearer_auth(access_token)
        .json(&json!({
            "source": "azure",
            "connectionName": "Test SCIM Connection",
            "syncToUserManagement": true
        }))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.get("id").is_some());
    assert_eq!(body["connectionName"], "Test SCIM Connection");
}

#[mz_ore::test(tokio::test)]
async fn test_list_scim_configurations() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a SCIM configuration
    ctx.client
        .post(format!(
            "{}/frontegg/directory/resources/v1/configurations/scim2",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "source": "azure",
            "connectionName": "Test SCIM Connection",
            "syncToUserManagement": true
        }))
        .send()
        .await
        .unwrap();

    // Now, list SCIM configurations
    let list_response = ctx
        .client
        .get(format!(
            "{}/frontegg/directory/resources/v1/configurations/scim2",
            ctx.base_url
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(list_response.status(), 200);
    let body: serde_json::Value = list_response.json().await.unwrap();
    assert!(body.as_array().unwrap().len() > 0);
}

#[mz_ore::test(tokio::test)]
async fn test_delete_scim_configuration() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // First, create a SCIM configuration
    let create_response = ctx
        .client
        .post(format!(
            "{}/frontegg/directory/resources/v1/configurations/scim2",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "source": "azure",
            "connectionName": "Test SCIM Connection",
            "syncToUserManagement": true
        }))
        .send()
        .await
        .unwrap();

    let create_body: serde_json::Value = create_response.json().await.unwrap();
    let config_id = create_body["id"].as_str().unwrap();

    // Now, delete the SCIM configuration
    let delete_response = ctx
        .client
        .delete(format!(
            "{}/frontegg/directory/resources/v1/configurations/scim2/{}",
            ctx.base_url, config_id
        ))
        .bearer_auth(access_token.clone())
        .send()
        .await
        .unwrap();

    assert_eq!(delete_response.status(), 204);

    // Verify that the configuration is deleted
    let list_response = ctx
        .client
        .get(format!(
            "{}/frontegg/directory/resources/v1/configurations/scim2",
            ctx.base_url
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = list_response.json().await.unwrap();
    assert!(!body
        .as_array()
        .unwrap()
        .iter()
        .any(|config| config["id"] == config_id));
}

// Roles Tests

#[mz_ore::test(tokio::test)]
async fn test_get_roles() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    let response = ctx
        .client
        .get(format!("{}/identity/resources/roles/v2", ctx.base_url))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body.get("items").is_some() && body["items"].is_array());
    assert!(body.get("_metadata").is_some() && body["_metadata"].get("total_items").is_some());
    if let Some(items) = body["items"].as_array() {
        if !items.is_empty() {
            assert!(items[0].get("id").is_some());
            assert!(items[0].get("name").is_some());
        }
    }
}

#[mz_ore::test(tokio::test)]
async fn test_remove_users_from_group() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // Create a group
    let create_group_response = ctx
        .client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .json(&json!({
            "name": "Test Group",
            "description": "A test group"
        }))
        .send()
        .await
        .unwrap();

    let create_group_body: serde_json::Value = create_group_response.json().await.unwrap();
    let group_id = create_group_body["id"].as_str().unwrap();

    // Create users
    let mut user_ids = Vec::new();
    for email in &["user1@example.com", "user2@example.com"] {
        let create_user_response = ctx
            .client
            .post(format!("{}/identity/resources/users/v2", ctx.base_url))
            .bearer_auth(&access_token)
            .json(&json!({
                "email": email
            }))
            .send()
            .await
            .unwrap();

        let create_user_body: serde_json::Value = create_user_response.json().await.unwrap();
        user_ids.push(create_user_body["id"].as_str().unwrap().to_string());
    }

    // Add users to the group
    ctx.client
        .post(format!(
            "{}/frontegg/identity/resources/groups/v1/{}/users",
            ctx.base_url, group_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({ "userIds": user_ids.clone() }))
        .send()
        .await
        .unwrap();

    // Remove one user from the group
    let remove_user_response = ctx
        .client
        .delete(format!(
            "{}/frontegg/identity/resources/groups/v1/{}/users",
            ctx.base_url, group_id
        ))
        .bearer_auth(&access_token)
        .json(&json!({ "userIds": [user_ids[0].clone()] }))
        .send()
        .await
        .unwrap();

    assert_eq!(remove_user_response.status(), 200);

    // Verify that the user was removed from the group
    let get_group_response = ctx
        .client
        .get(format!(
            "{}/frontegg/identity/resources/groups/v1/{}",
            ctx.base_url, group_id
        ))
        .bearer_auth(access_token)
        .send()
        .await
        .unwrap();

    let get_group_body: serde_json::Value = get_group_response.json().await.unwrap();
    let group_users = get_group_body["users"].as_array().unwrap();
    assert_eq!(group_users.len(), 1);
    assert_eq!(group_users[0]["id"], user_ids[1]);
}

#[mz_ore::test(tokio::test)]
async fn test_get_users_v3() {
    let ctx = setup_test_context().await;
    let access_token = authenticate(&ctx).await.unwrap();

    // Create multiple users
    let emails = vec![
        "user1@example.com",
        "user2@example.com",
        "user3@example.com",
    ];
    for email in &emails {
        ctx.client
            .post(format!("{}/identity/resources/users/v2", ctx.base_url))
            .bearer_auth(&access_token)
            .json(&json!({
                "email": email
            }))
            .send()
            .await
            .unwrap();
    }

    // Test basic listing
    let response = ctx
        .client
        .get(format!("{}/identity/resources/users/v3", ctx.base_url))
        .bearer_auth(&access_token)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert!(body["items"].as_array().unwrap().len() >= 3);

    // Test filtering by email
    let response = ctx
        .client
        .get(format!(
            "{}/identity/resources/users/v3?_email=user1@example.com",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["items"].as_array().unwrap().len(), 1);
    assert_eq!(body["items"][0]["email"], "user1@example.com");

    // Test sorting
    let response = ctx
        .client
        .get(format!(
            "{}/identity/resources/users/v3?_sortBy=email&_order=DESC",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = response.json().await.unwrap();
    let items = body["items"].as_array().unwrap();
    assert!(items[0]["email"].as_str().unwrap() > items[1]["email"].as_str().unwrap());

    let response = ctx
        .client
        .get(format!(
            "{}/identity/resources/users/v3?_limit=2&_offset=1",
            ctx.base_url
        ))
        .bearer_auth(&access_token)
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["_metadata"]["total_items"].as_i64().unwrap() >= 3,
        true
    );

    // Test filtering by multiple IDs
    let response = ctx
        .client
        .get(format!("{}/identity/resources/users/v3", ctx.base_url))
        .bearer_auth(&access_token)
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = response.json().await.unwrap();
    let all_users = body["items"].as_array().unwrap();
    let ids = all_users
        .iter()
        .take(2)
        .map(|u| u["id"].as_str().unwrap())
        .collect::<Vec<_>>()
        .join(",");

    let response = ctx
        .client
        .get(format!(
            "{}/identity/resources/users/v3?ids={}",
            ctx.base_url, ids
        ))
        .bearer_auth(&access_token)
        .send()
        .await
        .unwrap();

    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["items"].as_array().unwrap().len(), 2);
}

// Latency Test

#[mz_ore::test(tokio::test)]
async fn test_latency() {
    let ctx = setup_test_context().await;

    let start = std::time::Instant::now();
    let _ = authenticate(&ctx).await.unwrap();
    let duration = start.elapsed();

    assert!(
        duration >= Duration::from_millis(100),
        "Request completed faster than the configured latency"
    );
}
