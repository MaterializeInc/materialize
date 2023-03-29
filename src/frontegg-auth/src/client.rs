use reqwest::{Method, RequestBuilder};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio::sync::Mutex;
use std::{time::{SystemTime, Duration}};
use url::Url;

use crate::{error::Error, error::ApiError, config::{ClientConfig, ClientBuilder}, app_password::AppPassword, Claims};

const AUTH_PATH: [&str; 5] = ["identity", "resources", "auth", "v1", "api-token"];
const USERS_PATH: [&str; 4] = ["identity", "resources", "users", "v3"];
const CREATE_USERS_PATH: [&str; 5] = ["frontegg", "identity", "resources", "users", "v2"];
const APP_PASSWORDS_PATH: [&str; 5] = ["identity", "resources", "users", "api-tokens", "v1"];
const CREATE_APP_PASSWORDS_PATH: [&str; 6] =  ["frontegg", "identity", "resources", "users", "api-tokens", "v1"];

pub mod user;
pub mod app_password;

/// An API client for Frontegg.
///
/// The API client is designed to be wrapped in an [`Arc`] and used from
/// multiple threads simultaneously. A successful authentication response is
/// shared by all threads.
///
/// [`Arc`]: std::sync::Arc
#[derive(Debug)]
pub struct Client {
    pub(crate) inner: reqwest::Client,
    pub(crate) app_password: AppPassword,
    pub(crate) endpoint: Url,
    pub(crate) auth: Mutex<Option<Auth>>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiTokenResponse {
    pub expires: String,
    pub expires_in: i64,
    pub access_token: String,
    pub refresh_token: String,
}

impl Client {
    /// Creates a new `Client` from its required configuration parameters.
    pub fn new(config: ClientConfig) -> Client {
        ClientBuilder::default().build(config)
    }

    /// Creates a builder for a `Client` that allows for customization of
    /// optional parameters.
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    fn build_request<P>(&self, method: Method, path: P) -> RequestBuilder
    where
        P: IntoIterator,
        P::Item: AsRef<str>,
    {
        let mut url = self.endpoint.clone();
        url.path_segments_mut()
            .expect("builder validated URL can be a base")
            .clear()
            .extend(path);
        self.inner.request(method, url)
    }

    async fn send_request<T>(&self, req: RequestBuilder) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        let token = self.auth().await?.token;
        let req = req.bearer_auth(token);
        self.send_unauthenticated_request(req).await
    }

    async fn send_unauthenticated_request<T>(&self, req: RequestBuilder) -> Result<T, Error>
    where
        T: DeserializeOwned,
    {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct ErrorResponse {
            #[serde(default)]
            message: Option<String>,
            #[serde(default)]
            errors: Vec<String>,
        }

        let res = req.send().await?;
        let status_code = res.status();
        if status_code.is_success() {
            Ok(res.json().await?)
        } else {
            match res.json::<ErrorResponse>().await {
                Ok(e) => {
                    let mut messages = e.errors;
                    messages.extend(e.message);
                    Err(Error::Api(ApiError {
                        status_code,
                        messages,
                    }))
                }
                Err(_) => Err(Error::Api(ApiError {
                    status_code,
                    messages: vec!["unable to decode error details".into()],
                })),
            }
        }
    }

    /// Authenticates with the server, if not already authenticated,
    /// and returns the authentication token.
    pub async fn auth(&self) -> Result<Auth, Error> {
        #[derive(Debug, Clone, Serialize)]
        #[serde(rename_all = "camelCase")]
        struct AuthenticationRequest<'a> {
            client_id: &'a str,
            secret: &'a str,
        }

        #[derive(Debug, Clone, Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct AuthenticationResponse {
            access_token: String,
            expires_in: u64,
        }

        let mut auth = self.auth.lock().await;
        match &*auth {
            Some(auth) if SystemTime::now() < auth.refresh_at => {
                return Ok(auth.clone());
            }
            _ => (),
        }
        let req = self.build_request(Method::POST, AUTH_PATH);
        let authentication_request = AuthenticationRequest {
            client_id: &self.app_password.client_id.to_string(),
            secret: &self.app_password.secret_key.to_string(),
        };
        let req = req.json(&authentication_request);
        let res: AuthenticationResponse = self.send_unauthenticated_request(req).await?;
        *auth = Some(Auth {
            token: res.access_token.clone(),
            // Refresh twice as frequently as we need to, to be safe.
            refresh_at: SystemTime::now() + (Duration::from_secs(res.expires_in) / 2),
        });
        Ok(auth.clone().unwrap())
    }
}

#[derive(Debug, Clone)]
pub struct Auth {
    pub token: String,
    pub refresh_at: SystemTime,
}

#[cfg(test)]
mod tests {
    use crate::{config::{ClientBuilder, ClientConfig}, app_password::AppPassword};
    use std::env;

    #[tokio::test]
    async fn test_app_password() {
        struct TestCase {
            app_password: AppPassword
        }

        for tc in [
            TestCase {
                app_password: AppPassword {
                    client_id: env::var("MZP_CLIENT_ID").unwrap().parse().unwrap(),
                    secret_key: env::var("MZP_SECRET_KEY").unwrap().parse().unwrap()
                }
            }
        ] {
            // let client_builder = ClientBuilder::default();
            // let client = client_builder.build(ClientConfig {
            //     app_password: tc.app_password
            // });
            // let app_passwords = client.list_app_passwords().await;
            // println!("Output: {:?}", app_passwords);
            // let users = client.list_users().await;
            // println!("Output: {:?}", users);
            // let app_password = client.create_app_password("description".to_string()).await;
            // println!("Output: {:?}", app_password);
            // let new_user = client.create_user(NewUser {
            //     email: String::from("joaquin+3@materialize.com"),
            //     name: String::from("joaquin"),
            //     provider: String::from("local"),
            //     role_ids: vec![
            //         String::from("15f29149-d2c7-49b3-a67e-4f10d36ffed6"),
            //         String::from("0a4d54f7-05d0-4f7a-8f57-6a2c2474b2c1")
            //     ]
            // }).await;
            // println!("Output: {:?}", new_user);
        }
    }
}
