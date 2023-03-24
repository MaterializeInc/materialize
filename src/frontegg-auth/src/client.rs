use reqwest::{Url, Method, RequestBuilder, Error};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::{time::{SystemTime, Duration}};

use crate::{app_password::AppPassword, ApiTokenResponse, ApiTokenArgs};

/// The default Frontegg endpoint.
pub const DEFAULT_ENDPOINT: Url = Url::parse("...").unwrap();

/// Configures a `Client`.
struct ClientBuilder {
    endpoint: Url,
}

/// Configures the required parameters of a `Client`.
struct ClientConfig {
    app_password: AppPassword
}

impl Default for ClientBuilder {
    fn default() -> ClientBuilder {
        ClientBuilder {
            endpoint: DEFAULT_ENDPOINT.clone(),
        }
    }
}

impl ClientBuilder {
    /// Overrides the default endpoint.
    fn endpoint(mut self, url: Url) -> ClientBuilder {
        self.endpoint = url;
        self
    }

    /// Creates a [`Client`] that incorporates the optional parameters
    /// configured on the builder and the specified required parameters.
    fn build(config: ClientConfig) -> Client {
        // ...
        Client {
            auth: todo!(),
            endpoint: DEFAULT_ENDPOINT,
            app_password: config.app_password,
            inner: reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("must build Client"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Auth {
    token: String,
    refresh_at: SystemTime,
}

struct Client {
    endpoint: Url,
    auth: Option<Auth>,
    app_password: AppPassword,

    // Reqwest HTTP client pool.
    inner: reqwest::Client,
}

impl Client {
    /// Authenticates with the server, if not already authenticated,
    /// and returns the authentication token.
    pub async fn auth_token(&self) -> String {
        // Authenticate if not authenticated.
        if let Some(auth) = self.auth.clone() {
            auth.token
        } else {
            #[derive(Serialize, Deserialize, Clone, Debug)]
            #[serde(rename_all = "camelCase")]
            pub struct AuthenticationRequest {
                pub client_id: String,
                pub secret: String,
            }

            #[derive(Debug, Clone, Deserialize)]
            #[serde(rename_all = "camelCase")]
            struct AuthenticationResponse {
                token: String,
                expires_in: u64,
            }


            // Authenticate.
            let req = self.build_request(Method::POST, &["identity", "resources", "auth", "v1", "api-token"]);
            let req = req.json(&AuthenticationRequest {
                client_id: self.app_password.client_id.to_string(),
                secret: self.app_password.secret_key.to_string(),
            });
            let res: AuthenticationResponse = self.send_unauthenticated_request(req).await?;
            self.auth = Some(Auth {
                token: res.token.clone(),
                // Refresh twice as frequently as we need to, to be safe.
                refresh_at: SystemTime::now() + (Duration::from_secs(res.expires_in) / 2),
            });
            res.token
        }
    }

    /// Creates a new user in the authenticated organization.
    pub async fn create_user(&self, /* ... */) {}

    /// Lists all users in the authenticated organization.
    pub async fn list_users(&self) -> Vec<User> {
        vec![]
    }

    /// Creates a new app password.
    pub async fn create_app_passwords(&self, /* ... */) {}

    /// Lists all existing app passwords.
    pub async fn list_app_passwords(&self) -> Vec<AppPassword> {
        vec![]
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

    fn request(&self, method: Method, path: &[&str]) {
        // Makes a request, performing authentication if necessary.
        // See `rust_frontegg` for code. Can probably be copied.

        let req = self.build_request(method, path);
        let token = self.auth_token().await?;
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
            // match res.json::<ErrorResponse>().await {
            //     Ok(e) => {
            //         let mut messages = e.errors;
            //         messages.extend(e.message);
            //         Err(Error::Api(ApiError {
            //             status_code,
            //             messages,
            //         }))
            //     }
            //     Err(_) => Err(Error::Api(ApiError {
            //         status_code,
            //         messages: vec!["unable to decode error details".into()],
            //     })),
            // }
        }
    }
}

// TODO: nice error type. Use `rust_frontegg` for inspiration.