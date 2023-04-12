use std::ops::Sub;

use tokio::sync::Mutex;

use crate::{ApiTokenResponse, AppPassword, Client, Error, REFRESH_SUFFIX};
use chrono::{DateTime, Duration, Utc};

pub struct UnverifiedAuthenticationConfig {
    pub admin_api_token_url: String,
    pub app_password: AppPassword,
}

pub struct UnverifiedAuthentication {
    pub admin_api_token_url: String,
    pub client: Client,
    pub auth: Mutex<Option<ApiTokenResponse>>,
    pub app_password: AppPassword,
}

impl UnverifiedAuthentication {
    /// Creates a new frontegg authentication without signature verification
    /// and handles the access token for a unique `app_password`.
    /// `UnverifiedAuthentication` should be NEVER used on the server side.
    /// It should only be used in the client side. E.g.: Front-end apps, CLI, etc.
    /// Unverified authentication avoids carrying certs to verify the tokens.
    pub fn new(config: UnverifiedAuthenticationConfig, client: Client) -> Self {
        UnverifiedAuthentication {
            admin_api_token_url: config.admin_api_token_url,
            client,
            auth: None.into(),
            app_password: config.app_password,
        }
    }

    /// Authenticates with the server, if not already authenticated,
    /// and returns the authentication token.
    pub async fn auth(&self) -> Result<ApiTokenResponse, Error> {
        let mut auth = self.auth.lock().await;
        let req: Result<ApiTokenResponse, Error>;

        match &*auth {
            Some(auth) => {
                let expire_at =
                    DateTime::parse_from_str(&auth.expires, "%a, %d %b %Y %H:%M:%S %Z").unwrap();
                // Refresh twice as frequently as we need to, to be safe.
                let expire_at = expire_at.sub(Duration::seconds(auth.expires_in / 2));
                let current_time = Utc::now();

                if current_time < expire_at {
                    return Ok(auth.clone());
                } else {
                    let refreshing_url = format!("{}{}", self.admin_api_token_url, REFRESH_SUFFIX);

                    // Access token needs a refresh
                    req = self
                        .client
                        .refresh_token(&refreshing_url, &auth.refresh_token)
                        .await;
                }
            }
            _ => {
                // No auth available in the client, request a new one.
                req = self
                    .client
                    .exchange_client_secret_for_token(
                        self.app_password.client_id,
                        self.app_password.secret_key,
                        &self.admin_api_token_url,
                    )
                    .await;
            }
        }

        let new_auth = req.unwrap();
        *auth = Some(new_auth);

        Ok(auth.clone().unwrap())
    }
}
