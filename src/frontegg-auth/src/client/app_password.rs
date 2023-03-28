use std::collections::BTreeMap;

use reqwest::Method;
use serde::Deserialize;

use crate::{error::Error, app_password::AppPassword};

use super::{Client, CREATE_APP_PASSWORDS_PATH, APP_PASSWORDS_PATH};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FronteggAppPassword {
    description: String,
    created_at: String,
}

impl Client {
    /// Lists all existing app passwords.
    pub async fn list_app_passwords(&self) -> Result<Vec<FronteggAppPassword>, Error> {
        let req = self.build_request(Method::GET, APP_PASSWORDS_PATH);
        let passwords: Vec<FronteggAppPassword> = self.send_request(req).await?;
        Ok(passwords)
    }

    /// Lists all existing app passwords.
    pub async fn create_app_password(&self, description: String) -> Result<AppPassword, Error> {
        let req = self.build_request(Method::POST, CREATE_APP_PASSWORDS_PATH);
        let mut body = BTreeMap::new();
        body.insert("description", description);

        let req = req.json(&body);

        let password: AppPassword = self.send_request(req).await?;
        Ok(password)
    }
}