// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! An API client for the Materialize cloud API.
//!
//! The cloud API is hosted at sync.cloud.materialize.com. Additional regional
//! endpoints are returned by the cloud API.

use url::Url;

pub struct CloudClient {
    endpoint: Url,
}

impl CloudClient {
    pub fn new(endpoint: Url) -> CloudClient {
        CloudClient { endpoint }
    }

    pub async fn list_cloud_regions(&self) -> Result<Vec<CloudRegion>, anyhow::Error> {
        todo!()
    }

    pub async fn list_environments(&self) -> Result<Vec<Environment>, anyhow::Error> {
        todo!()
    }

    pub async fn create_environment(&self) -> Result<(), anyhow::Error> {
        todo!()
    }
}

pub struct CloudRegion;
pub struct Environment;
