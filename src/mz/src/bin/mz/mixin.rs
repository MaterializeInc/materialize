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

//! Reusable arguments that can be mixed in to commands.

use url::Url;

#[derive(Debug, clap::Args)]
pub struct ProfileArg {
    /// Use the specified authentication profile.
    #[clap(long, env = "PROFILE")]
    pub profile: Option<String>,
}

#[derive(Debug, clap::Args)]
pub struct EndpointArgs {
    /// Use the specified cloud endpoint.
    #[clap(long, hidden = true, env = "CLOUD_ENDPOINT")]
    pub cloud_endpoint: Option<Url>,
    /// Use the specified admin endpoint.
    #[clap(long, hidden = true, env = "ADMIN_ENDPOINT")]
    pub admin_endpoint: Option<Url>,
}

#[derive(Debug, clap::Args)]
pub struct RegionArg {
    /// Use the specified region.
    #[clap(long, env = "REGION")]
    pub region: Option<String>,
}
