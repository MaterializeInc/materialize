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

use comfy_table::{Table, TableComponent};

use crate::{utils::CloudProviderRegion, CloudProviderAndRegion, FronteggAppPassword};

pub(crate) trait Tabled {
    fn new_table_builder() -> Table {
        let mut table = Table::new();
        table.set_style(TableComponent::HeaderLines, '-');
        table.set_style(TableComponent::MiddleHeaderIntersections, '+');
        table.set_style(TableComponent::RightHeaderIntersection, '-');
        table.set_style(TableComponent::LeftHeaderIntersection, '-');
        table.set_style(TableComponent::LeftBorder, ' ');
        table.set_style(TableComponent::RightBorder, ' ');
        table.set_style(TableComponent::BottomBorderIntersections, ' ');
        table.set_style(TableComponent::TopLeftCorner, ' ');
        table.set_style(TableComponent::TopRightCorner, ' ');
        table.set_style(TableComponent::TopBorder, ' ');
        table.set_style(TableComponent::BottomBorder, ' ');
        table.set_style(TableComponent::BottomLeftCorner, ' ');
        table.set_style(TableComponent::BottomRightCorner, ' ');
        table.set_style(TableComponent::TopBorderIntersections, ' ');
        table.remove_style(TableComponent::HorizontalLines);
        table.remove_style(TableComponent::MiddleIntersections);
        table.remove_style(TableComponent::LeftBorderIntersections);
        table.remove_style(TableComponent::RightBorderIntersections);

        table
    }

    fn table(&self) -> String;
}

pub(crate) struct ShowProfile {
    pub(crate) name: String,
}

impl Tabled for ShowProfile {
    fn table(&self) -> String {
        let mut table = Self::new_table_builder();

        table.set_header(vec!["profile"]);
        table.add_row(vec![self.name.clone()]);
        table.to_string()
    }
}

impl Tabled for Vec<ShowProfile> {
    fn table(&self) -> String {
        let mut table = Self::new_table_builder();

        table.set_header(vec!["profiles"]);
        for profile in self {
            table.add_row(vec![profile.name.clone()]);
        }
        table.to_string()
    }
}

impl Tabled for Vec<CloudProviderAndRegion> {
    fn table(&self) -> String {
        let mut table = Self::new_table_builder();

        table.set_header(vec!["region", "status"]);

        for row in self {
            let region = &row.region;
            let cloud_provider = &row.cloud_provider;

            match region {
                Some(_) => {
                    table.add_row(vec![
                        format!("{:}/{:}", cloud_provider.provider, cloud_provider.region),
                        "enabled".to_string(),
                    ]);
                }
                None => {
                    table.add_row(vec![
                        format!("{:}/{:}", cloud_provider.provider, cloud_provider.region),
                        "disabled".to_string(),
                    ]);
                }
            }
        }

        table.to_string()
    }
}

impl Tabled for Option<CloudProviderRegion> {
    fn table(&self) -> String {
        let mut table = Self::new_table_builder();

        table.set_header(vec!["region"]);

        if let Some(cloud_provider) = self {
            table.add_row(vec![cloud_provider.to_string()]);
        }

        table.to_string()
    }
}

impl Tabled for Vec<FronteggAppPassword> {
    fn table(&self) -> String {
        let mut table = Self::new_table_builder();
        table.set_header(vec!["name", "create at"]);

        for app_password in self {
            let mut name = app_password.description.clone();

            if name.len() > 20 {
                let short_name = name[..20].to_string();
                name = format!("{:}...", short_name);
            }

            table.add_row(vec![name, app_password.created_at.clone()]);
        }

        table.to_string()
    }
}
