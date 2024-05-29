// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use guppy::graph::PackageMetadata;

const KEY_NAME: &str = "cargo-gazelle";

/// TODO(parkmycar): This should probably have a shape like:
///
/// ```text
/// pub struct CrateMetadata(Vec<MetadataTarget>);
///
/// pub enum MetadataTarget {
///     Lib(CommonMetadata),
///     // The first field is the name of the test.
///     Test(String, CommonMetadata),
/// }
/// ```
#[derive(Debug, serde::Deserialize)]
pub enum CrateMetadata {
    #[serde(rename = "lib")]
    Lib(CommonMetadata),
    #[serde(rename = "build")]
    Build(CommonMetadata),
}

impl CrateMetadata {
    pub fn new(package: &PackageMetadata) -> Option<Self> {
        let metadata = package.metadata_table().get(KEY_NAME)?;
        serde_json::from_value(metadata.clone()).ok()
    }

    pub fn lib(&self) -> Option<&CommonMetadata> {
        match self {
            CrateMetadata::Lib(m) => Some(m),
            _ => None,
        }
    }

    pub fn build(&self) -> Option<&CommonMetadata> {
        match self {
            CrateMetadata::Build(m) => Some(m),
            _ => None,
        }
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct CommonMetadata {
    /// Paths that will be added to the `compile_data` field of the generated Bazel target.
    compile_data: Option<Vec<camino::Utf8PathBuf>>,
    /// Paths that will be added to the `data` field of the generated Bazel target.
    data: Option<Vec<camino::Utf8PathBuf>>,
}

impl CommonMetadata {
    pub fn compile_data(&self) -> &[camino::Utf8PathBuf] {
        static EMPTY: Vec<camino::Utf8PathBuf> = Vec::new();
        self.compile_data.as_ref().unwrap_or(&EMPTY)
    }

    pub fn data(&self) -> &[camino::Utf8PathBuf] {
        static EMPTY: Vec<camino::Utf8PathBuf> = Vec::new();
        self.data.as_ref().unwrap_or(&EMPTY)
    }
}
