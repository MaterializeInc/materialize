// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::Cow;
use std::collections::BTreeMap;

use guppy::graph::PackageMetadata;
use std::sync::LazyLock;

use crate::targets::{AdditiveContent, RustTestSize};

const KEY_NAME: &str = "cargo-gazelle";

/// Name that should be used to specify extra config for library tests.
///
/// e.g. `package.metadata.cargo-gazelle.test.lib`
const LIB_TEST_NAME: &str = "lib";

/// Name that should be used to specify extra config for doc tests.
///
/// e.g. `package.metadata.cargo-gazelle.test.doc`
const DOC_TEST_NAME: &str = "doc";

/// Global configuration for generating `BUILD` files.
#[derive(Debug, Clone)]
pub struct GlobalConfig {
    pub ignored_crates: Vec<Cow<'static, str>>,
    pub proto_build_crates: Vec<Cow<'static, str>>,
}

impl Default for GlobalConfig {
    fn default() -> Self {
        GlobalConfig {
            ignored_crates: vec!["workspace-hack".into()],
            proto_build_crates: vec!["prost_build".into(), "tonic_build".into()],
        }
    }
}

impl GlobalConfig {
    /// Returns `true` if the named dependency should be included, `false` if it should be ignored.
    pub fn include_dep(&self, name: &str) -> bool {
        !self.ignored_crates.contains(&Cow::Borrowed(name))
    }
}

/// Extra configuration for the Bazel targets generated for a crate.
///
/// We should try to make generating `BUILD.bazel` files is as seamless as possible, but there are
/// instances where this isn't possible. For example, some tests rely on text-based snapshot files
/// that Bazel needs to know about so it can include them in the sandbox. But Rust/Cargo has no way
/// of formally declaring a dependency on these files so we must manually specify them.
///
#[derive(Default, Debug, serde::Deserialize)]
pub struct CrateConfig {
    /// Should we skip generating a `BUILD.bazel` file entirely for this crate.
    #[serde(default)]
    skip_generating: bool,
    /// Additive content we paste at the bottom of the generated `BUILD.bazel` file.
    additive_content: Option<String>,

    /// Extra config for the `rust_library` target.
    #[serde(default)]
    lib: LibraryConfig,
    /// Extra config for the `cargo_build_script` target.
    #[serde(default)]
    build: BuildConfig,
    /// Extra config for any test targets.
    #[serde(alias = "test")]
    #[serde(default)]
    tests: BTreeMap<String, TestConfig>,
    /// Extra config for any binary targets.
    #[serde(alias = "binary")]
    #[serde(default)]
    binaries: BTreeMap<String, BinaryConfig>,
}

impl CrateConfig {
    pub fn new(package: &PackageMetadata) -> Self {
        package
            .metadata_table()
            .get(KEY_NAME)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default()
    }

    pub fn skip_generating(&self) -> bool {
        self.skip_generating
    }

    pub fn additive_content(&self) -> Option<AdditiveContent> {
        self.additive_content
            .as_ref()
            .map(|s| AdditiveContent::new(s.as_str()))
    }

    pub fn lib(&self) -> &LibraryConfig {
        &self.lib
    }

    pub fn lib_test(&self) -> &TestConfig {
        self.test(LIB_TEST_NAME)
    }

    pub fn doc_test(&self) -> &TestConfig {
        self.test(DOC_TEST_NAME)
    }

    pub fn build(&self) -> &BuildConfig {
        &self.build
    }

    pub fn test(&self, name: &str) -> &TestConfig {
        static EMPTY_TEST: LazyLock<TestConfig> = LazyLock::new(TestConfig::default);
        self.tests.get(name).unwrap_or(&*EMPTY_TEST)
    }

    pub fn binary(&self, name: &str) -> &BinaryConfig {
        static EMPTY_BINARY: LazyLock<BinaryConfig> = LazyLock::new(BinaryConfig::default);
        self.binaries.get(name).unwrap_or(&*EMPTY_BINARY)
    }
}

/// Extra configuration for a [`RustLibrary`] target.
///
/// [`RustLibrary`]: crate::targets::RustLibrary
#[derive(Default, Debug, serde::Deserialize)]
pub struct LibraryConfig {
    #[serde(flatten)]
    common: CommonConfig,

    /// By default Bazel enables all features of a crate. If this field is set we'll override that
    /// behavior and only set the specified features.
    features_override: Option<Vec<String>>,
    /// Extra dependencies to include.
    #[serde(default)]
    extra_deps: Vec<String>,
    /// Extra proc macro dependencies to include.
    #[serde(default)]
    extra_proc_macro_deps: Vec<String>,
    /// Should we disable pipelined compilation for this library.
    #[serde(default)]
    disable_pipelining: Option<bool>,
}

impl LibraryConfig {
    pub fn common(&self) -> &CommonConfig {
        &self.common
    }

    pub fn features_override(&self) -> Option<&Vec<String>> {
        self.features_override.as_ref()
    }

    pub fn extra_deps(&self) -> &[String] {
        &self.extra_deps
    }

    pub fn extra_proc_macro_deps(&self) -> &[String] {
        &self.extra_proc_macro_deps
    }

    pub fn disable_pipelining(&self) -> Option<bool> {
        self.disable_pipelining
    }
}

/// Extra configuration for a [`CargoBuildScript`] target.
///
/// [`CargoBuildScript`]: crate::targets::CargoBuildScript
#[derive(Default, Debug, serde::Deserialize)]
pub struct BuildConfig {
    #[serde(flatten)]
    common: CommonConfig,

    /// Environment variables to set for the build script.
    #[serde(default)]
    build_script_env: BTreeMap<String, String>,
    /// Skip the automatic search for protobuf files.
    #[serde(default)]
    skip_proto_search: bool,
}

impl BuildConfig {
    pub fn common(&self) -> &CommonConfig {
        &self.common
    }

    pub fn build_script_env(&self) -> &BTreeMap<String, String> {
        &self.build_script_env
    }

    pub fn skip_proto_search(&self) -> bool {
        self.skip_proto_search
    }
}

/// Extra configuration for a [`RustTest`] target.
///
/// [`RustTest`]: crate::targets::RustTest
#[derive(Default, Debug, serde::Deserialize)]
pub struct TestConfig {
    #[serde(flatten)]
    common: CommonConfig,

    /// ["size"](https://bazel.build/reference/be/common-definitions#common-attributes-tests)
    /// of the test target, this defines how many resources Bazel provides to the test.
    size: Option<RustTestSize>,
    /// Set of environment variables to set when the test is executed.
    #[serde(default)]
    env: BTreeMap<String, String>,
}

impl TestConfig {
    pub fn common(&self) -> &CommonConfig {
        &self.common
    }

    pub fn size(&self) -> Option<&RustTestSize> {
        self.size.as_ref()
    }

    pub fn env(&self) -> &BTreeMap<String, String> {
        &self.env
    }
}

/// Extra configuration for a [`RustBinary`] target.
///
/// [`RustBinary`]: crate::targets::RustBinary
#[derive(Default, Debug, serde::Deserialize)]
pub struct BinaryConfig {
    #[serde(flatten)]
    common: CommonConfig,

    /// Additional environment variables that get set when invoked by `bazel run`.
    #[serde(default)]
    env: BTreeMap<String, String>,
}

impl BinaryConfig {
    pub fn common(&self) -> &CommonConfig {
        &self.common
    }

    pub fn env(&self) -> &BTreeMap<String, String> {
        &self.env
    }
}

/// Extra config that is common among all target types.
#[derive(Default, Debug, serde::Deserialize)]
pub struct CommonConfig {
    /// Skip generating this target.
    #[serde(default)]
    skip: bool,
    /// Paths that will be added to the `compile_data` field of the generated Bazel target.
    #[serde(default)]
    compile_data: Vec<String>,
    /// Paths that will be added to the `data` field of the generated Bazel target.
    #[serde(default)]
    data: Vec<String>,
    /// Extra flags that should be passed to the Rust compiler.
    #[serde(default)]
    rustc_flags: Vec<String>,
    /// Set of environment variables to set for the Rust compiler.
    #[serde(default)]
    rustc_env: BTreeMap<String, String>,
}

impl CommonConfig {
    pub fn skip(&self) -> bool {
        self.skip
    }

    /// Returns a tuple of `(<non-glob paths>, <glob paths, if any>)`.
    pub fn compile_data(&self) -> (Vec<&String>, Option<Vec<&String>>) {
        let paths: Vec<_> = self
            .compile_data
            .iter()
            .filter(|s| !s.contains('*'))
            .collect();
        let globs: Vec<_> = self
            .compile_data
            .iter()
            .filter(|s| s.contains('*'))
            .collect();

        let globs = if globs.is_empty() { None } else { Some(globs) };

        (paths, globs)
    }

    /// Returns a tuple of `(<non-glob paths>, <glob paths, if any>)`.
    pub fn data(&self) -> (Vec<&String>, Option<Vec<&String>>) {
        let paths: Vec<_> = self.data.iter().filter(|s| !s.contains('*')).collect();
        let globs: Vec<_> = self.data.iter().filter(|s| s.contains('*')).collect();

        let globs = if globs.is_empty() { None } else { Some(globs) };

        (paths, globs)
    }

    pub fn rustc_flags(&self) -> &[String] {
        &self.rustc_flags
    }

    pub fn rustc_env(&self) -> &BTreeMap<String, String> {
        &self.rustc_env
    }
}
