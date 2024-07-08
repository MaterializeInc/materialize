// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Definitions for the "rules_rust" Bazel targets.

use convert_case::{Case, Casing};
use guppy::graph::feature::{FeatureLabel, FeatureSet, StandardFeatures};
use guppy::graph::{
    BuildTarget, BuildTargetId, BuildTargetKind, DependencyDirection, PackageMetadata,
};
use guppy::platform::EnabledTernary;
use guppy::DependencyKind;

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write};
use std::str::FromStr;

use crate::config::{CrateConfig, GlobalConfig};
use crate::context::CrateContext;
use crate::platforms::PlatformVariant;
use crate::rules::Rule;
use crate::{Dict, Field, FileGroup, Glob, List, QuotedString, Select};

use super::{AutoIndentingWriter, ToBazelDefinition};

/// Name given to the Bazel [`filegroup`](https://bazel.build/reference/be/general#filegroup) that
/// exports all protobuf files.
const PROTO_FILEGROUP_NAME: &str = "all_protos";

pub trait RustTarget: ToBazelDefinition {
    /// Returns the Bazel rules that need to be loaded for this target.
    fn rules(&self) -> Vec<Rule>;
}

impl<T: RustTarget> RustTarget for Option<T> {
    fn rules(&self) -> Vec<Rule> {
        match self {
            Some(t) => t.rules(),
            None => vec![],
        }
    }
}

/// [`rust_library`](https://bazelbuild.github.io/rules_rust/defs.html#rust_library)
#[derive(Debug)]
pub struct RustLibrary {
    name: Field<QuotedString>,
    is_proc_macro: bool,
    features: Field<List<QuotedString>>,
    aliases: Field<Aliases>,
    deps: Field<List<QuotedString>>,
    proc_macro_deps: Field<List<QuotedString>>,
    data: Field<List<QuotedString>>,
    compile_data: Field<List<QuotedString>>,
    rustc_flags: Field<List<QuotedString>>,
    rustc_env: Field<Dict<QuotedString, QuotedString>>,
    unit_test: Option<RustTest>,
    doc_tests: Option<RustDocTest>,
}

impl RustTarget for RustLibrary {
    fn rules(&self) -> Vec<Rule> {
        let primary_rule = if self.is_proc_macro {
            Rule::RustProcMacro
        } else {
            Rule::RustLibrary
        };

        vec![primary_rule, Rule::RustTest, Rule::RustDocTest]
    }
}

impl RustLibrary {
    pub fn generate(
        config: &GlobalConfig,
        metadata: &PackageMetadata,
        crate_config: &CrateConfig,
        build_script: Option<&CargoBuildScript>,
    ) -> Result<Option<Self>, anyhow::Error> {
        if crate_config.lib().common().skip() {
            return Ok(None);
        }

        let name = metadata.name().to_case(Case::Snake);
        let name = QuotedString::new(name);

        // Collect all of the crate features.
        //
        // Note: Cargo features and Bazel don't work together very well, so by
        // default we just enable all features.
        let features: List<_> = if let Some(x) = crate_config.lib().features_override() {
            x.into_iter().map(QuotedString::from).collect()
        } else {
            let (common, extras) = crate_features(config, metadata)?;
            let mut features = List::new(common);

            if !extras.is_empty() {
                let select: Select<List<QuotedString>> = Select::new(extras, vec![]);
                features = features.concat_other(select);
            }

            features
        };

        // Collect all dependencies.
        let all_deps = WorkspaceDependencies::new(config, metadata);
        let (deps, extra_deps) = all_deps.iter(DependencyKind::Normal, false);
        let mut deps = List::new(deps).concat_other(AllCrateDeps::default().normal());

        // Add the build script as a dependency, if we have one.
        if let Some(build_script) = build_script {
            let build_script_target = format!(":{}", build_script.name.value.unquoted());
            deps.push_front(QuotedString::new(build_script_target));
        }
        // Add extra platform deps if there are any.
        if !extra_deps.is_empty() {
            let select: Select<List<QuotedString>> = Select::new(extra_deps, vec![]);
            deps = deps.concat_other(select);
        }

        // Collect all proc macro dependencies.
        let (proc_macro_deps, extra_proc_macro_deps) = all_deps.iter(DependencyKind::Normal, true);
        let mut proc_macro_deps =
            List::new(proc_macro_deps).concat_other(AllCrateDeps::default().proc_macro());

        // Add extra platform deps if there are any.
        if !extra_proc_macro_deps.is_empty() {
            let select: Select<List<QuotedString>> = Select::new(extra_proc_macro_deps, vec![]);
            proc_macro_deps = proc_macro_deps.concat_other(select);
        }

        // For every library we also generate the tests targets.
        let unit_test = RustTest::library(config, metadata, crate_config)?;
        let doc_tests = RustDocTest::generate(config, metadata, crate_config)?;

        // Extend with any extra config specified in the Cargo.toml.
        let lib_common = crate_config.lib().common();

        deps.extend(crate_config.lib().extra_deps());
        proc_macro_deps.extend(crate_config.lib().extra_proc_macro_deps());

        let (paths, globs) = lib_common.data();
        let data = List::new(paths).concat_other(globs.map(Glob::new));

        let (paths, globs) = lib_common.compile_data();
        let compile_data = List::new(paths).concat_other(globs.map(Glob::new));

        let rustc_flags = List::new(lib_common.rustc_flags());
        let rustc_env = Dict::new(lib_common.rustc_env());

        Ok(Some(RustLibrary {
            name: Field::new("name", name),
            is_proc_macro: metadata.is_proc_macro(),
            features: Field::new("crate_features", features),
            aliases: Field::new("aliases", Aliases::default().normal().proc_macro()),
            deps: Field::new("deps", deps),
            proc_macro_deps: Field::new("proc_macro_deps", proc_macro_deps),
            data: Field::new("data", data),
            compile_data: Field::new("compile_data", compile_data),
            rustc_flags: Field::new("rustc_flags", rustc_flags),
            rustc_env: Field::new("rustc_env", rustc_env),
            unit_test,
            doc_tests,
        }))
    }
}

impl ToBazelDefinition for RustLibrary {
    fn format(&self, w: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let mut w = AutoIndentingWriter::new(w);

        let kind = if self.is_proc_macro {
            "rust_proc_macro"
        } else {
            "rust_library"
        };

        writeln!(w, "{kind}(")?;
        {
            let mut w = w.indent();

            self.name.format(&mut w)?;

            writeln!(w, r#"srcs = glob(["src/**/*.rs"]),"#)?;

            self.features.format(&mut w)?;
            self.aliases.format(&mut w)?;
            self.deps.format(&mut w)?;
            self.proc_macro_deps.format(&mut w)?;
            self.compile_data.format(&mut w)?;
            self.data.format(&mut w)?;
            self.rustc_flags.format(&mut w)?;
            self.rustc_env.format(&mut w)?;
        }
        writeln!(w, ")\n")?;

        self.unit_test.format(&mut w)?;
        writeln!(w)?;
        self.doc_tests.format(&mut w)?;

        Ok(())
    }
}

/// [`rust_binary`](https://bazelbuild.github.io/rules_rust/defs.html#rust_binary)
#[derive(Debug)]
pub struct RustBinary {
    name: Field<QuotedString>,
    crate_root: Field<QuotedString>,
    features: Field<List<QuotedString>>,
    aliases: Field<Aliases>,
    deps: Field<List<QuotedString>>,
    proc_macro_deps: Field<List<QuotedString>>,
    data: Field<List<QuotedString>>,
    compile_data: Field<List<QuotedString>>,
    env: Field<Dict<QuotedString, QuotedString>>,
    rustc_flags: Field<List<QuotedString>>,
    rustc_env: Field<Dict<QuotedString, QuotedString>>,
}

impl RustTarget for RustBinary {
    fn rules(&self) -> Vec<Rule> {
        vec![Rule::RustBinary]
    }
}

impl RustBinary {
    pub fn generate(
        config: &GlobalConfig,
        metadata: &PackageMetadata,
        crate_config: &CrateConfig,
        target: &BuildTarget,
    ) -> Result<Option<Self>, anyhow::Error> {
        let crate_root_path = metadata
            .manifest_path()
            .parent()
            .ok_or_else(|| anyhow::anyhow!("crate is at the root of the filesystem?"))?;
        let name = match target.id() {
            BuildTargetId::Binary(name) => name,
            x => panic!(
                "can only generate `rust_binary` rules for binary build targets, found {x:?}"
            ),
        };
        let name = name.to_case(Case::Snake);

        let maybe_library = metadata
            .build_targets()
            .find(|target| matches!(target.id(), BuildTargetId::Library));

        // Adjust the target name to avoid a possible conflict.
        tracing::debug!(
            maybe_library = ?maybe_library.as_ref().map(|t| t.name()),
            binary_name = name,
            "name for rust_binary"
        );
        let target_name = match &maybe_library {
            Some(library) if library.name() == name => QuotedString::new(format!("{}_bin", name)),
            _ => QuotedString::new(&name),
        };

        if crate_config.binary(&name).common().skip() {
            return Ok(None);
        }

        let binary_path = target.path();
        let binary_path = binary_path
            .strip_prefix(crate_root_path)
            .map_err(|_| anyhow::anyhow!("binary is not inside workspace?"))?;
        let binary_path = QuotedString::new(binary_path.to_string());

        // Collect all dependencies.
        let all_deps = WorkspaceDependencies::new(config, metadata);
        let (deps, extra_deps) = all_deps.iter(DependencyKind::Normal, false);
        let (proc_macro_deps, extra_proc_macro_deps) = all_deps.iter(DependencyKind::Normal, true);

        let mut deps: List<QuotedString> =
            List::new(deps).concat_other(AllCrateDeps::default().normal());
        // Add extra platform deps if there are any.
        if !extra_deps.is_empty() {
            let select: Select<List<QuotedString>> = Select::new(extra_deps, vec![]);
            deps = deps.concat_other(select);
        }

        let mut proc_macro_deps: List<QuotedString> =
            List::new(proc_macro_deps).concat_other(AllCrateDeps::default().proc_macro());
        // Add extra platform deps if there are any.
        if !extra_proc_macro_deps.is_empty() {
            let select: Select<List<QuotedString>> = Select::new(extra_proc_macro_deps, vec![]);
            proc_macro_deps = proc_macro_deps.concat_other(select);
        }

        // Add the library crate as a dep if it isn't already.
        if maybe_library.is_some() {
            let dep = format!(":{}", metadata.name().to_case(Case::Snake));
            if metadata.is_proc_macro() {
                if !proc_macro_deps.iter().any(|d| d.unquoted().ends_with(&dep)) {
                    proc_macro_deps.push_front(dep);
                }
            } else {
                if !deps.iter().any(|d| d.unquoted().ends_with(&dep)) {
                    deps.push_front(dep);
                }
            }
        }

        // Extend with any extra config specified in the Cargo.toml.
        let bin_config = crate_config.binary(&name);

        deps.extend(crate_config.lib().extra_deps());
        proc_macro_deps.extend(crate_config.lib().extra_proc_macro_deps());

        let (paths, globs) = bin_config.common().data();
        let data = List::new(paths).concat_other(globs.map(Glob::new));

        let (paths, globs) = bin_config.common().compile_data();
        let compile_data = List::new(paths).concat_other(globs.map(Glob::new));

        let env = Dict::new(bin_config.env());
        let rustc_flags = List::new(bin_config.common().rustc_flags());
        let rustc_env = Dict::new(bin_config.common().rustc_env());

        Ok(Some(RustBinary {
            name: Field::new("name", target_name),
            crate_root: Field::new("crate_root", binary_path),
            features: Field::new("features", List::empty()),
            aliases: Field::new("aliases", Aliases::default().normal().proc_macro()),
            deps: Field::new("deps", deps),
            proc_macro_deps: Field::new("proc_macro_deps", proc_macro_deps),
            data: Field::new("data", data),
            compile_data: Field::new("compile_data", compile_data),
            env: Field::new("env", env),
            rustc_flags: Field::new("rustc_flags", rustc_flags),
            rustc_env: Field::new("rustc_env", rustc_env),
        }))
    }
}

impl ToBazelDefinition for RustBinary {
    fn format(&self, w: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let mut w = AutoIndentingWriter::new(w);

        writeln!(w, "rust_binary(")?;
        {
            let mut w = w.indent();

            self.name.format(&mut w)?;
            self.crate_root.format(&mut w)?;

            writeln!(w, r#"srcs = glob(["src/**/*.rs"]),"#)?;

            self.features.format(&mut w)?;
            self.aliases.format(&mut w)?;
            self.deps.format(&mut w)?;
            self.proc_macro_deps.format(&mut w)?;
            self.compile_data.format(&mut w)?;
            self.data.format(&mut w)?;
            self.env.format(&mut w)?;
            self.rustc_flags.format(&mut w)?;
            self.rustc_env.format(&mut w)?;
        }
        writeln!(w, ")\n")?;

        Ok(())
    }
}

/// [`rust_test`](https://bazelbuild.github.io/rules_rust/defs.html#rust_test)
#[derive(Debug)]
pub struct RustTest {
    name: Field<QuotedString>,
    kind: RustTestKind,
    aliases: Field<Aliases>,
    deps: Field<List<QuotedString>>,
    proc_macro_deps: Field<List<QuotedString>>,
    size: Field<RustTestSize>,
    data: Field<List<QuotedString>>,
    compile_data: Field<List<QuotedString>>,
    env: Field<Dict<QuotedString, QuotedString>>,
    rustc_flags: Field<List<QuotedString>>,
    rustc_env: Field<Dict<QuotedString, QuotedString>>,
}

impl RustTarget for RustTest {
    fn rules(&self) -> Vec<Rule> {
        vec![Rule::RustTest]
    }
}

impl RustTest {
    fn common(
        config: &GlobalConfig,
        metadata: &PackageMetadata,
        crate_config: &CrateConfig,
        name: &str,
        kind: RustTestKind,
        size: RustTestSize,
    ) -> Result<Option<Self>, anyhow::Error> {
        let test_config = crate_config.test(name);

        if test_config.common().skip() {
            return Ok(None);
        }

        let crate_name = metadata.name().to_case(Case::Snake);
        let name = QuotedString::new(format!("{crate_name}_{}_tests", name));

        // Collect all dependencies.
        let all_deps = WorkspaceDependencies::new(config, metadata);
        let (deps, extra_deps) = all_deps.iter(DependencyKind::Development, false);
        let (proc_macro_deps, extra_proc_macro_deps) =
            all_deps.iter(DependencyKind::Development, true);

        let mut deps: List<QuotedString> =
            List::new(deps).concat_other(AllCrateDeps::default().normal().normal_dev());
        // Add extra platform deps if there are any.
        if !extra_deps.is_empty() {
            let select: Select<List<QuotedString>> = Select::new(extra_deps, vec![]);
            deps = deps.concat_other(select);
        }

        let mut proc_macro_deps: List<QuotedString> = List::new(proc_macro_deps)
            .concat_other(AllCrateDeps::default().proc_macro().proc_macro_dev());
        // Add extra platform deps if there are any.
        if !extra_proc_macro_deps.is_empty() {
            let select: Select<List<QuotedString>> = Select::new(extra_proc_macro_deps, vec![]);
            proc_macro_deps = proc_macro_deps.concat_other(select);
        }

        if matches!(kind, RustTestKind::Integration(_)) {
            let dep = format!(":{crate_name}");
            if metadata.is_proc_macro() {
                if !proc_macro_deps.iter().any(|d| d.unquoted().ends_with(&dep)) {
                    proc_macro_deps.push_front(dep);
                }
            } else {
                if !deps.iter().any(|d| d.unquoted().ends_with(&dep)) {
                    deps.push_front(dep);
                }
            }
        }

        let aliases = Aliases::default()
            .normal()
            .normal_dev()
            .proc_macro()
            .proc_macro_dev();

        // Extend with any extra config specified in the Cargo.toml.
        let test_common = test_config.common();

        let (paths, globs) = test_common.data();
        let data = List::new(paths).concat_other(globs.map(Glob::new));

        let (paths, globs) = test_common.compile_data();
        let compile_data = List::new(paths).concat_other(globs.map(Glob::new));

        let env = Dict::new(test_config.env());
        let rustc_flags = List::new(test_common.rustc_flags());
        let rustc_env = Dict::new(test_common.rustc_env());

        // Use the size provided from the config, if one was provided.
        let size = test_config.size().copied().unwrap_or(size);

        Ok(Some(RustTest {
            name: Field::new("name", name),
            kind,
            aliases: Field::new("aliases", aliases),
            deps: Field::new("deps", deps),
            proc_macro_deps: Field::new("proc_macro_deps", proc_macro_deps),
            size: Field::new("size", size),
            data: Field::new("data", data),
            compile_data: Field::new("compile_data", compile_data),
            env: Field::new("env", env),
            rustc_flags: Field::new("rustc_flags", rustc_flags),
            rustc_env: Field::new("rustc_env", rustc_env),
        }))
    }

    pub fn library(
        config: &GlobalConfig,
        metadata: &PackageMetadata,
        crate_config: &CrateConfig,
    ) -> Result<Option<Self>, anyhow::Error> {
        let crate_name = metadata.name().to_case(Case::Snake);
        Self::common(
            config,
            metadata,
            crate_config,
            "lib",
            RustTestKind::library(crate_name),
            RustTestSize::Medium,
        )
    }

    pub fn integration(
        config: &GlobalConfig,
        metadata: &PackageMetadata,
        crate_config: &CrateConfig,
        target: &BuildTarget,
    ) -> Result<Option<Self>, anyhow::Error> {
        let crate_root_path = metadata
            .manifest_path()
            .parent()
            .ok_or_else(|| anyhow::anyhow!("crate is at the root of the filesystem?"))?;
        assert_eq!(
            BuildTargetKind::Binary,
            target.kind(),
            "can only generate integration tests for binary build targets"
        );

        let test_target = target.path();
        let test_target = test_target
            .strip_prefix(crate_root_path)
            .map_err(|_| anyhow::anyhow!("integration test is not inside workspace?"))?;

        Self::common(
            config,
            metadata,
            crate_config,
            target.name(),
            RustTestKind::integration([test_target.to_string()]),
            RustTestSize::Large,
        )
    }
}

impl ToBazelDefinition for RustTest {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let mut w = AutoIndentingWriter::new(writer);

        writeln!(w, "rust_test(")?;
        {
            let mut w = w.indent();

            self.name.format(&mut w)?;
            self.kind.format(&mut w)?;
            self.aliases.format(&mut w)?;
            self.deps.format(&mut w)?;
            self.proc_macro_deps.format(&mut w)?;
            self.size.format(&mut w)?;

            self.compile_data.format(&mut w)?;
            self.data.format(&mut w)?;
            self.env.format(&mut w)?;
            self.rustc_flags.format(&mut w)?;
            self.rustc_env.format(&mut w)?;
        }
        writeln!(w, ")")?;

        Ok(())
    }
}

#[derive(Debug)]
pub enum RustTestKind {
    Library(Field<QuotedString>),
    Integration(Field<List<QuotedString>>),
}

impl RustTestKind {
    pub fn library(crate_name: impl Into<String>) -> Self {
        let crate_name = QuotedString::new(format!(":{}", crate_name.into()));
        Self::Library(Field::new("crate", crate_name))
    }

    pub fn integration(srcs: impl IntoIterator<Item = String>) -> Self {
        let srcs = srcs.into_iter().map(QuotedString::new).collect();
        Self::Integration(Field::new("srcs", srcs))
    }
}

impl ToBazelDefinition for RustTestKind {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        match self {
            RustTestKind::Library(field) => field.format(writer)?,
            RustTestKind::Integration(srcs) => srcs.format(writer)?,
        }
        Ok(())
    }
}

/// Size of the Bazel Test.
///
/// <https://bazel.build/reference/be/common-definitions#common-attributes-tests>
#[derive(Debug, Clone, Copy, Default)]
pub enum RustTestSize {
    Small,
    Medium,
    #[default]
    Large,
    Enormous,
}

impl ToBazelDefinition for RustTestSize {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        use RustTestSize::*;
        let s = match self {
            Small => "small",
            Medium => "medium",
            Large => "large",
            Enormous => "enormous",
        };
        write!(writer, "\"{s}\"")
    }
}

impl FromStr for RustTestSize {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let size = match s {
            "small" => RustTestSize::Small,
            "medium" => RustTestSize::Medium,
            "large" => RustTestSize::Large,
            "enormous" => RustTestSize::Enormous,
            other => return Err(other.to_string()),
        };
        Ok(size)
    }
}

impl<'de> serde::Deserialize<'de> for RustTestSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        RustTestSize::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// [`rust_doc_test`](http://bazelbuild.github.io/rules_rust/rust_doc.html#rust_doc_test).
#[derive(Debug)]
pub struct RustDocTest {
    name: Field<QuotedString>,
    crate_: Field<QuotedString>,
    deps: Field<List<QuotedString>>,
}

impl RustDocTest {
    pub fn generate(
        config: &GlobalConfig,
        metadata: &PackageMetadata,
        crate_config: &CrateConfig,
    ) -> Result<Option<Self>, anyhow::Error> {
        if crate_config.doc_test().common().skip() {
            return Ok(None);
        }

        let crate_name = metadata.name().to_case(Case::Snake);
        let name = QuotedString::new(format!("{crate_name}_doc_test"));
        let crate_ = QuotedString::new(format!(":{crate_name}"));

        // Collect all dependencies.
        let all_deps = WorkspaceDependencies::new(config, metadata);
        let (deps, extra_deps) = all_deps.iter(DependencyKind::Development, false);
        let mut deps: List<QuotedString> =
            List::new(deps).concat_other(AllCrateDeps::default().normal().normal_dev());

        // Add extra platform deps if there are any.
        if !extra_deps.is_empty() {
            let select: Select<List<QuotedString>> = Select::new(extra_deps, vec![]);
            deps = deps.concat_other(select);
        }

        Ok(Some(RustDocTest {
            name: Field::new("name", name),
            crate_: Field::new("crate", crate_),
            deps: Field::new("deps", deps),
        }))
    }
}

impl ToBazelDefinition for RustDocTest {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let mut w = AutoIndentingWriter::new(writer);

        writeln!(w, "rust_doc_test(")?;
        {
            let mut w = w.indent();
            self.name.format(&mut w)?;
            self.crate_.format(&mut w)?;
            self.deps.format(&mut w)?;
        }
        writeln!(w, "\n)")?;

        Ok(())
    }
}

/// [`cargo_build_script`](http://bazelbuild.github.io/rules_rust/cargo.html#cargo_build_script)
#[derive(Debug)]
pub struct CargoBuildScript {
    pub name: Field<QuotedString>,
    pub script_src: Field<List<QuotedString>>,
    pub deps: Field<List<QuotedString>>,
    pub proc_macro_deps: Field<List<QuotedString>>,
    pub build_script_env: Field<Dict<QuotedString, QuotedString>>,
    pub data: Field<List<QuotedString>>,
    pub compile_data: Field<List<QuotedString>>,
    pub rustc_flags: Field<List<QuotedString>>,
    pub rustc_env: Field<Dict<QuotedString, QuotedString>>,
    pub extras: Vec<Box<dyn ToBazelDefinition>>,
}

impl RustTarget for CargoBuildScript {
    fn rules(&self) -> Vec<Rule> {
        vec![Rule::CargoBuildScript]
    }
}

impl CargoBuildScript {
    pub fn generate(
        config: &GlobalConfig,
        context: &CrateContext,
        crate_config: &CrateConfig,
        metadata: &PackageMetadata,
    ) -> Result<Option<Self>, anyhow::Error> {
        let crate_name = metadata.name().to_case(Case::Snake);
        let name = QuotedString::new(format!("{crate_name}_build_script"));

        // Determine the source for this build script.
        let Some(build_script_target) = metadata.build_target(&BuildTargetId::BuildScript) else {
            return Ok(None);
        };

        // Build scripts _should_ only ever exist at `build.rs`, but guard
        // against them existing somewhere else.
        let crate_root_path = metadata
            .manifest_path()
            .parent()
            .ok_or_else(|| anyhow::anyhow!("package is at the root of the filesystem?"))?;
        let script_src = build_script_target
            .path()
            .strip_prefix(crate_root_path)
            .map_err(|_| anyhow::anyhow!("build script is not inside of crate"))?;
        let script_src = Field::new(
            "srcs",
            List::new(vec![QuotedString::new(script_src.to_string())]),
        );

        // Collect all dependencies.
        let all_deps = WorkspaceDependencies::new(config, metadata);
        let (deps, extra_deps) = all_deps.iter(DependencyKind::Build, false);
        let (proc_macro_deps, extra_proc_macro_deps) =
            all_deps.iter(DependencyKind::Development, true);

        let mut deps: List<QuotedString> =
            List::new(deps).concat_other(AllCrateDeps::default().normal().build());
        // Add extra platform deps if there are any.
        if !extra_deps.is_empty() {
            let select: Select<List<QuotedString>> = Select::new(extra_deps, vec![]);
            deps = deps.concat_other(select);
        }

        let mut proc_macro_deps: List<QuotedString> = List::new(proc_macro_deps)
            .concat_other(AllCrateDeps::default().proc_macro().build_proc_macro());
        // Add extra platform deps if there are any.
        if !extra_proc_macro_deps.is_empty() {
            let select: Select<List<QuotedString>> = Select::new(extra_proc_macro_deps, vec![]);
            proc_macro_deps = proc_macro_deps.concat_other(select);
        }

        // Generate any extra targets that we need.
        let mut extras: Vec<Box<dyn ToBazelDefinition>> = Vec::new();
        let mut data: List<QuotedString> = List::empty();

        // Generate a filegroup for any files this build script depends on.
        let mut protos = context
            .build_script
            .as_ref()
            .map(|b| b.generated_protos.clone())
            .unwrap_or_default();

        // Make sure to add transitive dependencies.
        let crate_filename = crate_root_path
            .file_name()
            .ok_or_else(|| anyhow::anyhow!("crate is at the root of the filesystem?"))?;
        let proto_dependencies = context.build_script.as_ref().map(|b| &b.proto_dependencies);
        if let Some(deps) = proto_dependencies {
            let transitive_deps: BTreeSet<_> = deps
                .iter()
                // Google related dependencies are included via "well known types".
                .filter(|p| !p.starts_with("google"))
                // Imports from within the same crate are already included.
                .filter(|p| !p.starts_with(crate_filename))
                // This assume the root of the protobuf path is a crate, which might not be true?
                .map(|p| p.components().next().unwrap())
                // TODO(parkmcar): This is a bit hacky, we need to consider where
                // we are relative to the workspace root.
                .map(|name| format!("//src/{name}:{PROTO_FILEGROUP_NAME}"))
                // Collect into a `BTreeSet` to de-dupe.
                .collect();

            protos.extend(transitive_deps);
        }

        if !protos.is_empty() {
            let proto_filegroup = FileGroup::new(PROTO_FILEGROUP_NAME, protos);
            extras.push(Box::new(proto_filegroup));

            // Make sure to include this file group in the build script data!
            data.push_back(format!(":{PROTO_FILEGROUP_NAME}"));
        }

        // Check for any metadata specified in the Cargo.toml.
        let build_common = crate_config.build().common();

        let (paths, globs) = build_common.data();
        data.extend(paths);
        let data = data.concat_other(globs.map(Glob::new));

        let (paths, globs) = build_common.compile_data();
        let compile_data = List::new(paths).concat_other(globs.map(Glob::new));

        let rustc_flags = List::new(build_common.rustc_flags());
        let rustc_env = Dict::new(build_common.rustc_env());
        let build_script_env = Dict::new(crate_config.build().build_script_env());

        Ok(Some(CargoBuildScript {
            name: Field::new("name", name),
            script_src,
            deps: Field::new("deps", deps),
            proc_macro_deps: Field::new("proc_macro_deps", proc_macro_deps),
            build_script_env: Field::new("build_script_env", build_script_env),
            data: Field::new("data", data),
            compile_data: Field::new("compile_data", compile_data),
            rustc_flags: Field::new("rustc_flags", rustc_flags),
            rustc_env: Field::new("rustc_env", rustc_env),
            extras,
        }))
    }
}

impl ToBazelDefinition for CargoBuildScript {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let mut w = AutoIndentingWriter::new(writer);

        // Write any extra targets this build script depends on.
        for extra in &self.extras {
            extra.format(&mut w)?;
            writeln!(w)?;
        }

        writeln!(w, "cargo_build_script(")?;
        {
            let mut w = w.indent();

            self.name.format(&mut w)?;
            self.script_src.format(&mut w)?;
            self.deps.format(&mut w)?;
            self.proc_macro_deps.format(&mut w)?;
            self.build_script_env.format(&mut w)?;

            self.data.format(&mut w)?;
            self.compile_data.format(&mut w)?;
            self.rustc_flags.format(&mut w)?;
            self.rustc_env.format(&mut w)?;
        }
        writeln!(w, ")")?;

        Ok(())
    }
}

/// An opaque blob of text that we treat as a target.
#[derive(Debug)]
pub struct AdditiveContent<'a>(Cow<'a, str>);

impl<'a> AdditiveContent<'a> {
    pub fn new(s: &'a str) -> Self {
        AdditiveContent(s.into())
    }
}

impl<'a> ToBazelDefinition for AdditiveContent<'a> {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        writeln!(writer, "{}", self.0)?;
        Ok(())
    }
}

impl<'a> RustTarget for AdditiveContent<'a> {
    fn rules(&self) -> Vec<Rule> {
        vec![]
    }
}

type AllCrateDeps = CratesUniverseMacro<AllCrateDeps_>;
type Aliases = CratesUniverseMacro<Aliases_>;

/// [`crates_universe`](http://bazelbuild.github.io/rules_rust/crate_universe.html) exposes a few
/// macros that make it easier to define depedencies and aliases.
#[derive(Default, Debug)]
struct CratesUniverseMacro<Name> {
    name: Name,
    fields: Vec<MacroOption>,
}

impl<Name> CratesUniverseMacro<Name> {
    pub fn normal(mut self) -> Self {
        self.fields.push(MacroOption::Normal);
        self
    }

    pub fn proc_macro(mut self) -> Self {
        self.fields.push(MacroOption::ProcMacro);
        self
    }

    pub fn normal_dev(mut self) -> Self {
        self.fields.push(MacroOption::NormalDev);
        self
    }

    pub fn proc_macro_dev(mut self) -> Self {
        self.fields.push(MacroOption::ProcMacroDev);
        self
    }

    pub fn build(mut self) -> Self {
        self.fields.push(MacroOption::Build);
        self
    }

    pub fn build_proc_macro(mut self) -> Self {
        self.fields.push(MacroOption::BuildProcMacro);
        self
    }
}

impl<N: Named> ToBazelDefinition for CratesUniverseMacro<N> {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        let mut w = AutoIndentingWriter::new(writer);

        write!(w, "{}", self.name.name())?;

        match &self.fields[..] {
            [] => write!(w, "()")?,
            [one] => write!(w, "({} = True)", one.to_bazel_definition())?,
            multiple => {
                write!(w, "(")?;
                for item in multiple {
                    let mut w = w.indent();
                    writeln!(w)?;
                    write!(w, "{} = True,", item.to_bazel_definition())?;
                }
                write!(w, "\n)")?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
enum MacroOption {
    Normal,
    ProcMacro,
    NormalDev,
    ProcMacroDev,
    Build,
    BuildProcMacro,
}

impl ToBazelDefinition for MacroOption {
    fn format(&self, writer: &mut dyn fmt::Write) -> Result<(), fmt::Error> {
        match self {
            MacroOption::Normal => write!(writer, "normal"),
            MacroOption::ProcMacro => write!(writer, "proc_macro"),
            MacroOption::NormalDev => write!(writer, "normal_dev"),
            MacroOption::ProcMacroDev => write!(writer, "proc_macro_dev"),
            MacroOption::Build => write!(writer, "build"),
            MacroOption::BuildProcMacro => write!(writer, "build_proc_macro"),
        }
    }
}

/// A hack for const generic strings.
trait Named: std::fmt::Debug {
    fn name(&self) -> &'static str;
}
#[derive(Default, Debug)]
struct AllCrateDeps_;
impl Named for AllCrateDeps_ {
    fn name(&self) -> &'static str {
        "all_crate_deps"
    }
}
#[derive(Default, Debug)]
struct Aliases_;
impl Named for Aliases_ {
    fn name(&self) -> &'static str {
        "aliases"
    }
}

struct WorkspaceDependencies<'a> {
    config: &'a GlobalConfig,
    package: &'a PackageMetadata<'a>,
}

impl<'a> WorkspaceDependencies<'a> {
    pub fn new(config: &'a GlobalConfig, package: &'a PackageMetadata<'a>) -> Self {
        WorkspaceDependencies { config, package }
    }

    /// Returns a set of dependencies that are common to all platforms, and
    /// then any additional dependencies that need to be enabled for a specific
    /// platform.
    pub fn iter(
        &self,
        kind: DependencyKind,
        proc_macro: bool,
    ) -> (BTreeSet<String>, BTreeMap<PlatformVariant, Vec<String>>) {
        let feature_set = platform_feature_sets(self.package);

        let dependencies: BTreeMap<_, _> = feature_set
            .into_iter()
            .map(|(platform, feature_set)| {
                // Convert the feature set to the set of packages it enables.
                let deps: BTreeSet<_> = feature_set
                    .to_package_set()
                    .links(DependencyDirection::Reverse)
                    // Filter down to only direct dependencies.
                    .filter(|link| link.from().id() == self.package.id())
                    // Tests and build scipts can rely on normal dependencies, so always make sure they're
                    // included.
                    .filter(move |link| link.req_for_kind(kind).is_present())
                    .map(|link| link.to())
                    // Ignore deps filtered out by the global config.
                    .filter(|meta| self.config.include_dep(meta.name()))
                    // Filter proc_macro deps.
                    .filter(move |meta| meta.is_proc_macro() == proc_macro)
                    // Filter map down to only deps in the workspace, and their path.
                    .filter_map(|meta| meta.source().workspace_path().map(|p| (p, meta)))
                    .map(|(path, meta)| {
                        let crate_name = meta.name().to_case(Case::Snake);
                        format!("//{}:{}", path, crate_name)
                    })
                    .collect();

                (platform, deps)
            })
            .collect();

        // Dependencies that are common to all platforms.
        let common = dependencies
            .iter()
            .fold(None, |common, (_variant, set)| match common {
                None => Some(set.clone()),
                Some(common) => Some(common.intersection(set).cloned().collect()),
            })
            .unwrap_or_default();

        // Extra features for each platform that need to be enabled.
        let extras: BTreeMap<_, _> = dependencies
            .into_iter()
            .filter_map(|(variant, features)| {
                let extra: Vec<_> = features.difference(&common).cloned().collect();
                if extra.is_empty() {
                    None
                } else {
                    Some((variant, extra))
                }
            })
            .collect();

        (common, extras)
    }
}

/// Returns a set of Cargo features that are common to all platforms, and then
/// any additional features that need to be enabled for a specific platform.
pub fn crate_features<'a>(
    config: &'a GlobalConfig,
    package: &'a PackageMetadata<'a>,
) -> Result<(BTreeSet<String>, BTreeMap<PlatformVariant, Vec<String>>), anyhow::Error> {
    // Resolve feature sets for all of the platforms we care about.
    let feature_sets = platform_feature_sets(package);

    // Filter down to just the feature labels for this crate.
    let features: BTreeMap<_, _> = feature_sets
        .into_iter()
        .map(|(platform, feature_set)| {
            // Filter down to the features for just this crate.
            let features: BTreeSet<_> = feature_set
                .features_for(package.id())
                .expect("package id should be known")
                .expect("package id should be in the feature set")
                .into_iter()
                .filter_map(|feature| match feature.label() {
                    FeatureLabel::Base => None,
                    FeatureLabel::Named(f) => Some(f),
                    FeatureLabel::OptionalDependency(f) => Some(f),
                })
                // TODO(parkmycar): We shouldn't ignore features based on name, but if
                // enabling that feature would result in us depending on a crate we
                // want to ignore.
                .filter(|name| config.include_dep(name))
                .map(|s| s.to_string())
                .collect();

            (platform, features)
        })
        .collect();

    // Features that are common to all platforms.
    let common = features
        .iter()
        .fold(None, |common, (_variant, set)| match common {
            None => Some(set.clone()),
            Some(common) => Some(common.intersection(set).cloned().collect()),
        })
        .unwrap_or_default();

    // Extra features for each platform that need to be enabled.
    let extras: BTreeMap<_, _> = features
        .into_iter()
        .filter_map(|(variant, features)| {
            let extra: Vec<_> = features.difference(&common).cloned().collect();
            if extra.is_empty() {
                None
            } else {
                Some((variant, extra))
            }
        })
        .collect();

    Ok((common, extras))
}

/// Returns a [`FeatureSet`] of reverse dependencies (in other words, all
/// crates that depend on) for the provided package, for every platform that we
/// support.
///
/// TODO(parkmycar): Make the list of platforms configurable.
pub fn platform_feature_sets<'a>(
    package: &'a PackageMetadata<'a>,
) -> BTreeMap<PlatformVariant, FeatureSet> {
    // Resolve a feature graph for all crates that depend on this one.
    let dependents = package
        .to_package_query(DependencyDirection::Reverse)
        .resolve()
        .to_feature_set(StandardFeatures::Default);

    PlatformVariant::all()
        .iter()
        .map(|p| {
            // Resolve all features enabled for the specified platform.
            let feature_set = dependents
                .to_feature_query(DependencyDirection::Forward)
                .resolve_with_fn(|_query, cond_link| {
                    // Note: We don't currently generate different targets for
                    // dev or build dependencies, but possibly could if need be.
                    let normal_enabled = cond_link.normal().enabled_on(p.spec());
                    let dev_enabled = cond_link.dev().enabled_on(p.spec());
                    let build_enabled = cond_link.build().enabled_on(p.spec());

                    matches!(normal_enabled, EnabledTernary::Enabled)
                        || matches!(dev_enabled, EnabledTernary::Enabled)
                        || matches!(build_enabled, EnabledTernary::Enabled)
                });

            (*p, feature_set)
        })
        .collect()
}
