// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::Write;

use anyhow::Context;
use cargo_toml::Manifest;
use clap::Parser;

use cargo_gazelle::args::Args;
use cargo_gazelle::config::{CrateConfig, GlobalConfig};
use cargo_gazelle::context::CrateContext;
use cargo_gazelle::header::BazelHeader;
use cargo_gazelle::targets::{CargoBuildScript, RustBinary, RustLibrary, RustTarget, RustTest};
use cargo_gazelle::BazelBuildFile;
use guppy::graph::{BuildTargetId, PackageMetadata};
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::try_parse()?;
    let path = args.path;

    let graph = guppy::MetadataCommand::new()
        .manifest_path(&path)
        .build_graph()
        .context("building crate graph")?;
    let manifest = Manifest::from_path(&path).context("reading manifest")?;

    // Generate for either a single package, or an entire workspace.
    let packages: Box<dyn Iterator<Item = PackageMetadata>> = match manifest.package {
        Some(package) => {
            let package_metadata = graph.workspace().member_by_name(package.name)?;
            Box::new(std::iter::once(package_metadata))
        }
        None => Box::new(graph.workspace().iter()),
    };

    let config = GlobalConfig::default();

    for package in packages {
        let crate_config = CrateConfig::new(&package);
        let additive_content = crate_config.additive_content();
        if crate_config.skip_generating() {
            tracing::info!(path = ?package.manifest_path(), "skipping, because crate config");
            continue;
        }

        tracing::info!(path = ?package.manifest_path(), "generating");

        let error_context = format!("generating {}", package.name());
        let crate_context = CrateContext::generate(&config, &package).context(error_context)?;

        let build_script =
            CargoBuildScript::generate(&config, &crate_context, &crate_config, &package)?;
        let library =
            RustLibrary::generate(&config, &package, &crate_config, build_script.as_ref())?;

        let integration_tests: Vec<_> = package
            .build_targets()
            .filter(|target| matches!(target.id(), BuildTargetId::Test(_)))
            .map(|target| RustTest::integration(&config, &package, &crate_config, &target))
            .collect::<Result<_, _>>()?;

        let binaries: Vec<_> = package
            .build_targets()
            .filter(|target| matches!(target.id(), BuildTargetId::Binary(_)))
            .map(|target| RustBinary::generate(&config, &package, &crate_config, &target))
            .collect::<Result<_, _>>()?;

        #[allow(clippy::as_conversions)]
        let targets: Vec<&dyn RustTarget> = [&library as &dyn RustTarget]
            .into_iter()
            .chain(build_script.iter().map(|x| x as &dyn RustTarget))
            .chain(integration_tests.iter().map(|x| x as &dyn RustTarget))
            .chain(binaries.iter().map(|x| x as &dyn RustTarget))
            .chain(additive_content.as_ref().map(|x| x as &dyn RustTarget))
            .collect();

        let bazel_file = BazelBuildFile {
            header: BazelHeader::generate(&targets[..]),
            targets,
        };

        // Useful when iterating.
        // println!("{bazel_file}");

        let crate_path = package
            .manifest_path()
            .parent()
            .ok_or_else(|| anyhow::anyhow!("Should have at least a Cargo.toml component"))?;
        let build_path = crate_path.join("BUILD.bazel");
        tracing::debug!(?crate_path, "Writing BUILD.bazel file");

        let mut build_file = std::fs::File::options()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&build_path)?;

        let contents = bazel_file.to_string();
        build_file.write_all(contents.as_bytes())?;
        build_file.flush()?;
    }

    Ok(())
}
