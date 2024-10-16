// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::Context;
use cargo_toml::Manifest;

use cargo_gazelle::config::{CrateConfig, GlobalConfig};
use cargo_gazelle::context::CrateContext;
use cargo_gazelle::header::BazelHeader;
use cargo_gazelle::targets::{CargoBuildScript, RustLibrary, RustTarget};
use cargo_gazelle::BazelBuildFile;

pub fn main() -> Result<(), anyhow::Error> {
    let path = "../../../src/proto/Cargo.toml";

    let graph = guppy::MetadataCommand::new()
        .manifest_path(path)
        .build_graph()
        .context("building crate graph")?;
    let manifest = Manifest::from_path(path).context("reading manifest")?;

    let package = graph
        .workspace()
        .member_by_name(manifest.package().name())?;
    let config = GlobalConfig::default();

    let crate_config = CrateConfig::new(&package);
    let crate_context = CrateContext::generate(&config, &crate_config, &package)?;

    let build_script =
        CargoBuildScript::generate(&config, &crate_context, &crate_config, &package)?.unwrap();
    let library = RustLibrary::generate(&config, &package, &crate_config, Some(&build_script))?;

    #[allow(clippy::as_conversions)]
    let targets = vec![
        Box::new(library) as Box<dyn RustTarget>,
        Box::new(build_script),
    ];
    let bazel_file = BazelBuildFile {
        header: BazelHeader::generate(&targets[..]),
        targets,
    };

    println!("{bazel_file}");

    Ok(())
}
