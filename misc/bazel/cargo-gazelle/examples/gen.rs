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

use cargo_gazelle::context::CrateContext;
use cargo_gazelle::header::BazelHeader;
use cargo_gazelle::targets::{CargoBuildScript, RustLibrary, RustTarget};
use cargo_gazelle::{BazelBuildFile, Config};

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
    let config = Config::default();

    let crate_context = CrateContext::generate(&config, &package)?;

    let build_script = CargoBuildScript::generate(&config, &crate_context, &package)?.unwrap();
    let library = RustLibrary::generate(&config, &package, Some(&build_script))?;

    #[allow(clippy::as_conversions)]
    let targets = vec![&library as &dyn RustTarget, &build_script];
    let bazel_file = BazelBuildFile {
        header: BazelHeader::generate(&targets[..]),
        targets,
    };

    println!("{bazel_file}");

    Ok(())
}
