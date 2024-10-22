// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use cargo_gazelle::args::Args;
use cargo_gazelle::config::{CrateConfig, GlobalConfig};
use cargo_gazelle::context::CrateContext;
use cargo_gazelle::header::BazelHeader;
use cargo_gazelle::targets::{CargoBuildScript, RustBinary, RustLibrary, RustTarget, RustTest};
use cargo_gazelle::BazelBuildFile;
use cargo_toml::Manifest;
use clap::Parser;
use guppy::graph::{BuildTargetId, PackageMetadata};
use md5::{Digest, Md5};
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let args = Args::try_parse()?;
    tracing::debug!(?args, "Running with Args");
    let path = args.path;

    if args.formatter.is_none() {
        tracing::warn!("Skipping formatting of BUILD.bazel files");
    }
    if args.check {
        tracing::warn!("Running in 'check' mode, won't generate any updates.");
    }

    let mut command = guppy::MetadataCommand::new();
    command.manifest_path(&path);

    // Note: In the past we've seen the way metadata gets generated to change between Cargo
    // versions which introduces skew with how BUILD.bazel files are generated.
    if let Some(cargo_binary) = &args.cargo {
        command.cargo_path(cargo_binary);
    }

    let graph = command.build_graph().context("building crate graph")?;
    let manifest = Manifest::from_path(&path).context("reading manifest")?;

    // Generate for either a single package, or an entire workspace.
    let packages: Box<dyn Iterator<Item = PackageMetadata>> = match manifest.package {
        Some(package) => {
            let package_metadata = graph.workspace().member_by_name(package.name)?;
            Box::new(std::iter::once(package_metadata))
        }
        None => Box::new(graph.workspace().iter()),
    };

    let config = Arc::new(GlobalConfig::default());

    std::thread::scope(|s| {
        let mut handles = Vec::new();

        // Process all of the build files in parallel.
        for package in packages {
            let config = Arc::clone(&config);
            let formatter = args.formatter.clone();

            let handle = s.spawn(move || {
                let Some(bazel_build) = generage_build_bazel(&config, &package)? else {
                    return Ok::<_, anyhow::Error>(None);
                };
                let bazel_build_str = bazel_build.to_string();

                // Useful when iterating.
                // println!("{bazel_build}");

                // Place the BUILD.bazel file next to the Cargo.toml file.
                let crate_path = package.manifest_path().parent().ok_or_else(|| {
                    anyhow::anyhow!("Should have at least a Cargo.toml component")
                })?;
                let build_path = crate_path.join("BUILD.bazel").into_std_path_buf();

                // Write to a temp file that we'll swap into place.
                let mut temp_file = tempfile::NamedTempFile::new().context("creating tempfile")?;
                tracing::debug!(?temp_file, "Writing BUILD.bazel file");
                temp_file
                    .write_all(bazel_build_str.as_bytes())
                    .context("writing temp file")?;
                temp_file.flush().context("flushing temp file")?;

                // Format the generated build file, if a formatter is provided.
                if let Some(formatter_exec) = &formatter {
                    let result = std::process::Command::new(formatter_exec)
                        .args(["-type", "build"])
                        .arg(temp_file.path())
                        .output()
                        .context("executing formatter")?;
                    if !result.status.success() {
                        let msg = String::from_utf8_lossy(&result.stderr[..]);
                        anyhow::bail!("failed to format {build_path:?}, err: {msg}");
                    }
                } else {
                    tracing::debug!(?crate_path, "skipping formatting");
                }

                let temp_file_hash = hash_file(temp_file.path()).context("hash temp file")?;
                let existing_hash = hash_file(&build_path).context("hashing existing file")?;

                // If the file didn't change then there's no reason to swap it in.
                if temp_file_hash == existing_hash {
                    tracing::debug!(?build_path, "didn't change, skipping");
                    let _ = temp_file.close();
                    Ok(None)
                } else {
                    Ok(Some((temp_file, build_path)))
                }
            });
            handles.push(handle);
        }

        // Collect all of the results, bailing if any fail.
        let results: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().expect("failed to join!"))
            .collect::<Result<_, anyhow::Error>>()
            .context("genrating a BUILD.bazel file")?;

        let updates: Vec<_> = results.into_iter().filter_map(|x| x).collect();

        if args.check && !updates.is_empty() {
            // If we're in 'check' mode and have changes, then report an error.
            let mut changes = Vec::new();
            for (temp_file, dst_path) in updates {
                let _ = temp_file.close();
                changes.push(dst_path);
            }
            Err(anyhow::anyhow!(
                "Generated files would have changed:\n{changes:?}"
            ))
        } else {
            // If everything succeeded then swap all of our files into their final path.
            for (temp_file, dst_path) in updates {
                // Note: Moving this file into place isn't atomic because some
                // of our CI jobs run across multiple volumes where atomic
                // moves are not possible.
                let (_file, temp_path) = temp_file.keep().context("keeping temp file")?;
                std::fs::copy(temp_path, dst_path).context("copying over temp file")?;
            }
            Ok::<_, anyhow::Error>(())
        }
    })?;

    Ok(())
}

/// Given the [`PackageMetadata`] for a single crate, generates a `BUILD.bazel` file.
fn generage_build_bazel<'a>(
    config: &'a GlobalConfig,
    package: &'a PackageMetadata<'a>,
) -> Result<Option<BazelBuildFile>, anyhow::Error> {
    let crate_config = CrateConfig::new(package);
    tracing::debug!(?crate_config, "found config");
    if crate_config.skip_generating() {
        tracing::info!(path = ?package.manifest_path(), "skipping, because crate config");
        return Ok(None);
    }

    let additive_content = crate_config.additive_content();

    tracing::info!(path = ?package.manifest_path(), "generating");

    let error_context = format!("generating {}", package.name());
    let crate_context =
        CrateContext::generate(config, &crate_config, package).context(error_context)?;

    let build_script = CargoBuildScript::generate(config, &crate_context, &crate_config, package)?;
    let library = RustLibrary::generate(config, package, &crate_config, build_script.as_ref())?;

    let integration_tests: Vec<_> = package
        .build_targets()
        .filter(|target| matches!(target.id(), BuildTargetId::Test(_)))
        .map(|target| RustTest::integration(config, package, &crate_config, &target))
        .collect::<Result<_, _>>()?;

    let binaries: Vec<_> = package
        .build_targets()
        .filter(|target| matches!(target.id(), BuildTargetId::Binary(_)))
        .map(|target| RustBinary::generate(config, package, &crate_config, &target))
        .collect::<Result<_, _>>()?;

    #[allow(clippy::as_conversions)]
    let targets: Vec<Box<dyn RustTarget>> = [Box::new(library) as Box<dyn RustTarget>]
        .into_iter()
        .chain(
            build_script
                .into_iter()
                .map(|t| Box::new(t) as Box<dyn RustTarget>),
        )
        .chain(
            integration_tests
                .into_iter()
                .map(|t| Box::new(t) as Box<dyn RustTarget>),
        )
        .chain(
            binaries
                .into_iter()
                .map(|t| Box::new(t) as Box<dyn RustTarget>),
        )
        .chain(additive_content.map(|t| Box::new(t) as Box<dyn RustTarget>))
        .collect();

    Ok(Some(BazelBuildFile {
        header: BazelHeader::generate(&targets[..]),
        targets,
    }))
}

/// Returns an [`md5`] hash of a file, returning `None` if the specified `path` doesn't exist.
fn hash_file(file: &Path) -> Result<Option<Vec<u8>>, anyhow::Error> {
    if !file.exists() {
        return Ok(None);
    }

    let file = std::fs::File::open(file).context("openning file")?;
    let mut reader = std::io::BufReader::new(file);
    let mut contents = Vec::new();
    reader.read_to_end(&mut contents).context("reading file")?;

    let mut file_hasher = Md5::new();
    file_hasher.update(&contents[..]);
    let file_hash = file_hasher.finalize();

    Ok(Some(file_hash.to_vec()))
}
