// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

use std::collections::BTreeMap;
use std::io::{BufReader, Write};
use std::{env, fs};

use anyhow::Context;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

/// The path of a protobuf file and its [`md5`] hash.
///
/// We store a hash of all the files to make sure they don't accidentally change, which would
/// invalidate our snapshotted types, and could silently introduce bugs.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
struct ProtoHash {
    name: String,
    md5: String,
}

const PROTO_DIRECTORY: &str = "protos";
const PROTO_HASHES: &str = "protos/hashes.json";

fn main() -> anyhow::Result<()> {
    env::set_var("PROTOC", protobuf_src::protoc());

    // Read in the persisted hashes from disk.
    let hashes = fs::File::open(PROTO_HASHES).context("opening proto hashes")?;
    let reader = BufReader::new(&hashes);
    let hashes: Vec<ProtoHash> = serde_json::from_reader(reader)?;
    let mut persisted: BTreeMap<String, String> =
        hashes.into_iter().map(|e| (e.name, e.md5)).collect();

    let package_regex = regex::bytes::Regex::new("package objects(_v[\\d]+)?;").unwrap();
    // Discover all of the protobuf files on disk.
    let protos: Result<BTreeMap<String, (String, Vec<u8>)>, anyhow::Error> =
        fs::read_dir(PROTO_DIRECTORY)?
            // If we fail to read one file, fail everything.
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            // Filter to only files with the .proto extension.
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .map(|e| e.to_string_lossy().contains("proto"))
                    .unwrap_or(false)
            })
            .map(|file| {
                let path = file.path();

                // Hash the entire file.
                let mut hasher = Md5::new();
                let buffer = std::fs::read(&path).expect("To be able to read file");
                let found_package_directive = package_regex
                    .find(&buffer)
                    .with_context(|| {
                        format!(
                            "couldn't find `package objects_v<version>;` in {}",
                            path.display()
                        )
                    })?
                    .as_bytes()
                    .to_vec();
                let buffer = package_regex.replace(&buffer, b"package objects;".as_slice());
                hasher.update(buffer);

                let name = path
                    .file_name()
                    .expect("To have file name")
                    .to_str()
                    .expect("UTF-8")
                    .to_string();
                let hash = format!("{:x}", hasher.finalize());

                Ok((name, (hash, found_package_directive)))
            })
            .collect();

    let protos = protos?;

    // After validating our hashes we'll re-write the file if any new protos
    // have been added.
    let mut to_persist: Vec<ProtoHash> = Vec::new();
    let mut any_new = false;

    // Check the persisted hashes against what we just read in from disk.
    for (name, (hash, package_directive)) in protos {
        if package_directive
            != format!("package {};", name.strip_suffix(".proto").unwrap()).into_bytes()
        {
            anyhow::bail!("{name} must package directive with same name as the file");
        }
        match persisted.remove(&name) {
            // Hashes have changed!
            Some(og_hash) if hash != og_hash => {
                anyhow::bail!(error_message(og_hash, hash, name));
            }
            // Found a proto file on disk that we didn't have persisted, we'll just persist it.
            None => {
                to_persist.push(ProtoHash { name, md5: hash });
                any_new = true;
            }
            // We match!
            Some(_) => to_persist.push(ProtoHash { name, md5: hash }),
        }
    }

    // Put `objects.proto` at the end, always
    let mut sorted_to_persist = to_persist.clone();
    sorted_to_persist.sort_by(|a, b| {
        use std::cmp::Ordering::*;
        if a.name == "objects.proto" && b.name == "objects.proto" {
            Equal
        } else if a.name == "objects.proto" {
            Greater
        } else if b.name == "objects.proto" {
            Less
        } else {
            a.name.cmp(&b.name)
        }
    });

    let objects_hash = sorted_to_persist
        .iter()
        .find(|o| o.name == "objects.proto")
        .ok_or(anyhow::anyhow!("no hash for objects.proto file"))?;

    if let Some(latest_version) = sorted_to_persist
        .iter()
        .rev()
        .filter(|o| o.name != "objects.proto")
        .next()
    {
        if objects_hash.md5 != latest_version.md5 {
            anyhow::bail!(
                "objects.proto hash ({}) != the latest file's hash ({})",
                objects_hash.md5,
                latest_version.md5
            );
        }
    }

    // Check if there are any proto files we should have had hashes for, but didn't exist.
    if !persisted.is_empty() {
        anyhow::bail!("Have persisted hashes, but no files on disk? {persisted:#?}");
    }

    // Write the hashes back out to disk if and only if there are new protos. We
    // don't do this unconditionally or we'll get stuck in a rebuild loop:
    // executing this build script will change the mtime on the hashes file,
    // which will force the next compile to rebuild the crate, even if nothing
    // else has changed.
    //
    // We also rewrite the file if it has been put out of order.
    if any_new || sorted_to_persist != to_persist {
        let mut file = fs::File::options()
            .write(true)
            .truncate(true)
            .open(PROTO_HASHES)
            .context("opening hashes file to write")?;
        serde_json::to_writer_pretty(&mut file, &sorted_to_persist).context("persisting hashes")?;
        write!(&mut file, "\n").context("writing newline")?;
    }

    // Generate protos!
    let paths: Vec<_> = to_persist
        .iter()
        .map(|entry| format!("protos/{}", entry.name))
        .collect();

    const ATTR: &str = "#[derive(Eq, PartialOrd, Ord, ::serde::Serialize, ::serde::Deserialize)]";
    const ARBITRARY_ATTR: &str = "#[derive(::proptest_derive::Arbitrary)]";

    // 'as' is okay here because we're using it to define the type of the empty slice, which is
    // necessary since the method takes the slice as a generic arg.
    #[allow(clippy::as_conversions)]
    prost_build::Config::new()
        .btree_map(["."])
        .bytes(["."])
        .message_attribute(".", ATTR)
        // Note(parkmycar): This is annoying, but we need to manually specify each oneof so we can
        // get them to implement Eq, PartialEq, and Ord. If you define a new oneof you should add
        // it here.
        .enum_attribute("CatalogItem.value", ATTR)
        .enum_attribute("ClusterConfig.variant", ATTR)
        .enum_attribute("GlobalId.value", ATTR)
        .enum_attribute("ClusterId.value", ATTR)
        .enum_attribute("DatabaseId.value", ATTR)
        .enum_attribute("SchemaId.value", ATTR)
        .enum_attribute("RoleId.value", ATTR)
        .enum_attribute("ReplicaConfig.location", ATTR)
        .enum_attribute("AuditLogEventV1.details", ATTR)
        .enum_attribute("AuditLogKey.event", ATTR)
        .enum_attribute("StorageUsageKey.usage", ATTR)
        .enum_attribute("ResolvedDatabaseSpecifier.value", ATTR)
        // We derive Arbitrary for all protobuf types for wire compatibility testing.
        .message_attribute(".", ARBITRARY_ATTR)
        .enum_attribute(".", ARBITRARY_ATTR)
        .compile_protos(
            &paths,
            &[ /*
                  This is purposefully empty, and we should never
                  add any includes because we don't want to allow
                  our protos to have dependencies. This allows us
                  to ensure our snapshots can't silently change.
                */
            ] as &[&str],
        )?;

    Ok(())
}

/// A (hopefully) helpful error message that describes what to do when the hashes differ.
fn error_message(og_hash: String, hash: String, filename: String) -> String {
    let title = "Hashes changed for the persisted protobuf files!";
    let body1 = format!("If you changed '{filename}' without first making a snapshot, then you need to copy '{filename}' and rename it with a suffix like '_vX.proto'.");
    let body2 = format!(
        "Otherwise you can update the hash for '{filename}' in '{PROTO_HASHES}' to be '{hash}'."
    );
    let hashes = format!("persisted_hash({og_hash}) != current_hash({hash})\nFile: {filename}");

    format!("{title}\n\n{body1}\n{body2}\n\n{hashes}")
}
