// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufReader, Write};

use anyhow::Context;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

/// The path of a protobuf file and its [`md5`] hash.
///
/// We store a hash of all the files to make sure they don't accidentally change, which would
/// invalidate our snapshotted types, and could silently introduce bugs.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct ProtoHash {
    name: String,
    md5: String,
}

const PROTO_DIRECTORY: &str = "protos";
const PROTO_HASHES: &str = "protos/hashes.json";

fn main() -> anyhow::Result<()> {
    println!("cargo:rerun-if-changed={PROTO_DIRECTORY}");

    // Read in the persisted hashes from disk.
    let hashes = fs::File::open(PROTO_HASHES).context("opening proto hashes")?;
    let reader = BufReader::new(&hashes);
    let hashes: Vec<ProtoHash> = serde_json::from_reader(reader)?;
    let mut persisted: BTreeMap<String, String> =
        hashes.into_iter().map(|e| (e.name, e.md5)).collect();

    // Discover all of the protobuf files on disk.
    let protos: BTreeMap<String, String> = fs::read_dir(PROTO_DIRECTORY)?
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
            hasher.update(buffer);

            let name = path
                .file_name()
                .expect("To have file name")
                .to_str()
                .expect("UTF-8")
                .to_string();
            let hash = format!("{:x}", hasher.finalize());

            (name, hash)
        })
        .collect();

    // After validating our hashes we'll re-write the file if any new protos
    // have been added.
    let mut to_persist: Vec<ProtoHash> = Vec::new();
    let mut any_new = false;

    // Check the persisted hashes against what we just read in from disk.
    for (name, hash) in protos {
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

    // Check if there are any proto files we should have had hashes for, but didn't exist.
    if !persisted.is_empty() {
        anyhow::bail!("Have persisted hashes, but no files on disk? {persisted:#?}");
    }

    // Write the hashes back out to disk if and only if there are new protos. We
    // don't do this unconditionally or we'll get stuck in a rebuild loop:
    // executing this build script will change the mtime on the hashes file,
    // which will force the next compile to rebuild the crate, even if nothing
    // else has changed.
    if any_new {
        let mut file = fs::File::options()
            .write(true)
            .truncate(true)
            .open(PROTO_HASHES)
            .context("opening hashes file to write")?;
        serde_json::to_writer_pretty(&mut file, &to_persist).context("persisting hashes")?;
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
    // DO NOT change how JSON serialization works for these objects. The catalog relies on the JSON
    // serialization of these objects remaining stable for a specific objects_vX version. If you
    // want to change the JSON serialization format then follow these steps:
    //
    //   1. Create a new version of the `objects.proto` file.
    //   2. Update the path of .proto files given to this compile block so that it is only the
    //      previous .proto files.
    //   3. Add a new `prost_build::Config::new()...compile_protos(...)` block that only compiles
    //      the new and all future .proto files with the changed JSON serialization.
    //
    // Once we delete all the `.proto` that use the old JSON serialization, then we can delete
    // the compile block for them as well.
    prost_build::Config::new()
        .protoc_executable(mz_build_tools::protoc())
        .btree_map(["."])
        .bytes(["."])
        .message_attribute(".", ATTR)
        // Note(parkmycar): This is annoying, but we need to manually specify each oneof so we can
        // get them to implement Eq, PartialEq, and Ord. If you define a new oneof you should add
        // it here.
        .enum_attribute("CatalogItem.value", ATTR)
        .enum_attribute("ClusterConfig.variant", ATTR)
        .enum_attribute("GlobalId.value", ATTR)
        .enum_attribute("CatalogItemId.value", ATTR)
        .enum_attribute("ClusterId.value", ATTR)
        .enum_attribute("DatabaseId.value", ATTR)
        .enum_attribute("SchemaId.value", ATTR)
        .enum_attribute("ReplicaId.value", ATTR)
        .enum_attribute("RoleId.value", ATTR)
        .enum_attribute("NetworkPolicyId.value", ATTR)
        .enum_attribute("NetworkPolicyRule.action", ATTR)
        .enum_attribute("NetworkPolicyRule.direction", ATTR)
        .enum_attribute("ReplicaConfig.location", ATTR)
        .enum_attribute("AuditLogEventV1.details", ATTR)
        .enum_attribute("AuditLogKey.event", ATTR)
        .enum_attribute("StorageUsageKey.usage", ATTR)
        .enum_attribute("ResolvedDatabaseSpecifier.value", ATTR)
        .enum_attribute("CommentKey.object", ATTR)
        .enum_attribute("CommentKey.sub_component", ATTR)
        .enum_attribute("ResolvedDatabaseSpecifier.spec", ATTR)
        .enum_attribute("SchemaSpecifier.spec", ATTR)
        .enum_attribute("RoleVars.Entry.val", ATTR)
        .enum_attribute("StateUpdateKind.kind", ATTR)
        .enum_attribute("ClusterScheduleOptionValue.value", ATTR)
        .enum_attribute("ClusterSchedule.value", ATTR)
        .enum_attribute("CreateOrDropClusterReplicaReasonV1.reason", ATTR)
        .enum_attribute("RefreshDecisionWithReasonV1.decision", ATTR)
        .enum_attribute("RefreshDecisionWithReasonV2.decision", ATTR)
        // Serialize/deserialize the top-level enum in the persist-backed
        // catalog as "internally tagged"[^1] to set up persist pushdown
        // statistics for success.
        //
        // [^1]: https://serde.rs/enum-representations.html#internally-tagged
        .enum_attribute("StateUpdateKind.kind", "#[serde(tag = \"kind\")]")
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
