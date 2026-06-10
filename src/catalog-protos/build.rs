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
use std::io::Write;
use std::path::PathBuf;

use anyhow::Context;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};

/// The path of an object definition file and its [`md5`] hash.
///
/// We store a hash of all the files to make sure they don't accidentally change, which would
/// invalidate our snapshotted types, and could silently introduce bugs.
#[derive(Debug, Clone, Deserialize, Serialize)]
struct ObjectsHash {
    name: String,
    md5: String,
}

const OBJECTS_HASHES: &str = "objects_hashes.json";

fn main() -> anyhow::Result<()> {
    let crate_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    // Read in the persisted hashes from disk.
    let hashes_path = crate_root.join(OBJECTS_HASHES);
    let hashes_json = fs::read_to_string(&hashes_path)?;
    let hashes: Vec<ObjectsHash> = serde_json::from_str(&hashes_json)?;
    let mut persisted: BTreeMap<String, String> =
        hashes.into_iter().map(|e| (e.name, e.md5)).collect();

    // Discover all of the object definition files on disk.
    let src_dir = crate_root.join("src");
    let objects: BTreeMap<String, String> = fs::read_dir(src_dir)?
        // If we fail to read one file, fail everything.
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        // Filter to only files with the of the form `objects*.rs`.
        .filter(|entry| {
            let name = entry.file_name();
            let s = name.to_string_lossy();
            s.starts_with("objects") && s.ends_with(".rs")
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

    // After validating our hashes we'll re-write the file if any new object definitions
    // have been added.
    let mut to_persist: Vec<ObjectsHash> = Vec::new();
    let mut any_new = false;

    // Check the persisted hashes against what we just read in from disk.
    for (name, hash) in objects {
        match persisted.remove(&name) {
            // Hashes have changed!
            Some(og_hash) if hash != og_hash => {
                anyhow::bail!(error_message(og_hash, hash, name));
            }
            // Found an objects file on disk that we didn't have persisted, we'll just persist it.
            None => {
                to_persist.push(ObjectsHash { name, md5: hash });
                any_new = true;
            }
            // We match!
            Some(_) => to_persist.push(ObjectsHash { name, md5: hash }),
        }
    }

    // Check if there are any objects files we should have had hashes for, but didn't exist.
    if !persisted.is_empty() {
        anyhow::bail!("Have persisted hashes, but no files on disk? {persisted:#?}");
    }

    // Write the hashes back out to disk if and only if there are new object definitions. We
    // don't do this unconditionally or we'll get stuck in a rebuild loop:
    // executing this build script will change the mtime on the hashes file,
    // which will force the next compile to rebuild the crate, even if nothing
    // else has changed.
    if any_new {
        let mut file = fs::File::options()
            .write(true)
            .truncate(true)
            .open(hashes_path)
            .context("opening hashes file to write")?;
        serde_json::to_writer_pretty(&mut file, &to_persist).context("persisting hashes")?;
        write!(&mut file, "\n").context("writing newline")?;
    }

    Ok(())
}

/// A (hopefully) helpful error message that describes what to do when the hashes differ.
fn error_message(og_hash: String, hash: String, filename: String) -> String {
    let title = "Hashes changed for the persisted object definition files!";
    let body1 = format!(
        "If you changed '{filename}' without first making a snapshot, then you need to copy '{filename}' and rename it with a suffix like '_vX.rs'."
    );
    let body2 = format!(
        "Otherwise you can update the hash for '{filename}' in '{OBJECTS_HASHES}' to be '{hash}'."
    );
    let hashes = format!("persisted_hash({og_hash}) != current_hash({hash})\nFile: {filename}");

    format!("{title}\n\n{body1}\n{body2}\n\n{hashes}")
}
