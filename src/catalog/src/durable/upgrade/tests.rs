// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use once_cell::sync::Lazy;
use std::collections::BTreeSet;
use std::fs;
use std::io::Write;

use mz_persist_types::Codec;
use mz_storage_types::sources::SourceData;

use crate::durable::impls::persist::state_update::StateUpdateKindRaw;
use crate::durable::upgrade::AllVersionsStateUpdateKind;

static PROTO_DIRECTORY: Lazy<String> =
    Lazy::new(|| format!("{}/protos", env!("CARGO_MANIFEST_DIR")));
const PROTO_EXT: &str = "proto";

static SNAPSHOT_DIRECTORY: Lazy<String> = Lazy::new(|| {
    format!(
        "{}/src/durable/upgrade/snapshots",
        env!("CARGO_MANIFEST_DIR")
    )
});
const SNAPSHOT_EXT: &str = "txt";

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // too slow
fn test_proto_serialization_stability() {
    let protos: BTreeSet<_> = read_file_names(&PROTO_DIRECTORY, PROTO_EXT)
        // Remove `objects.proto`.
        //
        // `objects.proto` is allowed to change and we don't have a good
        // mechanism to force people to update `objects.txt` any time `objects.proto` changes. To
        // avoid this rot we just don't test the contents of `objects.proto`. Additionally,
        // `objects.proto` will always be identical to the most recent `objects_vX.proto`.
        .filter(|name| name != "objects")
        .collect();

    let snapshot_files: BTreeSet<_> = read_file_names(&SNAPSHOT_DIRECTORY, SNAPSHOT_EXT).collect();

    let unknown_snapshots: Vec<_> = snapshot_files.difference(&protos).collect();
    if !unknown_snapshots.is_empty() {
        panic!("Have snapshots, but no proto files on disk? If a .proto file was deleted, then the .txt snapshot file must be deleted too. {unknown_snapshots:#?}");
    }

    let unencoded_protos: Vec<_> = protos.difference(&snapshot_files).collect();
    if !unencoded_protos.is_empty() {
        panic!("Missing encodings for some proto objects, try generating them with `generate_missing_encodings`. {unencoded_protos:#?}");
    }

    let base64_config = base64::Config::new(base64::CharacterSet::Standard, true);
    for snapshot_file in snapshot_files {
        let encoded_bytes = fs::read(format!("{}/{}.txt", *SNAPSHOT_DIRECTORY, snapshot_file))
            .expect("unable to read encoded file");
        let encoded_str = std::str::from_utf8(encoded_bytes.as_slice()).expect("valid UTF-8");
        let decoded = encoded_str
            .lines()
            .map(|s| base64::decode_config(s, base64_config).expect("valid base64"))
            .map(|b| SourceData::decode(&b).expect("valid proto"))
            .map(StateUpdateKindRaw::from)
            .map(|raw| {
                AllVersionsStateUpdateKind::try_from_raw(&snapshot_file, raw)
                    .expect("valid version and raw")
            })
            .map(|kind| kind.raw())
            .map(SourceData::from);

        // Reencode and compare the strings.
        let mut reencoded = String::new();
        let mut buf = vec![];
        for source_data in decoded {
            buf.clear();
            source_data.encode(&mut buf);
            base64::encode_config_buf(buf.as_slice(), base64_config, &mut reencoded);
            reencoded.push('\n');
        }

        // Consolidation in the catalog depends on stable serialization for SourceData.
        assert_eq!(
            encoded_str,
            reencoded.as_str(),
            "SourceData serde should be stable"
        )
    }
}

#[mz_ore::test]
#[cfg_attr(miri, ignore)] // not an actual test
#[ignore]
/// This is not a real test, it is a helper to generate encoded catalog objects
/// for other tests. When you want to generate new encodings, then run:
///
/// ```ignore
/// cargo test --package mz-catalog --lib durable::upgrade::tests::generate_missing_encodings -- --ignored
/// ```
fn generate_missing_encodings() {
    let protos: BTreeSet<_> = read_file_names(&PROTO_DIRECTORY, PROTO_EXT)
        .filter(|name| name != "objects")
        .collect();

    let snapshots: BTreeSet<_> = read_file_names(&SNAPSHOT_DIRECTORY, SNAPSHOT_EXT).collect();

    let unknown_snapshots: Vec<_> = snapshots.difference(&protos).collect();
    if !unknown_snapshots.is_empty() {
        panic!("Have snapshots, but no proto files on disk? {unknown_snapshots:#?}");
    }

    let base64_config = base64::Config::new(base64::CharacterSet::Standard, true);

    for to_encode in protos.difference(&snapshots) {
        let mut file = fs::File::options()
            .create_new(true)
            .write(true)
            .open(format!("{}/{}.txt", *SNAPSHOT_DIRECTORY, to_encode))
            .expect("file exists");
        let encoded_datas = AllVersionsStateUpdateKind::arbitrary_vec(to_encode)
            .expect("valid version")
            .into_iter()
            .map(|kind| kind.raw())
            .map(SourceData::from)
            .map(|source_data| {
                let mut buf = Vec::new();
                source_data.encode(&mut buf);
                buf
            })
            .map(|buf| {
                let mut encoded = String::new();
                base64::encode_config_buf(buf.as_slice(), base64_config, &mut encoded);
                encoded
            });
        for encoded_data in encoded_datas {
            write!(&mut file, "{encoded_data}\n").expect("unable to write file");
        }
    }
}

fn read_file_names<'a>(dir: &'a str, ext: &'a str) -> impl Iterator<Item = String> + 'a {
    fs::read_dir(dir)
        .expect("valid directory")
        // If we fail to read one file, fail everything.
        .collect::<Result<Vec<_>, _>>()
        .expect("unable to read directory")
        .into_iter()
        // Filter to only files with the .`ext` extension.
        .filter(move |entry| {
            entry
                .path()
                .extension()
                .map(|e| e.to_string_lossy().contains(ext))
                .unwrap_or(false)
        })
        // Remove file extension.
        .map(|entry| {
            entry
                .path()
                .file_stem()
                .expect("no filename")
                .to_str()
                .expect("UTF-8")
                .to_string()
        })
}
