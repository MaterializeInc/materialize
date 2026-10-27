// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Filesystem abstraction with optional in-memory overlays.
//!
//! The overlay only intercepts content reads. Directory walks, file
//! existence checks, and sibling metadata are still served from disk.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

/// Read-through filesystem with an optional in-memory overlay.
pub(crate) struct FileSystem {
    overlay: BTreeMap<PathBuf, String>,
}

impl FileSystem {
    /// Construct a filesystem with no overlay; reads always go to disk.
    pub(crate) fn new() -> Self {
        Self {
            overlay: BTreeMap::new(),
        }
    }

    /// Construct a filesystem with the given overlay; a read for a path
    /// present in `overlay` returns the overlay bytes, otherwise it falls
    /// back to disk.
    pub(crate) fn with_overlay(overlay: BTreeMap<PathBuf, String>) -> Self {
        Self { overlay }
    }

    /// Construct a filesystem from a JSON file mapping absolute paths to
    /// their contents (`{ "/abs/path": "contents", ... }`). Used by the
    /// `--overlay` flag on `test` and `explain` so the VSCode extension can
    /// surface unsaved buffers without writing them to disk.
    pub(crate) fn from_overlay_file(path: &Path) -> io::Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        let map: BTreeMap<PathBuf, String> = serde_json::from_str(&raw)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Self::with_overlay(map))
    }

    /// Read the file at `path`, consulting the overlay first.
    pub(crate) fn read_to_string(&self, path: &Path) -> io::Result<String> {
        if let Some(text) = self.overlay.get(path) {
            return Ok(text.clone());
        }
        std::fs::read_to_string(path)
    }

    /// Whether `path` is covered by an overlay entry. Used by callers that
    /// maintain disk-keyed caches: when a path is overlay-covered, the
    /// disk-derived cache key is meaningless and the cache must be
    /// bypassed.
    pub(crate) fn is_overlay(&self, path: &Path) -> bool {
        self.overlay.contains_key(path)
    }
}

impl Default for FileSystem {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn overlay_intercepts_read() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("a.sql");
        std::fs::write(&file, "disk").unwrap();

        let mut overlay = BTreeMap::new();
        overlay.insert(file.clone(), "buffer".to_string());
        let fs = FileSystem::with_overlay(overlay);

        assert_eq!(fs.read_to_string(&file).unwrap(), "buffer");
        assert!(fs.is_overlay(&file));
    }

    #[mz_ore::test]
    fn no_overlay_reads_disk() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("a.sql");
        std::fs::write(&file, "disk").unwrap();
        let fs = FileSystem::new();
        assert_eq!(fs.read_to_string(&file).unwrap(), "disk");
        assert!(!fs.is_overlay(&file));
    }

    #[mz_ore::test]
    fn from_overlay_file_round_trips() {
        let tmp = tempfile::tempdir().unwrap();
        let target = tmp.path().join("models/x.sql");
        let overlay_json = tmp.path().join("overlay.json");

        // Write the JSON manually so the format we accept is locked in.
        let body = format!(
            "{{\"{}\":\"SELECT 42\"}}",
            target.display().to_string().replace('\\', "\\\\"),
        );
        std::fs::write(&overlay_json, body).unwrap();

        let fs = FileSystem::from_overlay_file(&overlay_json).unwrap();
        assert_eq!(fs.read_to_string(&target).unwrap(), "SELECT 42");
        assert!(fs.is_overlay(&target));
    }

    #[mz_ore::test]
    fn from_overlay_file_rejects_garbage() {
        let tmp = tempfile::tempdir().unwrap();
        let overlay_json = tmp.path().join("overlay.json");
        std::fs::write(&overlay_json, "not json").unwrap();

        let err = FileSystem::from_overlay_file(&overlay_json)
            .err()
            .expect("garbage JSON must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
