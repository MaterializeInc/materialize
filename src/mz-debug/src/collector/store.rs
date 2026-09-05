// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! The on-disk ring buffer of completed snapshots.
//!
//! Layout under the store directory:
//!
//! ```text
//! <id>.zip           the snapshot
//! <id>.meta.json     its [`SnapshotMeta`], written after the zip is in place
//! tmp/<id>/          the work directory of a snapshot being taken
//! ```
//!
//! A snapshot is visible only once both files exist, and the meta file is
//! written last, so readers never observe a half-written zip. `tmp/` holds
//! nothing worth keeping across a restart and is wiped on open.

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::collector::snapshot::SnapshotCategories;
use crate::utils::zip_debug_folder;

/// The prefix of the directory a snapshot's files are rooted at inside its
/// zip, so an extracted snapshot looks like a CLI run's output directory.
const SNAPSHOT_ROOT_PREFIX: &str = "mz_debug_";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotKind {
    /// Taken by the collector on its own schedule.
    Periodic,
    /// Requested through the HTTP API.
    OnDemand,
}

impl SnapshotKind {
    fn id_suffix(self) -> &'static str {
        match self {
            SnapshotKind::Periodic => "periodic",
            SnapshotKind::OnDemand => "on-demand",
        }
    }
}

/// What the store knows about a completed snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotMeta {
    pub id: String,
    pub kind: SnapshotKind,
    pub started_at: DateTime<Utc>,
    pub completed_at: DateTime<Utc>,
    /// Size of the zip on disk.
    pub size_bytes: u64,
    pub categories: SnapshotCategories,
}

pub struct SnapshotStore {
    dir: PathBuf,
    retained_snapshots: usize,
    size_limit_bytes: u64,
}

impl SnapshotStore {
    /// Opens the store at `dir`, creating it if needed, and discards any
    /// leftovers of snapshots that did not complete before the last shutdown.
    pub fn open(dir: PathBuf, retained_snapshots: usize, size_limit_bytes: u64) -> Result<Self> {
        let store = Self {
            dir,
            retained_snapshots: retained_snapshots.max(1),
            size_limit_bytes,
        };
        fs::create_dir_all(&store.dir)
            .with_context(|| format!("Failed to create {}", store.dir.display()))?;
        if store.tmp_dir().exists() {
            fs::remove_dir_all(store.tmp_dir())
                .with_context(|| format!("Failed to clear {}", store.tmp_dir().display()))?;
        }
        fs::create_dir_all(store.tmp_dir())?;
        store.remove_orphans()?;
        Ok(store)
    }

    fn tmp_dir(&self) -> PathBuf {
        self.dir.join("tmp")
    }

    pub fn zip_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.zip"))
    }

    fn meta_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}.meta.json"))
    }

    /// The directory a snapshot with `id` is assembled in. Its basename is the
    /// root directory inside the resulting zip.
    pub fn workdir(&self, id: &str) -> PathBuf {
        self.tmp_dir()
            .join(id)
            .join(format!("{SNAPSHOT_ROOT_PREFIX}{id}"))
    }

    /// Allocates an id for a snapshot triggered at `at`.
    ///
    /// Ids sort chronologically as strings and are safe as file names on every
    /// filesystem the collector may run on, hence no colons. The rare
    /// collision (two triggers of one kind within a second) is resolved with a
    /// numeric suffix.
    pub fn new_id(&self, at: DateTime<Utc>, kind: SnapshotKind) -> String {
        let base = format!("{}-{}", at.format("%Y-%m-%dT%H-%M-%SZ"), kind.id_suffix());
        let mut id = base.clone();
        let mut n = 1;
        while self.zip_path(&id).exists() || self.tmp_dir().join(&id).exists() {
            n += 1;
            id = format!("{base}-{n}");
        }
        id
    }

    /// Zips the work directory of `id` into the store, publishes its metadata,
    /// removes the work directory, and applies retention.
    pub fn commit(
        &self,
        id: &str,
        kind: SnapshotKind,
        started_at: DateTime<Utc>,
        categories: SnapshotCategories,
    ) -> Result<SnapshotMeta> {
        let workdir = self.workdir(id);
        // Zip into tmp/ and rename so a crash mid-zip never leaves a partial
        // zip where a reader could pick it up.
        let staging_zip = self.tmp_dir().join(format!("{id}.zip"));
        zip_debug_folder(staging_zip.clone(), &workdir)
            .with_context(|| format!("Failed to zip {}", workdir.display()))?;
        let zip_path = self.zip_path(id);
        fs::rename(&staging_zip, &zip_path)
            .with_context(|| format!("Failed to move zip into {}", zip_path.display()))?;

        let meta = SnapshotMeta {
            id: id.to_owned(),
            kind,
            started_at,
            completed_at: Utc::now(),
            size_bytes: fs::metadata(&zip_path)?.len(),
            categories,
        };
        write_atomically(&self.meta_path(id), &serde_json::to_vec_pretty(&meta)?)?;

        if let Err(e) = fs::remove_dir_all(self.tmp_dir().join(id)) {
            warn!("Failed to remove work directory of snapshot {}: {}", id, e);
        }
        info!("Stored snapshot {} ({} bytes)", meta.id, meta.size_bytes);

        self.apply_retention()?;
        Ok(meta)
    }

    /// Discards the work directory of a snapshot that will not be committed.
    pub fn abandon(&self, id: &str) {
        if let Err(e) = fs::remove_dir_all(self.tmp_dir().join(id)) {
            warn!("Failed to remove work directory of snapshot {}: {}", id, e);
        }
    }

    /// All completed snapshots, oldest first.
    pub fn list(&self) -> Result<Vec<SnapshotMeta>> {
        let mut metas = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(id) = name.strip_suffix(".meta.json") else {
                continue;
            };
            match fs::read(&path).and_then(|bytes| {
                serde_json::from_slice::<SnapshotMeta>(&bytes).map_err(std::io::Error::other)
            }) {
                Ok(meta) if meta.id == id => metas.push(meta),
                Ok(meta) => warn!(
                    "Ignoring {}: metadata names snapshot {} instead",
                    path.display(),
                    meta.id
                ),
                Err(e) => warn!("Ignoring unreadable {}: {}", path.display(), e),
            }
        }
        metas.sort_by(|a, b| a.started_at.cmp(&b.started_at).then(a.id.cmp(&b.id)));
        Ok(metas)
    }

    pub fn get(&self, id: &str) -> Result<Option<SnapshotMeta>> {
        Ok(self.list()?.into_iter().find(|meta| meta.id == id))
    }

    pub fn latest(&self) -> Result<Option<SnapshotMeta>> {
        Ok(self.list()?.pop())
    }

    /// Deletes the oldest snapshots until both the count and the total size
    /// are within bounds. The newest snapshot is always kept, even when it
    /// alone exceeds the size cap, since a buffer that holds nothing is
    /// useless.
    fn apply_retention(&self) -> Result<()> {
        let mut metas = self.list()?;
        let mut total: u64 = metas.iter().map(|meta| meta.size_bytes).sum();
        while metas.len() > 1
            && (metas.len() > self.retained_snapshots || total > self.size_limit_bytes)
        {
            let oldest = metas.remove(0);
            total -= oldest.size_bytes;
            info!("Evicting snapshot {} from the buffer", oldest.id);
            self.delete(&oldest.id);
        }
        Ok(())
    }

    fn delete(&self, id: &str) {
        // The meta file goes first so a crash in between leaves an orphaned
        // zip, which `remove_orphans` cleans up, rather than metadata that
        // points at nothing.
        for path in [self.meta_path(id), self.zip_path(id)] {
            if let Err(e) = fs::remove_file(&path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    warn!("Failed to remove {}: {}", path.display(), e);
                }
            }
        }
    }

    /// Removes zips without metadata and metadata without a zip.
    fn remove_orphans(&self) -> Result<()> {
        for entry in fs::read_dir(&self.dir)? {
            let path = entry?.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let partner = if let Some(id) = name.strip_suffix(".meta.json") {
                self.zip_path(id)
            } else if let Some(id) = name.strip_suffix(".zip") {
                self.meta_path(id)
            } else {
                continue;
            };
            if !partner.exists() {
                warn!("Removing orphaned {}", path.display());
                if let Err(e) = fs::remove_file(&path) {
                    warn!("Failed to remove {}: {}", path.display(), e);
                }
            }
        }
        Ok(())
    }
}

fn write_atomically(path: &Path, contents: &[u8]) -> Result<()> {
    let tmp = path.with_extension("json.tmp");
    fs::write(&tmp, contents).with_context(|| format!("Failed to write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("Failed to move into {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use chrono::TimeZone;

    use super::*;

    fn categories() -> SnapshotCategories {
        SnapshotCategories {
            k8s: true,
            system_catalog: false,
            heap_profiles: true,
            prometheus_metrics: true,
            cpu_profiles: false,
            cpu_profile_duration_seconds: 10,
        }
    }

    /// Takes a snapshot holding one file of `payload_len` bytes and commits it.
    fn snapshot(store: &SnapshotStore, at: DateTime<Utc>, payload_len: usize) -> SnapshotMeta {
        let id = store.new_id(at, SnapshotKind::Periodic);
        let workdir = store.workdir(&id);
        fs::create_dir_all(workdir.join("logs")).unwrap();
        // Pseudo-random, incompressible payload so the zip size tracks the
        // input size.
        let mut state: u32 = 0x9E37_79B9;
        let payload: Vec<u8> = (0..payload_len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                u8::try_from(state & 0xFF).expect("masked to a byte")
            })
            .collect();
        fs::write(workdir.join("logs").join("pod.log"), payload).unwrap();
        store
            .commit(&id, SnapshotKind::Periodic, at, categories())
            .unwrap()
    }

    fn at(secs: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, secs).unwrap()
    }

    #[mz_ore::test]
    fn commit_publishes_zip_rooted_like_a_cli_run() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path().to_owned(), 5, u64::MAX).unwrap();
        let meta = snapshot(&store, at(0), 100);

        assert_eq!(meta.id, "2026-01-02T03-04-00Z-periodic");
        assert_eq!(store.list().unwrap(), vec![meta.clone()]);
        assert!(
            !store.workdir(&meta.id).exists(),
            "work directory is removed"
        );

        let file = fs::File::open(store.zip_path(&meta.id)).unwrap();
        let mut zip = zip::ZipArchive::new(file).unwrap();
        let names: Vec<String> = (0..zip.len())
            .map(|i| zip.by_index(i).unwrap().name().to_owned())
            .collect();
        assert_eq!(
            names,
            vec!["mz_debug_2026-01-02T03-04-00Z-periodic/logs/pod.log"]
        );
        let mut contents = Vec::new();
        zip.by_index(0).unwrap().read_to_end(&mut contents).unwrap();
        assert_eq!(contents.len(), 100);
    }

    #[mz_ore::test]
    fn ids_are_unique_within_a_second() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path().to_owned(), 5, u64::MAX).unwrap();
        let first = snapshot(&store, at(0), 10);
        let second = snapshot(&store, at(0), 10);
        assert_ne!(first.id, second.id);
        assert_eq!(second.id, "2026-01-02T03-04-00Z-periodic-2");
        assert_eq!(store.latest().unwrap().unwrap().id, second.id);
    }

    #[mz_ore::test]
    fn retention_by_count_evicts_oldest() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path().to_owned(), 2, u64::MAX).unwrap();
        let a = snapshot(&store, at(0), 10);
        let b = snapshot(&store, at(1), 10);
        let c = snapshot(&store, at(2), 10);
        let ids: Vec<String> = store.list().unwrap().into_iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![b.id.clone(), c.id.clone()]);
        assert!(!store.zip_path(&a.id).exists());
        assert!(store.get(&a.id).unwrap().is_none());
    }

    #[mz_ore::test]
    fn retention_by_size_keeps_at_least_the_newest() {
        let dir = tempfile::tempdir().unwrap();
        // Each snapshot is a bit over 1000 bytes zipped, so two fit and three
        // do not.
        let store = SnapshotStore::open(dir.path().to_owned(), 100, 2_500).unwrap();
        let a = snapshot(&store, at(0), 1000);
        let b = snapshot(&store, at(1), 1000);
        assert_eq!(store.list().unwrap().len(), 2);
        let c = snapshot(&store, at(2), 1000);
        let ids: Vec<String> = store.list().unwrap().into_iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![b.id.clone(), c.id.clone()]);
        assert!(!store.zip_path(&a.id).exists());

        // A single snapshot larger than the whole cap is still kept.
        let huge = snapshot(&store, at(3), 10_000);
        let ids: Vec<String> = store.list().unwrap().into_iter().map(|m| m.id).collect();
        assert_eq!(ids, vec![huge.id]);
    }

    #[mz_ore::test]
    fn open_discards_leftovers_of_a_crash() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(dir.path().to_owned(), 5, u64::MAX).unwrap();
        let kept = snapshot(&store, at(0), 10);

        // A snapshot that crashed mid-way: work directory, staged zip, and a
        // zip that was renamed in but whose metadata never got written.
        let in_progress = store.new_id(at(1), SnapshotKind::OnDemand);
        fs::create_dir_all(store.workdir(&in_progress)).unwrap();
        fs::write(store.tmp_dir().join("staged.zip"), b"partial").unwrap();
        fs::write(store.zip_path("2026-01-02T03-04-02Z-periodic"), b"no meta").unwrap();
        // And metadata whose zip is gone.
        fs::write(
            store.meta_path("2026-01-02T03-04-03Z-periodic"),
            serde_json::to_vec(&SnapshotMeta {
                id: "2026-01-02T03-04-03Z-periodic".to_owned(),
                ..kept.clone()
            })
            .unwrap(),
        )
        .unwrap();

        let store = SnapshotStore::open(dir.path().to_owned(), 5, u64::MAX).unwrap();
        assert_eq!(store.list().unwrap(), vec![kept]);
        assert!(!store.tmp_dir().join(&in_progress).exists());
        assert!(!store.tmp_dir().join("staged.zip").exists());
        assert!(!store.zip_path("2026-01-02T03-04-02Z-periodic").exists());
        assert!(!store.meta_path("2026-01-02T03-04-03Z-periodic").exists());
    }
}
