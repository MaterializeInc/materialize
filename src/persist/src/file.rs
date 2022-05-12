// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! File backed implementations for testing and benchmarking.

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Instant;

use async_trait::async_trait;
use fail::fail_point;

use mz_ore::cast::CastFrom;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::Error;
use crate::location::{Atomicity, Blob, BlobMulti, BlobRead, ExternalError, LockInfo};

/// Configuration for opening a [FileBlob] or [FileBlobRead].
#[derive(Debug)]
pub struct FileBlobConfig {
    base_dir: PathBuf,
}

impl<P: AsRef<Path>> From<P> for FileBlobConfig {
    fn from(base_dir: P) -> Self {
        FileBlobConfig {
            base_dir: base_dir.as_ref().to_path_buf(),
        }
    }
}

#[derive(Debug)]
struct FileBlobCore {
    base_dir: Option<PathBuf>,
}

impl FileBlobCore {
    fn blob_path(&self, key: &str) -> Result<PathBuf, Error> {
        self.base_dir
            .as_ref()
            .map(|base_dir| base_dir.join(key))
            .ok_or_else(|| return Error::from("FileBlob unexpectedly closed"))
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let file_path = self.blob_path(key)?;
        let mut file = match File::open(file_path).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        Ok(Some(buf))
    }

    async fn list_keys(&self) -> Result<Vec<String>, Error> {
        let base_dir = match &self.base_dir {
            Some(base_dir) => base_dir.canonicalize()?,
            None => return Err(Error::from("FileBlob unexpectedly closed")),
        };
        let mut ret = vec![];

        let mut entries = fs::read_dir(&base_dir).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path().canonicalize()?;

            if !path.is_file() {
                // Ignore '.' and '..' directory entries if they come up.
                if path == base_dir {
                    continue;
                } else if let Some(parent) = base_dir.parent() {
                    if path == parent {
                        continue;
                    }
                } else {
                    return Err(Error::from(format!(
                        "unexpectedly found directory while iterating through FileBlob: {}",
                        path.display()
                    )));
                }
            }

            // The file name is guaranteed to be non-None iff the path is a
            // normal file.
            let file_name = path.file_name();
            if let Some(name) = file_name {
                let name = name.to_str();
                if let Some(name) = name {
                    ret.push(name.to_owned());
                }
            }
        }
        Ok(ret)
    }

    fn close(&mut self) -> Option<PathBuf> {
        self.base_dir.take()
    }
}

/// Implementation of [BlobRead] backed by files.
#[derive(Debug)]
pub struct FileBlobRead {
    core: FileBlobCore,
}

#[async_trait]
impl BlobRead for FileBlobRead {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.core.get(key).await
    }

    async fn list_keys(&self) -> Result<Vec<String>, Error> {
        self.core.list_keys().await
    }

    async fn close(&mut self) -> Result<bool, Error> {
        Ok(self.core.close().is_some())
    }
}

/// Implementation of [Blob] backed by files.
#[derive(Debug)]
pub struct FileBlob {
    core: FileBlobCore,
}

impl FileBlob {
    const LOCKFILE_PATH: &'static str = "LOCK";

    fn lockfile_path(base_dir: &Path) -> PathBuf {
        base_dir.join(Self::LOCKFILE_PATH)
    }
}

#[async_trait]
impl BlobRead for FileBlob {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.core.get(key).await
    }

    async fn list_keys(&self) -> Result<Vec<String>, Error> {
        self.core.list_keys().await
    }

    async fn close(&mut self) -> Result<bool, Error> {
        match self.core.close() {
            Some(base_dir) => {
                let lockfile_path = Self::lockfile_path(&base_dir);
                fs::remove_file(lockfile_path).await?;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

#[async_trait]
impl Blob for FileBlob {
    type Config = FileBlobConfig;
    type Read = FileBlobRead;

    /// Returns a new [FileBlob] which stores files under the given dir.
    ///
    /// To ensure directory-wide mutual exclusion, a LOCK file is placed in
    /// base_dir at construction time. If this file already exists (indicating
    /// that another FileBlob is already using the dir), an error is returned.
    ///
    /// The contents of `lock_info` are stored in the LOCK file and should
    /// include anything that would help debug an unexpected LOCK file, such as
    /// version, ip, worker number, etc.
    fn open_exclusive(config: FileBlobConfig, lock_info: LockInfo) -> Result<Self, Error> {
        let base_dir = config.base_dir;
        // This isn't async but this code is going away, so it doesn't matter.
        std::fs::create_dir_all(&base_dir)?;
        {
            let _ = file_storage_lock(&Self::lockfile_path(&base_dir), lock_info)?;
        }
        let core = FileBlobCore {
            base_dir: Some(base_dir),
        };
        Ok(FileBlob { core })
    }

    fn open_read(config: FileBlobConfig) -> Result<FileBlobRead, Error> {
        let core = FileBlobCore {
            base_dir: Some(config.base_dir),
        };
        Ok(FileBlobRead { core })
    }

    async fn set(&mut self, key: &str, value: Vec<u8>, atomic: Atomicity) -> Result<(), Error> {
        let file_path = self.core.blob_path(key)?;
        match atomic {
            Atomicity::RequireAtomic => {
                // To implement require_atomic, write to a temp file and rename
                // it into place.
                let mut tmp_name = file_path.clone();
                debug_assert_eq!(tmp_name.extension(), None);
                tmp_name.set_extension("tmp");
                // NB: Don't use create_new(true) for this so that if we have a
                // partial one from a previous crash, it will just get
                // overwritten (which is safe).
                let mut file = File::create(&tmp_name).await?;
                file.write_all(&value[..]).await?;

                fail_point!("fileblob_set_sync", |_| {
                    Err(Error::from(format!(
                        "FileBlob::set_sync fail point reached for file {:?}",
                        file_path
                    )))
                });

                file.sync_all().await?;
                fs::rename(tmp_name, &file_path).await?;
                // TODO: We also need to fsync the directory to be truly
                // confidant that this is permanently there. It doesn't seem
                // like this is available in the stdlib, find a crate for it?
            }
            Atomicity::AllowNonAtomic => {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .open(&file_path)
                    .await?;
                file.write_all(&value[..]).await?;

                fail_point!("fileblob_set_sync", |_| {
                    Err(Error::from(format!(
                        "FileBlob::set_sync fail point reached for file {:?}",
                        file_path
                    )))
                });

                file.sync_all().await?;
            }
        }
        Ok(())
    }

    async fn delete(&mut self, key: &str) -> Result<(), Error> {
        let file_path = self.core.blob_path(key)?;
        // TODO: strict correctness requires that we fsync the parent directory
        // as well after file removal.

        fail_point!("fileblob_delete_before", |_| {
            Err(Error::from(format!(
                "FileBlob::delete_before fail point reached for file {:?}",
                file_path
            )))
        });

        if let Err(err) = fs::remove_file(&file_path).await {
            // delete is documented to succeed if the key doesn't exist.
            if err.kind() != ErrorKind::NotFound {
                return Err(err.into());
            }
        };

        fail_point!("fileblob_delete_after", |_| {
            Err(Error::from(format!(
                "FileBlob::delete_after fail point reached for file {:?}",
                file_path
            )))
        });

        Ok(())
    }
}

/// Implementation of [BlobMulti] backed by files.
#[derive(Debug)]
pub struct FileBlobMulti {
    core: FileBlobCore,
}

impl FileBlobMulti {
    /// Opens the given location for non-exclusive read-write access.
    pub async fn open(config: FileBlobConfig) -> Result<Self, ExternalError> {
        let base_dir = config.base_dir;
        fs::create_dir_all(&base_dir).await.map_err(Error::from)?;
        let core = FileBlobCore {
            base_dir: Some(base_dir),
        };
        Ok(FileBlobMulti { core })
    }
}

#[async_trait]
impl BlobMulti for FileBlobMulti {
    async fn get(&self, _deadline: Instant, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        let value = self.core.get(key).await?;
        Ok(value)
    }

    async fn list_keys(&self, _deadline: Instant) -> Result<Vec<String>, ExternalError> {
        let keys = self.core.list_keys().await?;
        Ok(keys)
    }

    async fn set(
        &self,
        _deadline: Instant,
        key: &str,
        value: Vec<u8>,
        atomic: Atomicity,
    ) -> Result<(), ExternalError> {
        // TODO: Move this impl here once we delete FileBlob.
        let mut hack = FileBlob {
            core: FileBlobCore {
                base_dir: self.core.base_dir.clone(),
            },
        };
        hack.set(key, value, atomic).await?;
        Ok(())
    }

    async fn delete(&self, _deadline: Instant, key: &str) -> Result<(), ExternalError> {
        // TODO: Move this impl here once we delete FileBlob.
        let mut hack = FileBlob {
            core: FileBlobCore {
                base_dir: self.core.base_dir.clone(),
            },
        };
        hack.delete(key).await?;
        Ok(())
    }
}

fn file_storage_lock(lockfile_path: &Path, new_lock: LockInfo) -> Result<std::fs::File, Error> {
    // This isn't async but this code is going away, so it doesn't matter.
    use std::io::{Seek, Write};

    // TODO: flock this for good measure? There's all sorts of tricky edge cases
    // here when this gets called concurrently, and we'll have the same issues
    // when we add an s3 impl of Blob. Revisit this in a principled way.
    let mut lockfile = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(&lockfile_path)?;
    let _ = new_lock.check_reentrant_for(&lockfile_path, &mut lockfile)?;
    // Overwrite the data and then truncate the length if necessary. Truncating
    // first could produce a race condition where the file looks empty to a
    // process concurrently trying to lock it.
    lockfile.seek(std::io::SeekFrom::Start(0))?;
    let contents = new_lock.to_string().into_bytes();
    lockfile.write_all(&contents)?;
    lockfile.set_len(u64::cast_from(contents.len()))?;
    lockfile.sync_all()?;
    Ok(lockfile)
}

#[cfg(test)]
mod tests {
    use crate::location::tests::{blob_impl_test, blob_multi_impl_test};

    use super::*;

    #[tokio::test]
    async fn file_blob() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;
        let temp_dir_read = temp_dir.path().to_owned();
        blob_impl_test(
            move |t| {
                let instance_dir = temp_dir.path().join(t.path);
                FileBlob::open_exclusive(
                    instance_dir.into(),
                    (t.reentrance_id, "file_blob_test").into(),
                )
            },
            move |path| FileBlob::open_read(temp_dir_read.join(path).into()),
        )
        .await
    }

    #[tokio::test]
    async fn file_blob_multi() -> Result<(), ExternalError> {
        let temp_dir = tempfile::tempdir().map_err(Error::from)?;
        blob_multi_impl_test(move |path| {
            let instance_dir = temp_dir.path().join(path);
            FileBlobMulti::open(instance_dir.into())
        })
        .await
    }

    #[test]
    fn file_storage_lock_reentrance() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("file_storage_lock_reentrance");

        // Sanity check that overwriting the contents with shorter contents as
        // well as with longer contents both work.
        let _f1 = file_storage_lock(
            &path,
            LockInfo::new("reentrance0".to_owned(), "foo".repeat(5))?,
        )?;
        assert_eq!(
            std::fs::read_to_string(&path)?,
            "reentrance0\nfoofoofoofoofoo"
        );
        let _f2 = file_storage_lock(
            &path,
            LockInfo::new("reentrance0".to_owned(), "foo".to_owned())?,
        )?;
        assert_eq!(std::fs::read_to_string(&path)?, "reentrance0\nfoo");
        let _f3 = file_storage_lock(
            &path,
            LockInfo::new("reentrance0".to_owned(), "foo".repeat(3))?,
        )?;
        assert_eq!(std::fs::read_to_string(&path)?, "reentrance0\nfoofoofoo");

        drop(_f1);
        drop(_f2);
        drop(_f3);
        Ok(())
    }
}
