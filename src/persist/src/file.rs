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

use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::Error;
use crate::location::{Atomicity, BlobMulti, ExternalError};

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
}

/// Implementation of [Blob] backed by files.
#[derive(Debug)]
pub struct FileBlob {
    core: FileBlobCore,
}

impl FileBlob {
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

#[cfg(test)]
mod tests {
    use crate::location::tests::blob_multi_impl_test;

    use super::*;

    #[tokio::test]
    async fn file_blob_multi() -> Result<(), ExternalError> {
        let temp_dir = tempfile::tempdir().map_err(Error::from)?;
        blob_multi_impl_test(move |path| {
            let instance_dir = temp_dir.path().join(path);
            FileBlobMulti::open(instance_dir.into())
        })
        .await
    }
}
