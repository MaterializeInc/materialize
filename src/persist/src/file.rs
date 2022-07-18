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

use anyhow::anyhow;
use async_trait::async_trait;
use bytes::Bytes;
use fail::fail_point;

use tokio::fs::{self, File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::Error;
use crate::location::{Atomicity, Blob, ExternalError};

/// Configuration for opening a [FileBlob].
#[derive(Debug, Clone)]
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

/// Implementation of [Blob] backed by files.
#[derive(Debug)]
pub struct FileBlob {
    base_dir: PathBuf,
}

impl FileBlob {
    /// Opens the given location for non-exclusive read-write access.
    pub async fn open(config: FileBlobConfig) -> Result<Self, ExternalError> {
        let base_dir = config.base_dir;
        fs::create_dir_all(&base_dir).await.map_err(Error::from)?;
        Ok(FileBlob { base_dir })
    }

    fn blob_path(&self, key: &str) -> PathBuf {
        self.base_dir.join(key)
    }
}

#[async_trait]
impl Blob for FileBlob {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        let file_path = self.blob_path(key);
        let mut file = match File::open(file_path).await {
            Ok(file) => file,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;
        Ok(Some(buf))
    }

    async fn list_keys(&self) -> Result<Vec<String>, ExternalError> {
        let base_dir = self.base_dir.canonicalize()?;
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
                    return Err(ExternalError::from(anyhow!(
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

    async fn set(&self, key: &str, value: Bytes, atomic: Atomicity) -> Result<(), ExternalError> {
        let file_path = self.blob_path(key);
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
                    Err(ExternalError::from(anyhow!(
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
                    Err(ExternalError::from(anyhow!(
                        "FileBlob::set_sync fail point reached for file {:?}",
                        file_path
                    )))
                });

                file.sync_all().await?;
            }
        }
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), ExternalError> {
        let file_path = self.blob_path(key);
        // TODO: strict correctness requires that we fsync the parent directory
        // as well after file removal.

        fail_point!("fileblob_delete_before", |_| {
            Err(ExternalError::from(anyhow!(
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
            Err(ExternalError::from(anyhow!(
                "FileBlob::delete_after fail point reached for file {:?}",
                file_path
            )))
        });

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::location::tests::blob_impl_test;

    use super::*;

    #[tokio::test]
    async fn file_blob() -> Result<(), ExternalError> {
        let temp_dir = tempfile::tempdir().map_err(Error::from)?;
        blob_impl_test(move |path| {
            let instance_dir = temp_dir.path().join(path);
            FileBlob::open(instance_dir.into())
        })
        .await
    }
}
