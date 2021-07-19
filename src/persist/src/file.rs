// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! File backed implementations for testing and benchmarking.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write as StdWrite};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ore::cast::CastFrom;

use crate::error::Error;
use crate::storage::{Blob, Buffer, SeqNo};

/// Inner struct handles to separate files that store the data and metadata about the
/// most recently truncated sequence number for [FileBuffer].
struct FileBufferCore {
    dataz: File,
    metadata: File,
}

/// A naive implementation of [Buffer] backed by files.
pub struct FileBuffer {
    base_dir: Option<PathBuf>,
    dataz: Arc<Mutex<FileBufferCore>>,
    seqno: Range<SeqNo>,
    buf: Vec<u8>,
}

impl FileBuffer {
    const BUFFER_PATH: &'static str = "BUFFER";
    const LOCKFILE_PATH: &'static str = "LOCK";
    const METADATA_PATH: &'static str = "META";

    /// Returns a new [FileBuffer] which stores files under the given dir.
    ///
    /// To ensure directory-wide mutual exclusion, a LOCK file is placed in
    /// base_dir at construction time. If this file already exists (indicating
    /// that another FileBuffer is already using the dir), an error is returned
    /// from `new`.
    ///
    /// The contents of `lock_info` are stored in the LOCK file and should
    /// include anything that would help debug an unexpected LOCK file, such as
    /// version, ip, worker number, etc.
    ///
    /// The data is stored in a separate buffer file, and is formatted as a sequential list of
    /// chunks corresponding to each write. Each chunk consists of:
    /// `length` - A 64 bit unsigned int (little-endian) indicating the size of `data`.
    /// `data` - `length` bytes of data.
    /// `sequence_number` - A 64 bit unsigned int (little-endian) indicating the sequence number
    /// assigned to `data`.
    ///
    /// Additionally, the metadata about the last truncated sequence number is stored in a
    /// metadata file, which only ever contains a single 64 bit unsigned integer (also little-endian)
    /// that indicates the most recently truncated offset (ie all offsets less than this are truncated).
    pub fn new<P: AsRef<Path>>(base_dir: P, lock_info: &str) -> Result<Self, Error> {
        let base_dir = base_dir.as_ref();
        fs::create_dir_all(&base_dir)?;
        {
            let lockfile_path = Self::lockfile_path(&base_dir);
            let mut lockfile = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(lockfile_path)?;
            lockfile.write_all(lock_info.as_bytes())?;
            lockfile.sync_all()?;
        }
        let buffer_path = Self::buffer_path(&base_dir);
        let mut buffer_file = OpenOptions::new()
            .append(true)
            .read(true)
            .create(true)
            .open(&buffer_path)?;

        let metadata_path = Self::metadata_path(&base_dir);
        let mut metadata_file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(true)
            .open(&metadata_path)?;

        // Retrieve the last truncated sequence number from the metadata file
        // unless that file was just created, in which case default to 0.
        let mut bytes = Vec::new();
        metadata_file.read_to_end(&mut bytes)?;
        let len = bytes.len();
        let seqno_start = if len != 0 {
            let mut bytes = &bytes[..];
            let mut buf = [0u8; 8];
            // Decode sequence number
            bytes.read_exact(&mut buf).map_err(|e| {
                format!(
                    "could not read buffer metadata file: found {} bytes (expected 8) {:?}",
                    len, e,
                )
            })?;
            SeqNo(u64::from_le_bytes(buf))
        } else {
            metadata_file.write_all(&0u64.to_le_bytes())?;
            metadata_file.sync_all()?;
            SeqNo(0)
        };

        // Retrieve the most recently written sequence number in the buffer,
        // unless the buffer is empty in which case default to 0.
        let seqno_end = {
            let len = buffer_file.metadata()?.len();

            if len > 0 {
                let mut buf = [0u8; 8];
                buffer_file.seek(SeekFrom::End(-8))?;
                buffer_file.read_exact(&mut buf)?;
                // NB: seqno_end is exclusive, so we have to add one to the
                // seqno of the most recent write to reconstruct it.
                SeqNo(u64::from_le_bytes(buf) + 1)
            } else {
                SeqNo(0)
            }
        };

        if seqno_start > seqno_end {
            return Err(format!(
                "invalid sequence number range found for file buffer: start: {:?} end: {:?}",
                seqno_start, seqno_end,
            )
            .into());
        }

        Ok(FileBuffer {
            base_dir: Some(base_dir.to_owned()),
            dataz: Arc::new(Mutex::new(FileBufferCore {
                dataz: buffer_file,
                metadata: metadata_file,
            })),
            seqno: seqno_start..seqno_end,
            buf: Vec::new(),
        })
    }

    fn lockfile_path(base_dir: &Path) -> PathBuf {
        base_dir.join(Self::LOCKFILE_PATH)
    }

    fn buffer_path(base_dir: &Path) -> PathBuf {
        base_dir.join(Self::BUFFER_PATH)
    }

    fn metadata_path(base_dir: &Path) -> PathBuf {
        base_dir.join(Self::METADATA_PATH)
    }

    fn ensure_open(&self) -> Result<PathBuf, Error> {
        self.base_dir
            .clone()
            .ok_or_else(|| return Error::from("FileBuffer unexpectedly closed"))
    }
}

impl Buffer for FileBuffer {
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<SeqNo, Error> {
        self.ensure_open()?;

        let write_seqno = self.seqno.end;
        self.seqno = self.seqno.start..SeqNo(write_seqno.0 + 1);
        // Write length prefixed data, and then the sequence number.
        let len = u64::cast_from(buf.len());

        // NB: the write buffer never shrinks, and this pattern may not be what we want
        // under write workloads with rare very large writes.
        self.buf.clear();
        self.buf.extend(&len.to_le_bytes());
        self.buf.extend(buf);
        self.buf.extend(&write_seqno.0.to_le_bytes());

        let mut guard = self.dataz.lock()?;
        guard.dataz.write_all(&self.buf)?;
        guard.dataz.sync_all()?;
        Ok(write_seqno)
    }

    fn snapshot<F>(&self, mut logic: F) -> Result<Range<SeqNo>, Error>
    where
        F: FnMut(SeqNo, &[u8]) -> Result<(), Error>,
    {
        self.ensure_open()?;
        let mut bytes = Vec::new();

        {
            let mut guard = self.dataz.lock()?;
            // This file was opened with append mode, so any future writes
            // will reset the file cursor to the end before writing.
            guard.dataz.seek(SeekFrom::Start(0))?;
            guard.dataz.read_to_end(&mut bytes)?;
        }

        let mut bytes = &bytes[..];
        let mut len_raw = [0u8; 8];
        while !bytes.is_empty() {
            // Decode the data
            bytes.read_exact(&mut len_raw)?;
            let data_len = u64::from_le_bytes(len_raw);
            // TODO: could reuse the underlying buffer here to avoid allocating each time.
            let mut data = vec![0u8; usize::cast_from(data_len)];
            bytes.read_exact(&mut data[..])?;

            // Decode sequence number
            bytes.read_exact(&mut len_raw)?;
            let seqno = SeqNo(u64::from_le_bytes(len_raw));

            if seqno < self.seqno.start {
                // This record has already been truncated so we can ignore it.
                continue;
            } else if seqno >= self.seqno.end {
                return Err(format!(
                    "invalid sequence number {:?} found for buffer containing {:?}",
                    seqno, self.seqno
                )
                .into());
            }

            logic(seqno, &data)?;
        }
        Ok(self.seqno.clone())
    }

    /// Logically truncates the buffer so that future reads ignore writes at sequence numbers
    /// less than `upper`.
    ///
    /// TODO: actually reclaim disk space as part of truncating.
    fn truncate(&mut self, upper: SeqNo) -> Result<(), Error> {
        self.ensure_open()?;
        // TODO: Test the edge cases here.
        if upper <= self.seqno.start || upper > self.seqno.end {
            return Err(format!(
                "invalid truncation {:?} for buffer containing: {:?}",
                upper, self.seqno
            )
            .into());
        }
        self.seqno = upper..self.seqno.end;

        let mut guard = self.dataz.lock()?;
        guard.metadata.seek(SeekFrom::Start(0))?;
        guard.metadata.set_len(0)?;
        guard
            .metadata
            .write_all(&self.seqno.start.0.to_le_bytes())?;
        guard.metadata.sync_all()?;

        Ok(())
    }

    fn close(&mut self) -> Result<bool, Error> {
        if let Ok(base_dir) = self.ensure_open() {
            let lockfile_path = Self::lockfile_path(&base_dir);
            fs::remove_file(lockfile_path)?;
            self.base_dir = None;
            Ok(true)
        } else {
            // Already closed. Close implementations must be idempotent.
            Ok(false)
        }
    }
}

/// Implementation of [Blob] backed by files.
pub struct FileBlob {
    base_dir: Option<PathBuf>,
}

impl FileBlob {
    const LOCKFILE_PATH: &'static str = "LOCK";

    /// Returns a new [FileBlob] which stores files under the given dir.
    ///
    /// To ensure directory-wide mutual exclusion, a LOCK file is placed in
    /// base_dir at construction time. If this file already exists (indicating
    /// that another FileBlob is already using the dir), an error is returned
    /// from `new`.
    ///
    /// The contents of `lock_info` are stored in the LOCK file and should
    /// include anything that would help debug an unexpected LOCK file, such as
    /// version, ip, worker number, etc.
    pub fn new<P: AsRef<Path>>(base_dir: P, lock_info: &str) -> Result<Self, Error> {
        let base_dir = base_dir.as_ref();
        fs::create_dir_all(&base_dir)?;
        {
            let lockfile_path = Self::lockfile_path(&base_dir);
            let mut lockfile = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(lockfile_path)?;
            lockfile.write_all(lock_info.as_bytes())?;
            lockfile.sync_all()?;
        }
        Ok(FileBlob {
            base_dir: Some(base_dir.to_path_buf()),
        })
    }

    fn lockfile_path(base_dir: &Path) -> PathBuf {
        base_dir.join(Self::LOCKFILE_PATH)
    }

    fn blob_path(&self, key: &str) -> Result<PathBuf, Error> {
        self.base_dir
            .as_ref()
            .map(|base_dir| base_dir.join(key))
            .ok_or_else(|| return Error::from("FileBlob unexpectedly closed"))
    }
}

impl Blob for FileBlob {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let file_path = self.blob_path(key)?;
        let mut file = match File::open(file_path) {
            Ok(file) => file,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Ok(Some(buf))
    }

    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error> {
        let file_path = self.blob_path(key)?;
        let mut file = if allow_overwrite {
            File::create(file_path)?
        } else {
            OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(file_path)?
        };
        file.write_all(&value[..])?;
        file.sync_all()?;
        Ok(())
    }

    fn close(&mut self) -> Result<bool, Error> {
        if let Some(base_dir) = self.base_dir.as_ref() {
            let lockfile_path = Self::lockfile_path(&base_dir);
            fs::remove_file(lockfile_path)?;
            self.base_dir = None;
            Ok(true)
        } else {
            // Already closed. Close implementations must be idempotent.
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::tests::{blob_impl_test, buffer_impl_test};

    use super::*;

    #[test]
    fn file_blob() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;
        blob_impl_test(move |idx| {
            let instance_dir = temp_dir.path().join(idx);
            FileBlob::new(instance_dir, &"file_blob_test")
        })
    }

    #[test]
    fn file_buffer() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;
        buffer_impl_test(move |idx| {
            let instance_dir = temp_dir.path().join(idx);
            FileBuffer::new(instance_dir, &"file_buffer_test")
        })
    }
}
