// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! File backed implementations for testing and benchmarking.

use std::convert::TryInto;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write as StdWrite};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::mem::MemSnapshot;
use crate::persister::{Meta, Write};
use crate::storage::{Blob, Buffer, SeqNo};

/// A naive implementation of [Write] and [Meta] backed by a file.
#[derive(Clone, Debug)]
pub struct FileStream {
    dataz: Arc<Mutex<File>>,
    buf: Vec<u8>,
}

impl FileStream {
    /// Create a new FileStream with data stored at the specified path,
    ///
    /// If the file was previously created as a FileStream, new data is appended.
    pub fn new<P: AsRef<Path>>(name: P) -> Result<Self, Error> {
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .read(true)
            .open(name)?;
        Ok(FileStream {
            dataz: Arc::new(Mutex::new(file)),
            buf: Vec::new(),
        })
    }
}

impl Write for FileStream {
    fn write_sync(&mut self, updates: &[((String, String), u64, isize)]) -> Result<(), Error> {
        self.buf.clear();
        for ((key, val), ts, diff) in updates.iter() {
            let key_bytes = key.as_bytes();
            let key_bytes_len = key_bytes.len() as u64;
            let val_bytes = val.as_bytes();
            let val_bytes_len = val_bytes.len() as u64;

            self.buf.extend(&key_bytes_len.to_le_bytes());
            self.buf.extend(key_bytes);
            self.buf.extend(&val_bytes_len.to_le_bytes());
            self.buf.extend(val_bytes);
            self.buf.extend(&ts.to_le_bytes());
            self.buf.extend(&(*diff as i64).to_le_bytes());
        }

        self.buf.shrink_to_fit();

        let mut guard = self.dataz.lock()?;
        guard.write_all(&self.buf)?;
        // TODO: we will eventually want to change the interface to let us batch up
        // calls to `sync_all`.
        guard.sync_all()?;
        Ok(())
    }

    fn seal(&mut self, _upper: u64) -> Result<(), Error> {
        // No-op for now.
        Ok(())
    }
}

impl Meta for FileStream {
    type Snapshot = MemSnapshot;

    fn snapshot(&self) -> Result<Self::Snapshot, Error> {
        let mut bytes = Vec::new();
        let mut dataz = Vec::new();

        {
            let mut guard = self.dataz.lock()?;
            guard.seek(SeekFrom::Start(0))?;
            guard.read_to_end(&mut bytes)?;
        }

        let mut bytes = &bytes[..];
        let mut buf = [0u8; 8];
        while !bytes.is_empty() {
            // Decode key
            bytes.read_exact(&mut buf)?;
            let key_len = u64::from_le_bytes(buf);
            let mut key: Vec<u8> = vec![0u8; key_len.try_into().unwrap()];
            bytes.read_exact(&mut key[..])?;
            let key =
                String::from_utf8(key).map_err(|e| format!("key not valid utf-8: {:?}", e))?;

            // Decode val
            bytes.read_exact(&mut buf)?;
            let val_len = u64::from_le_bytes(buf);
            let mut val = vec![0u8; val_len.try_into().unwrap()];
            bytes.read_exact(&mut val[..])?;
            let val =
                String::from_utf8(val).map_err(|e| format!("val not valid utf-8: {:?}", e))?;

            // Decode time
            bytes.read_exact(&mut buf)?;
            let ts = u64::from_le_bytes(buf);

            // Decode diff
            bytes.read_exact(&mut buf)?;
            let diff = i64::from_le_bytes(buf);

            dataz.push(((key, val), ts, diff as isize));
        }
        Ok(MemSnapshot::new(dataz))
    }

    fn allow_compaction(&mut self, _ts: u64) -> Result<(), Error> {
        // No-op for now.
        Ok(())
    }
}

/// Inner struct handles to separate files that store the data and metadata about the
/// most recently truncated sequence number for [FileBuffer].
struct FileBufferCore {
    dataz: File,
    metadata: File,
}

/// A naive implementation of [Buffer] backed by files.
pub struct FileBuffer {
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
    pub fn new<P: AsRef<Path>>(base_dir: P, lock_info: &[u8]) -> Result<Self, Error> {
        let base_dir = base_dir.as_ref();
        fs::create_dir_all(&base_dir)?;
        {
            let lockfile_path = Self::lockfile_path(&base_dir);
            let mut lockfile = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(lockfile_path)?;
            lockfile.write_all(&lock_info)?;
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
                buffer_file.seek(SeekFrom::End(8))?;
                buffer_file.read_exact(&mut buf)?;
                SeqNo(u64::from_le_bytes(buf))
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
}

impl Buffer for FileBuffer {
    fn write_sync(&mut self, buf: Vec<u8>) -> Result<SeqNo, Error> {
        let write_seqno = self.seqno.end;
        self.seqno = self.seqno.start..SeqNo(write_seqno.0 + 1);
        // Write length prefixed data, and then the sequence number.
        let len = buf.len() as u64;

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
            let mut data = vec![0u8; data_len as usize];
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
}

/// Implementation of [Blob] backed by files.
pub struct FileBlob {
    base_dir: PathBuf,
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
    pub fn new<P: AsRef<Path>>(base_dir: P, lock_info: &[u8]) -> Result<Self, Error> {
        let base_dir = base_dir.as_ref();
        fs::create_dir_all(&base_dir)?;
        {
            let lockfile_path = Self::lockfile_path(&base_dir);
            let mut lockfile = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(lockfile_path)?;
            lockfile.write_all(&lock_info)?;
            lockfile.sync_all()?;
        }
        Ok(FileBlob {
            base_dir: base_dir.to_path_buf(),
        })
    }

    fn lockfile_path(base_dir: &Path) -> PathBuf {
        base_dir.join(Self::LOCKFILE_PATH)
    }

    fn blob_path(&self, key: &str) -> PathBuf {
        self.base_dir.join(key)
    }
}

impl Blob for FileBlob {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let file_path = self.blob_path(key);
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
        let file_path = self.blob_path(key);
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
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use crate::persister::Snapshot;
    use crate::storage::tests::{blob_impl_test, buffer_impl_test};

    use super::*;

    #[test]
    fn test_file_stream() -> Result<(), Error> {
        let data = vec![
            (("key1".to_string(), "val1".to_string()), 1, 1),
            (("key2".to_string(), "val2".to_string()), 1, 1),
        ];

        // Create a directory that will automatically be dropped after the test finishes.
        let temp_dir = tempfile::tempdir()?;
        let file_path = NamedTempFile::new_in(&temp_dir)?.into_temp_path();
        let mut file_stream = FileStream::new(file_path)?;
        file_stream.write_sync(&data)?;
        let mut snapshot = file_stream.snapshot()?;
        let snapshot_data = snapshot.read_to_end();
        assert_eq!(snapshot_data, data);

        Ok(())
    }

    #[test]
    fn file_blob() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;
        blob_impl_test(move |idx| {
            let instance_dir = temp_dir.path().join(format!("{}", idx));
            FileBlob::new(instance_dir, &"file_blob_test".as_bytes())
        })
    }

    #[test]
    fn file_buffer() -> Result<(), Error> {
        let temp_dir = tempfile::tempdir()?;
        buffer_impl_test(move |idx| {
            let instance_dir = temp_dir.path().join(format!("{}", idx));
            FileBuffer::new(instance_dir, &"file_buffer_test".as_bytes())
        })
    }
}
