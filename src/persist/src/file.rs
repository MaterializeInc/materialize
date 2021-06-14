// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! File backed implementations for testing and benchmarking.

use std::collections::{HashMap, HashSet};
use std::convert::TryInto;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write as StdWrite};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::error::Error;
use crate::mem::MemSnapshot;
use crate::persister::{Meta, Persister, Write};
use crate::{Id, Token};

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

/// Implementation of [Persister] backed by files.
pub struct FilePersister {
    base_path: PathBuf,
    registered: HashSet<Id>,
    dataz: HashMap<Id, FileStream>,
}

impl FilePersister {
    /// Constructs a new, empty FilePersister with the specified base path.
    pub fn new<P: AsRef<Path>>(base_path: P) -> Result<Self, Error> {
        fs::create_dir_all(&base_path)?;
        Ok(FilePersister {
            base_path: base_path.as_ref().to_path_buf(),
            registered: HashSet::new(),
            dataz: HashMap::new(),
        })
    }
}

impl Persister for FilePersister {
    type Write = FileStream;
    type Meta = FileStream;

    fn create_or_load(&mut self, id: Id) -> Result<Token<Self::Write, Self::Meta>, Error> {
        if self.registered.contains(&id) {
            return Err(format!("internal error: {:?} already registered", id).into());
        }

        self.registered.insert(id);
        let path = self.base_path.join(format!("file-persister-{}", id.0));
        let file_stream = FileStream::new(path)?;
        let p = self.dataz.entry(id).or_insert(file_stream);
        let t = Token {
            write: p.clone(),
            meta: p.clone(),
        };
        Ok(t)
    }

    /// Removes the persisted stream for `id`.
    ///
    /// Note that this function does not unregister `id` to ensure that each `id`
    /// is only ever registered exactly once.
    fn destroy(&mut self, id: Id) -> Result<(), Error> {
        if self.dataz.remove(&id).is_none() {
            return Err(format!("internal error: {:?} not registered", id).into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use tempfile::NamedTempFile;

    use crate::persister::Snapshot;

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
}
