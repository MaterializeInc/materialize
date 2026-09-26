// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Explicit, fixture-owned clock input for timestamp allocation and policy checks.
//!
//! The fixture publishes a little-endian epoch-millisecond integer under a file
//! lock. Readers retain open handles so fixture teardown cannot remove the clock
//! from still-draining tasks or replica processes. This clock must not drive
//! source timestamps, Persist leases, or scheduling timers.

use std::fs::File;
use std::io::{self, Read, Seek, Write};
use std::path::PathBuf;
use std::sync::Mutex;

use mz_ore::now::{EpochMillis, NowFn};

fn read(file: &mut File) -> io::Result<EpochMillis> {
    file.lock_shared()?;
    let result = (|| {
        if file.metadata()?.len() != 8 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid fixture clock length",
            ));
        }
        file.rewind()?;
        let mut bytes = [0; 8];
        file.read_exact(&mut bytes)?;
        Ok(EpochMillis::from_le_bytes(bytes))
    })();
    file.unlock()?;
    result
}

/// Publishes one clock value to a fixture-owned file. Keep the file identity
/// stable for readers that have opened it. The lock prevents torn observations
/// during concurrent calls, including backwards clock changes.
pub fn publish(file: &mut File, value: EpochMillis) -> io::Result<()> {
    file.lock()?;
    let result = (|| {
        file.rewind()?;
        file.write_all(&value.to_le_bytes())?;
        file.set_len(8)
    })();
    file.unlock()?;
    result
}

/// Opens an explicitly configured fixture clock, validating its initial value.
///
/// The fixture must keep the path available for starting or restarting readers.
/// Existing readers retain the final value after fixture teardown unlinks it.
/// Missing or malformed input fails closed.
/// Falling back to wall time would durably advance the shared oracle beyond the
/// fixture clock. Runtime errors panic because [`NowFn`] cannot return an error.
pub fn open(path: PathBuf) -> io::Result<NowFn> {
    let mut file = File::open(&path)?;
    read(&mut file)?;
    let file = Mutex::new(file);
    Ok(NowFn::from(move || {
        read(&mut file.lock().expect("fixture clock mutex poisoned")).unwrap_or_else(|err| {
            panic!("reading fixture timestamp clock {}: {err}", path.display())
        })
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[mz_ore::test]
    fn shared_changes_restarts_and_teardown() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("clock");
        let mut writer = File::create(&path).unwrap();
        publish(&mut writer, 100).unwrap();
        let reader = open(path.clone()).unwrap();
        publish(&mut writer, 1).unwrap();
        assert_eq!(reader(), 1);
        let restarted = open(path).unwrap();
        publish(&mut writer, 200).unwrap();
        assert_eq!(reader(), 200);
        assert_eq!(restarted(), 200);
        drop(directory);
        assert_eq!(reader(), 200);
        assert_eq!(restarted(), 200);
    }
}
