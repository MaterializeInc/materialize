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

use std::collections::HashMap;
use std::error::Error;

use mz_pid_file::{DevelopmentPidFile, PidFile};

#[test]
fn test_pid_file_basics() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pidfile");

    // Creating a PID file should create a file at the specified path.
    let pid_file = PidFile::open(&path)?;
    assert!(path.exists());

    // Attempting to open the PID file again should fail.
    match PidFile::open(&path) {
        Err(mz_pid_file::Error::AlreadyRunning { .. }) => (),
        Ok(_) => panic!("unexpected success when opening pid file"),
        Err(e) => return Err(e.into()),
    }

    // Dropping the PID file should remove it.
    drop(pid_file);
    assert!(!path.exists());

    // Using the explicit `remove` method should work too.
    let pid_file = PidFile::open(&path)?;
    assert!(path.exists());
    pid_file.remove()?;
    assert!(!path.exists());

    Ok(())
}

#[test]
fn test_development_pid_file() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pidfile");

    let port_metadata: HashMap<&str, i32> = vec![("joe", 42), ("shmoe", 666)].into_iter().collect();
    let port_metadata = serde_json::to_string(&port_metadata)?;
    let _development_pid_file = DevelopmentPidFile::open(&path, port_metadata.as_str())?;
    assert!(path.exists());

    let pid_contents = DevelopmentPidFile::read(&path)?;

    assert_eq!(std::process::id(), pid_contents.pid);
    assert_eq!(42, pid_contents.port_metadata["joe"]);
    assert_eq!(666, pid_contents.port_metadata["shmoe"]);

    Ok(())
}
