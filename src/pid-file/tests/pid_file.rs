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

use std::error::Error;

use mz_pid_file::PidFile;

#[test]
fn test_pid_file_basics() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("pidfile");

    // Creating a PID file should create a file at the specified path.
    let pid_file = PidFile::open(&path)?;
    assert!(path.exists());

    // PID contents should be accurate
    let pid = PidFile::read(&path)?;
    assert!(pid > 0);
    assert_eq!(std::process::id(), u32::try_from(pid).unwrap());

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
