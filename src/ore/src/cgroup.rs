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

//! Linux cgroup detection utilities.
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

/// An entry in /proc/self/cgroup
#[derive(Debug, PartialEq)]
pub struct CgroupEntry {
    subsystems: Vec<String>,
    root: PathBuf,
}

impl CgroupEntry {
    fn from_line(line: String) -> Option<CgroupEntry> {
        let mut fields = line.split(':');
        let subsystems = fields
            .nth(1)?
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_owned())
            .collect();
        let root = PathBuf::from(fields.next()?);
        Some(CgroupEntry { subsystems, root })
    }
}

/// Parses /proc/self/cgroup into a Vec<CgroupEntry>, if the file exists.
pub fn parse_proc_self_cgroup() -> Option<Vec<CgroupEntry>> {
    let file = File::open("/proc/self/cgroup").ok()?;
    let file = BufReader::new(file);
    Some(
        file.lines()
            .flatten()
            .filter_map(CgroupEntry::from_line)
            .collect(),
    )
}

/// An entry in /proc/self/mountinfo
#[derive(Debug, PartialEq)]
pub struct Mountinfo {
    root: PathBuf,
    mount_point: PathBuf,
    fs_type: String,
    super_opts: Vec<String>,
}

impl Mountinfo {
    fn from_line(line: String) -> Option<Mountinfo> {
        // https://www.kernel.org/doc/Documentation/filesystems/proc.txt
        let mut split = line.split(" - ");

        let mut mount_fields = split.next()?.split(' ');
        let root = PathBuf::from(mount_fields.nth(3)?);
        let mount_point = PathBuf::from(mount_fields.next()?);

        let mut fs_fields = split.next()?.split(' ');

        let fs_type = fs_fields.next()?.split('.').next()?.to_owned();
        let super_opts: Vec<String> = fs_fields
            .nth(1)?
            .split(',')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_owned())
            .collect();

        Some(Mountinfo {
            root,
            mount_point,
            fs_type,
            super_opts,
        })
    }
}

/// Parses /proc/self/mountinfo into vectors of Mountinfo objects,
/// returning (v2_mounts, v1_mounts).
pub fn parse_proc_self_mountinfo() -> Option<(Vec<Mountinfo>, Vec<Mountinfo>)> {
    let file = File::open("/proc/self/mountinfo").ok()?;
    let file = BufReader::new(file);
    Some(
        file.lines()
            .flatten()
            .filter_map(Mountinfo::from_line)
            .filter(|mi| mi.fs_type == "cgroup" || mi.fs_type == "cgroup2")
            .partition(|mi| mi.fs_type == "cgroup2"),
    )
}

/// Represents a cgroup memory limit, with both ram and swap maximums if they exist.
/// Fields should be None if a limit does not exist or when running on a platform without cgroup
/// support (ie: non-Linux platforms).
#[derive(Debug)]
pub struct MemoryLimit {
    /// Maximum RAM limit, in bytes, if a limit exists.
    pub max: Option<usize>,
    /// Maximum swap limit, in bytes, if a limit exists.
    pub swap_max: Option<usize>,
}

fn read_file_to_usize<P: AsRef<Path>>(path: P) -> Option<usize> {
    match std::fs::read_to_string(path.as_ref()) {
        Ok(s) => s.trim().parse::<usize>().ok(),
        Err(_) => None,
    }
}

/// Finds the mountpoint corresponding to the provided cgroup v2,
/// and reads the memory limits within.
fn read_v2_memory_limit(cgroups: &[CgroupEntry], mounts: &[Mountinfo]) -> Option<MemoryLimit> {
    // cgroups v2 only supports a single cgroup per process
    let mount = mounts.first()?;
    if mount.root != cgroups.first()?.root {
        // We don't support mixed v2/v1
        return None;
    }
    let mount_point = &mount.mount_point;
    let controllers = std::fs::read_to_string(mount_point.join("cgroup.controllers")).ok()?;
    let controllers: Vec<&str> = controllers.trim().split(' ').collect();
    if controllers.contains(&"memory") {
        let max = read_file_to_usize(mount_point.join("memory.max"));
        // Unlike v1, this is only the swap, not swap + memory.
        let swap_max = read_file_to_usize(mount_point.join("memory.swap.max"));
        return Some(MemoryLimit { max, swap_max });
    }
    None
}

/// Finds the cgroup v1 and mountpoint combination containing the memory controller,
/// and reads the memory limits within.
fn read_v1_memory_limit(cgroups: &[CgroupEntry], mounts: &[Mountinfo]) -> Option<MemoryLimit> {
    // https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
    let memory_cgroup = cgroups
        .into_iter()
        .find(|cgroup| cgroup.subsystems.iter().any(|s| s == "memory"))?;
    let memory_mount = mounts
        .iter()
        .find(|mi| mi.root == memory_cgroup.root && mi.super_opts.iter().any(|o| o == "memory"))?;
    let mount_point = &memory_mount.mount_point;
    let max = read_file_to_usize(mount_point.join("memory.limit_in_bytes"));
    // This is memory + swap, not just swap.
    let memsw_max = read_file_to_usize(mount_point.join("memory.memsw.limit_in_bytes"));
    let swap_max = match (max, memsw_max) {
        (Some(max), Some(memsw_max)) => Some(memsw_max - max),
        _ => None,
    };
    Some(MemoryLimit { max, swap_max })
}
/// Returns the cgroup (v1 or v2) memory limit if it exists.
pub fn detect_memory_limit() -> Option<MemoryLimit> {
    let (v2_mounts, v1_mounts) = parse_proc_self_mountinfo()?;
    let cgroups = parse_proc_self_cgroup()?;

    if !v2_mounts.is_empty() {
        return read_v2_memory_limit(&cgroups, &v2_mounts);
    }
    read_v1_memory_limit(&cgroups, &v1_mounts)
}

#[cfg(test)]
mod tests {
    use super::{CgroupEntry, Mountinfo};
    use std::path::PathBuf;

    #[test]
    fn test_cgroup_from_line() {
        // cgroups v2
        assert_eq!(
            CgroupEntry::from_line("0::/".to_owned()),
            Some(CgroupEntry {
                subsystems: vec![],
                root: PathBuf::from("/"),
            })
        );

        // cgroups v1
        assert_eq!(
            CgroupEntry::from_line("6:cpu,cpuacct:/kubepods/pod5b977639-f878-469b-94ee-47a4aa7e597a/dd55abbabd99bcb4d2ce17ffa77d6f811c90e09202f537c273962a8259cac8a0".to_owned()),
            Some(CgroupEntry {
                subsystems: vec!["cpu".to_owned(), "cpuacct".to_owned()],
                root: PathBuf::from("/kubepods/pod5b977639-f878-469b-94ee-47a4aa7e597a/dd55abbabd99bcb4d2ce17ffa77d6f811c90e09202f537c273962a8259cac8a0"),
            })
        );
        assert_eq!(
            CgroupEntry::from_line("5:memory:/kubepods/pod5b977639-f878-469b-94ee-47a4aa7e597a/dd55abbabd99bcb4d2ce17ffa77d6f811c90e09202f537c273962a8259cac8a0".to_owned()),
            Some(CgroupEntry {
                subsystems: vec!["memory".to_owned()],
                root: PathBuf::from("/kubepods/pod5b977639-f878-469b-94ee-47a4aa7e597a/dd55abbabd99bcb4d2ce17ffa77d6f811c90e09202f537c273962a8259cac8a0"),
            })
        );
    }

    #[test]
    fn test_mountinfo_from_line() {
        // Mount with optional field (master:305)
        assert_eq!(Mountinfo::from_line("863 758 0:63 / / rw,relatime master:305 - overlay overlay rw,seclabel,lowerdir=/var/lib/docker/overlay2/l/SUKWDHL7W7YZCJ6YI66I7Z5PR2:/var/lib/docker/overlay2/l/ORL2I23UNUGM7FYF4BSL5JUCAB:/var/lib/docker/overlay2/l/LLKK3J2EHGPF5IGGDSAQGRFHLV:/var/lib/docker/overlay2/l/JEQIUQIQTVNRBAGCU7SLV4KK4K:/var/lib/docker/overlay2/l/5DS7KSJCA7BHWAYWII7BI5DBC5:/var/lib/docker/overlay2/l/ZAGXZ62GNFPZFLNUDZ3JOZIMYR:/var/lib/docker/overlay2/l/6WVXMD372IA24ZXRWGGTIPEQPA,upperdir=/var/lib/docker/overlay2/5c7734eb769484f3469b234181365466eb30bcd7f31c912f4250c8d701637ee4/diff,workdir=/var/lib/docker/overlay2/5c7734eb769484f3469b234181365466eb30bcd7f31c912f4250c8d701637ee4/work".to_owned()),
        Some(Mountinfo{
            root: PathBuf::from("/"),
            mount_point: PathBuf::from("/"),
            fs_type: "overlay".to_owned(),
            super_opts: vec![
                "rw".to_owned(),
                "seclabel".to_owned(),
                "lowerdir=/var/lib/docker/overlay2/l/SUKWDHL7W7YZCJ6YI66I7Z5PR2:/var/lib/docker/overlay2/l/ORL2I23UNUGM7FYF4BSL5JUCAB:/var/lib/docker/overlay2/l/LLKK3J2EHGPF5IGGDSAQGRFHLV:/var/lib/docker/overlay2/l/JEQIUQIQTVNRBAGCU7SLV4KK4K:/var/lib/docker/overlay2/l/5DS7KSJCA7BHWAYWII7BI5DBC5:/var/lib/docker/overlay2/l/ZAGXZ62GNFPZFLNUDZ3JOZIMYR:/var/lib/docker/overlay2/l/6WVXMD372IA24ZXRWGGTIPEQPA".to_owned(),
                "upperdir=/var/lib/docker/overlay2/5c7734eb769484f3469b234181365466eb30bcd7f31c912f4250c8d701637ee4/diff".to_owned(),
                "workdir=/var/lib/docker/overlay2/5c7734eb769484f3469b234181365466eb30bcd7f31c912f4250c8d701637ee4/work".to_owned(),
            ],
        })
        );

        // cgroups v2
        assert_eq!(Mountinfo::from_line("868 867 0:27 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw,seclabel,nsdelegate,memory_recursiveprot".to_owned()), Some(Mountinfo{

            root: PathBuf::from("/"),
            mount_point: PathBuf::from("/sys/fs/cgroup"),
            fs_type: "cgroup2".to_owned(),
            super_opts: vec![
                "rw".to_owned(),
                "seclabel".to_owned(),
                "nsdelegate".to_owned(),
                "memory_recursiveprot".to_owned(),
            ],
        }));

        // cgroups v1
        assert_eq!(Mountinfo::from_line("702 697 0:30 /kubepods/pod5b977639-f878-469b-94ee-47a4aa7e597a/dd55abbabd99bcb4d2ce17ffa77d6f811c90e09202f537c273962a8259cac8a0 /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime master:13 - cgroup cgroup rw,memory".to_owned()), Some(Mountinfo{

            root: PathBuf::from("/kubepods/pod5b977639-f878-469b-94ee-47a4aa7e597a/dd55abbabd99bcb4d2ce17ffa77d6f811c90e09202f537c273962a8259cac8a0"),
            mount_point: PathBuf::from("/sys/fs/cgroup/memory"),
            fs_type: "cgroup".to_owned(),
            super_opts: vec![
                "rw".to_owned(),
                "memory".to_owned(),
            ],
        }));
    }
}
