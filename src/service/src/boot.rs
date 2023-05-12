// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Service boot helpers.

/// Emits a tracing event with system diagnostic information, for use during
/// service boot.
// NOTE(benesch): this is a macro because the tracing crate does not support
// dynamic targets, and we want the target of this event to be the name of
// the root module (e.g., "clusterd" or "environmentd"), not "mz_service".
#[macro_export]
macro_rules! emit_boot_diagnostics {
    ($build_info:expr $(,)?) => {{
        use $crate::boot::r#private::sysinfo::{System, SystemExt, CpuExt, CpuRefreshKind};
        use $crate::boot::r#private::tracing::info;
        use $crate::boot::r#private::tracing::level_filters::LevelFilter;
        use $crate::boot::r#private::cgroup;
        use $crate::boot::r#private::os_info;

        use $crate::boot::r#private::mz_build_info::BuildInfo;
        use $crate::boot::r#private::mz_ore::option::OptionExt;

        let build_info = $build_info;
        let os = os_info::get();
        let mut system = System::new();
        system.refresh_memory();
        system.refresh_cpu_specifics(CpuRefreshKind::new().with_frequency());
        let cpus = system.cpus();
        let limits = cgroup::detect_limits();
        info!(
            os.os_type = %os.os_type(),
            os.version = %os.version(),
            os.bitness = %os.bitness(),
            build.version = build_info.version,
            build.sha = build_info.sha,
            build.time = build_info.time,
            cpus.logical = cpus.len(),
            cpus.physical = %system.physical_core_count().display_or("<unknown>"),
            cpu0.brand = cpus[0].brand(),
            cpu0.frequency = cpus[0].frequency(),
            memory.total = system.total_memory(),
            memory.used = system.used_memory(),
            memory.limit = %limits.as_ref().and_then(|l| l.memory_max).display_or("<unknown>"),
            swap.total = system.total_swap(),
            swap.used = system.used_swap(),
            swap.limit = %limits.as_ref().and_then(|l| l.swap_max).display_or("<unknown>"),
            tracing.max_level = %LevelFilter::current(),
            "booting",
        );
    }};
}

// Implementation for the `emit_boot_event` macro.
#[doc(hidden)]
pub mod r#private {
    pub use {mz_build_info, mz_ore, os_info, sysinfo, tracing};

    // NOTE(benesch): this module contains a lot of complexity just to detect
    // the cgroup memory limit. Is it worth now that we're cloud native? It was
    // more obviously worthwhile in the days of the binary, when we needed to
    // put as much information about the user's system as possible into the log
    // files, to increase our ability to debug issues. Nowadays we can just look
    // in Kubernetes for the limit.
    pub mod cgroup {
        use std::fs::{self, File};
        use std::io::{BufRead, BufReader};
        use std::path::{Path, PathBuf};

        /// An entry in /proc/self/cgroup.
        #[derive(Debug, PartialEq)]
        struct CgroupEntry {
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

        /// Parses /proc/self/cgroup into a `Vec<CgroupEntry>`, if the file exists.
        fn parse_proc_self_cgroup() -> Option<Vec<CgroupEntry>> {
            let file = File::open("/proc/self/cgroup").ok()?;
            let file = BufReader::new(file);
            Some(
                file.lines()
                    .flatten()
                    .filter_map(CgroupEntry::from_line)
                    .collect(),
            )
        }

        /// An entry in /proc/self/mountinfo.
        #[derive(Debug, PartialEq)]
        struct MountInfo {
            root: PathBuf,
            mount_point: PathBuf,
            fs_type: String,
            super_opts: Vec<String>,
        }

        impl MountInfo {
            fn from_line(line: String) -> Option<MountInfo> {
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

                Some(MountInfo {
                    root,
                    mount_point,
                    fs_type,
                    super_opts,
                })
            }
        }

        /// Parses /proc/self/mountinfo into vectors of Mountinfo objects,
        /// returning (v2_mounts, v1_mounts).
        fn parse_proc_self_mountinfo() -> Option<(Vec<MountInfo>, Vec<MountInfo>)> {
            let file = File::open("/proc/self/mountinfo").ok()?;
            let file = BufReader::new(file);
            Some(
                file.lines()
                    .flatten()
                    .filter_map(MountInfo::from_line)
                    .filter(|mi| mi.fs_type == "cgroup" || mi.fs_type == "cgroup2")
                    .partition(|mi| mi.fs_type == "cgroup2"),
            )
        }

        /// Represents a cgroup limits, with both memory and swap maximums if they
        /// exist.
        ///
        /// Fields will be `None` if a limit does not exist or when running on a
        /// platform without cgroup support (i.e., non-Linux platforms).
        #[derive(Debug)]
        pub struct Limits {
            /// Maximum memory limit, in bytes, if a limit exists.
            pub memory_max: Option<usize>,
            /// Maximum swap limit, in bytes, if a limit exists.
            pub swap_max: Option<usize>,
        }

        fn parse_file<P>(path: P) -> Option<usize>
        where
            P: AsRef<Path>,
        {
            let s = fs::read_to_string(&path).ok()?;
            s.trim().parse().ok()
        }

        /// Finds the mountpoint corresponding to the provided cgroup v2, and reads
        /// the memory limits within.
        fn read_v2_memory_limit(cgroups: &[CgroupEntry], mounts: &[MountInfo]) -> Option<Limits> {
            // cgroups v2 only supports a single cgroup per process
            let mount = mounts.first()?;
            if mount.root != cgroups.first()?.root {
                // We don't support mixed v2/v1.
                return None;
            }
            let mount_point = &mount.mount_point;
            let controllers = fs::read_to_string(mount_point.join("cgroup.controllers")).ok()?;
            let mut controllers = controllers.trim().split(' ');
            if controllers.any(|c| c == "memory") {
                let memory_max = parse_file(mount_point.join("memory.max"));
                // Unlike v1, this is only the swap, not swap + memory.
                let swap_max = parse_file(mount_point.join("memory.swap.max"));
                return Some(Limits {
                    memory_max,
                    swap_max,
                });
            }
            None
        }

        /// Finds the cgroup v1 and mountpoint combination containing the memory
        /// controller, and reads the memory limits within.
        fn read_v1_memory_limit(cgroups: &[CgroupEntry], mounts: &[MountInfo]) -> Option<Limits> {
            // https://www.kernel.org/doc/Documentation/cgroup-v1/memory.txt
            let memory_cgroup = cgroups
                .into_iter()
                .find(|cgroup| cgroup.subsystems.iter().any(|s| s == "memory"))?;
            let memory_mount = mounts.iter().find(|mi| {
                mi.root == memory_cgroup.root && mi.super_opts.iter().any(|o| o == "memory")
            })?;
            let mount_point = &memory_mount.mount_point;
            let memory_max = parse_file(mount_point.join("memory.limit_in_bytes"));
            // This is memory + swap, not just swap.
            let memsw_max = parse_file(mount_point.join("memory.memsw.limit_in_bytes"));
            let swap_max = match (memory_max, memsw_max) {
                (Some(max), Some(memsw_max)) => Some(memsw_max - max),
                _ => None,
            };
            Some(Limits {
                memory_max,
                swap_max,
            })
        }

        /// Returns the cgroup (v1 or v2) limits, if tjey exists.
        pub fn detect_limits() -> Option<Limits> {
            let (v2_mounts, v1_mounts) = parse_proc_self_mountinfo()?;
            let cgroups = parse_proc_self_cgroup()?;
            if !v2_mounts.is_empty() {
                return read_v2_memory_limit(&cgroups, &v2_mounts);
            }
            read_v1_memory_limit(&cgroups, &v1_mounts)
        }

        #[cfg(test)]
        mod tests {
            use std::path::PathBuf;

            use super::{CgroupEntry, MountInfo};

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
                assert_eq!(MountInfo::from_line("863 758 0:63 / / rw,relatime master:305 - overlay overlay rw,seclabel,lowerdir=/var/lib/docker/overlay2/l/SUKWDHL7W7YZCJ6YI66I7Z5PR2:/var/lib/docker/overlay2/l/ORL2I23UNUGM7FYF4BSL5JUCAB:/var/lib/docker/overlay2/l/LLKK3J2EHGPF5IGGDSAQGRFHLV:/var/lib/docker/overlay2/l/JEQIUQIQTVNRBAGCU7SLV4KK4K:/var/lib/docker/overlay2/l/5DS7KSJCA7BHWAYWII7BI5DBC5:/var/lib/docker/overlay2/l/ZAGXZ62GNFPZFLNUDZ3JOZIMYR:/var/lib/docker/overlay2/l/6WVXMD372IA24ZXRWGGTIPEQPA,upperdir=/var/lib/docker/overlay2/5c7734eb769484f3469b234181365466eb30bcd7f31c912f4250c8d701637ee4/diff,workdir=/var/lib/docker/overlay2/5c7734eb769484f3469b234181365466eb30bcd7f31c912f4250c8d701637ee4/work".to_owned()),
                Some(MountInfo{
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
                assert_eq!(MountInfo::from_line("868 867 0:27 / /sys/fs/cgroup ro,nosuid,nodev,noexec,relatime - cgroup2 cgroup rw,seclabel,nsdelegate,memory_recursiveprot".to_owned()), Some(MountInfo{

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
                assert_eq!(MountInfo::from_line("702 697 0:30 /kubepods/pod5b977639-f878-469b-94ee-47a4aa7e597a/dd55abbabd99bcb4d2ce17ffa77d6f811c90e09202f537c273962a8259cac8a0 /sys/fs/cgroup/memory ro,nosuid,nodev,noexec,relatime master:13 - cgroup cgroup rw,memory".to_owned()), Some(MountInfo{

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
    }
}
