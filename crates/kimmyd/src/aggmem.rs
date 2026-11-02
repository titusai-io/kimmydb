//! The memory an aggregation may hold, derived from the memory kimmyd runs under (ADR-269).
//!
//! The budget counts encoded BSON, and the memory a request really takes is 2.5 to 5 times
//! that. A fixed default (1 GiB a request) would therefore let one request end a member in a
//! 2 GiB container. The defaults are instead worked out from what the process may use: the
//! memory limit of the cgroup it runs in, else the machine's RAM (`MemTotal`). The pool is a
//! tenth of it, and one request may hold half the pool. An explicit setting always wins.
//!
//! **The process's own cgroup.** The limit is not always in the root file. Under systemd (a
//! unit or scope with `MemoryMax=`) and under a host cgroup namespace, the process's limit is
//! at `/sys/fs/cgroup/<path in /proc/self/cgroup>/memory.max`, and a limit further up the
//! tree binds the process as well, so the tightest one from the process's cgroup to the root
//! is taken. In a private cgroup namespace (the usual container) `/proc/self/cgroup` shows
//! `/` and the root file is the container's own limit, which is the walk's only step.
//! cgroup v1 is read the same way under the `memory` controller's mount, and a hybrid layout
//! (v1 controllers with a v2 `unified` tree that carries no memory controller) falls to v1.

use std::fmt;
use std::path::{Path, PathBuf};

/// The root the files below are read under: the machine's own.
pub const SYSTEM_ROOT: &str = "/";

/// The cgroup v2 mount, and the limit file in each cgroup directory of it.
pub const CGROUP_V2_DIR: &str = "sys/fs/cgroup";
pub const CGROUP_V2_FILE: &str = "memory.max";
/// The cgroup v1 `memory` controller's mount, and the limit file in each directory of it.
pub const CGROUP_V1_DIR: &str = "sys/fs/cgroup/memory";
pub const CGROUP_V1_FILE: &str = "memory.limit_in_bytes";
/// The process's own cgroups, one line each (`0::/path` for v2, `N:controllers:/path`).
pub const SELF_CGROUP: &str = "proc/self/cgroup";
pub const MEMINFO: &str = "proc/meminfo";

/// The smallest a request limit may be.
pub const MIN_LIMIT_BYTES: usize = 1024 * 1024;

/// The smallest pool the defaults will derive, so a very small container still runs a
/// pipeline of a few documents.
pub const MIN_DERIVED_POOL_BYTES: usize = 2 * 1024 * 1024;

/// What the process is assumed to have when neither a limit nor the RAM can be read.
pub const ASSUMED_MEMORY_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Where the memory figure came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Origin {
    /// A cgroup memory limit, read from this file (as the process names it: absolute).
    Cgroup(String),
    /// The machine's RAM, `MemTotal`.
    Ram,
    /// Nothing could be read: [`ASSUMED_MEMORY_BYTES`] is assumed.
    Assumed,
}

impl fmt::Display for Origin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Origin::Cgroup(path) => write!(f, "the cgroup memory limit in {path}"),
            Origin::Ram => f.write_str("the machine's RAM (MemTotal)"),
            Origin::Assumed => {
                f.write_str("an assumed 2 GiB: no memory limit or RAM could be read")
            }
        }
    }
}

/// The memory the process may use, and where that was read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Memory {
    pub bytes: u64,
    pub origin: Origin,
}

/// `MemTotal` from the text of `/proc/meminfo`, in bytes.
fn mem_total(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|l| l.starts_with("MemTotal:"))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    kib.checked_mul(1024)
}

/// A cgroup limit file's number, or `None` for `max`, an unreadable value, zero, or one so
/// large that no limit is set (cgroup v1 reports a number near 2^63 for none).
fn cgroup_limit(text: &str) -> Option<u64> {
    let n: u64 = text.trim().parse().ok()?;
    (n > 0 && n < (1 << 60)).then_some(n)
}

/// The cgroup paths of the process, from the text of `/proc/self/cgroup`: the v2 one
/// (`0::/path`) and the one the v1 `memory` controller is in (`N:memory,..:/path`), each as
/// the components of a path under the mount. A path with a `.` or `..` in it, or none, is
/// the root.
#[derive(Debug, Default, PartialEq, Eq)]
struct SelfCgroup {
    v2: Vec<String>,
    v1_memory: Vec<String>,
}

fn path_components(path: &str) -> Vec<String> {
    let parts: Vec<&str> = path.split('/').filter(|c| !c.is_empty()).collect();
    if parts.iter().any(|c| matches!(*c, "." | "..")) {
        // Never walk out of the mount on the strength of a line in a file.
        return Vec::new();
    }
    parts.into_iter().map(str::to_string).collect()
}

fn parse_self_cgroup(text: &str) -> SelfCgroup {
    let mut found = SelfCgroup::default();
    for line in text.lines() {
        // `hierarchy-id:controllers:path`; a path may hold a colon, so split in three.
        let mut fields = line.splitn(3, ':');
        let (Some(id), Some(controllers), Some(path)) =
            (fields.next(), fields.next(), fields.next())
        else {
            continue;
        };
        if id == "0" && controllers.is_empty() {
            found.v2 = path_components(path);
        } else if controllers.split(',').any(|c| c == "memory") {
            found.v1_memory = path_components(path);
        }
    }
    found
}

/// The tightest limit on the way from the cgroup `components` of `mount` up to its root,
/// and the file it was in. `read` reads a path under the root being looked at. A level with
/// no file, `max`, nonsense, zero or a limit at or above `ram` binds nothing. The deepest
/// level wins a tie.
fn tightest(
    root: &Path,
    mount: &str,
    file: &str,
    components: &[String],
    ram: Option<u64>,
) -> Option<(u64, String)> {
    let mut best: Option<(u64, String)> = None;
    for depth in (0..=components.len()).rev() {
        let mut dir = PathBuf::from(mount);
        dir.extend(&components[..depth]);
        let relative = dir.join(file);
        let Some(n) = std::fs::read_to_string(root.join(&relative))
            .ok()
            .as_deref()
            .and_then(cgroup_limit)
            .filter(|n| ram.is_none_or(|ram| *n < ram))
        else {
            continue;
        };
        if best.as_ref().is_none_or(|(b, _)| n < *b) {
            best = Some((n, format!("/{}", relative.display())));
        }
    }
    best
}

/// Work out the memory the process may use, reading the files under `root` (the machine's
/// own are under `/`; a test gives a directory laid out the same way). The cgroup limit
/// counts only when it is below the RAM (a limit above the machine is no limit); v2 is read
/// before v1.
pub fn detect_in(root: &Path) -> Memory {
    let read = |relative: &str| std::fs::read_to_string(root.join(relative)).ok();
    let ram = read(MEMINFO).as_deref().and_then(mem_total);
    // With no readable `/proc/self/cgroup` the walk is the root alone: the file a container
    // with a private cgroup namespace has, as before.
    let own = read(SELF_CGROUP).as_deref().map(parse_self_cgroup).unwrap_or_default();
    let cgroup = tightest(root, CGROUP_V2_DIR, CGROUP_V2_FILE, &own.v2, ram)
        .or_else(|| tightest(root, CGROUP_V1_DIR, CGROUP_V1_FILE, &own.v1_memory, ram));
    match (cgroup, ram) {
        (Some((bytes, path)), _) => Memory { bytes, origin: Origin::Cgroup(path) },
        (None, Some(bytes)) => Memory { bytes, origin: Origin::Ram },
        (None, None) => Memory { bytes: ASSUMED_MEMORY_BYTES, origin: Origin::Assumed },
    }
}

/// [`detect_in`] over this machine's files.
pub fn detect_system() -> Memory {
    detect_in(Path::new(SYSTEM_ROOT))
}

/// Where a resolved figure came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Source {
    /// Set by the operator (file, environment or flag).
    Explicit,
    /// Worked out from the other setting.
    FromOther,
    /// Worked out from the memory the process runs under.
    Derived(Memory),
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Source::Explicit => f.write_str("set explicitly"),
            Source::FromOther => f.write_str("derived from the other aggregate setting"),
            Source::Derived(m) => {
                write!(f, "derived from {} of {} bytes", m.origin, m.bytes)
            }
        }
    }
}

/// The two figures an aggregation is held to, and where each came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Resolved {
    pub limit: usize,
    pub pool: usize,
    pub limit_source: Source,
    pub pool_source: Source,
}

impl Resolved {
    /// The startup line: the values and where they came from.
    pub fn describe(&self) -> String {
        format!(
            "aggregate memory: pool {} bytes ({}), per-request limit {} bytes ({})",
            self.pool, self.pool_source, self.limit, self.limit_source
        )
    }
}

/// Resolve the limit and the pool from what the operator set (`None` is unset) and the memory
/// the process runs under. The pool is a tenth of it (at least [`MIN_DERIVED_POOL_BYTES`])
/// and a request may hold half the pool (at least [`MIN_LIMIT_BYTES`], at most the pool).
/// A limit set alone raises the derived pool to at least that limit; a pool set alone
/// derives the limit from it.
pub fn resolve(limit: Option<usize>, pool: Option<usize>, memory: Memory) -> Resolved {
    let derived_pool =
        usize::try_from(memory.bytes / 10).unwrap_or(usize::MAX).max(MIN_DERIVED_POOL_BYTES);
    let derived = || Source::Derived(memory.clone());
    let half = |pool: usize| (pool / 2).max(MIN_LIMIT_BYTES).min(pool);
    match (limit, pool) {
        (None, None) => Resolved {
            limit: half(derived_pool),
            pool: derived_pool,
            limit_source: derived(),
            pool_source: derived(),
        },
        (Some(limit), None) => Resolved {
            limit,
            pool: derived_pool.max(limit),
            limit_source: Source::Explicit,
            pool_source: if derived_pool >= limit { derived() } else { Source::FromOther },
        },
        (None, Some(pool)) => Resolved {
            limit: half(pool),
            pool,
            limit_source: Source::FromOther,
            pool_source: Source::Explicit,
        },
        (Some(limit), Some(pool)) => {
            Resolved { limit, pool, limit_source: Source::Explicit, pool_source: Source::Explicit }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1 << 30;

    /// A directory laid out as `/` is: `proc/..`, `sys/fs/cgroup/..`.
    struct Machine(tempfile::TempDir);

    impl Machine {
        fn new() -> Self {
            Machine(tempfile::tempdir().unwrap())
        }

        /// A machine with `gib` GiB of RAM.
        fn with_ram(gib: u64) -> Self {
            let machine = Self::new();
            machine.file(
                MEMINFO,
                &format!("MemTotal:       {} kB\nMemFree:        1 kB\n", gib * 1024 * 1024),
            );
            machine
        }

        fn file(&self, relative: &str, text: &str) -> &Self {
            let path = self.0.path().join(relative);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, text).unwrap();
            self
        }

        fn detect(&self) -> Memory {
            detect_in(self.0.path())
        }
    }

    fn cgroup_origin(path: &str) -> Origin {
        Origin::Cgroup(path.to_string())
    }

    #[test]
    fn a_two_gib_container_gets_a_pool_of_about_two_hundred_mib_and_half_that_a_request() {
        // The root file only, as a container with a private cgroup namespace has it.
        let machine = Machine::with_ram(32);
        machine.file(SELF_CGROUP, "0::/\n").file("sys/fs/cgroup/memory.max", "2147483648\n");
        let memory = machine.detect();
        assert_eq!(
            memory,
            Memory { bytes: 2 * GIB, origin: cgroup_origin("/sys/fs/cgroup/memory.max") }
        );
        let r = resolve(None, None, memory.clone());
        assert_eq!(r.pool, 214_748_364, "a tenth of 2 GiB");
        assert_eq!(r.limit, 107_374_182, "half the pool");
        assert_eq!(r.limit_source, Source::Derived(memory));
        assert!(r.describe().contains("the cgroup memory limit in /sys/fs/cgroup/memory.max"));
        assert!(r.describe().contains("214748364"), "{}", r.describe());
    }

    #[test]
    fn the_process_own_cgroup_is_read_when_it_is_tighter_than_its_parents() {
        // A systemd service with MemoryMax=2G in a slice that has 8G, under a root with none.
        let machine = Machine::with_ram(32);
        machine
            .file(SELF_CGROUP, "0::/system.slice/kimmy.service\n")
            .file("sys/fs/cgroup/memory.max", "max\n")
            .file("sys/fs/cgroup/system.slice/memory.max", "8589934592\n")
            .file("sys/fs/cgroup/system.slice/kimmy.service/memory.max", "2147483648\n");
        assert_eq!(
            machine.detect(),
            Memory {
                bytes: 2 * GIB,
                origin: cgroup_origin("/sys/fs/cgroup/system.slice/kimmy.service/memory.max")
            }
        );
    }

    #[test]
    fn a_parent_cgroup_that_is_tighter_than_the_child_binds_the_process() {
        let machine = Machine::with_ram(32);
        machine
            .file(SELF_CGROUP, "0::/a/b/c\n")
            .file("sys/fs/cgroup/memory.max", "max\n")
            .file("sys/fs/cgroup/a/memory.max", "1073741824\n")
            .file("sys/fs/cgroup/a/b/memory.max", "max\n")
            .file("sys/fs/cgroup/a/b/c/memory.max", "4294967296\n");
        assert_eq!(
            machine.detect(),
            Memory { bytes: GIB, origin: cgroup_origin("/sys/fs/cgroup/a/memory.max") }
        );
        // The root's own limit counts too, and the tightest of all wins.
        machine.file("sys/fs/cgroup/memory.max", "536870912\n");
        assert_eq!(machine.detect().bytes, GIB / 2);
        assert_eq!(machine.detect().origin, cgroup_origin("/sys/fs/cgroup/memory.max"));
    }

    #[test]
    fn two_levels_with_the_same_limit_name_the_deepest() {
        let machine = Machine::with_ram(32);
        machine
            .file(SELF_CGROUP, "0::/a/b\n")
            .file("sys/fs/cgroup/a/memory.max", "2147483648")
            .file("sys/fs/cgroup/a/b/memory.max", "2147483648");
        assert_eq!(machine.detect().origin, cgroup_origin("/sys/fs/cgroup/a/b/memory.max"));
    }

    #[test]
    fn max_at_every_level_leaves_the_machines_ram() {
        let machine = Machine::with_ram(8);
        machine
            .file(SELF_CGROUP, "0::/a/b\n")
            .file("sys/fs/cgroup/memory.max", "max\n")
            .file("sys/fs/cgroup/a/memory.max", "max\n")
            .file("sys/fs/cgroup/a/b/memory.max", "max\n");
        let memory = machine.detect();
        assert_eq!(memory, Memory { bytes: 8 * GIB, origin: Origin::Ram });
        let r = resolve(None, None, memory);
        assert_eq!((r.pool, r.limit), (858_993_459, 429_496_729));
        assert!(r.describe().contains("RAM"), "{}", r.describe());
    }

    #[test]
    fn the_v1_memory_controllers_own_path_is_read_and_walked_up() {
        // Several v1 lines, the memory controller's among them, and no v2 line.
        let machine = Machine::with_ram(16);
        machine
            .file(
                SELF_CGROUP,
                "12:cpu,cpuacct:/docker/abc\n11:memory:/docker/abc\n1:name=systemd:/docker/abc\n",
            )
            .file("sys/fs/cgroup/memory/memory.limit_in_bytes", "9223372036854771712\n")
            .file("sys/fs/cgroup/memory/docker/memory.limit_in_bytes", "4294967296\n")
            .file("sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes", "1073741824\n");
        assert_eq!(
            machine.detect(),
            Memory {
                bytes: GIB,
                origin: cgroup_origin("/sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes")
            }
        );
        // A controller list that holds memory among others is the memory controller's.
        let machine = Machine::with_ram(16);
        machine
            .file(SELF_CGROUP, "4:cpu,memory,blkio:/x\n")
            .file("sys/fs/cgroup/memory/x/memory.limit_in_bytes", "2147483648\n");
        assert_eq!(machine.detect().bytes, 2 * GIB);
    }

    #[test]
    fn a_v1_root_file_alone_is_the_limit_as_before() {
        let machine = Machine::with_ram(16);
        machine
            .file(SELF_CGROUP, "11:memory:/\n")
            .file("sys/fs/cgroup/memory/memory.limit_in_bytes", "1073741824\n");
        assert_eq!(
            machine.detect(),
            Memory {
                bytes: GIB,
                origin: cgroup_origin("/sys/fs/cgroup/memory/memory.limit_in_bytes")
            }
        );
        // v1 with no limit reports a number near 2^63.
        machine.file("sys/fs/cgroup/memory/memory.limit_in_bytes", "9223372036854771712\n");
        assert_eq!(machine.detect().origin, Origin::Ram);
    }

    #[test]
    fn v2_is_read_before_v1_when_both_are_there_and_a_hybrid_layout_falls_to_v1() {
        // Several v1 lines and one v2 line, and both trees carry a limit: v2 first.
        let machine = Machine::with_ram(32);
        machine
            .file(
                SELF_CGROUP,
                "13:rdma:/\n12:pids:/user.slice\n6:memory:/user.slice/u1\n0::/user.slice/u1\n",
            )
            .file("sys/fs/cgroup/user.slice/u1/memory.max", "2147483648\n")
            .file("sys/fs/cgroup/memory/user.slice/u1/memory.limit_in_bytes", "1073741824\n");
        assert_eq!(machine.detect().bytes, 2 * GIB);

        // A hybrid layout: the v1 controllers are mounted and the v2 `unified` tree has no
        // memory controller, so there is no `memory.max` where the v2 path points.
        let machine = Machine::with_ram(32);
        machine
            .file(
                SELF_CGROUP,
                "11:memory:/system.slice/kimmy.service\n1:name=systemd:/system.slice/kimmy.service\n\
                 0::/system.slice/kimmy.service\n",
            )
            .file("sys/fs/cgroup/unified/system.slice/kimmy.service/cgroup.procs", "1\n")
            .file(
                "sys/fs/cgroup/memory/system.slice/kimmy.service/memory.limit_in_bytes",
                "3221225472\n",
            );
        assert_eq!(
            machine.detect(),
            Memory {
                bytes: 3 * GIB,
                origin: cgroup_origin(
                    "/sys/fs/cgroup/memory/system.slice/kimmy.service/memory.limit_in_bytes"
                )
            }
        );
    }

    #[test]
    fn a_level_without_a_file_binds_nothing_and_the_walk_goes_on() {
        // The service's directory has no `memory.max` (the controller is not enabled for it),
        // the slice above does.
        let machine = Machine::with_ram(32);
        machine
            .file(SELF_CGROUP, "0::/s.slice/svc\n")
            .file("sys/fs/cgroup/s.slice/svc/cgroup.procs", "1\n")
            .file("sys/fs/cgroup/s.slice/memory.max", "2147483648\n");
        assert_eq!(machine.detect().origin, cgroup_origin("/sys/fs/cgroup/s.slice/memory.max"));
        // A path that names directories that are not there at all is the root's, as a path a
        // private cgroup namespace cannot resolve is.
        let machine = Machine::with_ram(32);
        machine
            .file(SELF_CGROUP, "0::/nowhere/at/all\n")
            .file("sys/fs/cgroup/memory.max", "2147483648\n");
        assert_eq!(machine.detect().origin, cgroup_origin("/sys/fs/cgroup/memory.max"));
    }

    #[test]
    fn a_limit_at_or_above_the_ram_binds_nothing_at_its_level() {
        let machine = Machine::with_ram(16);
        machine
            .file(SELF_CGROUP, "0::/a\n")
            .file("sys/fs/cgroup/a/memory.max", "68719476736\n")
            .file("sys/fs/cgroup/memory.max", "4294967296\n");
        assert_eq!(machine.detect().origin, cgroup_origin("/sys/fs/cgroup/memory.max"));
        machine.file("sys/fs/cgroup/memory.max", "68719476736\n");
        assert_eq!(machine.detect().origin, Origin::Ram);
        // A limit of exactly the RAM is no limit either; one byte under it is one.
        machine.file("sys/fs/cgroup/memory.max", "17179869184\n");
        machine.file("sys/fs/cgroup/a/memory.max", "max\n");
        assert_eq!(machine.detect().origin, Origin::Ram);
        machine.file("sys/fs/cgroup/memory.max", "17179869183\n");
        assert_eq!(machine.detect().bytes, 17_179_869_183);
    }

    #[test]
    fn a_path_with_a_trailing_newline_or_crlf_odd_text_and_dots_is_handled() {
        let machine = Machine::with_ram(32);
        machine
            .file(SELF_CGROUP, "0::/a/b\n\n")
            .file("sys/fs/cgroup/a/b/memory.max", "2147483648")
            .file("sys/fs/cgroup/memory.max", "max");
        assert_eq!(machine.detect().bytes, 2 * GIB, "no trailing newline in the limit");
        // A path with `..` is not followed out of the mount: the root's file is read.
        let machine = Machine::with_ram(32);
        machine
            .file(SELF_CGROUP, "0::/../../etc\n")
            .file("sys/fs/cgroup/memory.max", "1073741824\n")
            .file("etc/memory.max", "1024\n");
        assert_eq!(machine.detect().bytes, GIB);
        // A colon in the path, and a line that is not a cgroup line, are not an error.
        let machine = Machine::with_ram(32);
        machine
            .file(SELF_CGROUP, "garbage\n0::/a:b\n")
            .file("sys/fs/cgroup/a:b/memory.max", "1073741824\n");
        assert_eq!(machine.detect().bytes, GIB);
    }

    #[test]
    fn the_parts_of_a_cgroup_file_are_read_as_the_kernel_writes_them() {
        assert_eq!(cgroup_limit("2147483648\n"), Some(2 * GIB));
        assert_eq!(cgroup_limit("max\n"), None);
        assert_eq!(cgroup_limit("lots"), None);
        assert_eq!(cgroup_limit("0"), None);
        assert_eq!(cgroup_limit("9223372036854771712"), None);
        // The largest that still counts is under 2^60; 2^60 itself is "no limit".
        assert_eq!(cgroup_limit("1152921504606846975"), Some((1 << 60) - 1));
        assert_eq!(cgroup_limit("1152921504606846976"), None);
        let own = parse_self_cgroup("0::/a/b\n5:memory:/c\n7:cpu:/d\n");
        assert_eq!(own.v2, ["a", "b"]);
        assert_eq!(own.v1_memory, ["c"]);
        // The v2 line is hierarchy 0 with no controllers; neither alone is it, and a line of
        // another controller is not the memory controller's.
        assert_eq!(parse_self_cgroup("5::/x\n"), SelfCgroup::default());
        assert_eq!(parse_self_cgroup("0:cpu:/y\n"), SelfCgroup::default());
        assert_eq!(parse_self_cgroup("3:cpu,cpuacct:/z\n"), SelfCgroup::default());
        assert_eq!(parse_self_cgroup("0::/\n"), SelfCgroup::default());
        assert_eq!(parse_self_cgroup(""), SelfCgroup::default());
        assert_eq!(mem_total("MemTotal:  1024 kB\n"), Some(1 << 20));
        assert_eq!(mem_total("nothing"), None);
    }

    #[test]
    fn with_nothing_to_read_two_gib_is_assumed() {
        let machine = Machine::new();
        let memory = machine.detect();
        assert_eq!(memory, Memory { bytes: 2 * GIB, origin: Origin::Assumed });
        assert!(resolve(None, None, memory).describe().contains("assumed"));
        // A cgroup v1 file that reports no limit, with no RAM to compare it with, is no limit.
        machine.file("sys/fs/cgroup/memory/memory.limit_in_bytes", "9223372036854771712\n");
        assert_eq!(machine.detect().origin, Origin::Assumed);
        // A cgroup limit alone, with no /proc/meminfo and no /proc/self/cgroup, still counts.
        machine.file("sys/fs/cgroup/memory.max", "1073741824");
        assert_eq!(machine.detect().origin, cgroup_origin("/sys/fs/cgroup/memory.max"));
        // And nonsense in either file is ignored.
        let machine = Machine::with_ram(16);
        machine
            .file("sys/fs/cgroup/memory.max", "lots\n")
            .file("sys/fs/cgroup/memory/memory.limit_in_bytes", "0\n");
        assert_eq!(machine.detect().origin, Origin::Ram);
    }

    #[test]
    fn an_explicit_value_wins_and_the_other_follows_from_it() {
        let memory = Memory { bytes: 2 * GIB, origin: Origin::Ram };
        let both = resolve(Some(50 << 20), Some(70 << 20), memory.clone());
        assert_eq!((both.limit, both.pool), (50 << 20, 70 << 20));
        assert_eq!((both.limit_source, both.pool_source), (Source::Explicit, Source::Explicit));
        // A limit alone keeps the derived pool, raised to hold it.
        let r = resolve(Some(50 << 20), None, memory.clone());
        assert_eq!((r.limit, r.pool), (50 << 20, 214_748_364));
        assert_eq!(r.pool_source, Source::Derived(memory.clone()));
        let r = resolve(Some(512 << 20), None, memory.clone());
        assert_eq!((r.limit, r.pool), (512 << 20, 512 << 20));
        assert_eq!(r.pool_source, Source::FromOther);
        // A pool alone gives half of it to a request.
        let r = resolve(None, Some(64 << 20), memory);
        assert_eq!((r.limit, r.pool), (32 << 20, 64 << 20));
        assert_eq!(r.limit_source, Source::FromOther);
    }

    #[test]
    fn a_tiny_machine_still_runs_small_pipelines() {
        let memory = Memory { bytes: 16 << 20, origin: Origin::Ram };
        let r = resolve(None, None, memory.clone());
        assert_eq!(r.pool, MIN_DERIVED_POOL_BYTES);
        assert_eq!(r.limit, MIN_LIMIT_BYTES);
        assert!(r.limit <= r.pool);
        // A pool of exactly the smallest, set alone, still holds one request's limit.
        let r = resolve(None, Some(MIN_LIMIT_BYTES), memory);
        assert_eq!((r.limit, r.pool), (MIN_LIMIT_BYTES, MIN_LIMIT_BYTES));
    }

    #[test]
    fn the_real_files_of_this_machine_give_a_usable_answer() {
        let memory = detect_system();
        assert!(memory.bytes >= 16 << 20, "{memory:?}");
        let r = resolve(None, None, memory);
        assert!(r.limit >= MIN_LIMIT_BYTES && r.pool >= r.limit);
    }
}
