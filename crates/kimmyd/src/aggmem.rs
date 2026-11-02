//! The memory an aggregation may hold, derived from the memory kimmyd runs under (ADR-269).
//!
//! The budget counts encoded BSON, and the memory a request really takes is 2.5 to 5 times
//! that. A fixed default (1 GiB a request) would therefore let one request end a member in a
//! 2 GiB container. The defaults are instead worked out from what the process may use: the
//! container's memory limit when it has one (cgroup v2 `memory.max`, else cgroup v1
//! `memory.limit_in_bytes`), else the machine's RAM (`MemTotal`). The pool is a tenth of
//! it, and one request may hold half the pool. An explicit setting always wins.

use std::fmt;

/// The files that say how much memory the process may use. Passed as a reader so a test can
/// give it fake ones.
pub const CGROUP_V2: &str = "/sys/fs/cgroup/memory.max";
pub const CGROUP_V1: &str = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
pub const MEMINFO: &str = "/proc/meminfo";

/// The smallest a request limit may be.
pub const MIN_LIMIT_BYTES: usize = 1024 * 1024;

/// The smallest pool the defaults will derive, so a very small container still runs a
/// pipeline of a few documents.
pub const MIN_DERIVED_POOL_BYTES: usize = 2 * 1024 * 1024;

/// What the process is assumed to have when neither a limit nor the RAM can be read.
pub const ASSUMED_MEMORY_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Where the memory figure came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Origin {
    /// A cgroup memory limit, read from this file.
    Cgroup(&'static str),
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// A cgroup limit file's number, or `None` for `max`, an unreadable value or one so large
/// that no limit is set (cgroup v1 reports a number near 2^63 for none).
fn cgroup_limit(text: &str) -> Option<u64> {
    let n: u64 = text.trim().parse().ok()?;
    (n > 0 && n < (1 << 60)).then_some(n)
}

/// Work out the memory the process may use. `read` returns a file's text, or `None` when it
/// cannot be read. A cgroup limit counts only when it is below the RAM (a limit above the
/// machine is no limit).
pub fn detect(read: impl Fn(&str) -> Option<String>) -> Memory {
    let ram = read(MEMINFO).as_deref().and_then(mem_total);
    let cgroup = [CGROUP_V2, CGROUP_V1]
        .into_iter()
        .find_map(|path| read(path).as_deref().and_then(cgroup_limit).map(|n| (n, path)))
        .filter(|(n, _)| ram.is_none_or(|ram| *n < ram));
    match (cgroup, ram) {
        (Some((bytes, path)), _) => Memory { bytes, origin: Origin::Cgroup(path) },
        (None, Some(bytes)) => Memory { bytes, origin: Origin::Ram },
        (None, None) => Memory { bytes: ASSUMED_MEMORY_BYTES, origin: Origin::Assumed },
    }
}

/// [`detect`] over this machine's files.
pub fn detect_system() -> Memory {
    detect(|path| std::fs::read_to_string(path).ok())
}

/// Where a resolved figure came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    let derived = Source::Derived(memory);
    let half = |pool: usize| (pool / 2).max(MIN_LIMIT_BYTES).min(pool);
    match (limit, pool) {
        (None, None) => Resolved {
            limit: half(derived_pool),
            pool: derived_pool,
            limit_source: derived,
            pool_source: derived,
        },
        (Some(limit), None) => Resolved {
            limit,
            pool: derived_pool.max(limit),
            limit_source: Source::Explicit,
            pool_source: if derived_pool >= limit { derived } else { Source::FromOther },
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
    use std::collections::HashMap;

    const GIB: u64 = 1 << 30;

    /// A reader over fake files.
    fn files(list: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let map: HashMap<String, String> =
            list.iter().map(|(k, v)| ((*k).to_string(), (*v).to_string())).collect();
        move |path| map.get(path).cloned()
    }

    fn meminfo(kib: u64) -> String {
        format!("MemTotal:       {kib} kB\nMemFree:        1 kB\n")
    }

    #[test]
    fn a_two_gib_container_gets_a_pool_of_about_two_hundred_mib_and_half_that_a_request() {
        let ram = meminfo(32 * 1024 * 1024);
        let read = files(&[(MEMINFO, &ram), (CGROUP_V2, "2147483648\n")]);
        let memory = detect(read);
        assert_eq!(memory, Memory { bytes: 2 * GIB, origin: Origin::Cgroup(CGROUP_V2) });
        let r = resolve(None, None, memory);
        assert_eq!(r.pool, 214_748_364, "a tenth of 2 GiB");
        assert_eq!(r.limit, 107_374_182, "half the pool");
        assert_eq!(r.limit_source, Source::Derived(memory));
        assert!(r.describe().contains("the cgroup memory limit in /sys/fs/cgroup/memory.max"));
        assert!(r.describe().contains("214748364"), "{}", r.describe());
    }

    #[test]
    fn a_cgroup_that_says_max_leaves_the_machines_ram() {
        let ram = meminfo(8 * 1024 * 1024); // 8 GiB
        let memory = detect(files(&[(MEMINFO, &ram), (CGROUP_V2, "max\n")]));
        assert_eq!(memory, Memory { bytes: 8 * GIB, origin: Origin::Ram });
        let r = resolve(None, None, memory);
        assert_eq!((r.pool, r.limit), (858_993_459, 429_496_729));
        assert!(r.describe().contains("RAM"), "{}", r.describe());
    }

    #[test]
    fn the_older_cgroup_file_counts_and_an_unset_one_does_not() {
        let ram = meminfo(16 * 1024 * 1024);
        // cgroup v1 with a 1 GiB limit.
        let memory = detect(files(&[(MEMINFO, &ram), (CGROUP_V1, "1073741824\n")]));
        assert_eq!(memory, Memory { bytes: GIB, origin: Origin::Cgroup(CGROUP_V1) });
        // cgroup v1 with no limit reports a number near 2^63.
        let memory = detect(files(&[(MEMINFO, &ram), (CGROUP_V1, "9223372036854771712\n")]));
        assert_eq!(memory.origin, Origin::Ram);
        // A limit above the machine's RAM is no limit; v2 is read before v1.
        let memory = detect(files(&[(MEMINFO, &ram), (CGROUP_V2, "68719476736\n")]));
        assert_eq!(memory.origin, Origin::Ram);
        let memory =
            detect(files(&[(MEMINFO, &ram), (CGROUP_V2, "2147483648"), (CGROUP_V1, "1073741824")]));
        assert_eq!(memory.bytes, 2 * GIB);
        // Nonsense in either file is ignored.
        let memory = detect(files(&[(MEMINFO, &ram), (CGROUP_V2, "lots\n"), (CGROUP_V1, "0\n")]));
        assert_eq!(memory.origin, Origin::Ram);
    }

    #[test]
    fn with_nothing_to_read_two_gib_is_assumed() {
        let memory = detect(files(&[]));
        assert_eq!(memory, Memory { bytes: 2 * GIB, origin: Origin::Assumed });
        assert!(resolve(None, None, memory).describe().contains("assumed"));
        // A cgroup limit alone, with no /proc/meminfo, still counts.
        let memory = detect(files(&[(CGROUP_V2, "1073741824")]));
        assert_eq!(memory.origin, Origin::Cgroup(CGROUP_V2));
    }

    #[test]
    fn an_explicit_value_wins_and_the_other_follows_from_it() {
        let memory = Memory { bytes: 2 * GIB, origin: Origin::Ram };
        let both = resolve(Some(50 << 20), Some(70 << 20), memory);
        assert_eq!((both.limit, both.pool), (50 << 20, 70 << 20));
        assert_eq!((both.limit_source, both.pool_source), (Source::Explicit, Source::Explicit));
        // A limit alone keeps the derived pool, raised to hold it.
        let r = resolve(Some(50 << 20), None, memory);
        assert_eq!((r.limit, r.pool), (50 << 20, 214_748_364));
        assert_eq!(r.pool_source, Source::Derived(memory));
        let r = resolve(Some(512 << 20), None, memory);
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
        let r = resolve(None, None, memory);
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
