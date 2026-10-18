//! What `hnsw_rs`'s reader would do with a snapshot's two files, decided
//! before it is asked to (ADR-241).
//!
//! `HnswIo::load_hnsw` trusts its files. A file it cannot read is not always
//! an error it returns: in 0.3.5 a description in dump format 2, and **any**
//! failure reading a point's vector (a data file cut short, say), end the
//! process with `std::process::exit(1)`; a count or a length the file
//! declares is allocated before it is read, so a corrupt one aborts the
//! process when the allocation fails; a vector length shorter than the
//! dimension is read past the end of its buffer (`slice::from_raw_parts` over
//! the declared dimension); and a dozen `assert!`s, `unwrap`s and indexes
//! panic. `catch_unwind` around the load contains only the panics. A snapshot
//! is derived state, and a file the library cannot be trusted with must cost
//! a rebuild, never the node, so [`check`] reads both files the way the
//! loader does and refuses anything that would take one of those paths, and
//! anything that is not the dump this build writes.
//!
//! The layout is `hnsw_rs` 0.3.5's (`hnswio.rs`), native-endian, `usize` as
//! eight bytes:
//!
//! - graph file (`index.hnsw.graph`): the description (`u32` magic, `u8`
//!   dump mode, `u8` max connections, an `f64` level scale in format 4 only,
//!   `u8` layer count, `usize` ef, `usize` point count, `usize` dimension, the
//!   distance's type name and the element's type name, each a `usize` length
//!   then the bytes); then a `u8` layer count and, for each layer, `u32`
//!   `MAGICLAYER`, a `usize` point count and each point (`u32` `MAGICPOINT`,
//!   `usize` origin id, `u8` layer, `i32` rank, and for each of the
//!   description's layers a `usize` neighbour count and each neighbour: a
//!   `usize` origin id, `u8` layer, `i32` rank, `f32` distance); then the
//!   entry point (`usize` origin id, `u8` layer, `i32` rank).
//! - data file (`index.hnsw.data`): `u32` `MAGICDATAP` and a `usize`
//!   dimension; then, for each point in the graph file's order, `u32`
//!   `MAGICDATAP`, a `u64` origin id, a `u64` byte length and the vector.
//!
//! The check is a full read of both files, which a load does anyway, and
//! allocates nothing a file declares: one flag per point and one vector's
//! bytes.

use std::fs::File;
use std::io::{BufReader, ErrorKind, Read};
use std::path::Path;

/// The file names `HnswIndex::save` dumps under: `<BASENAME>.hnsw.graph` and
/// `<BASENAME>.hnsw.data`.
pub(crate) const BASENAME: &str = "index";

/// The magics, as `hnsw_rs` 0.3.5 defines them.
const MAGICDESCR_2: u32 = 0x002a_677f;
const MAGICDESCR_3: u32 = 0x002a_6771;
const MAGICDESCR_4: u32 = 0x002a_6779;
const MAGICLAYER: u32 = 0x000a_676f;
const MAGICPOINT: u32 = 0x000a_678f;
const MAGICDATAP: u32 = 0xa67f_0000;

/// `hnsw_rs`'s `NB_LAYER_MAX`. A dump of a graph built with fewer is refused
/// by the library's own writer, and the loader indexes a point's sixteen
/// neighbour lists by the description's layer count.
const LAYERS: u8 = 16;

/// The dump format this build writes, and the only one it reads.
const FORMAT: u32 = 4;

/// The dump mode a `file_dump` writes: `Full`, which names every neighbour by
/// its layer and rank. The loader reads ranks only in this mode and would
/// link every neighbour to the first point of layer 0 in the other.
const FULL: u8 = 1;

/// The longest type name the loader reads.
const NAME_MAX: usize = 256;

/// What the caller knows the dump must describe.
pub(crate) struct Expect<'a> {
    /// The points in the graph: the snapshot's key count.
    pub(crate) points: usize,
    /// The width of every vector.
    pub(crate) dim: usize,
    /// The graph's `max_nb_connection`.
    pub(crate) connections: u8,
    /// The distance's type name; compared by its last path segment, as the
    /// loader compares it, so a compiler that spells the path differently does
    /// not cost a rebuild.
    pub(crate) distance: &'a str,
}

/// Refuse a dump `hnsw_rs` 0.3.5's `load_hnsw::<f32, _>` would end the
/// process on, abort on, read out of bounds for or panic on, or that does not
/// describe the graph `expect` names. `Ok` means the loader reads exactly
/// these bytes to the end of both files without taking any of those paths.
pub(crate) fn check(dir: &Path, expect: &Expect<'_>) -> Result<(), String> {
    let mut graph = Input::open(&dir.join(format!("{BASENAME}.hnsw.graph")), "graph file")?;
    let mut data = Input::open(&dir.join(format!("{BASENAME}.hnsw.data")), "data file")?;
    let layers = description(&mut graph, expect)?;

    // `load_hnsw` reads the data file's header before any point, and asserts
    // both fields.
    data.magic(MAGICDATAP, "the data file's header")?;
    let dim = data.usize("the data file's dimension")?;
    if dim != expect.dim {
        return Err(format!("the data file is {dim}-dimensional, not {}", expect.dim));
    }

    // `load_point_indexation`: the layers, each point's graph part and then
    // its vector, in one pass over both files as the loader makes it.
    let graph_layers = graph.u8("the layer count")?;
    if graph_layers != LAYERS {
        return Err(format!("the graph has {graph_layers} layers, not {LAYERS}"));
    }
    let mut seen = vec![false; expect.points];
    let mut loaded = 0usize;
    let mut sizes = [0usize; LAYERS as usize];
    // The highest rank a neighbour names in each layer, checked against the
    // layer's size once every layer is read: the loader indexes
    // `points_by_layer[layer][rank]` and panics past the end.
    let mut highest: [Option<usize>; LAYERS as usize] = [None; LAYERS as usize];
    let mut vector = vec![0u8; expect.dim * size_of::<f32>()];
    for (l, size) in sizes.iter_mut().enumerate() {
        graph.magic(MAGICLAYER, "a layer's start")?;
        let count = graph.usize("a layer's point count")?;
        // Allocated by the loader before a point is read.
        if count > expect.points - loaded {
            return Err(format!(
                "layer {l} declares {count} points, more than the {} left of {}",
                expect.points - loaded,
                expect.points
            ));
        }
        *size = count;
        for r in 0..count {
            graph.magic(MAGICPOINT, "a point's start")?;
            let origin = graph.usize("a point's origin id")?;
            let layer = graph.u8("a point's layer")?;
            let rank = graph.i32("a point's rank")?;
            if usize::from(layer) != l || usize::try_from(rank).ok() != Some(r) {
                return Err(format!(
                    "point {r} of layer {l} says it is point {rank} of layer {layer}"
                ));
            }
            if origin >= expect.points || std::mem::replace(&mut seen[origin], true) {
                return Err(format!(
                    "point {r} of layer {l} has origin id {origin}, out of range or seen before"
                ));
            }
            for _ in 0..layers {
                let neighbours = graph.usize("a neighbour count")?;
                // Allocated by the loader before a neighbour is read. A point
                // has at most one edge to each other point.
                if neighbours > expect.points {
                    return Err(format!(
                        "point {origin} declares {neighbours} neighbours in one layer, of {} points",
                        expect.points
                    ));
                }
                for _ in 0..neighbours {
                    let to = graph.usize("a neighbour's origin id")?;
                    let at = graph.u8("a neighbour's layer")?;
                    let rank = graph.i32("a neighbour's rank")?;
                    let distance = graph.f32("a neighbour's distance")?;
                    let Ok(rank) = usize::try_from(rank) else {
                        return Err(format!("point {origin} names a neighbour of rank {rank}"));
                    };
                    if to >= expect.points || at >= graph_layers {
                        return Err(format!(
                            "point {origin} names neighbour {to} in layer {at}, out of range"
                        ));
                    }
                    // Neighbour lists are sorted at load, by an `Ord` that
                    // panics on a NaN.
                    if distance.is_nan() {
                        return Err(format!("point {origin} has a NaN distance to {to}"));
                    }
                    let top = &mut highest[usize::from(at)];
                    *top = Some(top.map_or(rank, |t| t.max(rank)));
                }
            }

            // The vector: an error reading any part of it ends the process.
            data.magic(MAGICDATAP, "a vector's start")?;
            let id = data.u64("a vector's origin id")?;
            if usize::try_from(id).ok() != Some(origin) {
                return Err(format!("the vector after point {origin}'s says it is {id}'s"));
            }
            let bytes = data.u64("a vector's length")?;
            // Allocated before it is read, then read as `dim` floats whatever
            // it says: shorter is a read past the end of the buffer.
            if usize::try_from(bytes).ok() != Some(vector.len()) {
                return Err(format!(
                    "point {origin}'s vector is {bytes} bytes, not {}",
                    vector.len()
                ));
            }
            data.exact(&mut vector, "a vector")?;
            // A search computes distances from these, by the same `Ord`.
            if vector
                .chunks_exact(size_of::<f32>())
                .any(|b| !f32::from_ne_bytes(b.try_into().expect("four bytes")).is_finite())
            {
                return Err(format!("point {origin}'s vector is not finite"));
            }
            loaded += 1;
        }
    }
    if loaded != expect.points {
        return Err(format!("the graph holds {loaded} points, not {}", expect.points));
    }
    for (layer, top) in highest.iter().enumerate() {
        if let Some(rank) = top
            && *rank >= sizes[layer]
        {
            return Err(format!(
                "a neighbour is point {rank} of layer {layer}, which holds {}",
                sizes[layer]
            ));
        }
    }

    // The entry point, indexed the same way.
    let origin = graph.usize("the entry point's origin id")?;
    let layer = graph.u8("the entry point's layer")?;
    let rank = graph.i32("the entry point's rank")?;
    let fits = usize::from(layer) < sizes.len()
        && usize::try_from(rank).is_ok_and(|rank| rank < sizes[usize::from(layer)]);
    if !fits || origin >= expect.points {
        return Err(format!(
            "the entry point is point {rank} of layer {layer} (origin {origin}), out of range"
        ));
    }
    graph.end()?;
    data.end()
}

/// The description at the start of the graph file, as `load_description`
/// reads it. Returns the layer count every point's neighbourhood is read
/// over.
fn description(graph: &mut Input, expect: &Expect<'_>) -> Result<u8, String> {
    let format = match graph.u32("the description's magic")? {
        MAGICDESCR_2 => 2,
        MAGICDESCR_3 => 3,
        MAGICDESCR_4 => 4,
        other => return Err(format!("the graph file starts with {other:#010x}, not a dump")),
    };
    // Format 2 vectors were bincode, which 0.3.5 no longer reads: it ends the
    // process at the first point. This build writes format 4 and nothing else.
    if format != FORMAT {
        return Err(format!(
            "dump format {format}, not {FORMAT}: hnsw_rs 0.3.5 ends the process reading a \
             format-2 dump's first vector"
        ));
    }
    let mode = graph.u8("the dump mode")?;
    if mode != FULL {
        return Err(format!("dump mode {mode}, not {FULL} (full)"));
    }
    let connections = graph.u8("the connection count")?;
    if connections != expect.connections {
        return Err(format!("{connections} connections, not {}", expect.connections));
    }
    if format == 4 {
        let scale = graph.f64("the level scale")?;
        if !(scale.is_finite() && scale > 0.0) {
            return Err(format!("a level scale of {scale}"));
        }
    }
    let layers = graph.u8("the description's layer count")?;
    if layers != LAYERS {
        return Err(format!("the description has {layers} layers, not {LAYERS}"));
    }
    let _ef = graph.usize("ef")?;
    let points = graph.usize("the point count")?;
    if points != expect.points {
        return Err(format!("the dump holds {points} points, not {}", expect.points));
    }
    let dim = graph.usize("the dimension")?;
    if dim != expect.dim {
        return Err(format!("the dump is {dim}-dimensional, not {}", expect.dim));
    }
    let distance = graph.name("the distance's name")?;
    let wanted = expect.distance.rsplit("::").next().unwrap_or_default();
    if distance.rsplit("::").next() != Some(wanted) || wanted.is_empty() {
        return Err(format!("a dump of distance {distance:?}, not {:?}", expect.distance));
    }
    let element = graph.name("the element type's name")?;
    if element != std::any::type_name::<f32>() {
        return Err(format!("a dump of {element:?}, not f32"));
    }
    Ok(layers)
}

/// One of the two files, read field by field. Every refusal says which file
/// and what was being read.
struct Input {
    reader: BufReader<File>,
    what: &'static str,
}

impl Input {
    fn open(path: &Path, what: &'static str) -> Result<Self, String> {
        let file = File::open(path).map_err(|e| format!("the {what}: {e}"))?;
        Ok(Self { reader: BufReader::with_capacity(1 << 16, file), what })
    }

    fn exact(&mut self, buf: &mut [u8], field: &str) -> Result<(), String> {
        self.reader.read_exact(buf).map_err(|e| match e.kind() {
            ErrorKind::UnexpectedEof => format!("the {} ends inside {field}", self.what),
            _ => format!("the {}, reading {field}: {e}", self.what),
        })
    }

    fn bytes<const N: usize>(&mut self, field: &str) -> Result<[u8; N], String> {
        let mut buf = [0u8; N];
        self.exact(&mut buf, field)?;
        Ok(buf)
    }

    fn u8(&mut self, field: &str) -> Result<u8, String> {
        Ok(self.bytes::<1>(field)?[0])
    }

    fn u32(&mut self, field: &str) -> Result<u32, String> {
        Ok(u32::from_ne_bytes(self.bytes(field)?))
    }

    fn i32(&mut self, field: &str) -> Result<i32, String> {
        Ok(i32::from_ne_bytes(self.bytes(field)?))
    }

    fn u64(&mut self, field: &str) -> Result<u64, String> {
        Ok(u64::from_ne_bytes(self.bytes(field)?))
    }

    fn usize(&mut self, field: &str) -> Result<usize, String> {
        Ok(usize::from_ne_bytes(self.bytes(field)?))
    }

    fn f32(&mut self, field: &str) -> Result<f32, String> {
        Ok(f32::from_ne_bytes(self.bytes(field)?))
    }

    fn f64(&mut self, field: &str) -> Result<f64, String> {
        Ok(f64::from_ne_bytes(self.bytes(field)?))
    }

    fn magic(&mut self, magic: u32, field: &str) -> Result<(), String> {
        let found = self.u32(field)?;
        if found != magic {
            return Err(format!(
                "the {} has {found:#010x} at {field}, not {magic:#010x}",
                self.what
            ));
        }
        Ok(())
    }

    /// A type name: a length the loader refuses above [`NAME_MAX`], then
    /// bytes it `unwrap`s as UTF-8.
    fn name(&mut self, field: &str) -> Result<String, String> {
        let len = self.usize(field)?;
        if len > NAME_MAX {
            return Err(format!("{field} is {len} bytes long"));
        }
        let mut bytes = vec![0u8; len];
        self.exact(&mut bytes, field)?;
        String::from_utf8(bytes).map_err(|_| format!("{field} is not UTF-8"))
    }

    /// Nothing may follow: the loader stops at the entry point, so trailing
    /// bytes are a file this build did not write.
    fn end(&mut self) -> Result<(), String> {
        let mut byte = [0u8; 1];
        match self.reader.read(&mut byte) {
            Ok(0) => Ok(()),
            Ok(_) => Err(format!("the {} continues past its end", self.what)),
            Err(e) => Err(format!("the {}, reading past its end: {e}", self.what)),
        }
    }
}

#[cfg(test)]
mod tests {
    use hnsw_rs::api::AnnT;
    use hnsw_rs::prelude::{DistCosine, DistL2, Hnsw};

    use super::*;

    const POINTS: usize = 60;
    const DIM: usize = 4;

    /// A real dump of a small graph, as `HnswIndex::save` writes one, and the
    /// bytes of its two files.
    fn dumped() -> (tempfile::TempDir, Vec<u8>, Vec<u8>) {
        let dir = tempfile::tempdir().unwrap();
        let hnsw = Hnsw::<f32, DistCosine>::new(16, POINTS, 16, 200, DistCosine {});
        for i in 0..POINTS {
            let angle = i as f32 * 0.1;
            let v = [angle.cos(), angle.sin(), 0.5, (i % 7) as f32 * 0.1];
            hnsw.insert((&v[..], i));
        }
        hnsw.file_dump(dir.path(), BASENAME).unwrap();
        let graph = std::fs::read(dir.path().join("index.hnsw.graph")).unwrap();
        let data = std::fs::read(dir.path().join("index.hnsw.data")).unwrap();
        (dir, graph, data)
    }

    fn expect() -> Expect<'static> {
        Expect {
            points: POINTS,
            dim: DIM,
            connections: 16,
            distance: std::any::type_name::<DistCosine>(),
        }
    }

    /// The description's length, from its two name lengths.
    fn description_len(graph: &[u8]) -> usize {
        let distance = usize::from_ne_bytes(graph[39..47].try_into().unwrap());
        47 + distance
            + 8
            + usize::from_ne_bytes(graph[47 + distance..55 + distance].try_into().unwrap())
    }

    /// Where the first point of layer 0 starts in the graph file.
    fn first_point(graph: &[u8]) -> usize {
        description_len(graph) + 1 + 4 + 8
    }

    fn put(bytes: &mut [u8], at: usize, value: &[u8]) {
        bytes[at..at + value.len()].copy_from_slice(value);
    }

    type Spoil = fn(&mut Vec<u8>, &mut Vec<u8>);

    /// Every way of spoiling a dump this check refuses, with what the refusal
    /// names. Each is something `hnsw_rs` 0.3.5's loader would end the process
    /// on (exit), abort on (an allocation), read out of bounds for, or panic
    /// on, or a dump this build does not write.
    fn spoiled() -> Vec<(&'static str, Spoil, &'static str)> {
        vec![
            (
                "format 2",
                |g, _| {
                    put(g, 0, &MAGICDESCR_2.to_ne_bytes());
                    g.drain(6..14);
                },
                "dump format 2",
            ),
            (
                "format 3",
                |g, _| {
                    put(g, 0, &MAGICDESCR_3.to_ne_bytes());
                    g.drain(6..14);
                },
                "dump format 3",
            ),
            ("not a dump", |g, _| put(g, 0, &0xdead_beefu32.to_ne_bytes()), "not a dump"),
            ("light mode", |g, _| g[4] = 0, "dump mode 0"),
            ("connections", |g, _| g[5] = 8, "8 connections"),
            ("level scale", |g, _| put(g, 6, &f64::NAN.to_ne_bytes()), "level scale"),
            ("description layers", |g, _| g[14] = 17, "description has 17 layers"),
            ("point count", |g, _| put(g, 23, &61usize.to_ne_bytes()), "holds 61 points"),
            ("dimension", |g, _| put(g, 31, &5usize.to_ne_bytes()), "5-dimensional"),
            (
                "distance name length",
                |g, _| put(g, 39, &(1usize << 40).to_ne_bytes()),
                "bytes long",
            ),
            ("distance name UTF-8", |g, _| g[47] = 0xff, "not UTF-8"),
            (
                "element type",
                |g, _| {
                    let at = description_len(g) - 3;
                    put(g, at, b"f64");
                },
                "not f32",
            ),
            (
                "graph layers",
                |g, _| {
                    let at = description_len(g);
                    g[at] = 15;
                },
                "graph has 15 layers",
            ),
            (
                "layer magic",
                |g, _| {
                    let at = description_len(g) + 1;
                    put(g, at, &0u32.to_ne_bytes());
                },
                "a layer's start",
            ),
            (
                "layer count past memory",
                |g, _| {
                    let at = description_len(g) + 5;
                    put(g, at, &(1usize << 59).to_ne_bytes());
                },
                "declares",
            ),
            (
                "point magic",
                |g, _| {
                    let at = first_point(g);
                    put(g, at, &0u32.to_ne_bytes());
                },
                "a point's start",
            ),
            (
                "origin out of range",
                |g, _| {
                    let at = first_point(g) + 4;
                    put(g, at, &(POINTS + 1).to_ne_bytes());
                },
                "out of range or seen before",
            ),
            (
                "point rank",
                |g, _| {
                    let at = first_point(g) + 13;
                    put(g, at, &5i32.to_ne_bytes());
                },
                "says it is point 5",
            ),
            (
                "neighbour count past memory",
                |g, _| {
                    let at = first_point(g) + 17;
                    put(g, at, &(1usize << 58).to_ne_bytes());
                },
                "neighbours in one layer",
            ),
            (
                "neighbour layer",
                |g, _| {
                    let at = first_point(g) + 25 + 8;
                    g[at] = 16;
                },
                "out of range",
            ),
            (
                "negative neighbour rank",
                |g, _| {
                    let at = first_point(g) + 25 + 9;
                    put(g, at, &(-1i32).to_ne_bytes());
                },
                "rank -1",
            ),
            (
                "neighbour rank past its layer",
                |g, _| {
                    let at = first_point(g) + 25 + 9;
                    put(g, at, &i32::MAX.to_ne_bytes());
                },
                "a neighbour is point",
            ),
            (
                "NaN distance",
                |g, _| {
                    let at = first_point(g) + 25 + 13;
                    put(g, at, &f32::NAN.to_ne_bytes());
                },
                "NaN distance",
            ),
            (
                "entry point rank",
                |g, _| {
                    let at = g.len() - 4;
                    put(g, at, &(POINTS as i32).to_ne_bytes());
                },
                "the entry point",
            ),
            (
                "entry point layer",
                |g, _| {
                    let at = g.len() - 5;
                    g[at] = 16;
                },
                "the entry point",
            ),
            (
                "graph cut short",
                |g, _| {
                    g.pop();
                },
                "the graph file ends inside",
            ),
            ("graph continues", |g, _| g.push(0), "the graph file continues"),
            ("data magic", |_, d| put(d, 0, &0u32.to_ne_bytes()), "the data file has"),
            (
                "data dimension",
                |_, d| put(d, 4, &5usize.to_ne_bytes()),
                "data file is 5-dimensional",
            ),
            ("vector magic", |_, d| put(d, 12, &0u32.to_ne_bytes()), "a vector's start"),
            ("vector origin", |_, d| put(d, 16, &u64::MAX.to_ne_bytes()), "says it is"),
            (
                "vector shorter than its width",
                |_, d| put(d, 24, &12u64.to_ne_bytes()),
                "12 bytes, not 16",
            ),
            (
                "vector length past memory",
                |_, d| put(d, 24, &(1u64 << 62).to_ne_bytes()),
                "bytes, not 16",
            ),
            ("vector not finite", |_, d| put(d, 32, &f32::INFINITY.to_ne_bytes()), "not finite"),
            (
                "data cut short",
                |_, d| {
                    d.truncate(d.len() - 3);
                },
                "the data file ends inside",
            ),
            ("data continues", |_, d| d.push(0), "the data file continues"),
        ]
    }

    /// The audit this check is built from, re-runnable on a library upgrade:
    /// each spoiled dump handed to `hnsw_rs`'s loader directly, in a child
    /// process, and how the child ended. Prints a table; asserts nothing.
    ///
    /// ```bash
    /// cargo test -p kimmy-vector --lib -- --ignored --nocapture what_the_library_does
    /// ```
    #[test]
    #[ignore = "an audit table for a library upgrade; spawns a child per case"]
    fn what_the_library_does_with_each_spoiled_dump() {
        const CHILD: &str = "KIMMY_DUMP_AUDIT_CHILD";
        if let Ok(dir) = std::env::var(CHILD) {
            let loaded = std::panic::catch_unwind(|| {
                let mut io = hnsw_rs::hnswio::HnswIo::new(Path::new(&dir), BASENAME);
                io.load_hnsw::<f32, DistCosine>().map(|_| ()).map_err(|e| e.to_string())
            });
            match loaded {
                Ok(Ok(())) => println!("AUDIT loaded"),
                Ok(Err(e)) => println!("AUDIT returned an error: {e}"),
                Err(_) => println!("AUDIT panicked"),
            }
            return;
        }
        let name = "dump::tests::what_the_library_does_with_each_spoiled_dump";
        for (case, spoil, _) in spoiled() {
            let (dir, mut graph, mut data) = dumped();
            spoil(&mut graph, &mut data);
            std::fs::write(dir.path().join("index.hnsw.graph"), &graph).unwrap();
            std::fs::write(dir.path().join("index.hnsw.data"), &data).unwrap();
            let out = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", name, "--nocapture", "--ignored", "--test-threads=1"])
                .env(CHILD, dir.path())
                .output()
                .unwrap();
            let stdout = String::from_utf8_lossy(&out.stdout);
            let said = stdout.lines().find_map(|l| l.split_once("AUDIT ").map(|(_, said)| said));
            let ended = match (said, out.status.code()) {
                (Some(said), _) => said.to_string(),
                (None, Some(code)) => format!("ENDED THE PROCESS: exit({code})"),
                (None, None) => format!("ENDED THE PROCESS: {:?} (a signal)", out.status),
            };
            println!("{case:<32} {ended}");
        }
    }

    #[test]
    fn a_dump_as_saved_passes() {
        let (dir, _, _) = dumped();
        assert_eq!(check(dir.path(), &expect()), Ok(()));
    }

    #[test]
    fn every_spoiled_dump_is_refused_by_what_spoiled_it() {
        for (name, spoil, says) in spoiled() {
            let (dir, mut graph, mut data) = dumped();
            spoil(&mut graph, &mut data);
            std::fs::write(dir.path().join("index.hnsw.graph"), &graph).unwrap();
            std::fs::write(dir.path().join("index.hnsw.data"), &data).unwrap();
            match check(dir.path(), &expect()) {
                Err(e) => assert!(e.contains(says), "{name}: refused as {e:?}, not for {says:?}"),
                Ok(()) => panic!("{name}: passed"),
            }
        }
    }

    /// What the caller expects is held to as well: a dump of another shape
    /// is not this snapshot's graph.
    #[test]
    fn a_dump_of_another_shape_is_refused() {
        let (dir, _, _) = dumped();
        for (name, expect, says) in [
            ("count", Expect { points: POINTS + 1, ..expect() }, "holds 60 points, not 61"),
            ("width", Expect { dim: DIM + 1, ..expect() }, "4-dimensional, not 5"),
            ("connections", Expect { connections: 32, ..expect() }, "16 connections, not 32"),
            (
                "metric",
                Expect { distance: std::any::type_name::<DistL2>(), ..expect() },
                "distance",
            ),
        ] {
            let refused = check(dir.path(), &expect);
            assert!(refused.as_ref().is_err_and(|e| e.contains(says)), "{name}: {refused:?}");
        }
        std::fs::remove_file(dir.path().join("index.hnsw.data")).unwrap();
        let refused = check(dir.path(), &expect());
        assert!(refused.as_ref().is_err_and(|e| e.contains("the data file")), "{refused:?}");
    }
}
