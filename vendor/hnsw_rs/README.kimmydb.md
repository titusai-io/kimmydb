# hnsw_rs, as kimmydb builds it

This directory is `hnsw_rs` **0.3.5** exactly as published on crates.io
(`hnsw_rs-0.3.5.crate`, sha256
`c9b4dcb9840006fae9964e97dfdb354cd00b7a15d69d143caae7eeca0410af9f`), plus
`kimmydb.patch`, applied to `src/hnsw.rs`, and this file; the licence files are
the crate's own. The patch adds one function and changes nothing else:
`Hnsw::add_layer0_edges`, which adds edges at layer 0 past a neighbour list's
cap. kimmydb's build calls it once a graph is built, to link every point the
graph left with no edge into it, or outside the entry point's component, from
its nearest linked points: 0.3.5's insertion drops a new point from each
neighbour's list when the list is full of closer points, and a point dropped by
all of them is one no search reaches (ADR-241 in `docs/decisions.md`). The
crate has no public way to add an edge, which is why this is a patch.
`scripts/check-vendored-hnsw.sh` holds the tree to the published crate plus the
patch, byte for byte. To re-vendor a new release: download its `.crate`, check
its sha256 against the index's checksum, replace this directory with its
contents, port and re-apply `kimmydb.patch` (regenerate it with `diff -u`
against the pristine `src/hnsw.rs`), put this file back with the new version
and sha256, update both in the check script, and re-run the dump audit
(`what_the_library_does_with_each_spoiled_dump`) and the reachability
measurement (`reachability_distribution_by_collection_size`) before trusting
it: the dump check and the reachability constants are written against this
version's loader and graphs.
