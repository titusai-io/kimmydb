#!/usr/bin/env python3
"""Count the oplog entries in a backup that sit above the backup's own version vector.

Reads the backup file only. Every such entry was held as state on the member the backup
was taken from: a snapshot page's row, a scoped repair's, a digest repair's (ADR-160).
A backup does not carry those marks. A restore by 0.52.0 or later derives them again
from the backup's vector (ADR-244); a restore by 0.51 or earlier did not, and its first
open raised the restored member's vectors over these entries, so its pulls skipped
whatever their origins wrote below them.

So a total above 0 proves that a member restored from this backup by 0.51 or earlier
claims history it never took, for the origins listed. A total of 0 clears that one
restore and nothing else: a member that synced from an affected member carries the same
gap, and a backup taken of an affected member shows nothing above its vector. See
operations.md, "A member restored by 0.51 or earlier", for how to check every member.

Usage: python3 -I scripts/backup-held-rows.py BACKUP_FILE

Python 3 and nothing else; run it wherever a copy of the backup file is. It refuses, with
one line and a non-zero exit, whatever a restore of the file would refuse.

The format is the one `crates/kimmy-storage/src/backup.rs` writes: the magic
"KIMMYBK1", a format byte, the node id (16 bytes), the creation time (u64, big-endian
milliseconds), then records of (tag u8, key length u32, key, value length u32, value)
up to the end tag 0xFF. An oplog key (tag 6) is the stamp, a 10-byte HLC (wall
milliseconds u64 and counter u16, big-endian) then the origin's 16-byte node id; a
version vector record (tag 10) is the node id, then the HLC.
"""

import array
import os
import struct
import sys

FORMAT = 1
T_OPLOG = 6
T_OPLOG_VERSIONS = 10
# The tags a restore of this format knows; it refuses any other.
KNOWN_TAGS = range(1, 12)
END = 0xFF
HEADER = 8 + 1 + 16 + 8
STAMP = 10
NODE = 16


def refuse(message):
    sys.exit(f"backup-held-rows: {message}")


def hlc(raw):
    wall, counter = struct.unpack(">QH", raw)
    return (wall << 16) | counter


def shown(value):
    return f"{value >> 16}.{value & 0xFFFF}"


def read_exact(f, n, size):
    if f.tell() + n > size:
        refuse("truncated backup: it would not restore")
    return f.read(n)


def read(path):
    """Every oplog stamp by origin, the version vector, and the header's facts, refusing
    whatever a restore refuses."""
    stamps = {}
    vector = {}
    with open(path, "rb") as f:
        size = os.fstat(f.fileno()).st_size
        head = f.read(HEADER)
        if len(head) < HEADER or head[:8] != b"KIMMYBK1":
            refuse("not a KimmyDB backup")
        if head[8] != FORMAT:
            refuse(f"backup format version {head[8]} is not supported (expected {FORMAT})")
        node = head[9:25].hex()
        created = struct.unpack(">Q", head[25:33])[0]
        records = 0
        while True:
            tag = read_exact(f, 1, size)[0]
            if tag == END:
                break
            if tag not in KNOWN_TAGS:
                refuse(f"the backup contains table {tag}, which a restore of this format refuses")
            key = read_exact(f, struct.unpack(">I", read_exact(f, 4, size))[0], size)
            length = struct.unpack(">I", read_exact(f, 4, size))[0]
            if tag == T_OPLOG:
                if len(key) != STAMP + NODE:
                    refuse(f"an oplog key of {len(key)} bytes is not a stamp")
                if f.tell() + length > size:
                    refuse("truncated backup: it would not restore")
                f.seek(length, 1)
                stamps.setdefault(key[STAMP:].hex(), array.array("Q")).append(hlc(key[:STAMP]))
            else:
                value = read_exact(f, length, size)
                if tag == T_OPLOG_VERSIONS:
                    if len(key) != NODE:
                        refuse(f"a version vector key of {len(key)} bytes is not a node id")
                    if len(value) != STAMP:
                        refuse(f"a version vector value of {len(value)} bytes is not a stamp")
                    vector[key.hex()] = hlc(value)
            records += 1
    return node, created, records, stamps, vector


def main(path):
    try:
        node, created, records, stamps, vector = read(path)
    except OSError as e:
        refuse(f"cannot read {path}: {e.strerror or e}")
    print(f"backup of node {node}, taken at {created} ms, {records} records")
    total = 0
    for origin in sorted(set(stamps) | set(vector)):
        held = stamps.get(origin, array.array("Q"))
        at = vector.get(origin, 0)
        above = [s for s in held if s > at]
        total += len(above)
        line = (
            f"origin {origin}{' (the node itself)' if origin == node else ''}: "
            f"vector {shown(at)}, {len(held)} entries, {len(above)} above the vector"
        )
        if above:
            line += f" (lowest {shown(min(above))}, highest {shown(max(above))})"
        print(line)
    print(f"entries above the backup's vector: {total}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(__doc__)
    main(sys.argv[1])
