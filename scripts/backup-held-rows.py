#!/usr/bin/env python3
"""Count the oplog entries in a backup that sit above the backup's own version vector.

Reads the backup file only. Every such entry was held as state on the member the backup
was taken from: a snapshot page's row, a scoped repair's, a digest repair's (ADR-160).
A backup does not carry those marks. A restore by 0.52.0 or later derives them again
from the backup's vector (ADR-244); a restore by 0.51 or earlier did not, and its first
open raised the restored member's vectors over these entries, so its pulls skipped
whatever their origins wrote below them.

So a total above 0 says a member restored from this backup by 0.51 or earlier claims
history it never took, for the origins listed; 0 says that restore claimed nothing it
did not hold. See operations.md, "A member restored by 0.51 or earlier".

Usage: python3 -I scripts/backup-held-rows.py BACKUP_FILE

The format is the one `crates/kimmy-storage/src/backup.rs` writes: the magic
"KIMMYBK1", a format byte, the node id (16 bytes), the creation time (u64, big-endian
milliseconds), then records of (tag u8, key length u32, key, value length u32, value)
up to the end tag 0xFF. An oplog key (tag 6) is the stamp, a 10-byte HLC (wall
milliseconds u64 and counter u16, big-endian) then the origin's 16-byte node id; a
version vector record (tag 10) is the node id, then the HLC.
"""

import array
import struct
import sys

T_OPLOG = 6
T_OPLOG_VERSIONS = 10
END = 0xFF
HEADER = 8 + 1 + 16 + 8


def hlc(raw):
    wall, counter = struct.unpack(">QH", raw)
    return (wall << 16) | counter


def shown(value):
    return f"{value >> 16}.{value & 0xFFFF}"


def read_exact(f, n):
    raw = f.read(n)
    if len(raw) != n:
        sys.exit("truncated backup: it would not restore")
    return raw


def main(path):
    stamps = {}
    vector = {}
    with open(path, "rb") as f:
        head = f.read(HEADER)
        if len(head) < HEADER or head[:8] != b"KIMMYBK1":
            sys.exit("not a KimmyDB backup")
        node = head[9:25].hex()
        created = struct.unpack(">Q", head[25:33])[0]
        records = 0
        while True:
            tag = read_exact(f, 1)[0]
            if tag == END:
                break
            key = read_exact(f, struct.unpack(">I", read_exact(f, 4))[0])
            length = struct.unpack(">I", read_exact(f, 4))[0]
            if tag == T_OPLOG:
                f.seek(length, 1)
                stamps.setdefault(key[10:26].hex(), array.array("Q")).append(hlc(key[:10]))
            else:
                value = read_exact(f, length)
                if tag == T_OPLOG_VERSIONS:
                    vector[key.hex()] = hlc(value)
            records += 1
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
