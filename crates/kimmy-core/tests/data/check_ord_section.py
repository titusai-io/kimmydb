"""Q7 Step 2, first item: an independent byte writer for section 10 (the order state) of the compact form (`QMS` v2).

Run:  python3 -I -B check_ord_section.py [ord_section_goldens.json]      (default: the file next to this script)

`ord_section_goldens.json` is written by the Rust test `the_order_section_fixture_of_the_python_writer_is_current`
(`src/merge_state/compact_tests.rs`): for each of the six goldens, the full compact bytes the Rust side produces and
the order state spelled out BY VALUE (stamps and writers, no table indexes).

This script shares no code with the Rust side and imports nothing from the model. It re-derives, from the spec in the
module documentation of `compact.rs` (primitives and section 10), the bytes of section 10, and checks:
  1. it parses the writer table (section 1) and the life table (section 2) out of the Rust bytes, so that the life
     index of an entry's key and the writer index in a stamp are the ones the table says, not ones it was told;
  2. the section 10 it writes equals, byte for byte, the TAIL of the Rust bytes (section 10 is the last section);
  3. the length of the Rust bytes is the pinned one (10, 273, 509, 235, 329, 163), and the 128-bit hash of the bytes,
     computed here by a hash of its own, is the pinned one (`the_compact_golden_hashes_with_the_order_state_are_pinned`).
Exit status 0 and one line per golden when everything matches; any mismatch prints the first differing offset and exits 1.
"""
import json
import os
import sys

# The pins of `the_compact_golden_hashes_with_the_order_state_are_pinned`: name -> (hash, length, order entries).
PINS = {
    "empty": ("6a9f06d361f4fb8c789c079f6563280a", 10, 0),
    "170": ("79f811df1f679ba13896538b36c9e324", 273, 1),
    "mixed": ("329d92bb79aa4174a995b5ef401fb8a1", 509, 5),
    "delete": ("e695a485390d23417c6a5fd1d8f9ca41", 235, 0),
    "obs": ("1714242f8331d6b9ca194c73191730ed", 329, 2),
    "small": ("f41c2e8e7d18f3b932ed6a99aa0aa9fb", 163, 2),
}

M64 = (1 << 64) - 1


# ---- the 128-bit state hash: two lanes of a multiply-rotate mix over big-endian 8-byte words ----------------------------

def rotl(x, k):
    return ((x << k) | (x >> (64 - k))) & M64


def splitmix(z):
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & M64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & M64
    return z ^ (z >> 31)


def hash128(data):
    a, b = 0x243F6A8885A308D3, 0x13198A2E03707344
    for i in range(0, len(data), 8):
        chunk = data[i:i + 8]
        v = int.from_bytes(chunk + b"\0" * (8 - len(chunk)), "big")
        a = rotl(((a ^ v) * 0x9E3779B185EBCA87) & M64, 29)
        b = ((rotl(b, 31) ^ v) * 0xC2B2AE3D27D4EB4F) & M64
        a ^= b >> 17
        b ^= a >> 23
    n = len(data)
    x = splitmix(a ^ n)
    y = splitmix(b ^ rotl(n, 32) ^ x)
    return x.to_bytes(8, "big") + y.to_bytes(8, "big")


# ---- primitives ---------------------------------------------------------------------------------------------------------

def varint(n):
    """LEB128 of a non-negative integer, shortest form."""
    out = bytearray()
    while True:
        byte = n & 0x7F
        n >>= 7
        if n:
            out.append(byte | 0x80)
        else:
            out.append(byte)
            return bytes(out)


def read_varint(buf, pos):
    n, shift = 0, 0
    while True:
        byte = buf[pos]
        pos += 1
        n |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return n, pos
        shift += 7


def string(s):
    raw = s.encode("utf-8")
    return varint(len(raw)) + raw


# ---- the two tables, read from the Rust bytes ---------------------------------------------------------------------------

def read_tables(buf):
    """Section 1 (writers: a count, 34 bytes each: node 16 + epoch 18) and section 2 (lives: a count, each a stamp =
    10-byte HLC + writer index). Returns (writer_index: {(node_hex, epoch_hex): i}, life_index: {(hlc_hex, writer_i): i})."""
    pos = 0
    n, pos = read_varint(buf, pos)
    writers = {}
    for i in range(n):
        raw = buf[pos:pos + 34]
        pos += 34
        writers[(raw[:16].hex(), raw[16:].hex())] = i
    n, pos = read_varint(buf, pos)
    lives = {}
    for i in range(n):
        hlc = buf[pos:pos + 10].hex()
        pos += 10
        w, pos = read_varint(buf, pos)
        lives[(hlc, w)] = i
    return writers, lives


# ---- section 10 ---------------------------------------------------------------------------------------------------------

def section10(ords, writers, lives):
    def wid(w):
        return varint(writers[(w["node"], w["epoch"])])

    def stamp(s):
        return bytes.fromhex(s["hlc"]) + wid(s["writer"])

    def cand(c):
        return stamp(c["stamp"]) + varint(c["pos"])

    out = bytearray(varint(len(ords)))
    previous = None
    for e in ords:
        life = lives[(e["life"]["hlc"], writers[(e["life"]["writer"]["node"], e["life"]["writer"]["epoch"])])]
        # the key: the life index, then the path as a segment count and the segments; the root of a replace is the count 0
        segs = [] if e["at"] is None else e["at"]
        key = (life, segs)
        if previous is not None and not previous < key:
            raise SystemExit("order entries are not in strictly increasing key order: %r then %r" % (previous, key))
        previous = key
        out += varint(life)
        out += varint(len(segs))
        for s in segs:
            out += string(s)
        floor = e["floor"]
        if floor is None:
            out.append(0)
        elif floor["kind"] == "u":
            out.append(3 if floor["others"] else 1)
            out += stamp(floor["stamp"]) + bytes.fromhex(floor["prev"])
        elif floor["kind"] == "r":
            out.append(2)
            out += stamp(floor["stamp"])
        else:
            raise SystemExit("unknown floor kind %r" % floor["kind"])
        firsts = sorted(e["firsts"], key=lambda f: writers[(f["writer"]["node"], f["writer"]["epoch"])])
        out += varint(len(firsts))
        for f in firsts:
            out += wid(f["writer"]) + cand(f["cand"])
        out.append((1 if e["last"] is not None else 0) | (2 if e["lvl"] is not None else 0))
        if e["last"] is not None:
            out += cand(e["last"])
        if e["lvl"] is not None:
            out += stamp(e["lvl"])
    return bytes(out)


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(os.path.dirname(os.path.abspath(__file__)), "ord_section_goldens.json")
    with open(path) as f:
        goldens = json.load(f)
    names = [g["name"] for g in goldens]
    if sorted(names) != sorted(PINS):
        raise SystemExit("the fixture holds %r, the pins are for %r" % (names, sorted(PINS)))
    bad = 0
    for g in goldens:
        buf = bytes.fromhex(g["bytes"])
        want_hash, want_len, want_entries = PINS[g["name"]]
        writers, lives = read_tables(buf)
        sec = section10(g["ords"], writers, lives)
        tail = buf[len(buf) - len(sec):]
        problems = []
        if len(g["ords"]) != want_entries:
            problems.append("entries %d, pinned %d" % (len(g["ords"]), want_entries))
        if len(buf) != want_len:
            problems.append("length %d, pinned %d" % (len(buf), want_len))
        if tail != sec:
            first = next(i for i in range(len(sec)) if tail[i] != sec[i])
            problems.append("section 10 differs from the Rust tail at its offset %d (python %s, rust %s)" % (first, sec.hex(), tail.hex()))
        if hash128(buf).hex() != want_hash:
            problems.append("hash %s, pinned %s" % (hash128(buf).hex(), want_hash))
        print("%-7s %4d B, section 10 = %3d B (%d entries): %s" % (g["name"], len(buf), len(sec), len(g["ords"]), "OK" if not problems else "; ".join(problems)))
        bad += bool(problems)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
