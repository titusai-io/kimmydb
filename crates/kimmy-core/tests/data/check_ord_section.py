"""Q7 Step 2, first item: an independent byte writer for section 10 (the order state) of the compact form (`QMS` v2).

Run:  python3 -I -B check_ord_section.py [ord_section_goldens.json]      (default: the file next to this script)

`ord_section_goldens.json` is written by the Rust test `the_order_section_fixture_of_the_python_writer_is_current`
(`src/merge_state/compact_tests.rs`): for each of the six goldens, the full compact bytes the Rust side produces and
the order state spelled out BY VALUE (stamps and writers, no table indexes).

This script shares no code with the Rust side and imports nothing from the model. It re-derives, from the spec in the
module documentation of `compact.rs` (primitives and section 10), the bytes of section 10, and checks:
  1. it parses the writer table (section 1) and the life table (section 2) out of the Rust bytes, so that the life
     index of an entry's key and the writer index in a stamp are the ones the table says, not ones it was told;
  2. it walks sections 3 to 9 (skipping values; no semantics), finds the offset where section 10 begins, checks that
     the section 10 it writes starts EXACTLY there and runs exactly to the end of the bytes, and is byte for byte
     the Rust bytes from there;
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
    # NEW pins (`the_order_section_corpus_is_pinned`): states a Rust Writer builds, with every floor kind, a level mark,
    # the root key, several writers' firsts and positions of 128 and up (see `ord_section_states`).
    "floors_u": ("a9a3d1f267ae32cd8a3de81f1297f4af", 666, 9),
    "floor_r": ("382bb664c7fbba70de55f428285bc2bf", 752, 11),
    "merged": ("f0a1a9745f28949792f933ad926e68a3", 765, 11),
    "wide": ("07488610e95f806772a5d0f593d017c0", 8226, 140),
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


# ---- sections 3 to 9: a walker that only finds where each one ends ----------------------------------------------------

class Walk:
    def __init__(self, buf, pos):
        self.b, self.p = buf, pos

    def u8(self):
        v = self.b[self.p]
        self.p += 1
        return v

    def var(self):
        n, self.p = read_varint(self.b, self.p)
        return n

    def skip(self, n):
        self.p += n

    def string(self):
        self.skip(self.var())

    def path(self):
        for _ in range(self.var()):
            self.string()

    def stamp(self):
        self.skip(10)
        self.var()

    def opt_stamp(self):
        if self.u8():
            self.stamp()

    def payload(self, ty):
        if ty == 2:
            self.skip(16)
        else:
            self.var()

    def total(self):
        self.payload(self.u8())

    def value(self):
        tag = self.u8()
        if tag in (0, 5, 6, 15):
            pass
        elif tag in (1, 2, 10):
            self.skip(8)
        elif tag == 3:
            self.string()
        elif tag == 4:
            for _ in range(self.var()):
                self.value()
        elif tag == 7:
            self.skip(4)
        elif tag == 8:
            self.skip(16)
        elif tag == 9:
            self.skip(1)
        elif tag == 11:
            self.skip(12)
        elif tag == 12:
            self.skip(1)
            self.skip(self.var())
        elif tag == 13:
            self.skip(8)
        elif tag == 14:
            self.string()
            self.string()
        elif tag == 16:
            for _ in range(self.var()):
                self.string()
                self.value()
        else:
            raise SystemExit("unknown value tag %d at %d" % (tag, self.p))

    def vv(self):
        for _ in range(self.var()):
            self.var()
            self.skip(10)


def section10_start(buf):
    """The offset at which section 10 begins: the end of section 9, found by walking sections 1 to 9."""
    w = Walk(buf, 0)
    for _ in range(w.var()):            # 1 writers
        w.skip(34)
    for _ in range(w.var()):            # 2 lives table
        w.stamp()
    for _ in range(w.var()):            # 3 lives
        w.var()
        for _ in range(w.var()):
            w.stamp()
        for _ in range(w.var()):
            w.path()
        w.u8()
    for _ in range(w.var()):            # 4 dead
        w.var()
    for _ in range(w.var()):            # 5 deletes
        w.stamp()
        w.vv()
    for _ in range(w.var()):            # 6 regs
        w.var()
        w.path()
        flags = w.u8()
        if not flags & 8:
            w.stamp()
        w.value()
        if flags & 3:
            if flags & 3 == 2:
                w.stamp()
            for _ in range(w.var()):    # the generation's seen: (life, writer, opt stamp) -> typed total
                w.var()
                w.var()
                w.opt_stamp()
                w.total()
    for _ in range(w.var()):            # 7 slots
        w.var()
        w.path()
        w.var()
        w.opt_stamp()
        w.stamp()
        flags = w.u8()
        w.payload((flags >> 1) & 3)
        if flags & 1:
            w.stamp()
            w.payload((flags >> 3) & 3)
    w.vv()                              # 8 version vector
    for _ in range(w.var()):            # 9 losses
        w.u8()
        if w.u8():
            w.path()
        w.stamp()
        tag = w.u8()
        if tag == 0:
            w.value()
        elif tag == 1:
            w.total()
        elif tag == 2:
            for _ in range(w.var()):
                w.path()
        else:
            raise SystemExit("unknown loss value tag %d" % tag)
    return w.p


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
        start = section10_start(buf)
        tail = buf[start:]
        if start + len(sec) != len(buf):
            problems_pre = ["section 10 begins at %d and the writer's takes %d B, the bytes end at %d" % (start, len(sec), len(buf))]
        else:
            problems_pre = []
        problems = list(problems_pre)
        if len(g["ords"]) != want_entries:
            problems.append("entries %d, pinned %d" % (len(g["ords"]), want_entries))
        if len(buf) != want_len:
            problems.append("length %d, pinned %d" % (len(buf), want_len))
        if tail != sec:
            first = next((i for i in range(min(len(sec), len(tail))) if tail[i] != sec[i]), min(len(sec), len(tail)))
            problems.append("section 10 differs from the Rust tail at its offset %d (python %s, rust %s)" % (first, sec.hex(), tail.hex()))
        if hash128(buf).hex() != want_hash:
            problems.append("hash %s, pinned %s" % (hash128(buf).hex(), want_hash))
        print("%-7s %4d B, section 10 = %3d B (%d entries): %s" % (g["name"], len(buf), len(sec), len(g["ords"]), "OK" if not problems else "; ".join(problems)))
        bad += bool(problems)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    main()
