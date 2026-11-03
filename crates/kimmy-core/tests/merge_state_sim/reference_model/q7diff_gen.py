"""Differential test of the Rust `merge_state` module against the Python reference model: the
script generator and the expected answers.

Drives the reference model (model5.py, with pullmodel5.Node's normalised `finish`) through random
operation scripts and writes, for every step, what the Rust side (tests/merge_state_diff.rs) must
reproduce:

  * the result of every write (its stamp, or refused),
  * for every join: the `changed` flag and the fresh loss keys,
  * after every step, for every member: the 128-bit state hash of the Python state ENCODED IN THE RUST
    CANONICAL FORMAT (crates/kimmy-core/src/merge_state/codec.rs `write_body`, ported below), its length,
    and the same two for the COMPACT form (compact.rs, QMS version 2, written again below),
    the visible document and the hidden figure,
  * at the end (after a full-mesh sync): the oracle's answers (visible doc, alive lives, required heads,
    hidden figure, killed-and-superseded groups, and the stale/unsound verdicts on member 0's records).

Implements the Q7 design document (field-level merge with summing $inc counters and conflictResolved,
v7.2) by running its reference model; see model5.py.

Usage (standard library only):
  python3 -I q7diff_gen.py <out-dir> <n-scripts> [first-seed] [--plant NAME]
      write s<seed>.json for each seed; --plant changes the model on purpose (the Rust replay must
      then disagree): same-ignores-type, hidden-le, no-saturation, zero-absent
  python3 -I q7diff_gen.py --golden
      print the hash and canonical length of the golden vectors of tests.rs, computed from the model (the version 1
      form and, as <name>-compact, the compact form)
"""
import ast
import json
import os
import random
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import model5 as M  # noqa: E402

M64 = (1 << 64) - 1
I64_MAX = 2 ** 63 - 1


# ---------------------------------------------------------------------------------------------
# the model, as pullmodel5.Node runs it, without the oplog
# ---------------------------------------------------------------------------------------------
class D(M.Member):
    def finish(self, st, log, meta):
        s = self.state
        s.vv[st[1]] = st[0]
        s, _ = M.join(s, M.State())          # pullmodel5.NORMALIZE_WRITES
        self.state = s
        s.contained = s.contained | {st}
        log[st] = (frozenset(s.contained - {st}), meta)
        return st

    def absorb(self, other):
        before = self.state
        merged, fresh = M.join(before, other)
        self.clock = max([self.clock] + list(merged.vv.values()))
        changed = merged.canon() != before.canon()
        self.state = merged
        return changed, fresh

    def recv_loss(self, key, val):
        if key[2][0] < self.state.horizon() or key in self.state.losses:
            return False
        self.state.losses[key] = val
        return True


# ---------------------------------------------------------------------------------------------
# the Rust canonical encoding (crates/kimmy-core/src/merge_state/codec.rs), ported
# ---------------------------------------------------------------------------------------------
KIND = {"clash": 0, "deleteWon": 1, "insertLost": 2}


def k_wid(w):
    return (w[0], w[1])


def k_stamp(s):
    return (s[0], s[1][0], s[1][1])


def k_opt(g):
    return (0,) if g is None else (1, k_stamp(g))


class Enc:
    def __init__(self, log):
        self.b = bytearray()
        self.log = log

    def u8(self, v):
        self.b.append(v)

    def ln(self, n):
        self.b += n.to_bytes(8, "big")

    def i64(self, v):
        self.b += v.to_bytes(8, "big", signed=True)

    def i128(self, v):
        self.b += v.to_bytes(16, "big", signed=True)

    def s(self, x):
        e = x.encode("utf-8")
        self.ln(len(e))
        self.b += e

    def hlc(self, h):
        self.b += h.to_bytes(8, "big") + (0).to_bytes(2, "big")

    def wid(self, w):
        self.b += bytes([ord(w[0])]) * 16
        self.b += bytes(10) + w[1].to_bytes(8, "big")

    def stamp(self, st):
        self.hlc(st[0])
        self.wid(st[1])

    def opt_stamp(self, g):
        if g is None:
            self.u8(0)
        else:
            self.u8(1)
            self.stamp(g)

    def value(self, v):
        if v is None:
            self.u8(0)
        elif isinstance(v, bool):
            raise TypeError("bool")
        elif isinstance(v, int):
            self.u8(1)
            self.i64(v)
        elif isinstance(v, float):
            self.u8(2)
            self.b += struct.pack(">d", v)
        elif isinstance(v, str):
            self.u8(3)
            self.s(v)
        else:
            raise TypeError(type(v))

    def vv(self, vv):
        self.ln(len(vv))
        for w in sorted(vv, key=k_wid):
            self.wid(w)
            self.hlc(vv[w])

    def obs(self, m):
        self.ln(len(m))
        for k in sorted(m, key=lambda k: (k_stamp(k[0]), k_wid(k[1]), k_opt(k[2]))):
            self.stamp(k[0])
            self.wid(k[1])
            self.opt_stamp(k[2])
            self.i128(m[k])

    def obs_stamped(self, m):
        """A register's `obs`: for each counted slot its stamp and total when the base saw it."""
        self.ln(len(m))
        for k in sorted(m, key=lambda k: (k_stamp(k[0]), k_wid(k[1]), k_opt(k[2]))):
            self.stamp(k[0])
            self.wid(k[1])
            self.opt_stamp(k[2])
            self.stamp(m[k][0])
            self.i128(m[k][1])

    def gen(self, g):
        if g is None:
            self.u8(0)
        else:
            self.u8(1)
            self.stamp(g[0])
            self.obs(g[2] if len(g) > 2 else {})        # the starter's vv (g[1]) is not encoded

    def loss_value(self, key, v):
        kind, p, st = key
        if kind == "insertLost" and p == "_":
            self.u8(2)
            f = ast.literal_eval(v)
            self.ln(len(f))
            for x in f:
                self.s(x)
        elif self.log[st][1]["kind"] == "inc":
            self.u8(1)
            self.i128(v)
        else:
            self.u8(0)
            self.value(v)

    def state(self, st):
        self.ln(len(st.lives))
        for L in sorted(st.lives, key=k_stamp):
            seen, order, kind = st.lives[L]
            self.stamp(L)
            self.ln(len(seen))
            for d in sorted(seen, key=k_stamp):
                self.stamp(d)
            self.ln(len(order))
            for p in order:
                self.s(p)
            self.u8({"insert": 0, "upsert": 1}[kind])
        self.ln(len(st.dead))
        for L in sorted(st.dead, key=k_stamp):
            self.stamp(L)
        self.ln(len(st.deletes))
        for d in sorted(st.deletes, key=k_stamp):
            self.stamp(d)
            self.vv(st.deletes[d])
        self.ln(len(st.regs))
        for k in sorted(st.regs, key=lambda k: (k_stamp(k[0]), k[1].encode())):
            r = st.regs[k]
            self.stamp(k[0])
            self.s(k[1])
            self.stamp(r[0])
            self.value(r[1])
            self.obs_stamped(r[2])
            self.gen(r[3])
            self.u8(1 if len(r) > 4 and r[4] == "soi" else 0)
        self.ln(len(st.slots))
        for k in sorted(st.slots, key=lambda k: (k_stamp(k[0]), k[1].encode(), k_wid(k[2]), k_opt(k[3]))):
            sl = st.slots[k]
            self.stamp(k[0])
            self.s(k[1])
            self.wid(k[2])
            self.opt_stamp(k[3])
            self.stamp(sl[0])
            self.i128(sl[1])
        self.vv(st.vv)
        self.ln(len(st.losses))
        for key in sorted(st.losses, key=lambda k: (KIND[k[0]], k[1].encode(), k_stamp(k[2]))):
            self.u8(KIND[key[0]])
            self.s(key[1])
            self.stamp(key[2])
            self.loss_value(key, st.losses[key])
        return bytes(self.b)


# ---------------------------------------------------------------------------------------------
# the compact canonical form, QMS version 2 (crates/kimmy-core/src/merge_state/compact.rs), written
# again here from the format description in the module documentation of compact.rs, not from its code
# ---------------------------------------------------------------------------------------------
FLAG_GEN_OWN, FLAG_GEN_OTHER, FLAG_SOI, FLAG_STAMP_IS_LIFE = 1, 2, 4, 8


def k_path(p):
    """Paths order by their segments, each compared as UTF-8 bytes (the model's names have no dot)."""
    return [x.encode() for x in p.split(".")]


class EncCompact:
    """Collects the body as a list of byte strings and two kinds of reference: a writer, and a life.
    Once everything is collected, the sorted writer table and life table are known and the
    references become indexes."""

    def __init__(self, log):
        self.parts = []
        self.log = log

    # -- primitives
    def raw(self, b):
        self.parts.append(bytes(b))

    def u8(self, v):
        self.raw([v])

    def vi(self, v):
        """An unsigned LEB128 varint, shortest form."""
        assert v >= 0
        out = bytearray()
        while v >= 0x80:
            out.append((v & 0x7F) | 0x80)
            v >>= 7
        out.append(v)
        self.raw(out)

    def total(self, v):
        """A counter total: zigzag, then a varint."""
        self.vi((v << 1) if v >= 0 else (((-v) << 1) - 1))

    def i64(self, v):
        self.raw(v.to_bytes(8, "big", signed=True))

    def s(self, x):
        e = x.encode("utf-8")
        self.vi(len(e))
        self.raw(e)

    def path(self, p):
        segs = p.split(".")
        self.vi(len(segs))
        for x in segs:
            self.s(x)

    def hlc(self, h):
        self.raw(h.to_bytes(8, "big") + (0).to_bytes(2, "big"))

    def wid(self, w):
        self.parts.append(("w", w))

    def life(self, st):
        self.parts.append(("l", st))

    def stamp(self, st):
        self.hlc(st[0])
        self.wid(st[1])

    def opt_stamp(self, g):
        if g is None:
            self.u8(0)
        else:
            self.u8(1)
            self.stamp(g)

    def value(self, v):
        if v is None:
            self.u8(0)
        elif isinstance(v, bool):
            raise TypeError("bool")
        elif isinstance(v, int):
            self.u8(1)
            self.i64(v)
        elif isinstance(v, float):
            self.u8(2)
            self.raw(struct.pack(">d", v))
        elif isinstance(v, str):
            self.u8(3)
            self.s(v)
        else:
            raise TypeError(type(v))

    def vv(self, vv):
        self.vi(len(vv))
        for w in sorted(vv, key=k_wid):
            self.wid(w)
            self.hlc(vv[w])

    @staticmethod
    def obs_order(k):
        return (k_stamp(k[0]), k_wid(k[1]), k_opt(k[2]))

    def obs_key(self, k):
        self.life(k[0])
        self.wid(k[1])
        self.opt_stamp(k[2])

    def obs(self, m):
        self.vi(len(m))
        for k in sorted(m, key=self.obs_order):
            self.obs_key(k)
            self.total(m[k])

    def obs_stamped(self, m):
        """A register's `obs`: for each counted slot its stamp and total when the base saw it."""
        self.vi(len(m))
        for k in sorted(m, key=self.obs_order):
            self.obs_key(k)
            self.stamp(m[k][0])
            self.total(m[k][1])

    def loss_value(self, key, v):
        kind, p, st = key
        if kind == "insertLost" and p == "_":
            self.u8(2)
            f = ast.literal_eval(v)
            self.vi(len(f))
            for x in f:
                self.path(x)
        elif self.log[st][1]["kind"] == "inc":
            self.u8(1)
            self.total(v)
        else:
            self.u8(0)
            self.value(v)

    # -- the sections
    def sections(self, st):
        self.vi(len(st.lives))
        for L in sorted(st.lives, key=k_stamp):
            seen, order, kind = st.lives[L]
            self.life(L)
            self.vi(len(seen))
            for d in sorted(seen, key=k_stamp):
                self.stamp(d)
            self.vi(len(order))
            for p in order:
                self.path(p)
            self.u8({"insert": 0, "upsert": 1}[kind])
        self.vi(len(st.dead))
        for L in sorted(st.dead, key=k_stamp):
            self.life(L)
        self.vi(len(st.deletes))
        for d in sorted(st.deletes, key=k_stamp):
            self.stamp(d)
            self.vv(st.deletes[d])
        self.vi(len(st.regs))
        for k in sorted(st.regs, key=lambda k: (k_stamp(k[0]), k_path(k[1]))):
            r = st.regs[k]
            life, stamp, gen = k[0], r[0], r[3]
            flags = FLAG_SOI if len(r) > 4 and r[4] == "soi" else 0
            if stamp == life:
                flags |= FLAG_STAMP_IS_LIFE
            if gen is not None:
                flags |= FLAG_GEN_OWN if gen[0] == stamp else FLAG_GEN_OTHER
            self.life(life)
            self.path(k[1])
            self.u8(flags)
            if stamp != life:
                self.stamp(stamp)
            self.value(r[1])
            self.obs_stamped(r[2])
            if gen is not None:
                if gen[0] != stamp:
                    self.stamp(gen[0])
                self.obs(gen[2] if len(gen) > 2 else {})        # the starter's vv (gen[1]) is not encoded
        self.vi(len(st.slots))
        for k in sorted(st.slots, key=lambda k: (k_stamp(k[0]), k_path(k[1]), k_wid(k[2]), k_opt(k[3]))):
            sl = st.slots[k]
            self.life(k[0])
            self.path(k[1])
            self.wid(k[2])
            self.opt_stamp(k[3])
            self.stamp(sl[0])
            self.total(sl[1])
        self.vv(st.vv)
        self.vi(len(st.losses))
        for key in sorted(st.losses, key=lambda k: (KIND[k[0]], k_path(k[1]), k_stamp(k[2]))):
            self.u8(KIND[key[0]])
            if key[0] == "insertLost" and key[1] == "_":      # the whole document: its own tag, not a path
                self.u8(0)
            else:
                self.u8(1)
                self.path(key[1])
            self.stamp(key[2])
            self.loss_value(key, st.losses[key])

    def state(self, st):
        """The canonical body: the writer table, the life table, then the sections."""
        self.sections(st)
        lives = sorted({p[1] for p in self.parts if isinstance(p, tuple) and p[0] == "l"}, key=k_stamp)
        writers = {p[1] for p in self.parts if isinstance(p, tuple) and p[0] == "w"}
        writers |= {l[1] for l in lives}
        writers = sorted(writers, key=k_wid)
        widx = {w: i for i, w in enumerate(writers)}
        lidx = {l: i for i, l in enumerate(lives)}
        head = EncCompact(self.log)
        head.vi(len(writers))
        for w in writers:
            head.raw(bytes([ord(w[0])]) * 16 + bytes(10) + w[1].to_bytes(8, "big"))
        head.vi(len(lives))
        for l in lives:
            head.hlc(l[0])
            head.vi(widx[l[1]])
        out = bytearray()
        for p in head.parts + self.parts:
            if isinstance(p, tuple):
                out += self._varint_bytes(widx[p[1]] if p[0] == "w" else lidx[p[1]])
            else:
                out += p
        return bytes(out)

    @staticmethod
    def _varint_bytes(v):
        e = EncCompact(None)
        e.vi(v)
        return e.parts[0]

    def encode(self, st):
        """The whole state: `QMS`, the version byte 2, the body."""
        return b"QMS\x02" + self.state(st)


def rotl(x, r):
    return ((x << r) | (x >> (64 - r))) & M64


def splitmix(z):
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & M64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & M64
    return z ^ (z >> 31)


def hash128(data):
    P1, P2 = 0x9E3779B185EBCA87, 0xC2B2AE3D27D4EB4F
    a, b = 0x243F6A8885A308D3, 0x13198A2E03707344
    for i in range(0, len(data), 8):
        v = int.from_bytes(data[i:i + 8].ljust(8, b"\0"), "big")
        a = rotl(((a ^ v) * P1) & M64, 29)
        b = ((rotl(b, 31) ^ v) * P2) & M64
        a ^= b >> 17
        b ^= a >> 23
    n = len(data)
    x = splitmix(a ^ n)
    y = splitmix(b ^ rotl(n, 32) ^ x)
    return (x.to_bytes(8, "big") + y.to_bytes(8, "big")).hex()


# ---------------------------------------------------------------------------------------------
# JSON forms shared with the Rust side
# ---------------------------------------------------------------------------------------------
def jv(v):
    if v is None:
        return ["n"]
    if isinstance(v, int):
        return ["i", str(v)]
    if isinstance(v, float):
        return ["d", struct.pack(">d", v).hex()]
    return ["s", v]


def js(st):
    return [st[0], st[1][0], st[1][1]]


def jk(k):
    return [k[0], k[1], js(k[2])]


def jdoc(d):
    return None if d is None else {p: jv(v) for p, v in d.items()}


# ---------------------------------------------------------------------------------------------
# one script
# ---------------------------------------------------------------------------------------------
NAMES = "ABCD"


def pools(bound):
    if bound == I64_MAX:
        ints = [0, 1, 2, 7, -3, 500, 2 ** 62, -(2 ** 62), I64_MAX, -I64_MAX - 1]
        incs = [1, 5, -3, 20, 2 ** 62, -(2 ** 62), I64_MAX, -I64_MAX - 1]
    else:
        ints = [0, 1, 2, 7, -3, 500, 900, -900, bound, -bound - 1, 5000]
        incs = [1, 5, -3, 20, 200, -200, 600, -600, bound + 1]
    vals = ints + [2.5, 2.0, 0.0, -0.0, 1e300, "s", "t", "", "ü"]
    return ints, incs, vals


def gen_script(seed):
    rnd = random.Random(seed)
    ret = rnd.choice([10 ** 9, 10 ** 9, 10 ** 9, 3, 5, 8, 12])
    bound = rnd.choice([I64_MAX, I64_MAX, 1000])
    nm = rnd.choice([2, 3, 3, 4])
    steps = rnd.choice([20, 40, 60])
    ints, incs, vals = pools(bound)
    paths = ["x", "y", "n", "z"]
    ops = []
    if rnd.random() < 0.8:
        ops.append(["insert", 0, [["x", jv(100)], ["n", jv("a")]], 0])
    epoch_next = 1
    for _ in range(steps):
        m = rnd.randrange(nm)
        sk = rnd.choice([0, 0, 0, 4])
        x = rnd.random()
        if x < 0.30:
            r, s = rnd.sample(range(nm), 2)
            ops.append(["join", r, s])
            continue
        if x < 0.36:
            r, s = rnd.sample(range(nm), 2)
            ops.append(["loss", r, s, rnd.randrange(1 << 30)])
            continue
        if x < 0.40:
            ops.append(["backup", m])
            continue
        if x < 0.43:
            o = rnd.choice([i for i in range(nm) if i != m])
            ops.append(["restore", m, o, epoch_next])
            epoch_next += 1
            continue
        op = rnd.choice(["inc", "inc", "inc", "inc_zero_sum", "set", "set", "unset", "delete", "insert",
                         "replace", "upsert", "upsert_soi"])
        p = rnd.choice(paths)
        if op == "insert":
            ks = rnd.sample(paths, rnd.choice([1, 1, 2, 3]))
            ops.append(["insert", m, [[k, jv(rnd.choice(vals + [None]))] for k in ks], sk])
        elif op == "upsert":
            ops.append(["upsert", m, rnd.choice(["x", "y", "n"]), rnd.choice(incs), sk])
        elif op == "upsert_soi":
            # $setOnInsert, $set and $inc on disjoint paths (a conflicting update is refused up front)
            fs = rnd.sample(["y", "z", "n"], rnd.choice([1, 1, 2]))
            sets = [] if rnd.random() < 0.5 else [[k, jv(rnd.choice(vals))] for k in
                                                   rnd.sample([q for q in ["n", "x", "w"] if q not in fs], 1)]
            free = [q for q in ["x", "y", "n", "z", "u"] if q not in fs and q not in [k for k, _ in sets]]
            ops.append(["upsert_soi", m, [[k, jv(rnd.choice(vals))] for k in fs], rnd.choice(free),
                        rnd.choice(incs), sets, sk])
        elif op == "inc_zero_sum":
            # +n then -n by one member: a counter that returns to where it was
            n = rnd.choice(incs)
            ops.append(["inc", m, p, n, sk])
            if n != -I64_MAX - 1:
                ops.append(["inc", m, p, -n, sk])
        elif op == "replace":
            ks = rnd.sample(paths, rnd.choice([1, 2]))
            ops.append(["replace", m, [[k, jv(rnd.choice(vals + [None]))] for k in ks], sk])
        elif op == "inc":
            ops.append(["inc", m, p, rnd.choice(incs), sk])
        elif op == "set":
            ops.append(["set", m, p, jv(rnd.choice(vals + [None])), sk])
        elif op == "unset":
            ops.append(["unset", m, p, sk])
        else:
            ops.append(["delete", m, sk])
    return {"seed": seed, "ret": ret, "bound": bound, "members": nm, "ops": ops}


def unjv(v):
    if v[0] == "n":
        return None
    if v[0] == "i":
        return int(v[1])
    if v[0] == "d":
        return struct.unpack(">d", bytes.fromhex(v[1]))[0]
    return v[1]


def snap(ms, log):
    out = []
    for m in ms:
        b = Enc(log).state(m.state)
        cb = EncCompact(log).state(m.state)
        out.append({"h": hash128(b), "len": len(b), "ch": hash128(cb), "clen": len(cb),
                    "v": jdoc(m.state.visible()),
                    "hid": {p: str(h) for p, h in m.state.hidden_all().items()}})
    return out


def run_script(sc, dump_canon=False):
    M.RET = sc["ret"]
    M.BOUND = sc["bound"]
    ms = [D(NAMES[i]) for i in range(sc["members"])]
    saved = [None] * len(ms)
    log = {}
    steps = []
    for op in sc["ops"]:
        kind = op[0]
        res = None
        if kind == "insert":
            _, m, fields, sk = op
            r = ms[m].insert({k: unjv(v) for k, v in fields}, log, sk)
            res = None if r is None else js(r)
        elif kind == "upsert":
            _, m, p, n, sk = op
            r = ms[m].upsert_inc(p, n, log, sk)
            res = None if r is None else js(r)
        elif kind == "upsert_soi":
            _, m, fields, p, n, sets, sk = op
            r = ms[m].upsert_soi({k: unjv(v) for k, v in fields}, p, n, log, sk, sets={k: unjv(v) for k, v in sets})
            res = None if r is None else js(r)
        elif kind == "replace":
            _, m, fields, sk = op
            r = ms[m].write("replace", None, {k: unjv(v) for k, v in fields}, log, sk)
            res = None if r is None else js(r)
        elif kind in ("inc", "set", "unset"):
            m, p, arg, sk = op[1], op[2], op[3] if kind != "unset" else None, op[-1]
            if kind == "set":
                arg = unjv(arg)
            r = ms[m].write(kind, p, arg, log, sk)
            res = None if r is None else js(r)
        elif kind == "delete":
            _, m, sk = op
            r = ms[m].write("delete", None, None, log, sk)
            res = None if r is None else js(r)
        elif kind == "join":
            _, r, s = op
            changed, fresh = ms[r].absorb(ms[s].state.copy())
            res = {"changed": changed, "fresh": [jk(k) for k in sorted(fresh, key=lambda k: (KIND[k[0]], k[1], k_stamp(k[2])))]}
        elif kind == "loss":
            _, r, s, pick = op
            keys = sorted(ms[s].state.losses, key=lambda k: (KIND[k[0]], k[1], k_stamp(k[2])))
            if keys:
                key = keys[pick % len(keys)]
                res = {"key": jk(key), "added": ms[r].recv_loss(key, ms[s].state.losses[key])}
        elif kind == "backup":
            _, m = op
            saved[m] = ms[m].state.copy()
        elif kind == "restore":
            _, m, o, ep = op
            if saved[m] is not None:
                ms[o].absorb(ms[m].state.copy())     # its writes exist somewhere before the restore
                ms[m].state = saved[m].copy()
                ms[m].epoch = ep
                res = True
        steps.append({"res": res, "after": snap(ms, log)})
    # final full-mesh sync (model5.sync over whole states)
    for _ in range(12):
        before = [m.state.canon() for m in ms]
        for r in ms:
            for s in ms:
                if r is not s:
                    r.absorb(s.state.copy())
        if [m.state.canon() for m in ms] == before:
            break
    fin = {"after": snap(ms, log)}
    if dump_canon:
        fin["canon"] = [Enc(log).state(m.state).hex() for m in ms]
    want, alive, dels = M.oracle(log)
    must = M.loss_oracle(log, alive, dels)
    have = sorted(ms[0].state.losses, key=lambda k: (KIND[k[0]], k[1], k_stamp(k[2])))
    fin["oracle"] = {
        "doc": jdoc(want),
        "alive": sorted([js(L) for L in alive]),
        "dels": sorted([js(d) for d in dels]),
        "must": sorted([[p, js(st)] for p, st in must]),
        "hidden": {p: str(h) for p, h in M.hidden_oracle(log).items()},
        "ksa": sorted([sorted([[p, js(st)] for p, st in g]) for g in M.killed_superseded_any(log)]),
        "have": [jk(k) for k in have],
        "stale": sorted([jk(k) for k in M.stale_records(set(have), log)]),
        "unsound": [jk(k) for k in have if not M.sound_record(k, log, alive, dels)],
        "converged": len({m.state.canon() for m in ms}) == 1,
        "log": len(log),
    }
    return {"steps": steps, "final": fin}


def plant(which):
    """Change the model on purpose: the Rust replay must then disagree with the scripts."""
    if which == "same-ignores-type":
        M.same = lambda a, b: a == b
    elif which == "hidden-le":
        def hidden(self, p):
            al = set(self.alive())
            g = self.gen_of(p)
            if not g:
                return 0
            seen = g[2] if len(g) > 2 else {}
            return sum(v[1] - seen.get((k[0], k[2], k[3]), 0) for k, v in self.slots.items()
                       if k[1] == p and k[0] in al and (k[3] is None or k[3] <= g[0]))
        M.State.hidden = hidden
    elif which == "no-saturation":
        orig = M.State.value

        def value(self, p):
            b = M.BOUND
            M.BOUND = 10 ** 40
            try:
                return orig(self, p)
            finally:
                M.BOUND = b
        M.State.value = value
    elif which == "zero-absent":
        # the model before the B1 fix: a counter whose increments net to 0 is absent
        def value(self, p):
            regs = [(r, k) for k, r in self.live_regs(p)]
            win = max(regs, key=lambda x: x[0][0])[0] if regs else None
            if win is not None and not M.numeric(win[1]):
                return win[1]
            base = win[1] if win is not None else None
            obs = win[2] if win is not None else {}
            total, seen = (base if base is not None else 0), base is not None
            for k in sorted(self.counted(p), key=repr):
                o = obs.get(k[0:1] + k[2:])
                d = self.slots[k][1] - (o[1] if o else 0)
                if d:
                    total += d
                    seen = True
            return max(min(total, M.BOUND), -M.BOUND - 1) if seen else None
        M.State.value = value
    else:
        raise SystemExit("unknown plant " + which)


def golden():
    """Hash and canonical length of the golden vectors of merge_state/tests.rs, from the model."""
    M.RET = 10 ** 9
    M.BOUND = I64_MAX
    out = {}

    def h(name, st, log):
        b = Enc(log).state(st)
        cb = EncCompact(log).state(st)
        out[name] = [hash128(b), len(b)]
        out[name + "-compact"] = [hash128(cb), len(cb)]

    h("empty", M.State(), {})
    # 170: x=100 inserted by A, then +20 by B and +50 by C, all joined into A
    log = {}
    a, b, c = D("A"), D("B"), D("C")
    a.insert({"x": 100}, log)
    b.absorb(a.state.copy())
    c.absorb(a.state.copy())
    b.write("inc", "x", 20, log)
    c.write("inc", "x", 50, log)
    a.absorb(b.state.copy())
    a.absorb(c.state.copy())
    h("170", a.state, log)
    # mixed: a superseded first insert, an upsert with $setOnInsert (dropped for the alive insert), a clash
    log = {}
    a, b, c, o = D("A"), D("B"), D("C"), D("O")
    a.insert({"x": 1, "n": "a"}, log)
    ea = a.state.copy()
    b.clock = 5
    b.insert({"y": 2, "n": "b"}, log)
    eb = b.state.copy()
    c.upsert_soi({"theme": "dark"}, "views", 1, log, 0, sets={"lang": 1.5})
    ec = c.state.copy()
    for e in (ea, eb, ec):
        o.absorb(e)
    h("mixed", o.state, log)
    # delete: A inserts and deletes; B and C write into the insert without having seen the delete
    log = {}
    a, b, c, o = D("A"), D("B"), D("C"), D("O")
    a.insert({"x": 1, "n": "a"}, log)
    e0 = a.state.copy()
    b.absorb(e0.copy())
    c.absorb(e0.copy())
    a.write("delete", None, None, log)
    e1 = a.state.copy()
    b.write("set", "x", 5, log)
    e2 = b.state.copy()
    c.write("inc", "y", 2, log)
    e3 = c.state.copy()
    for e in (e0, e1, e2, e3):
        o.absorb(e)
    h("delete", o.state, log)
    # obs: bases written after counted increments (pins the layout of a register's obs and of a
    # generation's seen totals): A inserts {x, y}; B adds 7 to y concurrently; A adds 3 to x and sets
    # x to 20, adds 2 to y and sets y to a string
    log = {}
    a, b, o = D("A"), D("B"), D("O")
    a.insert({"x": 10, "y": 5}, log)
    e0 = a.state.copy()
    b.absorb(e0.copy())
    b.write("inc", "y", 7, log)
    e1 = b.state.copy()
    a.write("inc", "x", 3, log)
    a.write("set", "x", 20, log)
    a.write("inc", "y", 2, log)
    a.write("set", "y", "t", log)
    e2 = a.state.copy()
    for e in (e0, e1, e2):
        o.absorb(e)
    h("obs", o.state, log)
    print(json.dumps(out))


def main():
    args = sys.argv[1:]
    if args == ["--golden"]:
        golden()
        return
    if "--plant" in args:
        i = args.index("--plant")
        plant(args[i + 1])
        del args[i:i + 2]
    out = args[0]
    n = int(args[1])
    first = int(args[2]) if len(args) > 2 else 0
    os.makedirs(out, exist_ok=True)
    for seed in range(first, first + n):
        sc = gen_script(seed)
        sc["expect"] = run_script(sc)
        with open(os.path.join(out, f"s{seed:05d}.json"), "w") as f:
            json.dump(sc, f)
    print(f"wrote {n} scripts to {out}")


if __name__ == "__main__":
    main()
