"""Differential test of the Rust `merge_state` module on NESTED documents against the stage A Python
reference model: the script generator and the expected answers.

Drives the vendored A2.3 model (`model6.py`, with `pullmodel6.Node`'s normalised
`finish`) through random operation scripts and writes, for every step, what the Rust side
(tests/merge_state_diff_nested.rs) must reproduce:

  * the result of every write (its stamp, or refused / a no-op),
  * for every join: the `changed` flag and the fresh loss keys; for every loss entry: acceptance,
  * after every step, for every member: the visible document (every present path, the order of a
    level left out), the hidden figure, and a DIGEST of the state: the lives, deletes, dead lives,
    registers (stamp, value, generation, `$setOnInsert` mark), counter slots (stamp, total and the absorbed pair), version
    vector and loss records, as sorted text lines hashed together (`--explain` prints the lines),
  * at the end (after a full-mesh sync): the oracle's answers (visible document, alive lives, required
    heads, hidden figure, killed-and-superseded groups, and the stale/unsound verdicts on member 0's
    records).

THE VENDORED MODEL IS NEVER EDITED, and no longer patched: its `same` is the Rust diff (bit pattern for doubles, the
representation for a Decimal128, type strict), and the harness asserts that at import. It sets the model as the Rust side is
today: `ABSORBED = True` (the absorbed counter cell, Q6) and `NO_IMPLICIT_MARKER = False` (the implicit marker, Q2, is built). `--plant NAME`
flips one of the model's own mutation switches, so that the Rust replay MUST disagree (the self-check of the comparison).
States are compared with `exact()`, never through the repr strings of `canon()` alone (the bits of a NaN).

THE RESTRICTED SUBSET. The Rust side does not implement everything stage A2 may change, and none of
these restrictions is lifted here (each is lifted deliberately when the Rust side implements it):

  * nulls (Null versus Unset): no value is `None`; `$set x null` is never drawn;
  * every drawn write is dry-run on a copy of the writer and dropped if it would take two stamps (a leaf write under
    an absent map, and a map-creating `$inc`, are drawn: the implicit marker, Q2, is built);
  * multi-operator updates (one stamp): no `setmany`, and the two-write upsert is not drawn;
  * restore and covered-entry cases (Q3): no backup, no restore;
  * order state and field order (Q4): no order-only write is drawn (a replace or a `$set` of a
    document whose content is the writer's but whose key order is not), and the visible documents are
    compared without the order of a level;

Usage (standard library only):
  python3 -I q7diff_gen6.py <out-dir> <n-scripts> [first-seed] [--plant NAME]
      write s<seed>.json for each seed
  python3 -I q7diff_gen6.py --explain <seed> [step]
      print the digest lines of every member after the given step (default: the final sync) of that script
"""
import ast
import copy
import importlib.util
import json
import os
import random
import struct
import sys

HERE = os.path.dirname(os.path.abspath(__file__))


def _load(name, fname):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, fname))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


# the vendored files, byte for byte; pullmodel6.py imports its model as `model6`
M = _load("model6", "model6.py")
PM = _load("pullmodel6", "pullmodel6.py")

I64_MAX = 2 ** 63 - 1


# The vendored `same()` is the diff the Rust side uses (bit pattern for doubles, representation for Decimal128, type strict): the
# harness no longer patches it; it asserts what it relies on. (The scripts draw no NaN and no Dec128; the Rust side tests both.)
_nan1 = struct.unpack(">d", bytes.fromhex("7ff8000000000001"))[0]
_nan2 = struct.unpack(">d", bytes.fromhex("7ff8000000000002"))[0]
assert M.same(0.0, 0.0) and not M.same(0.0, -0.0)
assert M.same(_nan1, _nan1) and not M.same(_nan1, _nan2)
assert M.same(M.Dec128("1.0"), M.Dec128("1.0")) and not M.same(M.Dec128("1.0"), M.Dec128("1.00"))
assert not M.same(M.Dec128("1"), "1") and not M.same(1, 1.0)
# the harness runs the model as the Rust side is today: the absorbed counter cell, and the implicit map marker (Q2)
M.ABSORBED = True
M.NO_IMPLICIT_MARKER = False


def _x(v):
    """A value as something that tells apart what the repr strings of `canon()` do not: the bits of a double (a NaN payload, -0.0),
    a Dec128 from the plain string of the same text."""
    if isinstance(v, float):
        return ("f", struct.pack(">d", v).hex())
    if isinstance(v, tuple):
        return ("t", tuple(_x(e) for e in v))
    if isinstance(v, dict):
        return ("d", tuple((k, _x(e)) for k, e in v.items()))
    return (type(v).__name__, v)


def exact(st):
    """The state, compared exactly: `canon()` plus the register and loss values by their bits (never through repr strings alone)."""
    return (st.canon(), tuple(sorted((repr(k), _x(r[1])) for k, r in st.regs.items())),
            tuple(sorted((repr(k), _x(v)) for k, v in st.losses.items())))


class D(PM.Node):
    """pullmodel6.Node without the oplog protocol: a member that absorbs whole states."""

    def absorb(self, other):
        before = self.state
        merged, fresh = M.join(before, other)
        self.clock = max([self.clock] + list(merged.vv.values()))
        changed = exact(merged) != exact(before)
        self.state = merged
        return changed, fresh

    def recv_loss(self, key, val):
        if key[2][0] < self.state.horizon() or key in self.state.losses:
            return False
        self.state.losses[key] = val
        return True


def light_copy(m):
    """A copy of a member that a trial write may change: state and clock only."""
    t = D(m.name)
    t.epoch, t.clock = m.epoch, m.clock
    t.state = m.state.copy()
    t.known = getattr(m, "known", 0)
    return t


# ---- value encodings -----------------------------------------------------------------------------

def jv(v):
    """A script value: ['i', '123'], ['d', 'bits'], ['s', text], ['a', [values]], ['m', [[name, value]]]."""
    if isinstance(v, dict):
        return ["m", [[k, jv(x)] for k, x in v.items()]]
    if isinstance(v, tuple):
        return ["a", [jv(x) for x in v]]
    if isinstance(v, bool):
        raise ValueError("no booleans in this subset")
    if isinstance(v, int):
        return ["i", str(v)]
    if isinstance(v, float):
        return ["d", struct.pack(">d", v).hex()]
    return ["s", v]


def unjv(j):
    t = j[0]
    if t == "m":
        return {k: unjv(x) for k, x in j[1]}
    if t == "a":
        return tuple(unjv(x) for x in j[1])
    if t == "i":
        return int(j[1])
    if t == "d":
        return struct.unpack(">d", bytes.fromhex(j[1]))[0]
    return j[1]


def fs(st):
    return "%d:%s:%d" % (st[0], st[1][0], st[1][1])


def fw(w):
    return "%s:%d" % (w[0], w[1])


def fv(v):
    """The digest form of a register value (the Rust side prints the same)."""
    if v is None:
        return "u"
    if isinstance(v, str) and v == M.MAP:
        return "m"
    if isinstance(v, str) and v == M.MAPI:
        return "mi"
    if isinstance(v, tuple):
        return "a[" + ",".join(fv(x) for x in v) + "]"
    if isinstance(v, int):
        return "i:%d" % v
    if isinstance(v, float):
        return "d:" + struct.pack(">d", v).hex()
    return "s:" + json.dumps(v)


def fg(g):
    return "-" if g is None else fs(g)


def fobs(d):
    return "[" + ",".join(sorted("%s|%s|%s=%s" % (fs(k[0]), fw(k[1]), fg(k[2]), val)
                                 for k, val in d.items())) + "]"


# THE REGISTER `obs` IS DEAD under ABSORBED = True. The model still keeps a register's `obs` map (what its base counted); the Rust
# cell has none (a slot's absorbed pair replaces it), so nothing may depend on it. Checked, not assumed, on EVERY state this harness
# encodes or digests that has any register obs entry: every read of the model is the same with the obs maps cleared as with them.
# (An obs key can name a slot of a life that was collected past the horizon, so `obs` may be non-empty with an empty absorbed
# store; seed 792 of the nested generator is such a state. Such a key can matter only if the cells of that life come back from a state
# older than the horizon, which is outside the retention claim.) A mismatch fails the run.
def _obs_reads(s):
    paths = sorted({k[1] for k in list(s.regs) + list(s.slots)})
    slot_paths = sorted({k[1] for k in s.slots})
    return (repr(s.visible()), repr(s.hidden_all()), repr(s.order_report()),
            repr([(p, s.value(p)) for p in paths]),
            repr([(p, sorted(map(repr, s.counted(p)))) for p in slot_paths]))


# THE STRUCTURAL FORM of the same claim, asserted on EVERY digested state (it is what makes the obs redundant for every LATER join
# as well, not only for the reads of the present state): for every register obs key whose slot the state holds, the slot's absorbed
# pair exists and is at least as late as the obs entry (`absorbed[k][0] >= obs[k][0]`). A join takes the max of both components and
# drops both with the life, so the property carries through every later join. An obs key whose slot is gone (see above) is dangling
# and is not part of the claim.
def check_obs_structural(s):
    for (L, p), r in s.regs.items():
        for (l2, w2, g2), o in r[2].items():
            k = (l2, p, w2, g2)
            if k in s.slots:
                a = s.absorbed.get(k)
                assert a is not None and a[0] >= o[0], \
                    "a register obs entry %r of %r is not covered by the slot's absorbed pair %r" % (o, k, a)


# What the shape of seed 792 is counted by, per script (REPORTED on stdout by `main`, one line per script; nothing in the script file or in
# any digest depends on it): over the states the script digests, how many hold a register obs key whose slot is gone (a dangling obs
# key), how many keys that is, and how many hold a dangling generation `seen` key. A `seen` key is dangling the same way (its slot is
# gone); it lasts as long as the register holding the generation (only a NON-numeric base starts a generation, so a numeric `$set` or an
# `$unset` copies the winner's, dangling keys included), is dead for every read (`hidden` reads `seen` only for the slots of alive lives)
# and is bounded by the slots alive at the generation's start.
DANGLING = {"states": 0, "obs_states": 0, "obs_keys": 0, "seen_states": 0}


def _count_dangling(s):
    DANGLING["states"] += 1
    obs = seen = 0
    for (L, p), r in s.regs.items():
        obs += sum(1 for (l2, w2, g2) in r[2] if (l2, p, w2, g2) not in s.slots)
        g = r[3] if len(r) > 3 else None
        if g and len(g) > 2:
            seen += sum(1 for (l2, w2, g2) in g[2] if (l2, p, w2, g2) not in s.slots)
    DANGLING["obs_keys"] += obs
    DANGLING["obs_states"] += 1 if obs else 0
    DANGLING["seen_states"] += 1 if seen else 0


def check_obs_dead(s):
    check_obs_structural(s)
    _count_dangling(s)
    if not any(r[2] for r in s.regs.values()):
        return
    cleared = s.copy()
    cleared.regs = {k: (r[0], r[1], {}) + tuple(r[3:]) for k, r in s.regs.items()}
    assert _obs_reads(s) == _obs_reads(cleared), "a read of the model depends on a register's obs under ABSORBED"


def digest_lines(s):
    check_obs_dead(s)
    out = []
    for L, v in s.lives.items():
        out.append("life %s seen[%s] %s" % (fs(L), ",".join(sorted(fs(d) for d in v[0])), v[2]))
    for L in s.dead:
        out.append("dead " + fs(L))
    for d, vv in s.deletes.items():
        out.append("del %s vv[%s]" % (fs(d), ",".join(sorted("%s=%d" % (fw(w), h) for w, h in vv.items()))))
    for (L, p), r in s.regs.items():
        gen = r[3]
        if gen is None:
            g = "-"
        else:
            g = fs(gen[0]) + "/seen" + fobs({k: str(t) for k, t in gen[2].items()})
        soi = 1 if len(r) > 4 and r[4] == "soi" else 0
        out.append("reg %s %s %s %s gen=%s soi=%d" % (fs(L), p, fs(r[0]), fv(r[1]), g, soi))
    assert set(s.absorbed) <= set(s.slots), "an absorbed pair without its slot"
    for (L, p, w, g), (st, tot) in s.slots.items():
        a = s.absorbed.get((L, p, w, g))
        ab = "-" if a is None else "%s,%d" % (fs(a[0]), a[1])
        out.append("slot %s %s %s %s %s %d abs=%s" % (fs(L), p, fw(w), fg(g), fs(st), tot, ab))
    for w, h in s.vv.items():
        out.append("vv %s %d" % (fw(w), h))
    for (kind, path, st), val in s.losses.items():
        if path == "_" and isinstance(val, str):
            # the model stores the field list of a lost whole document as its repr
            vs = "f[" + ",".join(sorted(ast.literal_eval(val))) + "]"
        else:
            vs = fv(val)
        out.append("loss %s %s %s %s" % (kind, path, fs(st), vs))
    return sorted(out)


M64 = (1 << 64) - 1


def rotl(x, r):
    return ((x << r) | (x >> (64 - r))) & M64


def splitmix(z):
    z = (z + 0x9E3779B97F4A7C15) & M64
    z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & M64
    z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & M64
    return z ^ (z >> 31)


def hash128(data):
    """A 128-bit hash of the digest text (a comparison tool, not a format; tests/merge_state_diff_nested.rs
    has the same function)."""
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
    return "%016x%016x" % (x, y)


def digest(s):
    return hash128("\n".join(digest_lines(s)).encode())


def flat_visible(v):
    """The nested visible document as the Rust `visible()` map: every present path, a map marked 'm'."""
    if v is None:
        return None
    out = {}

    def walk(d, prefix):
        for k, x in d.items():
            q = prefix + k
            if isinstance(x, dict):
                out[q] = ["m"]
                walk(x, q + ".")
            else:
                out[q] = jv(x)
    walk(v, "")
    return out


def js(st):
    return [st[0], st[1][0], st[1][1]]


def jk(k):
    return [k[0], k[1], js(k[2])]


def kkey(k):
    return (k[0], k[1], k[2])


# ---- one script -----------------------------------------------------------------------------------

NAMES = "ABCD"
SCALARS_DRAW = [0, 1, 7, 500, -3, "s", "t", "", 2.5, 2.0, 0.0, -0.0, 1e300, (1, 2), (), (1, 3), (2.5, "s"), (2.0,)]


def pools(bound):
    if bound == I64_MAX:
        ints = [0, 1, 2, 7, -3, 500, 2 ** 62, -(2 ** 62), I64_MAX, -I64_MAX - 1]
        incs = [1, 5, 20, -3, 50, 2 ** 62, -(2 ** 62), I64_MAX, -I64_MAX - 1]
    else:
        ints = [0, 1, 2, 7, -3, 500, 900, -900, bound, -bound - 1, 5000]
        incs = [1, 5, 20, -3, 50, 200, -200, 600, -600, bound + 1]
    return ints, incs


def trial_ok(m, apply):
    """Dry-run `apply(member, log)` on a copy: the write must be one the Rust side models, that is
    one stamp at most and no order-only write. Returns True if the op may be drawn."""
    t = light_copy(m)
    log = {}
    apply(t, log)
    if len(log) > 1:
        return False
    for _, (_, meta) in log.items():
        if meta["kind"] == "reg" and not meta["paths"]:
            return False                     # an order-only write
    return True


# Field names no dotted spelling can carry, and which of them this model takes. Paths here ARE dotted strings, so a
# name with a dot cannot be spelled at all. `_` cannot be taken: the model names the whole document `_` in a loss
# record (`("insertLost", "_", life)`), so a field `_` would share its record key with the document's. The EMPTY
# name can: a top-level "" and a "" below a map ("a." is the path a, "") agree with the Rust side in every script
# drawn (checked at 1000 scripts), though the model keys the root document's order state by "" too
# (`parent_of(p) or ""`, `self.ords[(L, "")]` in `model6.py`): the scripts draw no order-only write and the
# order of a level is not compared (see the restricted subset in `merge_state_diff_nested.rs`), so that key is never
# read for a field named "". The sim and the join-law tests have all three names.
ODD_LEAVES = ["", "a.", "a.d."]
ODD_PARENTS = {"a": [{"": 1}, {"": 2, "b": 1}, {"d": {"": 5}}, {"b": 1, "": {"": 1}}],
               "": [{"b": 1}, {"": 1}, {"b": {"c": 2}, "": 3}]}


def draw_odd(rnd, m, ints, incs, sk):
    """One write at an empty name (at the top or below a map), or of a sub-document that has one."""
    k = rnd.choice(["set", "set", "inc", "unset", "setp"])
    if k == "set":
        p = rnd.choice(ODD_LEAVES)
        v = rnd.choice(ints + SCALARS_DRAW)
        return (["set", m, p, jv(v), sk], lambda t, log, p=p, v=v, sk=sk: t.write("set", p, v, log, sk))
    if k == "inc":
        p = rnd.choice(ODD_LEAVES)
        n = rnd.choice(incs)
        return (["inc", m, p, n, sk], lambda t, log, p=p, n=n, sk=sk: t.write("inc", p, n, log, sk))
    if k == "unset":
        p = rnd.choice(ODD_LEAVES + ["a"])
        return (["unset", m, p, sk], lambda t, log, p=p, sk=sk: t.write("unset", p, None, log, sk))
    p = rnd.choice(sorted(ODD_PARENTS))
    v = copy.deepcopy(rnd.choice(ODD_PARENTS[p]))
    return (["set", m, p, jv(v), sk], lambda t, log, p=p, v=v, sk=sk: t.write("set", p, v, log, sk))


def draw_candidate(rnd, ms, m, ints, incs, nm):
    """One candidate op, as (script op, apply function) or None."""
    x = rnd.random()
    sk = rnd.choice([0, 0, 0, 4])
    mem = ms[m]
    alive = bool(mem.state.alive())
    if not alive:
        k = rnd.choice(["insert", "insert", "insert", "upsert"])
        if k == "upsert":
            p = rnd.choice(["x", "y", "n"])
            n = rnd.choice(incs)
            return (["upsert", m, p, n, sk], lambda t, log, p=p, n=n, sk=sk: t.upsert_inc(p, n, log, sk))
        doc = PM.rand_doc(rnd) or {"x": rnd.choice(ints)}
        return (["insert", m, jv(doc), sk], lambda t, log, doc=doc, sk=sk: t.insert(doc, log, sk))
    if rnd.random() < 1 / 8:          # one write in eight is at an empty name below a map
        return draw_odd(rnd, m, ints, incs, sk)
    k = rnd.choice(["inc"] * 4 + ["set"] * 4 + ["setp"] * 3 + ["unset"] * 2 + ["unsetp", "replace", "replace",
                    "delete", "below", "insert", "array"])
    if k == "inc":
        p = rnd.choice(["acct.balance", "acct.balance", "a.b", "a.c", "a.d.e", "x", "y", "n"])
        n = rnd.choice(incs)
        return (["inc", m, p, n, sk], lambda t, log, p=p, n=n, sk=sk: t.write("inc", p, n, log, sk))
    if k == "set":
        p = rnd.choice(PM.LEAVES)
        v = rnd.choice(ints + SCALARS_DRAW)
        return (["set", m, p, jv(v), sk], lambda t, log, p=p, v=v, sk=sk: t.write("set", p, v, log, sk))
    if k == "setp":
        p = rnd.choice(PM.PARENTS)
        v = copy.deepcopy(rnd.choice(PM.SUBDOCS[p] * 2 + [5, "s", (1, 2)]))
        return (["set", m, p, jv(v), sk], lambda t, log, p=p, v=v, sk=sk: t.write("set", p, v, log, sk))
    if k == "array":
        p = rnd.choice(["items", "items", "a.list"])
        v = tuple(rnd.choice([1, 2, 3, "s"]) for _ in range(rnd.randrange(3)))
        return (["set", m, p, jv(v), sk], lambda t, log, p=p, v=v, sk=sk: t.write("set", p, v, log, sk))
    if k == "unset":
        p = rnd.choice(PM.LEAVES)
        return (["unset", m, p, sk], lambda t, log, p=p, sk=sk: t.write("unset", p, None, log, sk))
    if k == "unsetp":
        p = rnd.choice(PM.PARENTS + ["items"])
        return (["unset", m, p, sk], lambda t, log, p=p, sk=sk: t.write("unset", p, None, log, sk))
    if k == "replace":
        doc = PM.rand_doc(rnd)
        return (["replace", m, jv(doc), sk], lambda t, log, doc=doc, sk=sk: t.write("replace", None, doc, log, sk))
    if k == "delete":
        if rnd.random() < 0.25:
            return (["delete", m, sk], lambda t, log, sk=sk: t.write("delete", None, None, log, sk))
        return None
    if k == "below":
        # a write below a scalar or an array, on purpose: the writer must refuse it
        p = rnd.choice(["x.k", "items.0", "n.a.b", "a.b.c", "acct.balance.q"])
        kind = rnd.choice(["set", "unset", "inc"])
        if kind == "set":
            return (["set", m, p, jv(1), sk], lambda t, log, p=p, sk=sk: t.write("set", p, 1, log, sk))
        if kind == "unset":
            return (["unset", m, p, sk], lambda t, log, p=p, sk=sk: t.write("unset", p, None, log, sk))
        return (["inc", m, p, 1, sk], lambda t, log, p=p, sk=sk: t.write("inc", p, 1, log, sk))
    doc = rnd.choice([{"a": {"b": 1}}, {"acct": {"balance": 10}}, {"x": 1, "a": {}}, {"items": (1,)},
                      {"a": {"b": 2, "c": 3, "d": {"e": 1}}, "acct": {"balance": 5}},
                      {"n": "q", "a": {"d": {"f": 1}}}])
    return (["insert", m, jv(doc), sk], lambda t, log, doc=doc, sk=sk: t.insert(doc, log, sk))


def snap(ms):
    return [{"v": flat_visible(m.state.visible()),
             "hid": {p: str(h) for p, h in m.state.hidden_all().items()},
             "dg": digest(m.state)} for m in ms]


def sort_losses(keys):
    return sorted(keys, key=kkey)


def build(seed, plants=(), explain_step=None, explained=None):
    for k in DANGLING:
        DANGLING[k] = 0
    rnd = random.Random(seed)
    ret = rnd.choice([10 ** 9, 10 ** 9, 10 ** 9, 3, 5, 8, 12])
    bound = rnd.choice([I64_MAX, I64_MAX, 1000])
    nm = rnd.choice([2, 3, 3, 4])
    steps = rnd.choice([20, 40, 60])
    ints, incs = pools(bound)
    M.RET, M.BOUND = ret, bound
    for pl in plants:
        setattr(M, PLANTS[pl][0], PLANTS[pl][1])
    ms = [D(NAMES[i]) for i in range(nm)]
    log = {}
    ops, stepsout = [], []

    def record(op, res):
        ops.append(op)
        stepsout.append({"res": res, "after": snap(ms)})
        if explain_step is not None and len(ops) - 1 == explain_step:
            explained.extend((mm.name, digest_lines(mm.state)) for mm in ms)

    for _ in range(steps):
        m = rnd.randrange(nm)
        x = rnd.random()
        if x < 0.30:
            r, s = rnd.sample(range(nm), 2)
            changed, fresh = ms[r].absorb(ms[s].state.copy())
            record(["join", r, s], {"changed": changed, "fresh": [jk(k) for k in sort_losses(fresh)]})
            continue
        if x < 0.36:
            r, s = rnd.sample(range(nm), 2)
            pick = rnd.randrange(1 << 30)
            keys = sort_losses(ms[s].state.losses)
            res = None
            if keys:
                key = keys[pick % len(keys)]
                res = {"key": jk(key), "added": ms[r].recv_loss(key, ms[s].state.losses[key])}
            record(["loss", r, s, pick], res)
            continue
        cand = None
        for _try in range(8):
            c = draw_candidate(rnd, ms, m, ints, incs, nm)
            if c is not None and trial_ok(ms[m], c[1]):
                cand = c
                break
        if cand is None:
            continue
        op, apply = cand
        r = apply(ms[m], log)
        record(op, None if r is None else js(r))
    for _ in range(12):
        before = [exact(mm.state) for mm in ms]
        for r in ms:
            for s in ms:
                if r is not s:
                    r.absorb(s.state.copy())
        if [exact(mm.state) for mm in ms] == before:
            break
    fin = {"after": snap(ms)}
    want, alive, dels = M.oracle(log)
    must = M.loss_oracle(log, alive, dels)
    have = sort_losses(ms[0].state.losses)
    fin["oracle"] = {
        "doc": flat_visible(want),
        "alive": sorted(js(L) for L in alive),
        "dels": sorted(js(d) for d in dels),
        "must": sorted([p, js(st)] for p, st in must),
        "hidden": {p: str(h) for p, h in M.hidden_oracle(log).items()},
        "ksa": sorted(sorted([p, js(st)] for p, st in g) for g in M.killed_superseded_any(log)),
        "have": [jk(k) for k in have],
        "stale": sorted(jk(k) for k in M.stale_records(set(have), log)),
        "unsound": [jk(k) for k in have if not M.sound_record(k, log, alive, dels)],
        "converged": len({repr(exact(mm.state)) for mm in ms}) == 1,
        "log": len(log),
    }
    return {"seed": seed, "ret": ret, "bound": bound, "members": nm, "ops": ops,
            "expect": {"steps": stepsout, "final": fin}}, ms


# a change planted in the model on purpose: the Rust replay must disagree (the module attribute set)
PLANTS = {
    "scalar-hides-newer": ("TYPE_IGNORES_RECENCY", True),
    "marker-hides": ("MARKER_HIDES", True),
    "rewrite-unchanged": ("REWRITE_UNCHANGED", True),
    "no-implicit-marker": ("NO_IMPLICIT_MARKER", True),
}


def main():
    a = sys.argv[1:]
    if a and a[0] == "--explain":
        if len(a) > 2:                       # after step N
            ex = []
            build(int(a[1]), explain_step=int(a[2]), explained=ex)
            for name, lines in ex:
                print("== member", name)
                print("\n".join(lines))
            return
        sc, ms = build(int(a[1]))
        for mm in ms:
            print("== member", mm.name)
            print("\n".join(digest_lines(mm.state)))
        return
    plants = []
    if "--plant" in a:
        i = a.index("--plant")
        plants = [a[i + 1]]
        a = a[:i] + a[i + 2:]
    out, n = a[0], int(a[1])
    first = int(a[2]) if len(a) > 2 else 0
    os.makedirs(out, exist_ok=True)
    for seed in range(first, first + n):
        sc, _ = build(seed, plants)
        with open(os.path.join(out, "s%06d.json" % seed), "w") as f:
            json.dump(sc, f)
        print("dangling seed=%d states=%d obs_states=%d obs_keys=%d seen_states=%d" % (
            seed, DANGLING["states"], DANGLING["obs_states"], DANGLING["obs_keys"], DANGLING["seen_states"]))
        for pl in plants:                     # reset the switches for the next script
            setattr(M, PLANTS[pl][0], False)


if __name__ == "__main__":
    main()
