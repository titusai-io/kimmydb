"""Differential test of the Rust `merge_state` module on NESTED documents against the stage A Python
reference model: the script generator and the expected answers.

Drives the vendored stage A model (`model6_stageA.py`, with `pullmodel6_stageA.Node`'s normalised
`finish`) through random operation scripts and writes, for every step, what the Rust side
(tests/merge_state_diff_nested.rs) must reproduce:

  * the result of every write (its stamp, or refused / a no-op),
  * for every join: the `changed` flag and the fresh loss keys; for every loss entry: acceptance,
  * after every step, for every member: the visible document (every present path, the order of a
    level left out), the hidden figure, and a DIGEST of the state: the lives, deletes, dead lives,
    registers (stamp, value, observed totals, generation, `$setOnInsert` mark), counter slots, version
    vector and loss records, as sorted text lines hashed together (`--explain` prints the lines),
  * at the end (after a full-mesh sync): the oracle's answers (visible document, alive lives, required
    heads, hidden figure, killed-and-superseded groups, and the stale/unsound verdicts on member 0's
    records).

THE VENDORED MODEL IS NEVER EDITED. Two things are changed here, in the generator, on the imported module:

  * `same` (the write diff's equality) compares by bit pattern, and a Decimal128 by its bytes (the
    ruling of 2026-11-03: a no-op is a write that leaves the stored representation byte-identical);
    the vendored one compares `0.0` with `-0.0` as equal numbers;
  * `--plant NAME` flips one of the model's own mutation switches, so that the Rust replay MUST
    disagree (the self-check of the comparison).

THE RESTRICTED SUBSET. The Rust side does not implement everything stage A2 may change, and none of
these restrictions is lifted here (each is lifted deliberately when the Rust side implements it):

  * nulls (Null versus Unset): no value is `None`; `$set x null` is never drawn;
  * a map-creating `$inc` and any leaf write under an absent map (the implicit marker, Q2): every
    drawn write is dry-run on a copy of the writer and dropped if it would write a marker it did not
    name, or two stamps;
  * multi-operator updates (one stamp): no `setmany`, and the two-write upsert is not drawn;
  * restore and covered-entry cases (Q3): no backup, no restore;
  * order state and field order (Q4): no order-only write is drawn (a replace or a `$set` of a
    document whose content is the writer's but whose key order is not), and the visible documents are
    compared without the order of a level;
  * the absorbed counter rule (Q6): the stage A model has none, as the Rust side.

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


# the vendored files, byte for byte; pullmodel6_stageA.py imports its model as `model6`
M = _load("model6", "model6_stageA.py")
PM = _load("pullmodel6_stageA", "pullmodel6_stageA.py")

I64_MAX = 2 ** 63 - 1


def _bits(x):
    return struct.pack(">d", x) if isinstance(x, float) else x


def same_by_bits(a, b):
    """The diff, as ruled 2026-11-03: a no-op is a write that leaves the stored representation
    byte-identical, so doubles compare by bit pattern (0.0 is not -0.0; a NaN is its own bits), at any
    depth of an array, and types are strict. The vendored `same` compares numbers."""
    if isinstance(a, tuple) and isinstance(b, tuple):
        return len(a) == len(b) and all(same_by_bits(x, y) for x, y in zip(a, b))
    return type(a) is type(b) and not isinstance(a, tuple) and _bits(a) == _bits(b)


M.same = same_by_bits


class D(PM.Node):
    """pullmodel6_stageA.Node without the oplog protocol: a member that absorbs whole states."""

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


def digest_lines(s):
    out = []
    for L, v in s.lives.items():
        out.append("life %s seen[%s] %s" % (fs(L), ",".join(sorted(fs(d) for d in v[0])), v[2]))
    for L in s.dead:
        out.append("dead " + fs(L))
    for d, vv in s.deletes.items():
        out.append("del %s vv[%s]" % (fs(d), ",".join(sorted("%s=%d" % (fw(w), h) for w, h in vv.items()))))
    for (L, p), r in s.regs.items():
        obs = fobs({k: "%s,%s" % (fs(o[0]), o[1]) for k, o in r[2].items()})
        gen = r[3]
        if gen is None:
            g = "-"
        else:
            g = fs(gen[0]) + "/seen" + fobs({k: str(t) for k, t in gen[2].items()})
        soi = 1 if len(r) > 4 and r[4] == "soi" else 0
        out.append("reg %s %s %s %s obs%s gen=%s soi=%d" % (fs(L), p, fs(r[0]), fv(r[1]), obs, g, soi))
    for (L, p, w, g), (st, tot) in s.slots.items():
        out.append("slot %s %s %s %s %s %d" % (fs(L), p, fw(w), fg(g), fs(st), tot))
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
    NO implicit marker, one stamp at most, and no order-only write. Returns True if the op may be drawn."""
    t = light_copy(m)
    log = {}
    apply(t, log)
    if len(log) > 1:
        return False
    for _, (_, meta) in log.items():
        if any(isinstance(v, str) and v == M.MAPI for v in meta.get("vals", {}).values()):
            return False
        if meta["kind"] == "reg" and not meta["paths"]:
            return False                     # an order-only write
    return True


# Field names no dotted spelling can carry, and which of them this model takes. Paths here ARE dotted strings, so a
# name with a dot cannot be spelled at all. `_` cannot be taken: the model names the whole document `_` in a loss
# record (`("insertLost", "_", life)`), so a field `_` would share its record key with the document's. The EMPTY
# name can: a top-level "" and a "" below a map ("a." is the path a, "") agree with the Rust side in every script
# drawn (checked at 1000 scripts), though the model keys the root document's order state by "" too
# (`parent_of(p) or ""`, `self.ords[(L, "")]` in `model6_stageA.py`): the scripts draw no order-only write and the
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
        before = [mm.state.canon() for mm in ms]
        for r in ms:
            for s in ms:
                if r is not s:
                    r.absorb(s.state.copy())
        if [mm.state.canon() for mm in ms] == before:
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
        "converged": len({mm.state.canon() for mm in ms}) == 1,
        "log": len(log),
    }
    return {"seed": seed, "ret": ret, "bound": bound, "members": nm, "ops": ops,
            "expect": {"steps": stepsout, "final": fin}}, ms


# a change planted in the model on purpose: the Rust replay must disagree (the module attribute set)
PLANTS = {
    "scalar-hides-newer": ("TYPE_IGNORES_RECENCY", True),
    "marker-hides": ("MARKER_HIDES", True),
    "rewrite-unchanged": ("REWRITE_UNCHANGED", True),
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
        for pl in plants:                     # reset the switches for the next script
            setattr(M, PLANTS[pl][0], False)


if __name__ == "__main__":
    main()
