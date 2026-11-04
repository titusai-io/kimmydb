"""Q7 Step 0, Rust item O2: the ORDER differential. Random scripts of local writes AND receives, run on the A2.3 Python model (model6.py 72da18eb); the Rust
test (`tests/merge_state_ord_diff.rs`) replays each script through `Writer`s and compares, after every step, the acting member's
ordered document and order state, and the result of the step (the stamp it got, or none for a refusal or a no-op).

Upserts of NESTED paths are drawn (A2.3 names their ancestors, as the Rust writer does since eb40bbcf): in the nested scripts the
incremented path of an `upsert_inc` and the paths of an `upsert_soi` (creation fields, `$set` fields, incremented path, pairwise
disjoint) come from the nested pool.

Usage:   Q7MODEL_DIR=<q7step0-model> python3 -I -B gen_ord_differential.py <out-dir> [seed-to-show]
         (-B and sys.dont_write_bytecode: the model directory must not get a __pycache__)

Model switches (the restrictions are those of the Rust side today): ABSORBED = False (the counter cell has no `absorbed` yet;
Rust reads the winner's `obs` only), NO_IMPLICIT_MARKER = True (a leaf write under an absent map writes no marker: item O4 is not built;
the writer still names the ancestors to the order state), retention 10**9 (the associativity of the order join is claimed within
retention only). No field is named "" (the model keys the root of a replace by "" and cannot tell it from a top-level field of that
name: the Rust keys them OrdAt::Root and OrdAt::Field([""]) and has its own test); no name has a dot.

300 scripts: 150 over flat documents, 150 over nested ones, seeds fixed (flat 1..150, nested 1001..1150), 3 or 4 members, 30 steps. A
step is a write by one member (insert, set, unset, inc, replace, a multi-operator update, delete, an upserting inc, a $setOnInsert
upsert) or a receive (a member joins another member's current state). Every step records the digest (FNV-1a 64 of the canonical
text) of the acting member's ordered document and of its order state AFTER joining the empty state (the Rust writer's state is
normalised; the model's is not until the next join): the text is defined below and written by the Rust test in the same words. A
disagreement is a finding: re-run with a seed to print the model's text for every step.
"""
import hashlib, json, os, random, sys

sys.dont_write_bytecode = True
HERE = os.path.dirname(os.path.abspath(__file__))
MODEL = os.environ["Q7MODEL_DIR"]
sys.path.insert(0, MODEL)
import model6 as M            # noqa: E402

M.ABSORBED = False
M.NO_IMPLICIT_MARKER = True
M.RET = 10 ** 9

FIELDS = ["a", "b", "c", "d", "e", "f"]
NPATHS = ["a", "b", "c", "m", "m.x", "m.y", "m.z", "m.x.p", "m.x.q", "n", "n.k", "n.k.j"]
SUBNAMES = ["x", "y", "z", "p", "q"]


# ---- canonical text (the Rust test writes the same) ------------------------------------------------------------------------

def fnv(text):
    h = 0xcbf29ce484222325
    for b in text.encode("utf-8"):
        h ^= b
        h = (h * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return "%016x" % h


def st_text(s):
    return "%d.%s" % (s[0], s[1][0])


def val_text(v):
    if v == M.NULL:
        return "null"
    if isinstance(v, bool):
        raise AssertionError("bool")
    if isinstance(v, int):
        return "i:%d" % v
    if isinstance(v, float):
        import struct
        return "d:%016x" % struct.unpack("<Q", struct.pack("<d", v))[0]
    if isinstance(v, str):
        return "s:" + v
    if isinstance(v, tuple):
        return "a[" + ",".join(val_text(x) for x in v) + "]"
    raise AssertionError(v)


def doc_text(doc):
    """Pre-order lines `path=value` (`map` at a map) of an ordered visible document; `-` when no life is alive."""
    if doc is None:
        return "-"
    out = []

    def walk(d, prefix):
        for k, v in d.items():
            p = prefix + k
            if isinstance(v, dict):
                out.append(p + "=map")
                walk(v, p + ".")
            else:
                out.append(p + "=" + val_text(v))
    walk(doc, "")
    return "\n".join(out)


def floor_text(f):
    if f is None:
        return "-"
    if f[1] == "u":
        return "u:%s:%d:%d" % (st_text(f[0]), f[2], 1 if (len(f) > 3 and f[3]) else 0)
    return "r:%s" % st_text(f[0])


def ord_text(o):
    firsts = ";".join("%s=%s@%d" % (w[0], st_text(c[0]), c[1]) for w, c in sorted(o.firsts.items()))
    last = "-" if o.last is None else "%s@%d" % (st_text(o.last[0]), o.last[1])
    lvl = "-" if o.lvl is None else st_text(o.lvl)
    return "floor=%s firsts=[%s] last=%s lvl=%s" % (floor_text(o.floor), firsts, last, lvl)


def ords_text(ords):
    """One line per (life, path), sorted by life stamp then path segments (the root first). The root of a replace is keyed "" in the
    model; no scenario names a field "", so "" is the root."""
    rows = []
    for (L, p), o in ords.items():
        segs = [] if p == "" else p.split(".")
        rows.append(((L[0], L[1][0], L[1][1], segs), "%s@%s %s" % (st_text(L), "R" if p == "" else p, ord_text(o))))
    rows.sort(key=lambda r: r[0])
    return "\n".join(r[1] for r in rows)


def member_digest(m):
    norm, _ = M.join(m.state, M.State())
    doc = m.state.visible()
    return fnv(doc_text(doc) + "\n--\n" + ords_text(norm.ords)), doc_text(doc), ords_text(norm.ords)


# ---- JSON for the Rust side ------------------------------------------------------------------------------------------------

def j_stamp(s):
    return None if s is None else [s[0], s[1][0], s[1][1]]


def j_arg(a):
    if isinstance(a, dict):
        return {"t": "doc", "v": [[k, j_arg(v)] for k, v in a.items()]}
    if a is None:
        return {"t": "null"}
    if a == M.NULL:
        return {"t": "null"}
    if isinstance(a, bool):
        raise AssertionError
    if isinstance(a, int):
        return {"t": "i", "v": a}
    if isinstance(a, float):
        return {"t": "d", "v": repr(a)}
    if isinstance(a, str):
        return {"t": "s", "v": a}
    if isinstance(a, tuple):
        return {"t": "a", "v": [j_arg(x) for x in a]}
    raise AssertionError(a)


# ---- random scripts --------------------------------------------------------------------------------------------------------

def rvalue(rnd, nested, depth=0):
    r = rnd.random()
    if nested and depth < 2 and r < 0.3:
        names = rnd.sample(SUBNAMES, rnd.randint(0, 3))
        return {n: rvalue(rnd, nested, depth + 1) for n in names}
    if r < 0.55:
        return rnd.randint(-3, 9)
    if r < 0.75:
        return rnd.choice(["s", "t", "u"])
    if r < 0.85:
        return rnd.choice([0.5, 1.5, 2.25, -1.0])
    if r < 0.92:
        return None
    return tuple(rnd.randint(0, 3) for _ in range(rnd.randint(0, 3)))


def rdoc(rnd, nested):
    names = rnd.sample(FIELDS if not nested else ["a", "b", "c", "m", "n"], rnd.randint(0, 4))
    return {n: rvalue(rnd, nested) for n in names}


def disjoint(paths):
    return all(not (a == b or a.startswith(b + ".") or b.startswith(a + ".")) for i, a in enumerate(paths) for b in paths[i + 1:])


def script(kind, seed, steps=30):
    rnd = random.Random(seed)
    nested = kind == "nested"
    pool = NPATHS if nested else FIELDS
    nm = rnd.choice([3, 4])
    names = "ABCD"[:nm]
    ms = {n: M.Member(n) for n in names}
    log = {}
    out = []
    shown = []
    for i in range(steps):
        who = names[0] if i == 0 else rnd.choice(names)
        m = ms[who]
        alive = bool(m.state.alive())
        r = rnd.random()
        if i > 0 and r < 0.33:
            frm = rnd.choice([n for n in names if n != who])
            m.receive_entry(("w", ("snap", seed, i), M.wire(ms[frm].state)))
            d, dt, ot = member_digest(m)
            out.append({"k": "recv", "m": who, "from": frm, "d": d})
            shown.append((who, "recv " + frm, dt, ot))
            continue
        skew = rnd.choice([0, 0, 0, 0, 1, 3])
        if not alive:
            k = rnd.random()
            if k < 0.75:
                doc = rdoc(rnd, nested)
                st = m.insert(doc, log, skew)
                step = {"op": "insert", "arg": j_arg(doc)}
            elif k < 0.88:
                p = rnd.choice(NPATHS if nested else FIELDS)       # (A2.3: an upsert of a nested path names its ancestors)
                n = rnd.randint(-2, 5)
                st = m.upsert_inc(p, n, log, skew)
                step = {"op": "upsert_inc", "path": p.split("."), "arg": j_arg(n)}
            else:
                if nested:
                    # the paths of the creation, the $set fields and the incremented field may be nested; they must be disjoint
                    for _ in range(50):
                        ps = rnd.sample(NPATHS, rnd.randint(1, 4))
                        if disjoint(ps):
                            break
                    else:
                        ps = ps[:1]
                    p, rest = ps[0], ps[1:]
                    cut = rnd.randint(0, len(rest))
                    flds = {n_: rnd.randint(0, 9) for n_ in rest[:cut]}
                    sets = {n_: rnd.randint(0, 9) for n_ in rest[cut:]}
                else:
                    flds = {n_: rnd.randint(0, 9) for n_ in rnd.sample(FIELDS[:3], rnd.randint(0, 3))}
                    free = [f for f in FIELDS if f not in flds]
                    p = rnd.choice(free)
                    sets = {n_: rnd.randint(0, 9) for n_ in rnd.sample([f for f in free if f != p], rnd.randint(0, 2))}
                n = rnd.randint(1, 4)
                st = m.upsert_soi(flds, p, n, log, skew, sets=sets)
                step = {"op": "upsert_soi", "path": p.split("."),
                        "arg": {"fields": j_arg(flds), "n": j_arg(n), "sets": j_arg(sets)}}
        else:
            k = rnd.random()
            if k < 0.30:
                p, v = rnd.choice(pool), rvalue(rnd, nested)
                st = m.write("set", p, v, log, skew)
                step = {"op": "set", "path": p.split("."), "arg": j_arg(v)}
            elif k < 0.44:
                p = rnd.choice(pool)
                st = m.write("unset", p, None, log, skew)
                step = {"op": "unset", "path": p.split("."), "arg": None}
            elif k < 0.58:
                p, n = rnd.choice(pool), rnd.randint(-2, 4)
                st = m.write("inc", p, n, log, skew)
                step = {"op": "inc", "path": p.split("."), "arg": j_arg(n)}
            elif k < 0.67:
                doc = rdoc(rnd, nested)
                st = m.write("replace", None, doc, log, skew)
                step = {"op": "replace", "path": None, "arg": j_arg(doc)}
            elif k < 0.75:
                # the same fields and values in another order: a write of the order alone (a replace, or a $set of a map)
                vis = m.state.visible() or {}
                maps = [""] + [p for p in NPATHS if isinstance(M.getp(vis, p), dict)]
                at = rnd.choice(maps)
                cur = vis if at == "" else M.getp(vis, at)
                keys = list(cur)
                rnd.shuffle(keys)
                doc = {key: cur[key] for key in keys}
                if at == "":
                    st = m.write("replace", None, doc, log, skew)
                    step = {"op": "replace", "path": None, "arg": j_arg(doc)}
                else:
                    st = m.write("set", at, doc, log, skew)
                    step = {"op": "set", "path": at.split("."), "arg": j_arg(doc)}
            elif k < 0.90:
                for _ in range(20):
                    ps = rnd.sample(pool, rnd.randint(1, 3))
                    if disjoint(ps):
                        break
                else:
                    ps = ps[:1]
                ops = []
                for p in ps:
                    kind_ = rnd.choice(["set", "set", "unset", "inc"])
                    ops.append((kind_, p, rvalue(rnd, nested) if kind_ == "set" else (rnd.randint(-1, 3) if kind_ == "inc" else None)))
                st = m.write("update", None, ops, log, skew)
                step = {"op": "update", "path": None, "arg": [[o, p.split("."), j_arg(a)] for o, p, a in ops]}
            elif k < 0.95:
                st = m.write("delete", None, None, log, skew)
                step = {"op": "delete", "path": None, "arg": None}
            else:
                doc = rdoc(rnd, nested)
                st = m.insert(doc, log, skew)
                step = {"op": "insert", "arg": j_arg(doc)}
        d, dt, ot = member_digest(m)
        step.update({"k": "op", "m": who, "skew": skew, "res": j_stamp(st), "d": d})
        out.append(step)
        shown.append((who, step["op"] + " " + json.dumps(step.get("arg")), dt, ot))
    # a full-mesh sync: every member receives every other, twice
    for _ in range(2):
        for a in names:
            for b in names:
                if a != b:
                    ms[a].receive_entry(("w", ("sync", seed), M.wire(ms[b].state)))
    final = {n: member_digest(ms[n])[0] for n in names}
    assert len(set(final.values())) == 1, ("the model's members disagree after a full sync", kind, seed, final)
    return {"kind": kind, "seed": seed, "members": nm, "steps": out, "final": final[names[0]]}, shown


def main(outdir, show=None):
    sha = hashlib.sha256(open(os.path.join(MODEL, "model6.py"), "rb").read()).hexdigest()
    scripts = []
    stats = {"ops": 0, "recvs": 0, "refused": 0, "upserts": 0, "nested upserts": 0, "nested upserts applied": 0}
    for kind, seeds in (("flat", range(1, 151)), ("nested", range(1001, 1151))):
        for seed in seeds:
            sc, shown = script(kind, seed)
            if show is not None and seed == int(show):
                for i, (who, what, dt, ot) in enumerate(shown):
                    print("== step %d %s %s\n%s\n--\n%s" % (i, who, what, dt, ot))
            scripts.append(sc)
            for s in sc["steps"]:
                if s["k"] == "recv":
                    stats["recvs"] += 1
                else:
                    stats["ops"] += 1
                    stats["refused"] += s["res"] is None
                    if s["op"] in ("upsert_inc", "upsert_soi"):
                        stats["upserts"] += 1
                        paths = ["".join(s["path"])] + ([f[0] for f in s["arg"]["fields"]["v"] + s["arg"]["sets"]["v"]] if s["op"] == "upsert_soi" else [])
                        isn = any("." in q or len(s["path"]) > 1 for q in paths)
                        stats["nested upserts"] += isn
                        stats["nested upserts applied"] += isn and s["res"] is not None
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "ord_diff_scripts.json"), "w") as f:
        json.dump({"model6_sha256": sha, "scripts": scripts}, f, separators=(",", ":"), sort_keys=True)
    print("scripts", len(scripts), stats)


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else None)
