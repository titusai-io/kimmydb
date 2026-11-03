"""Q7 Step 0, Rust item O1: the materialised-order goldens and the order-join fixtures, from the A2-FINAL Python model.

Reads (never edits) the model in q7step0-model-A2-final/ (model6.py sha256 recorded in HASHES.txt next to this file), runs named
scenarios (the order scenarios of probes_order.py plus the hybrid-floor, update-bit and level-floor cases the Rust port must
reproduce) and writes two JSON fixtures that the Rust tests read:

  ord_goldens.json    per scenario: the STATES of every writer entry (the states the Rust side joins, in many delivery
                      orders), the expected ORDERED document of the observer (pre-order list of [dotted path, value|"map"]), the
                      observer's order state (every Ord), and what the model itself checked (orders tried, ABSORBED on/off).
  ord_join_laws.json  the Ords of probe_ord_join_laws (150 nested runs, steps=14): per path the distinct entry Ords, which the
                      Rust test joins pairwise and in triples with PINNED counts.

Usage:   Q7MODEL_DIR=<q7step0-model-A2-final> python3 -I gen_ord_goldens.py <out-dir>

`model6.py` is NOT in this repository (the reference model lives with the Q7 review files); the fixtures record its sha256
(0506450e0afc4c307a6ebce032df4f1b1588e719cde6c148c5ff033a8df7f85a) and the Rust tests check it. A new scenario here needs a new
name in `SCENARIOS` of `src/merge_state/ord_tests.rs` and its order pinned there.
"""
import hashlib, itertools, json, os, random, sys

HERE = os.path.dirname(os.path.abspath(__file__))
MODEL = os.environ["Q7MODEL_DIR"]
sys.path.insert(0, MODEL)
import model6 as M, pullmodel6 as P            # noqa: E402
from probes_order import Sc                    # noqa: E402

# ---- JSON encoders ----------------------------------------------------------------------------------------------------


def j_epoch(e):
    """An epoch is a Python int; a restore's epoch (start hlc << 64) does not fit a JSON number: a decimal string then."""
    return e if e < 2 ** 53 else str(e)


def j_stamp(s):
    return None if s is None else [s[0], s[1][0], j_epoch(s[1][1])]


def j_writer(w):
    return [w[0], j_epoch(w[1])]


def j_path(p):
    return None if p == "" else p.split(".")


def j_val(v):
    if v is None:
        return {"t": "unset"}
    if v == M.NULL:
        return {"t": "null"}
    if v == M.MAP:
        return {"t": "map"}
    assert v != M.MAPI, "implicit markers are not in the Rust model yet (item O4)"
    if isinstance(v, bool):
        raise AssertionError("bool")
    if isinstance(v, int):
        return {"t": "i", "v": v}
    if isinstance(v, float):
        return {"t": "d", "v": repr(v)}
    if isinstance(v, str):
        return {"t": "s", "v": v}
    if isinstance(v, tuple):
        return {"t": "a", "v": [j_val(x) for x in v]}
    raise AssertionError(v)


def j_gen(g):
    if g is None:
        return None
    return {"stamp": j_stamp(g[0]), "vv": [[j_writer(w), h] for w, h in sorted(g[1].items())],
            "seen": [[j_stamp(k[0]), j_writer(k[1]), j_stamp(k[2]), t] for k, t in sorted(g[2].items(), key=repr)] if len(g) > 2 else []}


def j_obs_key(k):          # (life, writer, gen)
    return [j_stamp(k[0]), j_writer(k[1]), j_stamp(k[2])]


def j_floor(f):
    if f is None:
        return None
    if f[1] == "u":
        return ["u", j_stamp(f[0]), f[2], bool(f[3]) if len(f) > 3 else False]
    assert f[1] == "r" and len(f) == 2
    return ["r", j_stamp(f[0])]


def j_ord(o):
    return {"floor": j_floor(o.floor),
            "firsts": [[j_writer(w), j_stamp(c[0]), c[1]] for w, c in sorted(o.firsts.items())],
            "last": None if o.last is None else [j_stamp(o.last[0]), o.last[1]],
            "lvl": j_stamp(o.lvl)}


def j_ords(ords):
    return [[j_stamp(k[0]), j_path(k[1]), j_ord(o)] for k, o in sorted(ords.items(), key=lambda kv: (kv[0][0], kv[0][1].split(".") if kv[0][1] else []))]


def j_state(s):
    regs = []
    for (L, p), r in sorted(s.regs.items(), key=lambda kv: (kv[0][0], kv[0][1].split("."))):
        obs = [[*j_obs_key(k), j_stamp(v[0]), v[1]] for k, v in sorted(r[2].items(), key=repr)]
        regs.append([j_stamp(L), p.split("."), j_stamp(r[0]), j_val(r[1]), obs, j_gen(r[3]) if len(r) > 3 else None,
                     len(r) > 4 and r[4] == "soi"])
    slots = [[j_stamp(k[0]), k[1].split("."), j_writer(k[2]), j_stamp(k[3]), j_stamp(v[0]), v[1]]
             for k, v in sorted(s.slots.items(), key=lambda kv: (kv[0][0], kv[0][1].split("."), kv[0][2], repr(kv[0][3])))]
    lives = [[j_stamp(L), [j_stamp(d) for d in sorted(v[0])], [p.split(".") for p in v[1]], v[2]] for L, v in sorted(s.lives.items())]
    return {"lives": lives, "dead": [j_stamp(L) for L in sorted(s.dead)],
            "deletes": [[j_stamp(d), [[j_writer(w), h] for w, h in sorted(vv.items())]] for d, vv in sorted(s.deletes.items())],
            "regs": regs, "slots": slots, "vv": [[j_writer(w), h] for w, h in sorted(s.vv.items())],
            "ords": j_ords(s.ords)}


def flat(doc, prefix=()):
    """Pre-order [dotted path as segment list, value|"map"] of an ordered visible document (dict in field order)."""
    out = []
    for k, v in doc.items():
        if isinstance(v, dict):
            out.append([list(prefix + (k,)), "map"])
            out += flat(v, prefix + (k,))
        else:
            out.append([list(prefix + (k,)), j_val(v)])
    return out


# ---- scenarios --------------------------------------------------------------------------------------------------------

class NoInit(Sc):
    """Members I, A, B, C that hold nothing: the first writes create the lives."""
    def __init__(self):
        M.RET = 10 ** 9
        self.cls, self.log, self.writes = M.Member, {}, []
        self.I, self.A, self.B, self.C = (M.Member(n) for n in "IABC")
        self.m = {"I": self.I, "A": self.A, "B": self.B, "C": self.C}
        self.i0 = None


# `known` of a write: the highest HLC its author knew before the write (what a `u` floor remembers), recorded at tick
KNOWN = {}
_tick = M.Member.tick


def _tick_recording(self, skew=0):
    st = _tick(self, skew)
    KNOWN[st] = self.known
    return st


M.Member.tick = _tick_recording

SCEN = {}


def scenario(f):
    SCEN[f.__name__[2:]] = f
    return f


@scenario
def s_sequential_rules():
    """One writer: set keeps the place, unset then set appends, inc of an absent field appends, setmany in update order,
    a re-ordering replace, a whole sub-document set."""
    sc = Sc({"b": 1, "a": 2, "m": {"q": 1, "p": 2}})
    for st in [("A", "set", "b", 5), ("A", "unset", "b", None), ("A", "set", "b", 6), ("A", "inc", "a", 1), ("A", "inc", "z", 1),
               ("A", "set", "m.p", 9), ("A", "set", "m.r", 1), ("A", "unset", "m.q", None), ("A", "set", "m.q", 2),
               ("A", "setmany", None, [("zz", 1), ("yy", 2)])]:
        sc.op(*st)
    cur = sc.A.state.visible()
    sc.op("A", "replace", None, dict(reversed(list(cur.items()))))
    sc.op("A", "set", "m", {"r": 0, "p": 0})
    return sc


@scenario
def s_concurrent_creation_a_first():
    sc = Sc({"a": 1})
    sc.op("A", "set", "c", 1, clock=0)
    sc.op("B", "set", "d", 1, clock=5)
    return sc


@scenario
def s_concurrent_creation_b_first():
    sc = Sc({"a": 1})
    sc.op("A", "set", "c", 1, clock=5)
    sc.op("B", "set", "d", 1, clock=0)
    return sc


@scenario
def s_same_new_path_from_two_writers():
    sc = Sc({"a": 1})
    sc.op("A", "set", "c", 1, clock=10)
    sc.op("B", "set", "d", 1, clock=11)
    sc.op("C", "set", "c", 2, clock=12)
    return sc


@scenario
def s_unset_then_concurrent_set():
    sc = Sc({"p": 1, "q": 2})
    sc.op("B", "unset", "p", clock=5)
    sc.op("A", "set", "p", 9, clock=7)
    sc.op("C", "set", "r", 1, clock=6)
    return sc


@scenario
def s_set_then_concurrent_unset():
    sc = Sc({"p": 1, "q": 2})
    sc.op("A", "set", "p", 9, clock=5)
    sc.op("B", "unset", "p", clock=7)
    return sc


@scenario
def s_replace_reorders_even_unchanged():
    sc = Sc({"a": 1, "b": 2, "c": 3})
    sc.op("A", "replace", None, {"c": 3, "b": 2, "a": 1}, clock=20)
    sc.op("B", "set", "b", 7, clock=10)
    return sc


@scenario
def s_approximate_case():
    sc = Sc({"p": 1, "z": 0})
    sc.op("A", "set", "p", 2, clock=10)
    sc.op("B", "unset", "p", clock=12)
    sc.op("A", "set", "p", 3, clock=14)
    sc.op("C", "set", "q", 1, clock=15)
    sc.op("A", "set", "p", 4, clock=16)
    sc.op("C", "set", "r", 1, clock=17)
    return sc


@scenario
def s_write_below_a_map_is_a_write_to_the_map():
    sc = Sc({"x": 1, "a": {"b": 1}, "z": 2})
    sc.op("A", "unset", "a", clock=5)
    sc.op("B", "inc", "a.b", 1, clock=6)
    sc.op("C", "set", "w", 1, clock=8)
    return sc


@scenario
def s_level_floor_replace():
    sc = Sc({"a": 1, "b": 2, "m": {"p": 1, "q": 2}})
    sc.op("B", "set", "c", 3, clock=10)
    sc.op("C", "set", "m.r", 5, clock=11)
    sc.op("A", "replace", None, {"b": 2, "m": {"q": 2, "p": 1}, "a": 1}, clock=20)
    return sc


@scenario
def s_level_floor_subdocument_set():
    sc = Sc({"m": {"p": 1, "q": 2}})
    sc.op("C", "set", "m.r", 5, clock=11)
    sc.op("A", "set", "m", {"q": 1, "p": 2}, clock=20)
    return sc


@scenario
def s_level_floor_nested_subdocument_set():
    sc = Sc({"a": {"d": {"x": 1, "y": 2}, "e": 3}})
    sc.op("C", "set", "a.d.k", 5, clock=11)
    sc.op("B", "set", "a.f", 6, clock=12)
    sc.op("A", "set", "a.d", {"y": 7, "x": 8}, clock=20)
    return sc


@scenario
def s_survivor_increment_after_unset():
    sc = Sc({"x": 1, "a": 1})
    sc.op("B", "inc", "a", 1, clock=2)
    sc.op("C", "unset", "a", clock=4)
    sc.op("B", "set", "b", 7, clock=5)
    return sc


@scenario
def s_survivor_placement_a_inc_before_c():
    sc = Sc({"a": 1, "b": 2})
    sc.op("A", "inc", "a", 1, clock=2)
    sc.op("A", "set", "c", 7, clock=3)
    sc.op("B", "unset", "a", clock=4)
    return sc


@scenario
def s_survivor_placement_b_unset_knew_c():
    sc = Sc({"a": 1, "m": {"p": 1}})
    sc.op("B", "set", "m.r", 3, clock=2)
    sc.op("A", "set", "c", 7, clock=3)
    sc.op("A", "unset", "m", clock=4)
    return sc


@scenario
def s_replace_drops_a_field_then_set_appends():
    sc = Sc({"a": 1, "b": 2, "c": 3})
    sc.op("A", "replace", None, {"c": 3, "a": 1})
    sc.op("A", "set", "b", 5)
    return sc


@scenario
def s_hybrid_noop_unset_rides_on_an_update():
    """A unsets a; B (unaware) sets a; A then updates {set c, unset a} where a is already absent at A: the unset is a no-op
    component whose floor rides on the update (the hybrid floor), so a=5 goes AFTER c and before e, which C creates later."""
    sc = Sc({"a": 1, "b": 2})
    sc.op("A", "unset", "a", clock=10)
    sc.op("B", "set", "a", 5, clock=11)
    sc.op("A", "update", None, [("set", "c", 7), ("unset", "a", None)], clock=12)
    sc.op("C", "set", "e", 1, clock=13)       # created after the update: a is placed (by the floor) BEFORE it
    return sc


@scenario
def s_update_bit_survivor_after_all_the_updates_fields():
    """The u floor of an update that also wrote other fields (the bit): an increment the unsetter never saw goes after ALL
    of that update's fields."""
    sc = Sc({"x": 1, "a": 1})
    sc.op("B", "inc", "a", 1, clock=2)
    sc.op("C", "set", "w", 9, clock=3)
    sc.op("A", "update", None, [("set", "z", 1), ("set", "y", 2), ("unset", "a", None)], clock=5)
    return sc


@scenario
def s_update_without_bit_survivor_by_prev():
    """The same unset on its own (no bit): the survivor is placed by `prev` and its own write."""
    sc = Sc({"x": 1, "a": 1})
    sc.op("B", "inc", "a", 1, clock=2)
    sc.op("C", "set", "w", 9, clock=3)
    sc.op("A", "unset", "a", clock=5)
    return sc


@scenario
def s_survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew():
    """The increment (stamp 5) is above `prev` of the unset (the insert), so it is re-created at its own stamp, after the field
    d that C created at stamp 3: not at `prev`, which would put it before d."""
    sc = Sc({"a": 1, "b": 2})
    sc.op("A", "inc", "a", 1, clock=5)
    sc.op("C", "set", "d", 1, clock=3)
    sc.op("B", "unset", "a", clock=7)
    return sc


@scenario
def s_slot_above_the_floor_is_not_a_survivor():
    """B's update (stamp 6) `$set z`, `$inc a` is above C's unset of a (stamp 4): the increment is an ordinary write at
    position 1 after z, not a survivor (a survivor would take position 0 and tie with z)."""
    sc = Sc({"x": 1, "a": 1})
    sc.op("C", "unset", "a", clock=4)
    sc.op("B", "update", None, [("set", "z", 1), ("inc", "a", 1)], clock=6)
    return sc


@scenario
def s_descendant_above_the_floor_is_not_a_survivor():
    """The same for a register below a map: B's update `$set z`, `$set a.b` (stamp 6) is above C's unset of a (stamp 4)."""
    sc = Sc({"x": 1, "a": {"b": 1}})
    sc.op("C", "unset", "a", clock=4)
    sc.op("B", "update", None, [("set", "z", 1), ("set", "a.b", 2)], clock=6)
    return sc


@scenario
def s_survivor_older_than_the_fields_own_write_lowers_its_position():
    """B unsets a (stamp 4) and sets it again (stamp 6); C's increment (stamp 2) B never saw survives the unset, so a is
    re-created at the increment, before d (stamp 3), not at B's own later write."""
    sc = Sc({"x": 1, "a": 1})
    sc.op("C", "inc", "a", 1, clock=2)
    sc.op("A", "set", "d", 1, clock=3)
    sc.op("B", "unset", "a", clock=4)
    sc.op("B", "set", "a", 9, clock=6)
    return sc


@scenario
def s_descendant_survivor_of_an_unset_map():
    sc = Sc({"a": {"b": 1, "c": 2}, "z": 0})
    sc.op("B", "set", "a.d", 4, clock=3)
    sc.op("C", "set", "y", 6, clock=4)
    sc.op("A", "unset", "a", clock=6)
    return sc


@scenario
def s_concurrent_replaces():
    sc = Sc({"a": 1, "b": 2, "c": 3})
    sc.op("A", "replace", None, {"c": 3, "b": 2, "a": 1}, clock=20)
    sc.op("B", "replace", None, {"b": 2, "a": 1, "c": 3, "d": 4}, clock=21)
    sc.op("C", "set", "e", 5, clock=22)
    return sc


@scenario
def s_inc_unset_replace_concurrent():
    sc = Sc({"a": 1, "b": 2})
    sc.op("A", "replace", None, {"b": 2, "c": 3}, clock=20)
    sc.op("B", "inc", "a", 5, clock=10)
    sc.op("C", "unset", "b", clock=12)
    return sc


@scenario
def s_delete_reinsert_with_a_concurrent_set():
    sc = Sc({"x": 1, "y": 2})
    sc.op("A", "delete", None, clock=10)
    sc.op("A", "insert", None, {"y": 3, "x": 4}, clock=11)
    sc.op("B", "set", "c", 9, clock=5)
    return sc


@scenario
def s_two_lives_by_two_upserts():
    sc = NoInit()
    sc.writes.append((sc.A, sc.A.upsert_inc("x", 1, sc.log)))
    sc.writes.append((sc.B, sc.B.upsert_inc("y", 2, sc.log)))
    sc.op("A", "set", "z", 3, clock=5)
    return sc


@scenario
def s_update_with_inc_and_unset_of_a_map():
    sc = Sc({"a": 1, "b": 2, "s": "str", "k": {"u": 1}, "j": {"v": 1}})
    sc.op("A", "update", None, [("set", "z", 1), ("unset", "k", None), ("inc", "y", 2), ("set", "j.w", 3)], clock=9)
    sc.op("B", "set", "k.v", 5, clock=4)
    return sc


# ---- the run -----------------------------------------------------------------------------------------------------------

def ent_of(sc, i):
    return sc.ent(sc.I, sc.i0)


def observer(sc, ents):
    seed = [] if sc.i0 is None else [sc.ent(sc.I, sc.i0)]
    o = M.fresh_observer("O", seed)
    for e in ents:
        o.receive_entry(e)
    return o


def canon_noloss(st):
    c = st.canon()
    return c[:6] + c[7:]


def run_scenario(name, fn):
    out = {}
    res = []
    for absorbed in (True, False):
        M.ABSORBED = absorbed
        try:
            sc = fn()
            ents = sc.entries()
            seed = [] if sc.i0 is None else [sc.ent(sc.I, sc.i0)]
            all_ents = seed + ents
            o = observer(sc, ents)
            vis = o.state.visible()
            res.append((M.tyrepr(M.ordered(vis)), canon_noloss(o.state)))
            if absorbed:
                out["entries"] = [j_state(M.unwire(e[2])) for e in all_ents]
                out["entry_stamps"] = [j_stamp(e[1]) for e in all_ents]
                # the writes themselves, for the replay of the feed: who, into which life, what the author knew, the items
                writes = []
                for e in all_ents:
                    meta = sc.log[e[1]][1]
                    writes.append({"stamp": j_stamp(e[1]), "life": j_stamp(meta.get("life")), "known": KNOWN[e[1]],
                                   "items": [[j_path(p), mode] for p, mode in meta.get("items", [])]})
                out["writes"] = writes
                out["expected"] = flat(vis)
                out["ords"] = j_ords(o.state.ords)
                out["absorbed_slots"] = len(o.state.absorbed)
                # every delivery order of the entries to a fresh observer: the model's own convergence check
                n = len(ents)
                if n <= 6:
                    orders = list(itertools.permutations(range(n)))
                else:
                    rnd = random.Random(n)
                    orders = [tuple(range(n)), tuple(reversed(range(n)))]
                    for _ in range(300):
                        k = list(range(n)); rnd.shuffle(k); orders.append(tuple(k))
                seen = set()
                for od in orders:
                    oo = M.fresh_observer("O", seed)
                    for i in od:
                        oo.receive_entry(ents[i])
                    seen.add((M.tyrepr(M.ordered(oo.state.visible())), canon_noloss(oo.state)))
                assert len(seen) == 1 and next(iter(seen)) == res[0], (name, "delivery orders disagree")
                out["model_orders_checked"] = len(orders)
                out["seed_entries"] = len(seed)
        finally:
            M.ABSORBED = True
    assert res[0][0] == res[1][0], (name, "the ordered document depends on ABSORBED", res)
    out["absorbed_does_not_matter"] = True
    out["python_ordered_tyrepr"] = res[0][0]
    return out


def join_law_fixture():
    """probes_order.probe_ord_join_laws: the distinct Ords of the writer entries of 150 nested runs, per (life, path)."""
    per_path, pool = [], {}
    pairs = triples = distinct_keys = 0
    bad = 0
    for seed in range(150):
        ms, log, ok = P.run(seed, steps=14, nested=True)
        by_key = {}
        for m in ms:
            for e in m.oplog.values():
                if e[0] != "w":
                    continue
                for k, o in M.unwire(e[2]).ords.items():
                    by_key.setdefault(k, {})[repr(o.key())] = o
        for k, d in by_key.items():
            os_ = list(d.values())
            if len(os_) > 1:
                distinct_keys += 1
            for a_, b_, c_ in itertools.permutations(os_, 3) if len(os_) >= 3 else []:
                triples += 1
                if M.ord_join(M.ord_join(a_, b_), c_).key() != M.ord_join(a_, M.ord_join(b_, c_)).key():
                    bad += 1
            idx = []
            for o in os_:
                # the key kind is part of the entry: the root of a replace keeps a level mark and nothing else
                key = json.dumps({**j_ord(o), "root": k[1] == ""}, sort_keys=True)
                idx.append(pool.setdefault(key, len(pool)))
            per_path.append(idx)
    assert bad == 0
    ords = [json.loads(k) for k, _ in sorted(pool.items(), key=lambda kv: kv[1])]
    return {"paths_with_several": distinct_keys, "triples": triples, "ords": ords, "per_path": per_path}


def main(outdir):
    os.makedirs(outdir, exist_ok=True)
    sha = hashlib.sha256(open(os.path.join(MODEL, "model6.py"), "rb").read()).hexdigest()
    golden = {"model6_sha256": sha, "scenarios": {}}
    for name, fn in SCEN.items():
        golden["scenarios"][name] = run_scenario(name, fn)
        print("scenario", name, "ok:", len(golden["scenarios"][name]["expected"]), "fields,",
              golden["scenarios"][name]["model_orders_checked"], "delivery orders")
    with open(os.path.join(outdir, "ord_goldens.json"), "w") as f:
        json.dump(golden, f, separators=(",", ":"), sort_keys=True)
    jl = join_law_fixture()
    jl["model6_sha256"] = sha
    with open(os.path.join(outdir, "ord_join_laws.json"), "w") as f:
        json.dump(jl, f, separators=(",", ":"), sort_keys=True)
    with open(os.path.join(outdir, "HASHES.txt"), "w") as f:
        f.write(f"{sha}  model6.py (q7step0-model-A2-final)\n")
        for n in ("pullmodel6.py", "probes_order.py"):
            f.write(hashlib.sha256(open(os.path.join(MODEL, n), "rb").read()).hexdigest() + f"  {n}\n")
        f.write(f"python {sys.version.split()[0]}\n")
    print("join-law fixture:", jl["paths_with_several"], "paths with several Ords,", jl["triples"], "triples")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "out"))
