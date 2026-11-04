"""Q7 Step 0, Rust items O1 and O2: the materialised-order goldens and the order-join fixtures, from the A2.1 Python model.

Reads (never edits) the model in the reviewed A2.1 tree (`q7step0-model/`, model6.py sha256 865808753a5f9a4b..., recorded in the
fixtures and in HASHES.txt next to the output), runs named scenarios (the order scenarios of probes_order.py plus the hybrid-floor,
update-bit and level-floor cases, and, for O2, scenarios in which a writer RECEIVES before it writes and the writer-side no-op rules)
and writes JSON fixtures that the Rust tests read:

  ord_goldens.json    per scenario: the STATES of every writer entry (the states the Rust side joins, in many delivery
                      orders), the expected ORDERED document of the observer (pre-order list of [dotted path, value|"map"]), the
                      observer's order state (every Ord), what the model itself checked (orders tried, ABSORBED on/off), the
                      writes themselves (stamp, life, what the author knew, order items: the feed's input), and the SCRIPT: the
                      initial document, then every write and every receive in program order (who, operator, path, operand,
                      clock, the stamp it got, or None when it was refused). The Rust tests replay the script through the Writer API.
  ord_join_laws.json  the Ords of probe_ord_join_laws (150 nested runs, steps=14): per path the distinct entry Ords, which the
                      Rust test joins pairwise and in triples with PINNED counts.
  ord_random_states.json  states of the model's random nested runs with the model's ordered document of each (ABSORBED = False).

Usage:   Q7MODEL_DIR=<q7step0-model> python3 -I -B gen_ord_goldens.py <out-dir>
         (-B: the model directory must not get a __pycache__; the script also sets sys.dont_write_bytecode)

The 30 scenarios of O1 give the same expected order, order state, entries and writes under A2.1 as under A2-final (the O2 round
compared the two fixtures field by field: only `model6_sha256` differs). `model6.py` is NOT in this repository (the reference model
lives with the Q7 review files); the fixtures record its sha256 and the Rust tests check it. A new scenario here needs a new name in
`SCENARIOS` of `src/merge_state/ord_tests.rs` and its order pinned there. In the model the root of a replace is keyed by "" (and a
top-level field named "" by "" too); the Rust keys them OrdAt::Root and OrdAt::Field([""]), told apart by context.
"""
import hashlib, itertools, json, os, random, sys

sys.dont_write_bytecode = True      # before the model is imported: the model directory is read-only to this script

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

def j_arg(a):
    """An operand of a recorded write: a value, a document (field order kept), or the operator list of an update."""
    if isinstance(a, dict):
        return {"t": "doc", "v": [[k, j_arg(v)] for k, v in a.items()]}
    return j_val(a)


def j_ops(ops):
    return [[o, p.split("."), j_arg(a)] for o, p, a in ops]


class RecSc(Sc):
    """`Sc` that records what is done to it, for the replay of the scenarios through the Writer API: the initial document, then
    every write (who, which operator, where, with what operand, the clock set before it, the stamp it got or None when it
    was refused) and every receive (who received which entry of whom), in program order."""
    def __init__(self, init):
        super().__init__(init)
        self.init_doc = init
        self.script = []

    def _record(self, who, op, p, arg, skew, clock, st):
        if op in ("update", "setmany"):
            jarg = j_ops(arg if op == "update" else [("set", q, v) for q, v in arg])
        else:
            jarg = None if arg is None else j_arg(arg)
        self.script.append({"k": "op", "who": who, "op": op, "path": None if p is None else p.split("."), "arg": jarg,
                            "skew": skew, "clock": clock, "stamp": None if st is None else j_stamp(st)})

    def op(self, who, op, p, arg=None, skew=0, clock=None):
        st = super().op(who, op, p, arg, skew=skew, clock=clock)
        self._record(who, op, p, arg, skew, clock, st)
        return st

    def refused(self, who, op, p, arg=None, skew=0, clock=None):
        """A write the writer refuses or finds a no-op: it mints no stamp and changes nothing."""
        m = self.m[who]
        if clock is not None:
            m.clock = clock
        before = m.state.canon()
        st = m.write(op, p, arg, self.log, skew)
        assert st is None and m.state.canon() == before, (who, op, p, arg)
        self._record(who, op, p, arg, skew, clock, None)

    def recv(self, who, frm, st):
        super().recv(who, frm, st)
        self.script.append({"k": "recv", "who": who, "from": frm, "stamp": j_stamp(st)})


class NoInit(RecSc):
    """Members I, A, B, C that hold nothing: the first writes create the lives."""
    def __init__(self):
        M.RET = 10 ** 9
        self.cls, self.log, self.writes = M.Member, {}, []
        self.I, self.A, self.B, self.C = (M.Member(n) for n in "IABC")
        self.m = {"I": self.I, "A": self.A, "B": self.B, "C": self.C}
        self.i0 = None
        self.init_doc = None
        self.script = []

    def upsert_soi(self, who, fields, p, n, sets=None):
        """A `$setOnInsert` upsert: two writes by one writer; the scenario's entry is the state after the second."""
        m = self.m[who]
        st = m.upsert_soi(fields, p, n, self.log, sets=sets)
        assert st is not None
        self.writes.append((m, st))
        self.script.append({"k": "op", "who": who, "op": "upsert_soi", "path": p.split("."), "skew": 0, "clock": None,
                            "stamp": j_stamp(st),
                            "arg": {"fields": j_arg(fields), "n": j_val(n), "sets": j_arg(sets or {})}})
        return st

    def upsert_inc(self, who, p, n):
        m = self.m[who]
        st = m.upsert_inc(p, n, self.log)
        assert st is not None
        self.writes.append((m, st))
        self.script.append({"k": "op", "who": who, "op": "upsert_inc", "path": p.split("."), "arg": j_val(n), "skew": 0,
                            "clock": None, "stamp": j_stamp(st)})
        return st


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
    sc = RecSc({"b": 1, "a": 2, "m": {"q": 1, "p": 2}})
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
    sc = RecSc({"a": 1})
    sc.op("A", "set", "c", 1, clock=0)
    sc.op("B", "set", "d", 1, clock=5)
    return sc


@scenario
def s_concurrent_creation_b_first():
    sc = RecSc({"a": 1})
    sc.op("A", "set", "c", 1, clock=5)
    sc.op("B", "set", "d", 1, clock=0)
    return sc


@scenario
def s_same_new_path_from_two_writers():
    sc = RecSc({"a": 1})
    sc.op("A", "set", "c", 1, clock=10)
    sc.op("B", "set", "d", 1, clock=11)
    sc.op("C", "set", "c", 2, clock=12)
    return sc


@scenario
def s_unset_then_concurrent_set():
    sc = RecSc({"p": 1, "q": 2})
    sc.op("B", "unset", "p", clock=5)
    sc.op("A", "set", "p", 9, clock=7)
    sc.op("C", "set", "r", 1, clock=6)
    return sc


@scenario
def s_set_then_concurrent_unset():
    sc = RecSc({"p": 1, "q": 2})
    sc.op("A", "set", "p", 9, clock=5)
    sc.op("B", "unset", "p", clock=7)
    return sc


@scenario
def s_replace_reorders_even_unchanged():
    sc = RecSc({"a": 1, "b": 2, "c": 3})
    sc.op("A", "replace", None, {"c": 3, "b": 2, "a": 1}, clock=20)
    sc.op("B", "set", "b", 7, clock=10)
    return sc


@scenario
def s_approximate_case():
    sc = RecSc({"p": 1, "z": 0})
    sc.op("A", "set", "p", 2, clock=10)
    sc.op("B", "unset", "p", clock=12)
    sc.op("A", "set", "p", 3, clock=14)
    sc.op("C", "set", "q", 1, clock=15)
    sc.op("A", "set", "p", 4, clock=16)
    sc.op("C", "set", "r", 1, clock=17)
    return sc


@scenario
def s_write_below_a_map_is_a_write_to_the_map():
    sc = RecSc({"x": 1, "a": {"b": 1}, "z": 2})
    sc.op("A", "unset", "a", clock=5)
    sc.op("B", "inc", "a.b", 1, clock=6)
    sc.op("C", "set", "w", 1, clock=8)
    return sc


@scenario
def s_level_floor_replace():
    sc = RecSc({"a": 1, "b": 2, "m": {"p": 1, "q": 2}})
    sc.op("B", "set", "c", 3, clock=10)
    sc.op("C", "set", "m.r", 5, clock=11)
    sc.op("A", "replace", None, {"b": 2, "m": {"q": 2, "p": 1}, "a": 1}, clock=20)
    return sc


@scenario
def s_level_floor_subdocument_set():
    sc = RecSc({"m": {"p": 1, "q": 2}})
    sc.op("C", "set", "m.r", 5, clock=11)
    sc.op("A", "set", "m", {"q": 1, "p": 2}, clock=20)
    return sc


@scenario
def s_level_floor_nested_subdocument_set():
    sc = RecSc({"a": {"d": {"x": 1, "y": 2}, "e": 3}})
    sc.op("C", "set", "a.d.k", 5, clock=11)
    sc.op("B", "set", "a.f", 6, clock=12)
    sc.op("A", "set", "a.d", {"y": 7, "x": 8}, clock=20)
    return sc


@scenario
def s_survivor_increment_after_unset():
    sc = RecSc({"x": 1, "a": 1})
    sc.op("B", "inc", "a", 1, clock=2)
    sc.op("C", "unset", "a", clock=4)
    sc.op("B", "set", "b", 7, clock=5)
    return sc


@scenario
def s_survivor_placement_a_inc_before_c():
    sc = RecSc({"a": 1, "b": 2})
    sc.op("A", "inc", "a", 1, clock=2)
    sc.op("A", "set", "c", 7, clock=3)
    sc.op("B", "unset", "a", clock=4)
    return sc


@scenario
def s_survivor_placement_b_unset_knew_c():
    sc = RecSc({"a": 1, "m": {"p": 1}})
    sc.op("B", "set", "m.r", 3, clock=2)
    sc.op("A", "set", "c", 7, clock=3)
    sc.op("A", "unset", "m", clock=4)
    return sc


@scenario
def s_replace_drops_a_field_then_set_appends():
    sc = RecSc({"a": 1, "b": 2, "c": 3})
    sc.op("A", "replace", None, {"c": 3, "a": 1})
    sc.op("A", "set", "b", 5)
    return sc


@scenario
def s_hybrid_noop_unset_rides_on_an_update():
    """A unsets a; B (unaware) sets a; A then updates {set c, unset a} where a is already absent at A: the unset is a no-op
    component whose floor rides on the update (the hybrid floor), so a=5 goes AFTER c and before e, which C creates later."""
    sc = RecSc({"a": 1, "b": 2})
    sc.op("A", "unset", "a", clock=10)
    sc.op("B", "set", "a", 5, clock=11)
    sc.op("A", "update", None, [("set", "c", 7), ("unset", "a", None)], clock=12)
    sc.op("C", "set", "e", 1, clock=13)       # created after the update: a is placed (by the floor) BEFORE it
    return sc


@scenario
def s_update_bit_survivor_after_all_the_updates_fields():
    """The u floor of an update that also wrote other fields (the bit): an increment the unsetter never saw goes after ALL
    of that update's fields."""
    sc = RecSc({"x": 1, "a": 1})
    sc.op("B", "inc", "a", 1, clock=2)
    sc.op("C", "set", "w", 9, clock=3)
    sc.op("A", "update", None, [("set", "z", 1), ("set", "y", 2), ("unset", "a", None)], clock=5)
    return sc


@scenario
def s_update_without_bit_survivor_by_prev():
    """The same unset on its own (no bit): the survivor is placed by `prev` and its own write."""
    sc = RecSc({"x": 1, "a": 1})
    sc.op("B", "inc", "a", 1, clock=2)
    sc.op("C", "set", "w", 9, clock=3)
    sc.op("A", "unset", "a", clock=5)
    return sc


@scenario
def s_survivor_keeps_its_own_stamp_when_it_is_after_what_the_unsetter_knew():
    """The increment (stamp 5) is above `prev` of the unset (the insert), so it is re-created at its own stamp, after the field
    d that C created at stamp 3: not at `prev`, which would put it before d."""
    sc = RecSc({"a": 1, "b": 2})
    sc.op("A", "inc", "a", 1, clock=5)
    sc.op("C", "set", "d", 1, clock=3)
    sc.op("B", "unset", "a", clock=7)
    return sc


@scenario
def s_slot_above_the_floor_is_not_a_survivor():
    """B's update (stamp 6) `$set z`, `$inc a` is above C's unset of a (stamp 4): the increment is an ordinary write at
    position 1 after z, not a survivor (a survivor would take position 0 and tie with z)."""
    sc = RecSc({"x": 1, "a": 1})
    sc.op("C", "unset", "a", clock=4)
    sc.op("B", "update", None, [("set", "z", 1), ("inc", "a", 1)], clock=6)
    return sc


@scenario
def s_descendant_above_the_floor_is_not_a_survivor():
    """The same for a register below a map: B's update `$set z`, `$set a.b` (stamp 6) is above C's unset of a (stamp 4)."""
    sc = RecSc({"x": 1, "a": {"b": 1}})
    sc.op("C", "unset", "a", clock=4)
    sc.op("B", "update", None, [("set", "z", 1), ("set", "a.b", 2)], clock=6)
    return sc


@scenario
def s_survivor_older_than_the_fields_own_write_lowers_its_position():
    """B unsets a (stamp 4) and sets it again (stamp 6); C's increment (stamp 2) B never saw survives the unset, so a is
    re-created at the increment, before d (stamp 3), not at B's own later write."""
    sc = RecSc({"x": 1, "a": 1})
    sc.op("C", "inc", "a", 1, clock=2)
    sc.op("A", "set", "d", 1, clock=3)
    sc.op("B", "unset", "a", clock=4)
    sc.op("B", "set", "a", 9, clock=6)
    return sc


@scenario
def s_replace_with_an_empty_document_has_no_update_bit():
    """O1-10 of the O1 review: A replaces the document with {} (a level mark on the root and unsets, no other field), over a
    field a that B incremented and A never saw. A level mark is not a field the write also wrote, so the update bit is clear and
    a is placed by `prev` and its own stamp, before the field d that C creates later."""
    sc = RecSc({"a": 1, "b": 2})
    sc.op("B", "inc", "a", 5, clock=2)
    sc.op("C", "set", "d", 1, clock=3)
    sc.op("A", "replace", None, {}, clock=6)
    return sc


@scenario
def s_descendant_survivor_of_an_unset_map():
    sc = RecSc({"a": {"b": 1, "c": 2}, "z": 0})
    sc.op("B", "set", "a.d", 4, clock=3)
    sc.op("C", "set", "y", 6, clock=4)
    sc.op("A", "unset", "a", clock=6)
    return sc


@scenario
def s_concurrent_replaces():
    sc = RecSc({"a": 1, "b": 2, "c": 3})
    sc.op("A", "replace", None, {"c": 3, "b": 2, "a": 1}, clock=20)
    sc.op("B", "replace", None, {"b": 2, "a": 1, "c": 3, "d": 4}, clock=21)
    sc.op("C", "set", "e", 5, clock=22)
    return sc


@scenario
def s_inc_unset_replace_concurrent():
    sc = RecSc({"a": 1, "b": 2})
    sc.op("A", "replace", None, {"b": 2, "c": 3}, clock=20)
    sc.op("B", "inc", "a", 5, clock=10)
    sc.op("C", "unset", "b", clock=12)
    return sc


@scenario
def s_delete_reinsert_with_a_concurrent_set():
    sc = RecSc({"x": 1, "y": 2})
    sc.op("A", "delete", None, clock=10)
    sc.op("A", "insert", None, {"y": 3, "x": 4}, clock=11)
    sc.op("B", "set", "c", 9, clock=5)
    return sc


@scenario
def s_two_lives_by_two_upserts():
    sc = NoInit()
    sc.upsert_inc("A", "x", 1)
    sc.upsert_inc("B", "y", 2)
    sc.op("A", "set", "z", 3, clock=5)
    return sc


@scenario
def s_update_with_inc_and_unset_of_a_map():
    sc = RecSc({"a": 1, "b": 2, "s": "str", "k": {"u": 1}, "j": {"v": 1}})
    sc.op("A", "update", None, [("set", "z", 1), ("unset", "k", None), ("inc", "y", 2), ("set", "j.w", 3)], clock=9)
    sc.op("B", "set", "k.v", 5, clock=4)
    return sc


# ---- O2: scenarios in which a writer RECEIVES before it writes, and the writer-side no-op rules -------------------------------
# (the first 30 scenarios above never let a writer receive anything but the seed insert: the O1 review's note that the goldens
# never test the receive-then-write feed. Each of these runs through the Writer API in Rust, which must join what it received
# with its own feed.)

@scenario
def s_receive_a_concurrent_unset_then_set():
    """B unsets p (5); A RECEIVES that entry, then sets p (7): A's write is above the floor it holds, so p is a fresh field at
    A, placed by A's write; C (which saw nothing) creates r (6) meanwhile."""
    sc = RecSc({"p": 1, "q": 2})
    u = sc.op("B", "unset", "p", clock=5)
    sc.recv("A", "B", u)
    sc.op("A", "set", "p", 9, clock=7)
    sc.op("C", "set", "r", 1, clock=6)
    return sc


@scenario
def s_receive_a_concurrent_replace_then_set():
    """B replaces the document in another order (20); A receives it, then sets b and a new field d; C creates e (22)."""
    sc = RecSc({"a": 1, "b": 2, "c": 3})
    r = sc.op("B", "replace", None, {"c": 3, "b": 2, "a": 1}, clock=20)
    sc.recv("A", "B", r)
    sc.op("A", "set", "b", 7)
    sc.op("A", "set", "d", 4)
    sc.op("C", "set", "e", 5, clock=22)
    return sc


@scenario
def s_receive_a_survivor_increment_then_unset():
    """B increments a (2); A receives it, then unsets a (4): A saw the increment, so it is no survivor of A's unset; C's w (3)
    and B's later b (5) are concurrent with the unset."""
    sc = RecSc({"x": 1, "a": 1})
    i = sc.op("B", "inc", "a", 1, clock=2)
    sc.recv("A", "B", i)
    sc.op("A", "unset", "a", clock=4)
    sc.op("C", "set", "w", 9, clock=3)
    sc.op("B", "set", "b", 7, clock=5)
    return sc


@scenario
def s_unset_then_receive_a_survivor_increment():
    """The other order: A unsets a (4), THEN receives B's increment (2), which A had not seen: it survives A's unset and is
    placed by the survivor clamp; A then sets e. C's w (3) is concurrent."""
    sc = RecSc({"x": 1, "a": 1})
    i = sc.op("B", "inc", "a", 1, clock=2)
    sc.op("C", "set", "w", 9, clock=3)
    sc.op("A", "unset", "a", clock=4)
    sc.recv("A", "B", i)
    sc.op("A", "set", "e", 1)
    return sc


@scenario
def s_two_writers_each_receive_the_others_write_then_write():
    """A sets p (5) and B sets q (6); each receives the other's entry; then A sets r and q, B unsets p and sets s."""
    sc = RecSc({"x": 1})
    a1 = sc.op("A", "set", "p", 1, clock=5)
    b1 = sc.op("B", "set", "q", 1, clock=6)
    sc.recv("A", "B", b1)
    sc.recv("B", "A", a1)
    sc.op("A", "set", "r", 1)
    sc.op("B", "unset", "p")
    sc.op("B", "set", "s", 1)
    sc.op("A", "set", "q", 2)
    return sc


@scenario
def s_hybrid_floor_on_an_update_after_a_receive():
    """C unsets b (10); A receives it, so b is absent at A; A updates {set c, unset b}: the unset is a no-op component whose
    floor rides on the update (prev is what A had seen after the receive). B, which never saw C's unset, sets b (11) meanwhile,
    and C creates e after the update (13)."""
    sc = RecSc({"a": 1, "b": 2})
    u = sc.op("C", "unset", "b", clock=10)
    sc.recv("A", "C", u)
    sc.op("A", "update", None, [("set", "c", 7), ("unset", "b", None)], clock=12)
    sc.op("B", "set", "b", 5, clock=11)
    sc.op("C", "set", "e", 1, clock=13)
    return sc


@scenario
def s_a_standalone_noop_unset_leaves_nothing():
    """A unsets a field it does not have: no stamp, no floor, no entry (the writer refuses it); so are an update of nothing
    but that unset, one whose other change is a no-op, three whose paths meet (the same path twice, a path and the map above it,
    the other way round) and a write below a scalar, single or in an update. B sets that field (6)
    and C creates e (7): the field goes where B put it."""
    sc = RecSc({"a": 1, "b": 2, "m": {"p": 1}})
    sc.refused("A", "unset", "nope", clock=5)
    sc.refused("A", "update", None, [("unset", "nope", None)])                       # an update of nothing but a no-op unset
    sc.refused("A", "update", None, [("set", "a", 1), ("unset", "nope", None)])      # the set changes nothing either
    sc.refused("A", "update", None, [("set", "a", 5), ("unset", "a", None)])         # one path twice
    sc.refused("A", "update", None, [("set", "m.p", 5), ("set", "m", {"q": 1})])     # a path and the map above it
    sc.refused("A", "update", None, [("unset", "m", None), ("set", "m.p", 5)])       # a map and the path below it
    sc.refused("A", "update", None, [("set", "a.b", 5)])                             # below a scalar
    sc.refused("A", "set", "a.b", 5)                                                 # below a scalar
    sc.op("B", "set", "nope", 1, clock=6)
    sc.op("C", "set", "e", 1, clock=7)
    return sc


@scenario
def s_a_subdocument_set_that_only_reorders_is_a_write():
    """A sets m to the same fields in another order: no leaf changes, the order does, so it is a write (an order state
    only). B sets a new field m.r concurrently; C creates z."""
    sc = RecSc({"m": {"p": 1, "q": 2}, "k": 0})
    sc.op("A", "set", "m", {"q": 2, "p": 1}, clock=10)
    sc.op("B", "set", "m.r", 5, clock=11)
    sc.op("C", "set", "z", 1, clock=12)
    return sc


@scenario
def s_a_writer_with_two_lives_writes_into_the_newest():
    """A and B each create the document by an upserting inc (two lives); B's entry reaches A, which holds both lives and sets z:
    the write goes into the newest life."""
    sc = NoInit()
    sc.upsert_inc("A", "x", 1)
    b = sc.upsert_inc("B", "y", 2)
    sc.recv("A", "B", b)
    sc.op("A", "set", "z", 3)
    return sc


@scenario
def s_inc_of_a_double_below_a_map_is_a_register_write():
    """A double base is no counter: B's inc of m.f is a register write of 2.5, and a write below a map is a write to the map,
    which A's concurrent unset of m (5) must not void. An inc by 0 of the double changes nothing and is no write."""
    sc = RecSc({"x": 1, "m": {"f": 1.5}})
    sc.refused("B", "inc", "m.f", 0)                      # 1.5 + 0 is the stored 1.5 byte for byte: no write
    sc.op("A", "unset", "m", clock=5)
    sc.op("B", "inc", "m.f", 1, clock=6)
    sc.op("C", "set", "w", 1, clock=8)
    return sc


@scenario
def s_a_set_on_insert_upsert_feeds_both_of_its_writes():
    """A's upsert (creation of a, b and c, then the increment of n) is two writes; B creates a life of its own by an
    upserting inc, and A then sets d."""
    sc = NoInit()
    sc.upsert_soi("A", {"a": 1, "b": 2}, "n", 3, sets={"c": 9})
    sc.upsert_inc("B", "e", 1)
    sc.op("A", "set", "d", 4)
    return sc


@scenario
def s_a_scalar_over_a_map_voids_the_places_of_its_fields():
    """A sets m (a map with p and q) to a scalar, then to a map again, then writes q and p: the unsets of the scalar write made
    the old places of p and q void, so the fields come back in the order they are written (q, then p), not in the order of
    the first insert. B creates w meanwhile."""
    sc = RecSc({"x": 1, "m": {"p": 1, "q": 2}})
    sc.op("A", "set", "m", 7, clock=10)
    sc.op("B", "set", "w", 1, clock=11)
    sc.op("A", "set", "m", {}, clock=12)
    sc.op("A", "set", "m.q", 3)
    sc.op("A", "set", "m.p", 4)
    return sc


@scenario
def s_updates_that_are_writes_by_what_they_name_alone():
    """An update of one unset of a field that exists is a write; an update that sets a sub-document to the same values in another
    order (the values are equal, only the names tell) is a write of the order alone; an update that increments a field below a
    map is a write to the map, which C's concurrent unset of the map (6) must void only up to its own stamp."""
    sc = RecSc({"x": 1, "a": 1, "m": {"p": 1, "q": 1}, "k": {"j": 1}})
    sc.op("A", "update", None, [("unset", "x", None)], clock=5)
    sc.op("A", "update", None, [("set", "m", {"q": 1, "p": 1})])
    sc.op("C", "unset", "k", clock=6)
    sc.op("B", "update", None, [("inc", "k.j", 1)], clock=7)
    sc.op("C", "set", "e", 1, clock=9)
    return sc


@scenario
def s_maps_that_survive_an_unset_through_empty_children_trail_in_path_order():
    """O2-3 of the O2 review. B, which A has not heard from, creates the empty maps m.x and m.y and k.z (stamps 2, 3, 4);
    A unsets m and k (6, 7), which it saw as {p} and {q}. m and k survive, visible through empty maps alone: they have no
    place (no standing candidate, no leaf below). A then creates n. The fields with a place come first (a, then n), and the
    two maps without one trail in path order: k, then m."""
    sc = RecSc({"a": 1, "m": {"p": 1}, "k": {"q": 1}})
    sc.op("B", "set", "m.x", {}, clock=0)
    sc.op("B", "set", "m.y", {})
    sc.op("B", "set", "k.z", {})
    sc.op("A", "unset", "m", clock=5)
    sc.op("A", "unset", "k")
    sc.op("A", "set", "n", 1)
    return sc


# A2.3 (the O2-2 fix): an upsert names the ancestors of every path it writes. The five shapes of the model tree's probes_a23.py,
# each followed by the writer's `$set c`; the scenario's entries are the writer's states after the upsert and after the set,
# joined by the observer in every delivery order (the probe joins the model's whole oplog: the same shapes, the same orders).

def _upsert_then_set(upsert):
    sc = NoInit()
    upsert(sc)
    sc.op("A", "set", "c", 2)
    return sc


@scenario
def s_a23_upsert_inc_of_a_nested_path_then_set():
    return _upsert_then_set(lambda sc: sc.upsert_inc("A", "a.b", 1))


@scenario
def s_a23_upsert_soi_no_field_inc_nested_then_set():
    return _upsert_then_set(lambda sc: sc.upsert_soi("A", {}, "a.b", 1))


@scenario
def s_a23_upsert_soi_field_inc_nested_then_set():
    return _upsert_then_set(lambda sc: sc.upsert_soi("A", {"s": 7}, "a.b", 1))


@scenario
def s_a23_upsert_soi_nested_field_then_set():
    return _upsert_then_set(lambda sc: sc.upsert_soi("A", {"a.b": 7}, "n", 1))


@scenario
def s_a23_upsert_soi_nested_set_field_then_set():
    return _upsert_then_set(lambda sc: sc.upsert_soi("A", {}, "n", 1, sets={"a.b": 7}))


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
                # what each writer shows right after its write (read-your-writes): the ordered document of the entry's state
                out["own"] = [None if M.unwire(e[2]).visible() is None else flat(M.unwire(e[2]).visible()) for e in all_ents]
                out["init"] = None if sc.init_doc is None else j_arg(sc.init_doc)
                out["script"] = sc.script
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


def _without_mapi(st):
    return all(r[1] != M.MAPI for r in st.regs.values())


def _flat_ordered(st):
    v = st.visible()
    return None if v is None else flat(v)


def _mutants():
    """Five mutants of the Rust read, as textual mutants of the model's born_of / _survivors (to choose the states that tell
    them from the design; the Rust test kills the mutants of the Rust)."""
    import inspect, textwrap

    def make(fn, subs):
        src = textwrap.dedent(inspect.getsource(fn))
        for old, new in subs:
            assert old in src, old
            src = src.replace(old, new, 1)
        ns = dict(M.__dict__)
        exec(src, ns)
        return ns[fn.__name__]

    bo, sv = M.State.born_of, M.State._survivors
    return {
        "born_of reads one life's entry, no fold over the alive lives": ("born_of", make(bo, [("o = ord_join(o, self.ords[(L, p)])", "o = ord_join(o, self.ords[(L, p)]); break")])),
        "no slot survivors of a descendant": ("_survivors", make(sv, [("            out.append(place(bq if bq is not None else (win[0], 0)))\n        out += slot_survivors(q)", "            out.append(place(bq if bq is not None else (win[0], 0)))")])),
        "level floor: min of the lives' lvl": ("born_of", make(bo, [("lv = po.lvl if lv is None else max(lv, po.lvl)", "lv = po.lvl if lv is None else min(lv, po.lvl)")])),
        "a descendant survivor at its register stamp, not its born_of": ("_survivors", make(sv, [("out.append(place(bq if bq is not None else (win[0], 0)))", "out.append(place((win[0], 0)))")])),
        "below built from registers only": ("_survivors", make(sv, [("for k in list(self.regs) + list(self.slots) if k[0] in al and is_under(k[1], p)", "for k in list(self.regs) if k[0] in al and is_under(k[1], p)")])),
    }


def random_states_fixture(seeds=300, steps=30):
    """O1-4 and O1-6 of the O1 review. States of the model's random nested runs (every writer entry of every member's oplog and
    every member's final state; no implicit markers: O4 is not built), with the model's ordered document of each under
    ABSORBED = False (the counter cell the Rust has). Chosen: for each of five mutants of the read the states it changes
    (smallest first), and a spread of others; and, from runs with ABSORBED = True, the states whose order or values the
    Q6 rule changes (the Rust has no `absorbed` yet: pinned as a known divergence)."""
    muts = _mutants()
    seen, pool = set(), []
    M.ABSORBED = False
    for seed in range(seeds):
        ms, log, ok = P.run(seed, steps=steps, nested=True)
        sts = [m.state for m in ms]
        for m in ms:
            sts += [M.unwire(e[2]) for e in m.oplog.values() if e[0] == "w"]
        for st in sts:
            c = st.canon()
            if c in seen or not _without_mapi(st) or st.visible() is None:
                continue
            seen.add(c)
            pool.append((seed, st))
    chosen, why = {}, {}
    for name, (meth, f) in muts.items():
        hits = []
        orig = getattr(M.State, meth)
        for i, (seed, st) in enumerate(pool):
            want = M.tyrepr(M.ordered(st.visible()))
            setattr(M.State, meth, f)
            try:
                got = M.tyrepr(M.ordered(st.visible()))
            finally:
                setattr(M.State, meth, orig)
            if got != want:
                hits.append((sum(len(x) for x in (st.regs, st.slots)), i))
        hits.sort()
        why[name] = len(hits)
        for _, i in hits[:6]:
            chosen.setdefault(i, []).append(name)
    spread = list(range(0, len(pool), max(1, len(pool) // 60)))
    for i in spread:
        chosen.setdefault(i, [])
    states = []
    for i in sorted(chosen):
        seed, st = pool[i]
        states.append({"seed": seed, "kills": chosen[i], "state": j_state(st), "expected": _flat_ordered(st),
                       "lives": len(st.alive())})
    # the Q6 rule: states whose order or values change with ABSORBED on
    M.ABSORBED = True
    differing = []
    for seed in range(seeds):
        ms, log, ok = P.run(seed, steps=steps, nested=True)
        for m in ms:
            for st in [m.state] + [M.unwire(e[2]) for e in m.oplog.values() if e[0] == "w"]:
                if not _without_mapi(st) or st.visible() is None:
                    continue
                on = _flat_ordered(st)
                M.ABSORBED = False
                off = _flat_ordered(st)
                M.ABSORBED = True
                if on != off:
                    differing.append((len(st.regs) + len(st.slots), seed, st, on, off))
    differing.sort(key=lambda x: (x[0], x[1]))
    absorbed = []
    seen2 = set()
    for n, seed, st, on, off in differing:
        if st.canon() in seen2:
            continue
        seen2.add(st.canon())
        absorbed.append({"seed": seed, "state": j_state(st), "expected_on": on, "expected_off": off})
        if len(absorbed) >= 4:
            break
    M.ABSORBED = True
    return {"states": states, "absorbed": absorbed, "pool": len(pool), "selected_by_mutant": why,
            "absorbed_differing_states": len(differing)}


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
    rs = random_states_fixture()
    rs["model6_sha256"] = sha
    with open(os.path.join(outdir, "ord_random_states.json"), "w") as f:
        json.dump(rs, f, separators=(",", ":"), sort_keys=True)
    print("random states:", len(rs["states"]), "chosen of", rs["pool"], "; kills", rs["selected_by_mutant"], "; absorbed:", len(rs["absorbed"]), "of", rs["absorbed_differing_states"])
    with open(os.path.join(outdir, "HASHES.txt"), "w") as f:
        f.write(f"{sha}  model6.py (q7step0-model-A2-final)\n")
        for n in ("pullmodel6.py", "probes_order.py"):
            f.write(hashlib.sha256(open(os.path.join(MODEL, n), "rb").read()).hexdigest() + f"  {n}\n")
        f.write(f"python {sys.version.split()[0]}\n")
    print("join-law fixture:", jl["paths_with_several"], "paths with several Ords,", jl["triples"], "triples")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "out"))
