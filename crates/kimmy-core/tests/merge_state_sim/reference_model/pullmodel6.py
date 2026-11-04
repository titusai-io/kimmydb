"""Q7 v4 P0 model: model6's merge state and join, replicated by the protocol the cluster actually runs.
(Rebased on the reviewer's pullmodel.py: threshold pulls, witnessed/servable vectors, ADR-224 holes, restores that
bring back the backup's vectors, loss entries stamped by the detecting member. v4: coverage vectors are keyed by
(node, epoch), V3M2; no whole-state repair anywhere; a snapshot happens only when a hole or retention asks.)

Original docstring:


model3.sync() hands every member every entry in every oplog, every round. The cluster does not:
  * a pull asks one threshold, the lowest point the puller's WITNESSED vector trails the
    peer's SERVABLE vector at (version.rs::behind), and is served every entry above it
    (docs/oplog.md "Version vectors");
  * after an exhausted window the puller raises witnessed to the peer's whole servable
    vector; after a truncated one, per origin, to the lower of the peer's coverage and the
    stamp the window reached (ADR-127);
  * a member appends an entry only if applying it changed its state (D11), so servable can
    pass an entry it never appended;
  * a whole-database snapshot grants the sender's servable vector, and where the grant
    raises servable it records a hole (ADR-224 `state_held`); a puller that trails an
    origin below a hole is answered BeyondHorizon and takes a snapshot
    (sync.rs::lacks_collected);
  * a restore brings back the backup's state, oplog, servable vector and holes; witnessed
    is re-derived; the member takes a new epoch.
Loss entries get a fresh stamp of the detecting member (the oplog key is the stamp, so a
loss entry cannot reuse the loser's). Vectors are keyed by node, as OPLOG_VERSIONS is.

Nothing here swaps whole states except a snapshot, and a snapshot happens only when the
protocol asks for one (a hole or, in `snap_rate`, a stand-in for retention's BeyondHorizon).
"""
import sys, os, random, itertools
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import model6 as M

SNAP_LOSS_ENTRIES = True     # D10: a join that detects a loss appends its entry (snapshot joins too)
HOLES = True                 # ADR-224 holes on a snapshot grant
APPEND_ALL_LOSS = False      # variant: a member appends every loss entry it witnesses
NORMALIZE_WRITES = True      # see finish()
COVERED_SKIP = True          # Q3 (A2): a writer entry whose version vector the receiver already covers is not joined (the plan's C8 pre-check)
START_HLC = True             # Q3 (A2): a never-witnessed origin's pull threshold is its epoch's start hlc, not 0


EPOCH_FROM_BACKUP = False      # mutant for V4M2
_EPOCH_RNG = random.Random(20261031)
EPOCH_KEYED = True      # V3M2: the oplog origin and the coverage vectors are (node, epoch)


def node(st):
    return st[1] if EPOCH_KEYED else st[1][0]


def okey(st):
    return (st[0], st[1][0], st[1][1])


class Node(M.Member):
    def __init__(self, name):
        super().__init__(name)
        self.oplog = {}
        self.servable, self.witnessed, self.hole = {}, {}, {}
        self.snaps = 0

    def _raise(self, vec, o, h):
        if h > vec.get(o, 0):
            vec[o] = h

    def _append(self, ent):
        st = ent[1]
        self.oplog[okey(st)] = ent
        self._raise(self.servable, node(st), st[0])
        self._raise(self.witnessed, node(st), st[0])

    def finish(self, st, log, meta):
        s = self.state
        s.vv[st[1]] = st[0]
        if NORMALIZE_WRITES:
            # the writer's own state, normalised as every receiver's join normalises it
            # (dead lives listed, their cells dropped); model3's write path does not do this
            s, _ = M.join(s, M.State())
            self.state = s
        s.contained = s.contained | {st}
        log[st] = (frozenset(s.contained - {st}), meta)
        self._append(("w", st, M.wire(s)))
        return st

    def _absorb(self, other, entry_id=None, append_entry=None, record_entries=True):
        before = self.state
        merged, fresh = M.join(before, other)
        self.clock = max([self.clock] + list(merged.vv.values()))
        changed = merged.canon() != before.canon()
        self.state = merged
        for k in sorted(fresh, key=repr):
            self.events.append(k)
            if record_entries:
                st = self.tick()
                self._append(("l", st, k, merged.losses[k]))
        return changed

    def receive_entry(self, ent):
        st = ent[1]
        if ent[0] == "w":
            if COVERED_SKIP and all(self.state.vv.get(w, 0) >= h for w, h in ent[2].vv.items()):
                self.covered_skips = getattr(self, "covered_skips", 0) + 1
            elif self._absorb(M.unwire(ent[2])) and okey(st) not in self.oplog:
                self._append(ent)
        else:
            key, val = ent[2], ent[3]
            if key[2][0] >= self.state.horizon() and key not in self.state.losses:
                self.state.losses[key] = val
                self.events.append(key)
                self._append(ent)
            elif APPEND_ALL_LOSS and okey(st) not in self.oplog:
                self._append(ent)
        self._raise(self.witnessed, node(st), st[0])

    def snapshot_from(self, src):
        self.snaps += 1
        self._absorb(src.state.copy(), record_entries=SNAP_LOSS_ENTRIES)
        for o, h in src.servable.items():
            if h > self.servable.get(o, 0):
                if HOLES:
                    self._raise(self.hole, o, h)
                self.servable[o] = h
            self._raise(self.witnessed, o, h)

    def backup(self):
        self.saved_epoch = self.epoch
        self.saved = [(self.state.copy(), dict(self.oplog), dict(self.servable), dict(self.hole))]

    def restore(self):
        if not self.saved:
            return False
        st, ol, sv, hole = self.saved[0]
        self.state, self.oplog = st.copy(), dict(ol)
        self.servable, self.hole = dict(sv), dict(hole)
        self.witnessed = dict(sv)
        if EPOCH_FROM_BACKUP:                   # the mutant: a counter kept in the store, which a backup carries
            self.epoch = self.saved_epoch + 1
        else:                                   # V4M2: the restore's start HLC plus a random 64 bits, never read from the store
            self.epoch = (self.clock << 64) | _EPOCH_RNG.getrandbits(64)
        self.restored = True
        return True


def start_hlc(o):
    """The start hlc of an origin's epoch: no entry of that origin is older (a restore's epoch is its start hlc plus 64 random bits)."""
    if START_HLC and isinstance(o, tuple) and isinstance(o[1], int) and o[1] > 0:
        return o[1] >> 64
    return 0


def pull(p, r, limit=None):
    """p pulls from r. Returns True if anything was applied or witnessed."""
    behind = [o for o, h in r.servable.items() if h > p.witnessed.get(o, 0)]
    if not behind:
        return False
    thr = min(p.witnessed.get(o, start_hlc(o)) for o in behind)
    if any(p.witnessed.get(o, 0) < h and p.witnessed.get(o, 0) < r.hole.get(o, 0)
           for o, h in r.servable.items()):
        p.snapshot_from(r)                      # BeyondHorizon (a hole): take the snapshot
        return True
    window = sorted((k, e) for k, e in r.oplog.items() if k[0] > thr)
    exhausted = limit is None or len(window) <= limit
    if not exhausted:
        window = window[:limit]
    for k, e in window:
        p.receive_entry(e)
    if exhausted:
        for o, h in r.servable.items():
            p._raise(p.witnessed, o, h)
    elif window:
        end = window[-1][0]
        for o, h in r.servable.items():
            eo = (end[1], end[2]) if isinstance(o, tuple) else end[1]
            reach = end[0] if o <= eo else end[0] - 1
            p._raise(p.witnessed, o, min(h, reach))
    return True


def redeliver(ms, order=1):
    """Overlapping windows re-send entries the receiver already holds (routine in the cluster), in any order."""
    for p in ms:
        for r in ms:
            if p is not r:
                for k, e in sorted(r.oplog.items(), reverse=order < 0):
                    p.receive_entry(e)


def settle(ms, rounds=6):
    """Beyond-retention regime (ADR-085): redeliver in both orders, repair by snapshot where members differ, repeat.
    Returns the rounds needed, or None when the state is still moving (a flip-flop)."""
    for n in range(rounds):
        redeliver(ms, 1)
        mid = [m.state.canon() for m in ms]
        redeliver(ms, -1)
        fixpoint(ms)
        if len({m.state.canon() for m in ms}) != 1:
            for p in ms:
                for r in ms:
                    if p is not r:
                        p.snapshot_from(r)
            fixpoint(ms)
        if len({m.state.canon() for m in ms}) == 1 and [m.state.canon() for m in ms] == mid:
            return n
    return None


def fixpoint(ms, rounds=40):
    for _ in range(rounds):
        before = [(m.state.canon(), tuple(sorted(m.witnessed.items()))) for m in ms]
        for p in ms:
            for r in ms:
                if p is not r:
                    pull(p, r)
        if [(m.state.canon(), tuple(sorted(m.witnessed.items()))) for m in ms] == before:
            return True
    return False


SUBDOCS = {"a": [{}, {"b": 1}, {"c": 2}, {"b": 1, "c": 2}, {"c": 2, "b": 1}, {"b": 0, "c": "s"}, {"d": {"e": 3}},
               {"b": 0, "d": {}}, {"d": {"e": 1, "f": 2}, "c": "s"}, {"c": 1, "d": {"f": 2, "e": 1}, "b": 4}],
           "a.d": [{}, {"e": 1}, {"f": 2}, {"f": 2, "e": 1}, {"e": 5, "g": 6}],
           "acct": [{}, {"balance": 150}, {"balance": 0}, {"balance": 100}]}
SCALARS = [5, "s", 2.5, (1, 2), ()]
LEAVES = ["x", "n", "y", "a.b", "a.c", "a.d.e", "a.d.f", "a.d.g", "acct.balance"]
PARENTS = ["a", "a.d", "acct"]


def rand_doc(rnd):
    """A whole replacement document, fields in a random order."""
    d = []
    for t in ("x", "n", "y"):
        if rnd.random() < 0.4:
            d.append((t, rnd.choice([7, 8, "s", 100])))
    if rnd.random() < 0.6:
        d.append(("a", rnd.choice(SUBDOCS["a"] + [5])))
    if rnd.random() < 0.6:
        d.append(("acct", rnd.choice(SUBDOCS["acct"] + ["s"])))
    if rnd.random() < 0.3:
        d.append(("items", rnd.choice([(1,), (1, 2), (3,)])))
    rnd.shuffle(d)
    return dict(d)


def shuffled(rnd, v):
    """The same document with the keys of every level in a random order."""
    if not isinstance(v, dict):
        return v
    ks = list(v)
    rnd.shuffle(ks)
    return {k: shuffled(rnd, v[k]) for k in ks}


def disjoint(a, b):
    return not (a == b or a.startswith(b + ".") or b.startswith(a + "."))


def draw_op(rnd, m, log, skew, nested, upsert_soi=False):
    """One client operation. nested=False reproduces the flat workload of the earlier model draw for draw."""
    if nested and rnd.random() < 0.6:
        k = rnd.choice(["inc", "inc", "inc", "set", "set", "set", "setp", "setp", "unset", "unset", "unsetp", "replace",
                        "shuffle", "setmany", "update", "update", "push", "incqty", "insert", "delete"])
        if k == "inc":
            m.write("inc", rnd.choice(["acct.balance", "acct.balance", "a.b", "a.c", "a.d.e", "x", "y"]), rnd.choice([1, 5, 20, -3, 50]), log, skew)
        elif k == "set":
            m.write("set", rnd.choice(LEAVES), rnd.choice([0, 1, 7, 500, "s", 2.5, None]), log, skew)   # None = a stored null
        elif k == "setp":
            t = rnd.choice(PARENTS)
            m.write("set", t, rnd.choice(SUBDOCS[t] * 2 + SCALARS), log, skew)
        elif k == "unset":
            m.write("unset", rnd.choice(LEAVES), None, log, skew)
        elif k == "unsetp":
            m.write("unset", rnd.choice(PARENTS + ["items"]), None, log, skew)
        elif k == "replace":
            m.write("replace", None, rand_doc(rnd), log, skew)
        elif k == "shuffle":
            vis = m.state.visible()
            if vis:
                m.write("replace", None, shuffled(rnd, vis), log, skew)
        elif k == "update":
            a, b, c = rnd.sample(LEAVES + PARENTS, 3)
            ops = []
            for q in (a, b, c):
                if not (all(disjoint(q, o[1]) for o in ops)):
                    continue
                kind = rnd.choice(["set", "unset", "inc", "inc"])
                if kind == "set":
                    ops.append(("set", q, rnd.choice(SUBDOCS[q]) if q in SUBDOCS else rnd.choice([0, 1, "s", None])))
                elif kind == "unset":
                    ops.append(("unset", q, None))
                else:
                    ops.append(("inc", q, rnd.choice([1, 5, -3])))
            if ops:
                m.write("update", None, ops, log, skew)
        elif k == "setmany":
            a, b = rnd.sample(LEAVES + PARENTS, 2)
            if disjoint(a, b):
                va = rnd.choice(SUBDOCS[a]) if a in SUBDOCS else rnd.choice([0, 1, "s"])
                vb = rnd.choice(SUBDOCS[b]) if b in SUBDOCS else rnd.choice([0, 1, "s"])
                m.write("setmany", None, [(a, va), (b, vb)], log, skew)
        elif k == "push":
            m.write("push", "items", rnd.choice([1, 2, 3]), log, skew)
        elif k == "incqty":
            m.write("incqty", "items", rnd.choice([1, 2]), log, skew)
        elif k == "delete":
            if rnd.random() < 0.25:
                m.write("delete", None, None, log, skew)
        else:
            m.insert(rnd.choice([{"a": {"b": 1}}, {"acct": {"balance": 10}}, {"x": 1, "a": {}}, {"items": (1,)},
                                 {"a": {"b": 2, "c": 3, "d": {"e": 1}}, "acct": {"balance": 5}}, {"n": "q", "a": {"d": {"f": 1}}}]), log, skew)
        return
    ops = ["inc", "inc", "inc", "set", "set", "unset", "delete", "insert", "replace", "upsert"] + (["upsert_soi"] if upsert_soi else [])
    op = rnd.choice(ops)
    p = rnd.choice(["x", "n", "y"])
    if op == "insert":
        m.insert({rnd.choice(["x", "y"]): rnd.choice([1, 2])}, log, skew)
    elif op == "upsert_soi":
        m.upsert_soi({rnd.choice(["y", "z"]): rnd.choice([1, 2])}, rnd.choice(["x", "y"]), rnd.choice([1, 2]), log, skew,
                     sets=rnd.choice([None, {"n": 5}]))
    elif op == "upsert":
        m.upsert_inc(rnd.choice(["x", "y"]), rnd.choice([1, 2]), log, skew)
    elif op == "replace":
        m.write("replace", None, {rnd.choice(["x", "n", "y"]): rnd.choice([7, 8, "s"])}, log, skew)
    elif op == "inc":
        m.write("inc", p, rnd.choice([1, 5, 20, -3]), log, skew)
    elif op == "set":
        m.write("set", p, rnd.choice([0, 7, 500, "s", 2.5]), log, skew)
    else:
        m.write(op, p, None, log, skew)


def run(seed, steps=18, snap_rate=0.06, restore=True, ret=None, sequential=False, nested=True):
    rnd = random.Random(seed)
    M.RET = ret if ret is not None else 10 ** 9
    ms = [Node(n) for n in M.NODES[:rnd.choice([3, 4])]]
    log = {}
    ms[0].insert({"x": 100, "n": "a", "a": {"b": 0, "c": 2, "d": {"e": 1}}, "acct": {"balance": 100}, "items": (1,)} if nested
                 else {"x": 100, "n": "a"}, log)
    for m in ms:
        m.backup()
    did_restore = False
    for i in range(steps):
        m = rnd.choice(ms)
        skew = 0 if sequential else rnd.choice([0, 0, 0, 4])
        x = 1.0 if sequential else rnd.random()
        if sequential:
            fixpoint(ms)
        if x < 0.45:
            p, r = rnd.sample(ms, 2)
            if rnd.random() < snap_rate:
                p.snapshot_from(r)              # stands in for retention's BeyondHorizon
            else:
                pull(p, r, limit=rnd.choice([None, None, 1, 2]))
            continue
        if restore and not sequential and not did_restore and i > steps // 2 and rnd.random() < 0.15:
            tgt = rnd.choice(ms)
            other = rnd.choice([o for o in ms if o is not tgt])
            pull(other, tgt)                    # its writes exist somewhere before the restore
            tgt.restore()
            did_restore = True
            continue
        draw_op(rnd, m, log, skew, nested, upsert_soi=True)
    settled = fixpoint(ms)
    return ms, log, settled


DEVIATIONS = []      # order mismatches explained by the ruling-(a) approximation, collected by the suite


def grade(ms, log):
    errs = []
    if len({m.state.canon() for m in ms}) != 1:
        names = ["lives", "dead", "deletes", "regs", "slots", "vv", "losses", "ords", "absorbed"]
        cs = [m.state.canon() for m in ms]
        errs.append(("diverged", tuple(n for i, n in enumerate(names) if len({c[i] for c in cs}) > 1)))
        return errs
    want, alive, dels = M.oracle(log)
    if any(m.state.visible() != want for m in ms):
        errs.append(("visible",))
    must = M.loss_oracle(log, alive, dels)
    have = set(ms[0].state.losses)
    if M.expansion_errors(log):
        errs.append(("leaf-effect-expansion",))
    if any(m.state.hidden_all() != M.hidden_oracle(log) for m in ms):
        errs.append(("hidden-state",))
    if M.stale_records(have, log):
        errs.append(("record-names-surviving-increment",))
    if any(not (grp & {(k[1], k[2]) for k in have}) for grp in M.killed_superseded_any(log)):
        errs.append(("record-missing-killed-superseded",))
    if must - {(k[1], k[2]) for k in have}:
        errs.append(("record-missing",))
    if any(not M.sound_record(k, log, alive, dels) for k in have):
        errs.append(("record-unsound",))
    for m in ms:
        if not m.restored and (len(m.events) != len(set(m.events)) or set(m.events) != have):
            errs.append(("emission", m.name))
            break
    oe, dev = M.order_errors(ms[0].state, log)
    errs += oe
    DEVIATIONS.extend(dev)
    return errs


def main(n, label):
    from collections import Counter
    c = Counter(); ex = {}
    snaps = restores = 0
    for seed in range(n):
        ms, log, settled = run(seed)
        snaps += any(m.snaps for m in ms)
        restores += any(m.restored for m in ms)
        if not settled:
            c["no fixpoint"] += 1
        for e in grade(ms, log):
            k = e[0] if e[0] != "diverged" else "diverged " + "+".join(e[1])
            c[k] += 1
            ex.setdefault(k, seed)
    print(f"[{label}] {n} runs ({snaps} with a snapshot, {restores} with a restore): problems {dict(c) or 0}; first seeds {ex}")


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 500
    which = sys.argv[2] if len(sys.argv) > 2 else "all"
    if which in ("all", "design"):
        main(n, "design: snapshot joins append loss entries, ADR-224 holes")
    if which in ("all", "model3"):
        SNAP_LOSS_ENTRIES = True
        globals()["SNAP_LOSS_ENTRIES"] = False
        main(n, "model3's choice: snapshot joins append no loss entries, ADR-224 holes")
        globals()["SNAP_LOSS_ENTRIES"] = True
    if which in ("all", "noholes"):
        globals()["HOLES"] = False
        main(n, "no holes (the gap NM6 named)")
        globals()["HOLES"] = True
