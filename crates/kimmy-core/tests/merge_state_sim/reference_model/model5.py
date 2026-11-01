# Reference model for the Q7 field-level merge (the Python model that crates/kimmy-core's
# `merge_state` module is a port of), standard library only. It implements the Q7 design
# document (field-level merge with summing $inc counters and conflictResolved, v7.2): the merge
# state and its join (design sections on lives, registers, counter slots, generations and the
# horizon), the read side and the hidden figure (D5), the loss records (D10), and the oracle that
# grades a run from its causal log. This copy is vendored for the differential test
# (tests/merge_state_diff.rs); it carries one change to the original, the g2 soundness rule
# in `sound_record` (a record must name a path its write wrote; a deleteWon needs a delete that
# killed the life). Run `python3 -I model5.py [runs]` for its random runs.
"""Q7 v4 model state and join (replicated by pullmodel5.py, which carries the cluster's coverage protocol).

v4 changes: (V3M1) the join filters by the JOINED horizon before it takes the union; (V3M3) the counting
generation follows the WINNING base: every base register carries its own generation.

v3 text follows.

Q7 v3 model: replicates the way the cluster does.

Replication = writer entries (each carries the writer's full state), loss-record entries, and snapshot
pages (state only, no oplog entry). A member serves only the entries in its own oplog; an entry is
appended to a member's oplog only if applying it changed that member's state (a dominated entry is
dropped). The run ends by pulling entries from oplogs to a fixpoint. No whole-state swaps at the end.

State (v3 rules):
  lives[L]   = (seen: frozenset of delete stamps the creator held, order: insert field order)   grow-only
  deletes[d] = vv of the deleting writer (its context)                                           grow-only
  regs[(L,p)]   = (stamp, value|None, obs{(L2,w,gen): (slot stamp, total)})     higher stamp wins
  slots[(L,p,w,gen)] = (stamp, total)                              higher stamp wins; w = (node, epoch)
  gens[(L,p)] = (stamp of the newest non-numeric base, vv of its writer)   higher stamp wins
  vv[w] = hlc                                                      max
  losses[(kind, path, loser_stamp)] = loser value                  grow-only, filtered by the horizon
A life is alive iff every delete in the state is in its `seen`. Cells are keyed by (life, path); cells of
dead lives are dropped. Visible value of p = newest base among alive lives + sum over counted slots
(gen == newest gen over alive lives) of (total - observed by that base).
Horizon H(state) = max(vv) - RETENTION: a pure function of the state; records with loser hlc < H are
filtered on every join.
"""
import copy, itertools, random, sys

GENESIS = None
HORIZON_FIRST = True      # V3M1
CELLS_REPORTED = True     # V6M1: writes into a superseded first insert are reported
SUPERSEDE = True          # Q2: concurrent first inserts, newest whole document wins
BOUND = 2 ** 63 - 1       # Q11: Int64 bound (tests lower it); the lower bound is -BOUND - 1
RET = 10 ** 9
SNAPS = [0]


def cov(vv, stamp):
    return vv.get(stamp[1], 0) >= stamp[0]


def same(a, b):
    """Q4 (ruled): the diff compares value AND type, as the reference does: `$set 2` over 2.0 is a write."""
    return type(a) is type(b) and a == b


def numeric(v):
    return v is None or (isinstance(v, int) and not isinstance(v, bool))


class State:
    def __init__(self):
        self.lives, self.deletes, self.regs, self.slots = {}, {}, {}, {}
        self.gens, self.vv, self.losses = {}, {}, {}
        self.dead = set()          # lives known dead; grow-only until their stamp passes the horizon
        self.contained = set()      # ORACLE ONLY

    def copy(self):
        return copy.deepcopy(self)

    def alive(self):
        return [L for L, v in self.lives.items()
                if L not in self.dead and all(d in v[0] for d in self.deletes if d[0] >= L[0] - RET)]

    def horizon(self):
        return (max(self.vv.values()) if self.vv else 0) - RET

    def canon(self):
        def _r(v):
            if isinstance(v, frozenset):
                return repr(sorted(v))
            if isinstance(v, tuple):
                return "(" + ",".join(_r(x) for x in v) + ")"
            return repr(v)
        f = lambda d: tuple(sorted(((repr(k), _r(v)) for k, v in d.items())))
        return (f(self.lives), repr(sorted(self.dead)), f(self.deletes), f(self.regs), f(self.slots),
                tuple(sorted(self.vv.items())), f(self.losses))

    # --- read side -----------------------------------------------------------
    def suppressed(self, k, r, al=None):
        """V6m2: the creation cells ($setOnInsert fields) of an upsert-created life are dropped when an explicit
        first insert of the same _id is alive: the insert's document is the base, the upsert applies on top."""
        if k[0] not in self.lives or self.lives[k[0]][2] != "upsert" or len(r) < 5 or r[4] != "soi":
            return False
        al = set(self.alive()) if al is None else al
        return any(self.lives[L][2] == "insert" for L in al)      # V7m1: any explicit insert, re-inserts included

    def live_regs(self, p):
        al = set(self.alive())
        return [(k, r) for k, r in self.regs.items() if k[1] == p and k[0] in al and not self.suppressed(k, r, al)]

    def winner(self, p):
        regs = [r for k, r in self.live_regs(p)]
        return max(regs, key=lambda r: r[0]) if regs else None

    def gen_of(self, p):
        """V3M3: the generation of the WINNING base: (starter stamp, starter's vv) or None."""
        w = self.winner(p)
        return w[3] if w is not None else None

    def counted(self, p):
        """Slots of the winning base's generation, and of NEWER generations (their starter lost: D6 keeps the
        increments). Slots of OLDER generations are overwritten (hidden)."""
        al = set(self.alive())
        g = self.gen_of(p)
        G = g[0] if g else None
        return {k: v for k, v in self.slots.items()
                if k[1] == p and k[0] in al and (k[3] == G or (k[3] is not None and (G is None or k[3] > G)))}

    def hidden(self, p):
        """V4M1/V5m1: the increments the winning generation overwrote, as STATE derived from the current merge: the
        NET SUM (signed), over the slots of older generations, of the part the starter of the winning generation had
        not seen (slot total minus the total the starter saw)."""
        al = set(self.alive())
        g = self.gen_of(p)
        if not g:
            return 0
        seen = g[2] if len(g) > 2 else {}
        return sum(v[1] - seen.get((k[0], k[2], k[3]), 0) for k, v in self.slots.items()
                   if k[1] == p and k[0] in al and (k[3] is None or k[3] < g[0]))

    def hidden_all(self):
        al = set(self.alive())
        paths = sorted({k[1] for k in self.slots if k[0] in al})
        return {p: h for p in paths for h in [self.hidden(p)] if h}

    def value(self, p):
        regs = [(r, k) for k, r in self.live_regs(p)]
        win = max(regs, key=lambda x: x[0][0])[0] if regs else None
        if win is not None and not numeric(win[1]):
            return win[1]
        base = win[1] if win is not None else None
        obs = win[2] if win is not None else {}
        total, seen = (base if base is not None else 0), base is not None
        for k in sorted(self.counted(p), key=repr):
            o = obs.get(k[0:1] + k[2:])
            d = self.slots[k][1] - (o[1] if o else 0)
            if d or o is None or o[0] != self.slots[k][0]:    # B1: the slot moved since the base saw it, even if it nets to 0
                total += d
                seen = True
        return max(min(total, BOUND), -BOUND - 1) if seen else None     # Q11: a merge that crosses either bound saturates

    def visible(self):
        if not self.alive():
            return None
        al = set(self.alive())
        paths = {k[1] for k, r in self.regs.items() if k[0] in al and not self.suppressed(k, r, al)} | {k[1] for k in self.slots if k[0] in al}
        out = {}
        for p in sorted(paths):
            v = self.value(p)
            if v is not None:
                out[p] = v
        return out


def obs_key(L, w, gen):
    return (L,) + (w, gen)


def _prefilter(st, H):
    st = st.copy()
    for d in [d for d in st.deletes if d[0] < H]:
        del st.deletes[d]
    for L in [L for L in st.dead if L[0] < H]:
        st.dead.discard(L)
        st.lives.pop(L, None)
    return st


def join(s, t, tag=None):
    """Join t into s. Returns (new_state, set of new loss keys).
    V3M1: the horizon of the JOINED vv is applied to both sides BEFORE the union and before aliveness."""
    H0 = max(max(s.vv.values(), default=0), max(t.vv.values(), default=0)) - RET
    if HORIZON_FIRST:
        s, t = _prefilter(s, H0), _prefilter(t, H0)
    out = s.copy()
    ok = lambda L: L is None or L not in out.dead
    for L, v in t.lives.items():
        if ok(L):
            out.lives.setdefault(L, v)
    out.dead |= {L for L in t.dead if ok(L)}
    for d, vv in t.deletes.items():
        out.deletes.setdefault(d, vv)
    for k, r in t.regs.items():
        if ok(k[0]) and (k not in out.regs or r[0] > out.regs[k][0]):
            out.regs[k] = copy.deepcopy(r)
    for k, v in t.slots.items():
        if ok(k[0]) and (k not in out.slots or v[0] > out.slots[k][0]):
            out.slots[k] = v
    for k, g in t.gens.items():
        if ok(k[0]) and (k not in out.gens or g[0] > out.gens[k][0]):
            out.gens[k] = copy.deepcopy(g)
    for w, h in t.vv.items():
        out.vv[w] = max(out.vv.get(w, 0), h)
    out.contained |= t.contained
    new = {}
    # Q2: concurrent FIRST inserts of one _id (explicit inserts whose creator had seen no delete): the newest
    # whole document wins, the others are superseded and reported. Re-inserts after a delete and upserts still merge.
    first = [L for L, v in out.lives.items() if v[2] == "insert" and not v[0]]
    superseded = set(first) - ({max(first)} if first else set())
    if SUPERSEDE:
        out.dead |= superseded
        # V6m1: a first insert that a delete already killed is dead, not lost to a newer insert
        killed = {L for L in superseded
                  if any(d not in out.lives[L][0] and d[0] >= L[0] - RET for d in out.deletes)}
        reported = superseded - killed
        for L in reported:
            new[("insertLost", "_", L)] = repr(out.lives[L][1])
    else:
        superseded, reported = set(), set()
    al = set(out.alive())
    out.dead |= {L for L in out.lives if L not in al}
    # (1) same-path register clashes: a register of s against a register of t, concurrent by the VVs
    paths = {k[1] for k in list(s.regs) + list(t.regs)}
    for p in paths:
        rs = [r for k, r in s.regs.items() if k[1] == p and k[0] in al]
        rt = [r for k, r in t.regs.items() if k[1] == p and k[0] in al]
        for a in rs:
            for b in rt:
                if a[0] != b[0] and not cov(t.vv, a[0]) and not cov(s.vv, b[0]):
                    lo = a if a[0] < b[0] else b
                    new[("clash", p, lo[0])] = lo[1]
    # (2) a cell in a dead life that no known delete saw
    for src in (s, t):
        for k, r in src.regs.items():
            if CELLS_REPORTED and k[0] in reported and r[0] != k[0]:       # V6M1: a write made into the losing life
                new[("insertLost", k[1], r[0])] = r[1]
            elif k[0] not in al and k[0] not in reported and not any(cov(vv, r[0]) for vv in out.deletes.values()):
                new[("deleteWon", k[1], r[0])] = r[1]
        for k, v in src.slots.items():
            if CELLS_REPORTED and k[0] in reported:
                new[("insertLost", k[1], v[0])] = v[1]
            elif k[0] not in al and k[0] not in reported and not any(cov(vv, v[0]) for vv in out.deletes.values()):
                new[("deleteWon", k[1], v[0])] = v[1]
    # V6m2: creation cells of an upsert life dropped for an alive explicit first insert
    insert_alive = [L for L in al if out.lives[L][2] == "insert"]
    if insert_alive:
        for src in (s, t):
            for k, r in src.regs.items():
                if k[0] in out.lives and out.lives[k[0]][2] == "upsert" and len(r) > 4 and r[4] == "soi" and k[0] in al:
                    new[("insertLost", k[1], r[0])] = r[1]
    # drop cells of dead lives; filter records by the horizon
    for store in (out.regs, out.slots, out.gens):
        for k in [k for k in store if k[0] not in al]:
            del store[k]
    H = out.horizon()
    for d in [d for d in out.deletes if d[0] < H]:
        del out.deletes[d]
    for L in [L for L in out.lives if L[0] < H and L in out.dead]:
        del out.lives[L]
    out.dead = {L for L in out.dead if L[0] >= H}
    out.losses = {}
    for key, val in list(s.losses.items()) + list(t.losses.items()) + list(new.items()):
        if key[2][0] >= H:
            out.losses.setdefault(key, val)
    fresh = {k for k in out.losses if k not in s.losses}
    return out, fresh


class Member:
    def __init__(self, name):
        self.name, self.epoch = name, 0
        self.state, self.clock = State(), 0
        self.oplog = {}            # id -> ('w', state) | ('l', key, value)
        self.events = []
        self.restored = False
        self.saved = []

    @property
    def wid(self):
        return (self.name, self.epoch)

    def tick(self, skew=0):
        self.clock = max([self.clock] + list(self.state.vv.values())) + 1 + skew
        return (self.clock, self.wid)

    # --- writes --------------------------------------------------------------
    def _write_life(self):
        al = self.state.alive()
        return max(al) if al else None

    def _seen(self, p):
        """V5m1: the totals of every slot of p (any generation, alive lives) as this writer sees them, kept with a
        generation it starts, so the figure counts only what the starter had not seen."""
        al = set(self.state.alive())
        return {(k[0], k[2], k[3]): v[1] for k, v in self.state.slots.items() if k[1] == p and k[0] in al}

    def _obs(self, p):
        return {(k[0], k[2], k[3]): (v[0], v[1]) for k, v in self.state.counted(p).items()}

    def _setreg(self, p, v, st, vvnow):
        s = self.state
        L = self._write_life()
        gen = (st, dict(vvnow), self._seen(p)) if not numeric(v) else s.gen_of(p)
        s.regs[(L, p)] = (st, v, self._obs(p), gen)
        if not numeric(v):
            s.gens[(L, p)] = (st, dict(vvnow))      # kept only for the V3M3 mutant

    def finish(self, st, log, meta):
        s = self.state
        s.vv[st[1]] = st[0]
        s.contained = s.contained | {st}
        log[st] = (frozenset(s.contained - {st}), meta)
        self.oplog[("w", st)] = ("w", st, s.copy())
        return st

    def insert(self, fields, log, skew=0):
        s = self.state
        if s.alive():
            return None
        st = self.tick(skew)
        s.lives[st] = (frozenset(s.deletes), tuple(fields), "insert")
        for p, v in fields.items():
            s.regs[(st, p)] = (st, v, {}, None if numeric(v) else (st, {**s.vv, st[1]: st[0]}, {}))
            if not numeric(v):
                s.gens[(st, p)] = (st, {st[1]: st[0]})
        s.vv[st[1]] = st[0]
        return self.finish(st, log, {"kind": "insert", "life": st, "paths": list(fields), "vals": dict(fields)})

    def upsert_soi(self, fields, p, n, log, skew=0, sets=None):
        """V6m2/V7m2/V8: an upsert that creates the document with $setOnInsert `fields`, applies $set `sets`
        and $inc p by n. Two sequential local writes by one writer: the creation (the life and its cells, the
        $setOnInsert cells tagged) and the increment. Only the tagged cells can be dropped."""
        s = self.state
        if s.alive():
            return None
        st = self.tick(skew)
        s.lives[st] = (frozenset(s.deletes), tuple(fields), "upsert")
        vals = {}
        gen = lambda v: None if numeric(v) else (st, {**s.vv, st[1]: st[0]}, {})     # M1: a non-numeric base starts a generation
        for q, v in fields.items():
            s.regs[(st, q)] = (st, v, {}, gen(v), "soi")
            if not numeric(v):
                s.gens[(st, q)] = (st, {st[1]: st[0]})
            vals[q] = v
        for q, v in (sets or {}).items():
            s.regs[(st, q)] = (st, v, {}, gen(v))
            if not numeric(v):
                s.gens[(st, q)] = (st, {st[1]: st[0]})
            vals[q] = v
        s.vv[st[1]] = st[0]
        self.finish(st, log, {"kind": "insert", "upsert": True, "insert": True, "life": st, "paths": list(vals),
                              "vals": vals, "soi": list(fields)})
        st2 = self.tick(skew)
        s = self.state                       # finish() normalised the writer's state into a new object
        s.vv[st2[1]] = st2[0]
        s.slots[(st, p, self.wid, None)] = (st2, n)
        return self.finish(st2, log, {"kind": "inc", "life": st, "paths": [p], "n": n, "gen": None})

    def upsert_inc(self, p, n, log, skew=0):
        s = self.state
        if s.alive():
            return None
        st = self.tick(skew)
        s.lives[st] = (frozenset(s.deletes), (), "upsert")
        s.slots[(st, p, self.wid, None)] = (st, n)
        return self.finish(st, log, {"kind": "inc", "life": st, "paths": [p], "n": n, "gen": None, "insert": True})

    def write(self, op, p, arg, log, skew=0):
        s = self.state
        if not s.alive():
            return None
        if op == "delete":
            st = self.tick(skew)
            s.vv[st[1]] = st[0]
            s.deletes[st] = dict(s.vv)
            return self.finish(st, log, {"kind": "delete", "paths": []})
        vis = s.visible()
        if op in ("set", "unset", "inc"):
            cur = vis.get(p)
            if op == "set" and same(cur, arg):
                return None
            if op == "unset" and cur is None:
                return None
            if op == "inc" and numeric(cur) and not (-BOUND - 1 <= (cur or 0) + arg <= BOUND):
                return None                           # Q11: a local $inc past the bound is refused (400)
            if op == "inc" and not numeric(cur):
                if not isinstance(cur, float):
                    return None                       # a string: 400
                op, arg = "set", cur + arg            # Nm5: a double base is a register write that starts a generation
            st = self.tick(skew)
            s.vv[st[1]] = st[0]
            L = self._write_life()
            if op == "inc":
                g = s.gen_of(p)
                G = g[0] if g else None
                old = s.slots.get((L, p, self.wid, G))
                s.slots[(L, p, self.wid, G)] = (st, (old[1] if old else 0) + arg)
                return self.finish(st, log, {"kind": "inc", "life": L, "paths": [p], "n": arg, "gen": G})
            self._setreg(p, arg if op == "set" else None, st, s.vv)
            return self.finish(st, log, {"kind": "reg", "life": L, "paths": [p], "vals": {p: arg if op == "set" else None}})
        if op == "replace":
            changes = {q: v for q, v in arg.items() if not same(vis.get(q), v)}
            drops = [q for q in vis if q not in arg]
            if not changes and not drops:
                return None
            st = self.tick(skew)
            s.vv[st[1]] = st[0]
            L = self._write_life()
            vals = {}
            for q, v in changes.items():
                self._setreg(q, v, st, s.vv)
                vals[q] = v
            for q in drops:
                self._setreg(q, None, st, s.vv)
                vals[q] = None
            return self.finish(st, log, {"kind": "reg", "life": L, "paths": list(vals), "vals": vals})
        return None

    # --- replication ---------------------------------------------------------
    def _absorb(self, other, entry_id=None, append_entry=None, record_entries=True):
        before = self.state
        merged, fresh = join(before, other)
        self.clock = max([self.clock] + list(merged.vv.values()))
        changed = merged.canon() != before.canon()
        self.state = merged
        for k in sorted(fresh, key=repr):
            self.events.append(k)
            if record_entries:
                self.oplog.setdefault(("l", k), ("l", k, merged.losses[k]))
        return changed

    def receive_entry(self, ent):
        if ent[0] == "w":
            if self._absorb(ent[2]) and ("w", ent[1]) not in self.oplog:
                self.oplog[("w", ent[1])] = ent
        else:
            key, val = ent[1], ent[2]
            if key[2][0] < self.state.horizon() or key in self.state.losses:
                return
            self.state.losses[key] = val
            self.events.append(key)
            self.oplog.setdefault(("l", key), ent)

    def receive_snapshot(self, other):
        # a snapshot page: state only; no oplog entry (v3, M1/NM6)
        self._absorb(other, record_entries=False)

    def restore(self):
        if not self.saved:
            return False
        st, ol = self.saved[0]
        self.state, self.oplog = st.copy(), dict(ol)
        self.epoch += 1
        self.restored = True
        return True


# ---------------------------------------------------------------------------
# oracle: independent of the state machinery; computed from the global causal log
# ---------------------------------------------------------------------------
def _explicit(m):
    """An explicit insert (insertOne/insertMany), not the creation write of an upsert."""
    return m["kind"] == "insert" and not m.get("upsert")


def _supp(log, m, p, alive):
    """V6m2/V7m1/V7m2: a $setOnInsert cell of an upsert creation is dropped when an explicit insert (a first insert or
    a re-insert after a delete) is alive."""
    return p in m.get("soi", []) and any(_explicit(log[L][1]) for L in alive)


def killed_lives(log):
    dels = [st for st, (ctx, m) in log.items() if m["kind"] == "delete"]
    return {st for st, (ctx, m) in log.items() if _explicit(m) and
            any(d not in ctx and d[0] >= st[0] - RET for d in dels)}


def superseded_lives(log, within=None):
    """Q2: of the concurrent first inserts (explicit inserts by a creator that had seen no delete), all but the newest."""
    dels = [st for st, (ctx, m) in log.items() if m["kind"] == "delete"]
    first = [st for st, (ctx, m) in log.items() if _explicit(m) and not any(d in ctx for d in dels)
             and (within is None or st in within)]
    return set(first) - ({max(first)} if first else set())


def _alive_lives(log, ins, dels):
    sup = superseded_lives(log)
    return {L for L in ins if L not in sup and all(d in log[L][0] for d in dels if d[0] >= L[0] - RET)}


def _alive_at(log, ctx, ins, dels):
    """Lives a writer with causal past `ctx` could see as alive."""
    sup = superseded_lives(log, within=ctx)
    return {L for L in ins if L in ctx and L not in sup and all(d in log[L][0] for d in dels if d in ctx and d[0] >= L[0] - RET)}


def make_gen(log):
    """Generation of a write, from the log alone (V3M3). In words: a non-numeric base starts its own generation;
    any other write belongs to the generation of the base that was winning at its writer (the highest-stamped
    register write in the writer's causal past, in a life the writer saw as alive)."""
    ins = {st for st, (ctx, m) in log.items() if m["kind"] == "insert" or m.get("insert")}
    dels = [st for st, (ctx, m) in log.items() if m["kind"] == "delete"]
    memo = {}

    def gen(st, p):
        if (st, p) in memo:
            return memo[(st, p)]
        ctx, m = log[st]
        if m["kind"] in ("insert", "reg") and not numeric(m["vals"][p]):
            memo[(st, p)] = st
            return st
        al = _alive_at(log, ctx, ins, dels)
        cands = [w for w in ctx if log[w][1]["kind"] in ("insert", "reg") and p in log[w][1]["paths"]
                 and log[w][1]["life"] in al and not _supp(log, log[w][1], p, al)]
        g = None
        if cands:
            g = gen(max(cands), p)
        memo[(st, p)] = g
        return g
    return gen


def oracle(log):
    """Returns (visible-doc or None, alive lives, deletes). The value of a path, in words: the winning base (the
    highest-stamped register write in an alive life), plus every increment that the winner's writer had not
    seen, except the increments the winner's generation overwrote (an increment made in an OLDER generation than
    the winner's, and not seen by the starter of the winner's generation). Increments made in a NEWER
    generation are kept even when its starter lost."""
    ins = {st: m for st, (ctx, m) in log.items() if m["kind"] == "insert" or m.get("insert")}
    dels = [st for st, (ctx, m) in log.items() if m["kind"] == "delete"]
    alive = _alive_lives(log, ins, dels)
    gen = make_gen(log)

    def writes_on(p, kinds):
        return [(st, ctx, m) for st, (ctx, m) in log.items()
                if m["kind"] in kinds and p in m.get("paths", []) and m.get("life") is not None]

    paths = {p for st, (ctx, m) in log.items() for p in m.get("paths", [])}
    doc = {}
    for p in sorted(paths):
        regs = [(st, ctx, m) for st, ctx, m in writes_on(p, ("insert", "reg"))
                if m["life"] in alive and not _supp(log, m, p, alive)]
        incs = [(st, ctx, m) for st, ctx, m in writes_on(p, ("inc",)) if m["life"] in alive]
        win = max(regs, key=lambda r: r[0]) if regs else None
        if win is not None and not numeric(win[2]["vals"][p]):
            doc[p] = win[2]["vals"][p]
            continue
        gW = gen(win[0], p) if win else None
        counted = [(st, ctx, m) for st, ctx, m in incs
                   if gen(st, p) == gW or (gen(st, p) is not None and (gW is None or gen(st, p) > gW))]
        base = win[2]["vals"][p] if win else None
        wctx = win[1] if win else frozenset()
        extra = [m["n"] for st, ctx, m in counted if st not in wctx]
        if base is None and not extra:
            continue
        doc[p] = max(min((base or 0) + sum(extra), BOUND), -BOUND - 1)
    return (doc if alive else None), alive, dels


def loss_oracle(log, alive, dels):
    """Heads that must be reported, as (path, stamp): a head needs a record of ANY kind (V3m2)."""
    must = set()
    gen = make_gen(log)
    sup = superseded_lives(log) if SUPERSEDE else set()
    killed = killed_lives(log)
    for L in sup - killed:
        must.add(("_", L))
    for st, (ctx, m) in log.items():      # V6M1: every head written into a losing first insert is reported
        if m.get("life") in sup - killed and m["kind"] in ("reg", "inc") and st != m.get("life"):
            for p in m.get("paths", []):
                later = [w for w, (c2, m2) in log.items() if st in c2 and p in m2.get("paths", [])
                         and m2.get("life") == m["life"]]
                if not later:
                    must.add((p, st))
    for st, (ctx, m) in log.items():      # V6m2: the dropped $setOnInsert cells are reported
        for p in m.get("soi", []):
            if m.get("life") in alive and _supp(log, m, p, alive):
                later = [w for w, (c2, m2) in log.items() if st in c2 and p in m2.get("paths", [])
                         and m2.get("life") == m["life"] and m2["kind"] in ("reg", "insert")]
                if not later:      # a cell overwritten in sequence by its own life's writer is no longer a head
                    must.add((p, st))
    paths = {p for st, (ctx, m) in log.items() for p in m.get("paths", [])}
    for p in paths:
        regs = [(st, ctx, m) for st, (ctx, m) in log.items()
                if m["kind"] in ("insert", "reg") and p in m["paths"] and m["life"] in alive
                and not _supp(log, m, p, alive)]
        heads = [r for r in regs if not any(r[0] in r2[1] for r2 in regs)]
        if heads:
            top = max(heads, key=lambda r: r[0])
            for h in heads:
                if h is not top:
                    must.add((p, h[0]))
        dead_regs = [(st, ctx, m) for st, (ctx, m) in log.items()
                     if m["kind"] in ("insert", "reg") and p in m["paths"] and m["life"] not in alive and m["life"] not in (sup - killed)
                     and not (m["life"] in sup and st == m["life"])]    # a superseded life's own fields: order-dependent
        for st, ctx, m in dead_regs:
            if not any(st in log[d][0] for d in dels) and not any(st in c2 for s2, c2, m2 in dead_regs):
                must.add((p, st))
        dead_incs = [(st, ctx, m) for st, (ctx, m) in log.items()
                     if m["kind"] == "inc" and p in m["paths"] and m["life"] not in alive and m["life"] not in (sup - killed)]
        by_slot = {}
        for st, ctx, m in dead_incs:
            by_slot.setdefault((m["life"], st[1], gen(st, p)), []).append(st)
        for sts in by_slot.values():
            head = max(sts)
            if not any(head in log[d][0] for d in dels):
                must.add((p, head))
    return must


def hidden_oracle(log):
    """V4M1: from D5's words. For each path with a winning numeric or non-numeric base: the whole total of every
    (life, writer, generation) group of increments in an OLDER generation than the winner's, whose latest
    increment is not in the causal past of the starter of the winner's generation."""
    ins = {st: m for st, (ctx, m) in log.items() if m["kind"] == "insert" or m.get("insert")}
    dels = [st for st, (ctx, m) in log.items() if m["kind"] == "delete"]
    alive = _alive_lives(log, ins, dels)
    gen = make_gen(log)
    out = {}
    paths = {p for st, (ctx, m) in log.items() for p in m.get("paths", [])}
    for p in sorted(paths):
        regs = [(st, m) for st, (ctx, m) in log.items()
                if m["kind"] in ("insert", "reg") and p in m["paths"] and m["life"] in alive
                and not _supp(log, m, p, alive)]
        if not regs:
            continue
        gW = gen(max(regs)[0], p)
        if gW is None:
            continue
        groups = {}
        for st, (ctx, m) in log.items():
            if m["kind"] == "inc" and p in m["paths"] and m["life"] in alive:
                groups.setdefault((m["life"], st[1], gen(st, p)), []).append(st)
        total = 0
        for (L, w, gi), sts in groups.items():
            if gi is None or gi < gW:
                total += sum(log[s_][1]["n"] for s_ in sts if s_ not in log[gW][0])
        if total:
            out[p] = total
    return out


def stale_records(records, log):
    """Section 17.3, second half: no record may name an increment that survives (is counted in the value)."""
    ins = {st: m for st, (ctx, m) in log.items() if m["kind"] == "insert" or m.get("insert")}
    dels = [st for st, (ctx, m) in log.items() if m["kind"] == "delete"]
    alive = _alive_lives(log, ins, dels)
    gen = make_gen(log)
    bad = []
    for kind, p, st in records:
        ctx, m = log[st]
        if m["kind"] != "inc" or m.get("life") not in alive:
            continue
        regs = [(w, m2) for w, (c2, m2) in log.items()
                if m2["kind"] in ("insert", "reg") and p in m2["paths"] and m2["life"] in alive
                and not _supp(log, m2, p, alive)]
        if not regs:
            bad.append((kind, p, st)); continue
        w = max(regs)[0]
        if not numeric(log[w][1]["vals"][p]):
            continue
        gW, gi = gen(w, p), gen(st, p)
        if gi == gW or (gi is not None and (gW is None or gi > gW)):
            bad.append((kind, p, st))
    return bad


def killed_superseded_any(log):
    """V7m4: for each first insert that is superseded AND killed by a delete that neither saw it nor was seen by it,
    the sets of keys of which at least one must be reported: insertLost for the whole insert, or deleteWon of one of
    its fields."""
    dels = [st for st, (ctx, m) in log.items() if m["kind"] == "delete"]
    out = []
    for L in superseded_lives(log):
        ctx, m = log[L]
        if (any(d not in ctx and d[0] >= L[0] - RET for d in dels)
                and not any(L in log[d][0] for d in dels)):      # killed, and no delete observed it
            out.append({("_", L)} | {(p, L) for p in m.get("paths", [])})
    return out


def _killers(log, life, dels):
    """Deletes that killed `life`: not seen by its creator and inside its retention."""
    return [d for d in dels if d not in log[life][0] and d[0] >= life[0] - RET]


def sound_record(key, log, alive, dels):
    """A record is sound only if the write it names wrote its path (`_` only for the whole lost insert), and the
    kind's own evidence exists: a newer concurrent register write (clash), a superseded first insert or a
    $setOnInsert cell (insertLost), a delete that killed the write's life and had not seen the write (deleteWon)."""
    kind, p, st = key
    if st not in log:
        return False
    ctx, m = log[st]
    if kind == "clash":
        return p in m.get("paths", []) and any(
            w != st and w > st and st not in c2 and w not in ctx and p in m2.get("paths", [])
            and m2["kind"] in ("insert", "reg") and m["kind"] in ("insert", "reg")
            for w, (c2, m2) in log.items() if w != st)
    if kind == "insertLost":
        if p == "_":
            return st in superseded_lives(log)
        return p in m.get("paths", []) and (m.get("life") in superseded_lives(log)
                                            or (m["kind"] == "insert" and p in m.get("soi", [])))
    if kind == "deleteWon":
        life = m.get("life")
        return (p in m.get("paths", []) and life is not None and life in log and life not in alive
                and any(st not in log[d][0] for d in _killers(log, life, dels)))
    return False


# ---------------------------------------------------------------------------
NODES = ["A", "B", "C", "D"]


def delivery_step(ms, rnd):
    r, s = rnd.sample(ms, 2)
    x = rnd.random()
    if x < 0.12:
        SNAPS[0] += 1
        r.receive_snapshot(s.state.copy())
    elif s.oplog:
        r.receive_entry(rnd.choice(list(s.oplog.values())))


def repair(ms):
    """Anti-entropy repair (B3 divergence finding -> snapshot): state-only transfers, then entries again."""
    for r in ms:
        for o in ms:
            if r is not o:
                r.receive_snapshot(o.state.copy())
    sync(ms)


def sync(ms):
    for _ in range(12):
        before = [m.state.canon() for m in ms]
        for r in ms:
            for s in ms:
                if r is not s:
                    for ent in list(s.oplog.values()):
                        r.receive_entry(ent)
        if [m.state.canon() for m in ms] == before:
            return


def run(seed, steps=18, ret=None, restore=True, sequential=False):
    global RET
    rnd = random.Random(seed)
    SNAPS[0] = 0
    RET = ret if ret is not None else 10 ** 9
    ms = [Member(n) for n in NODES[:rnd.choice([3, 4])]]
    log = {}
    ms[0].insert({"x": 100, "n": "a"}, log)
    for m in ms:
        m.saved.append((m.state.copy(), dict(m.oplog)))
    did_restore = False
    for i in range(steps):
        if sequential:
            m = rnd.choice(ms)
            sync(ms)
            skew = 0
        else:
            m = rnd.choice(ms)
            skew = rnd.choice([0, 0, 0, 4])
            if rnd.random() < 0.45:
                delivery_step(ms, rnd)
                continue
        if restore and not sequential and not did_restore and i > steps // 2 and rnd.random() < 0.15:
            tgt = rnd.choice(ms)
            other = rnd.choice([o for o in ms if o is not tgt])
            for ent in list(tgt.oplog.values()):     # its writes must exist somewhere before the restore
                other.receive_entry(ent)
            tgt.restore()
            did_restore = True
            continue
        op = rnd.choice(["inc", "inc", "inc", "set", "set", "unset", "delete", "insert", "replace", "upsert"])
        p = rnd.choice(["x", "n", "y"])
        if op == "insert":
            m.insert({rnd.choice(["x", "y"]): rnd.choice([1, 2])}, log, skew)
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
    sync(ms)
    ms[0].repaired = False
    if len({m.state.canon() for m in ms}) != 1:
        repair(ms)
        ms[0].repaired = True
    ms[0].snap_used = SNAPS[0] > 0
    return ms, log


def check(ms, log, strict_records=True):
    errs = []
    if len({m.state.canon() for m in ms}) != 1:
        return ["diverged"]
    want, alive, dels = oracle(log)
    for m in ms:
        got = m.state.visible()
        if got != want:
            errs.append(("visible", got, want))
            break
    if strict_records:
        must = loss_oracle(log, alive, dels)
        have = set(ms[0].state.losses)
        missing = must - {(k[1], k[2]) for k in have}
        if missing:
            errs.append(("record-missing", sorted(missing, key=repr)[:2]))
        for k in have:
            if not sound_record(k, log, alive, dels):
                errs.append(("record-unsound", k))
                break
        for m in ms:
            if not m.restored and sorted(m.events, key=repr) != sorted(set(m.events), key=repr):
                errs.append(("emitted-twice", m.name))
            if not m.restored and set(m.events) != have:
                errs.append(("not-emitted", m.name, sorted(have - set(m.events), key=repr)[:2]))
    return errs


def main(n=2000):
    bad, kinds = {}, {}
    ex = []
    rep_clean = rep_other = 0
    for seed in range(n):
        ms, log = run(seed)
        if ms[0].repaired:
            if ms[0].snap_used or any(m.restored for m in ms):
                rep_other += 1
            else:
                rep_clean += 1
        errs = check(ms, log)
        for k in ms[0].state.losses:
            kinds[k[0]] = kinds.get(k[0], 0) + 1
        for e in errs:
            key = e[0] if isinstance(e, tuple) else e
            bad[key] = bad.get(key, 0) + 1
            ex.append((seed, e))
    print(f"runs={n} problems={bad or 0} record kinds produced={kinds}")
    print("examples:", ex[:4])
    print(f"needed snapshot repair after entry pulls: {rep_other} runs with a snapshot or a restore, {rep_clean} runs with neither (must be 0)")
    fp = 0
    for seed in range(400):
        ms, log = run(seed, steps=14, sequential=True)
        if ms[0].state.losses:
            fp += 1
    print("sequential runs with any record (false positives):", fp, "of 400")
    gc_bad = 0
    for seed in range(300):
        ms, log = run(seed, ret=4, steps=24)
        for m in ms:
            if not m.restored and len(m.events) != len(set(m.events)):
                gc_bad += 1
    print("tiny-retention runs with a record emitted twice by a member that was not restored:", gc_bad, "of 300 (convergence is not claimed across a partition longer than retention)")


# ---------------------------------------------------------------------------
# Named probes, on the faithful (entries-only) replication
# ---------------------------------------------------------------------------
def ent(m, st):
    return m.oplog[("w", st)]


def fresh_observer(name, seed_entries):
    o = Member(name)
    for e in seed_entries:
        o.receive_entry(e)
    return o


def probe_P1_reinsert_same_field():
    """Delete, re-insert and a concurrent unseen $set of the SAME field, in all 6 delivery orders."""
    global RET
    RET = 10 ** 9
    log = {}
    I, A, B = Member("I"), Member("A"), Member("B")
    i0 = I.insert({"x": 1}, log)
    A.receive_entry(ent(I, i0)); B.receive_entry(ent(I, i0))
    d = A.write("delete", None, None, log)
    r = A.insert({"x": 2}, log)
    B.clock = 5
    z = B.write("set", "x", 3, log)
    outs = set()
    for order in itertools.permutations([ent(A, d), ent(A, r), ent(B, z)]):
        o = fresh_observer("O", [ent(I, i0)])
        for e in order:
            o.receive_entry(e)
        outs.add(repr(o.state.visible()))
    assert outs == {"{'x': 2}"}, outs
    return outs


def probe_P1b_disjoint_fields():
    global RET
    RET = 10 ** 9
    log = {}
    I, A, B = Member("I"), Member("A"), Member("B")
    i0 = I.insert({"x": 1}, log)
    A.receive_entry(ent(I, i0)); B.receive_entry(ent(I, i0))
    d = A.write("delete", None, None, log)
    r = A.insert({"y": 2}, log)
    B.clock = 5
    z = B.write("set", "z", 3, log)
    outs = set()
    for order in itertools.permutations([ent(A, d), ent(A, r), ent(B, z)]):
        o = fresh_observer("O", [ent(I, i0)])
        for e in order:
            o.receive_entry(e)
        outs.add(repr(o.state.visible()))
    assert outs == {"{'y': 2}"}, outs
    return outs


def probe_P2_no_genesis_reuse_after_gc():
    """A new document created after the delete's tombstone was collected must not be killed by a stale member."""
    global RET
    RET = 6
    log = {}
    A, C = Member("A"), Member("C")
    i1 = A.insert({"x": 1}, log)
    C.receive_entry(ent(A, i1))
    d = A.write("delete", None, None, log)
    C.receive_entry(ent(A, d))
    assert C.state.visible() is None
    # A's retention pass: a join with a far-future state of its own prunes the delete and the dead life
    far = A.state.copy()
    far.vv[("A", 0)] = 40
    A._absorb(far)
    assert not A.state.deletes
    A.clock = 40
    n = A.insert({"x": 9}, log)
    assert A.state.visible() == {"x": 9}
    # C still holds the delete (it has not run its retention yet)
    C.receive_entry(ent(A, n))
    out = C.state.visible()
    RET = 10 ** 9
    assert out == {"x": 9}, out
    return out


def probe_P4_delete_vs_inc():
    global RET
    RET = 10 ** 9
    log = {}
    A, B = Member("A"), Member("B")
    i0 = A.insert({"x": 100}, log)
    B.receive_entry(ent(A, i0))
    d = A.write("delete", None, None, log)
    inc = B.write("inc", "x", 20, log)
    res = set()
    for order in itertools.permutations([ent(A, d), ent(B, inc)]):
        o = fresh_observer("O", [ent(A, i0)])
        for e in order:
            o.receive_entry(e)
        res.add((repr(o.state.visible()), tuple(sorted(o.state.losses, key=repr))))
    assert len(res) == 1, res
    (vis, losses), = res
    assert vis == "None" and any(k[0] == "deleteWon" and k[1] == "x" for k in losses), res
    return res


def probe_P5_record_set_is_order_independent():
    """B@2, C@3 written after C saw B@2, A@6 concurrent with both: observers applying only writer entries."""
    global RET
    RET = 10 ** 9
    log = {}
    I, A, B, C = Member("I"), Member("A"), Member("B"), Member("C")
    i0 = I.insert({"x": 0}, log)
    for m in (A, B, C):
        m.receive_entry(ent(I, i0))
    b = B.write("set", "x", "b", log)
    C.receive_entry(ent(B, b))
    c = C.write("set", "x", "c", log)
    A.clock = 5
    a = A.write("set", "x", "a", log)
    canon = set()
    for order in itertools.permutations([ent(B, b), ent(C, c), ent(A, a)]):
        o = fresh_observer("O", [ent(I, i0)])
        for e in order:
            o.receive_entry(e)
        # observers also pull the loss entries that the other observers created: model a full pull
        canon.add(o.state.canon())
    # after pulling every member's loss entries, all observers must hold the same records
    obs = []
    for order in itertools.permutations([ent(B, b), ent(C, c), ent(A, a)]):
        o = fresh_observer("O", [ent(I, i0)])
        for e in order:
            o.receive_entry(e)
        obs.append(o)
    for o in obs:
        for p in obs:
            for e in list(p.oplog.values()):
                o.receive_entry(e)
    final = {o.state.canon() for o in obs}
    assert len(final) == 1, "record sets still differ after pulling loss entries"
    return len(final)


def probe3_concurrent_lives_then_delete():
    global RET
    RET = 10 ** 9
    log = {}
    I, A, B, C = Member("I"), Member("A"), Member("B"), Member("C")
    i0 = I.insert({"q": 0}, log)
    for m in (A, B, C):
        m.receive_entry(ent(I, i0))
    d0 = A.write("delete", None, None, log)
    B.receive_entry(ent(A, d0)); C.receive_entry(ent(A, d0))
    la = A.insert({"a": 10}, log)
    B.clock = 7
    lb = B.insert({"b": 20}, log)
    C.receive_entry(ent(B, lb))
    d1 = C.write("delete", None, None, log)
    vis = set()
    for order in itertools.permutations([ent(A, d0), ent(A, la), ent(B, lb), ent(C, d1)]):
        o = fresh_observer("O", [ent(I, i0)])
        for e in order:
            o.receive_entry(e)
        vis.add(repr(o.state.visible()))
    assert vis == {"None"}, vis
    return vis


def probe_oplogfaithful(max_cases=None):
    """3 writers, one $set x each, every write order, causal past, two clock skews; observers apply only
    writer entries in every order: all observers must hold identical states (records included)."""
    global RET
    RET = 10 ** 9
    cases = bad = 0
    for order in itertools.permutations("ABC"):
        for seen_bits in range(4):               # second writer sees first? third sees first/second?
            for skews in itertools.product([0, 5], repeat=3):
                log = {}
                I = Member("I")
                mem = {n: Member(n) for n in "ABC"}
                i0 = I.insert({"x": 0}, log)
                for m in mem.values():
                    m.receive_entry(ent(I, i0))
                stamps = []
                for idx, n in enumerate(order):
                    m = mem[n]
                    for j in range(idx):
                        if (seen_bits >> j) & 1 and j < idx:
                            m.receive_entry(ent(mem[order[j]], stamps[j]))
                    stamps.append(m.write("set", "x", f"{n}{idx}", log, skews[idx]))
                ents = [ent(mem[n], stamps[i]) for i, n in enumerate(order)]
                canons = set()
                for perm in itertools.permutations(ents):
                    o = fresh_observer("O", [ent(I, i0)])
                    for e in perm:
                        o.receive_entry(e)
                    canons.add(o.state.canon())
                cases += 1
                # every observer applied the same entries; only the loss ENTRIES (not yet pulled) may differ
                # so pull them from each other and require identical results
                obs = []
                for perm in itertools.permutations(ents):
                    o = fresh_observer("O", [ent(I, i0)])
                    for e in perm:
                        o.receive_entry(e)
                    obs.append(o)
                for o in obs:
                    for p in obs:
                        for e in list(p.oplog.values()):
                            o.receive_entry(e)
                if len({o.state.canon() for o in obs}) != 1:
                    bad += 1
    assert bad == 0, (bad, cases)
    return cases


def probe_gc_collected_record_does_not_return():
    global RET
    RET = 6
    log = {}
    I, A, B, C, D = (Member(n) for n in "IABCD")
    i0 = I.insert({"x": 0}, log)
    for m in (A, B, C, D):
        m.receive_entry(ent(I, i0))
    a = A.write("set", "x", "a", log)
    b = B.write("set", "x", "b", log, skew=2)
    D.receive_entry(ent(A, a)); D.receive_entry(ent(B, b))
    dd = D.write("set", "y", 1, log)                 # D's entry carries the loss record in its state
    assert D.state.losses
    C.receive_entry(ent(A, a)); C.receive_entry(ent(B, b))
    assert C.state.losses and len(C.events) == 1
    for n in range(5):                               # time passes: C advances far past retention
        C.write("set", "z", n, log, skew=10)
    far = C.state.copy()
    C._absorb(far)
    assert not C.state.losses, "the record should be collected by the horizon"
    C.receive_entry(ent(D, dd))                      # an old entry carrying the record is resent
    assert len(C.events) == 1 and not C.state.losses, (C.events, C.state.losses)
    RET = 10 ** 9
    return "no re-emission"


def probe_restore_conservation():
    global RET
    RET = 10 ** 9
    for epochs in (True, False):
        log = {}
        A, B, C = Member("A"), Member("B"), Member("C")
        i0 = A.insert({"x": 100}, log)
        for m in (B, C):
            m.receive_entry(ent(A, i0))
        B.saved.append((B.state.copy(), dict(B.oplog)))
        inc = B.write("inc", "x", 30, log)
        for m in (A, C):
            m.receive_entry(ent(B, inc))
        B.restore()
        if not epochs:
            B.epoch = 0                                # identity kept, no new epoch
        B.write("inc", "x", 5, log)
        sync([A, B, C])
        v = A.state.value("x")
        if epochs:
            assert v == 135, v
        else:
            assert v == 105, v                        # the silent loss the epochs prevent
    return "135 with epochs, 105 without"


def probes():
    for fn in (probe_P1_reinsert_same_field, probe_P1b_disjoint_fields, probe_P2_no_genesis_reuse_after_gc,
               probe_P4_delete_vs_inc, probe_P5_record_set_is_order_independent, probe3_concurrent_lives_then_delete,
               probe_oplogfaithful, probe_gc_collected_record_does_not_return, probe_restore_conservation):
        print("PASS" if fn() is not None else "FAIL", fn.__name__)



if __name__ == "__main__":
    # `probes()` is not run: its P2 case predates the normalised writer of pullmodel5.py (a delete is
    # not folded into the dead set until a join), and the Rust unit tests carry the probes.
    main(int(sys.argv[1]) if len(sys.argv) > 1 else 2000)
