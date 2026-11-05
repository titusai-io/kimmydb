"""q6diff_gen.py: the tracer of the Q6 swap harness' differential. It wraps the vendored, unmodified `swapmodel6a.py` (the model of
record, sha256 40a44c88...) and dumps JSONL traces that the Rust harness (`tests/swap_sim/trace.rs`) replays event by event,
comparing every member's state after every event, the entries each event mints, the counters before `finish`, and the verdict of
`finish`. Python decides: a disagreement is a finding about the port (or the model), never an exclusion.

Run (from any directory; no bytecode is written):
    python3 -I -B q6diff_gen.py scripted OUTDIR                       every scripted scenario, on the base and its mutants
    python3 -I -B q6diff_gen.py random OUTDIR --mode v6 --count 100   random runs of the model (grid of 36 configurations, seed = index)
        [--seed0 N] [--delete-bias] [--heavy] [--f 0.3 --routes hold|repair|hold,repair]   (the model's route probes, Q6_F / Q6_ROUTES)
    python3 -I -B q6diff_gen.py seed OUTDIR --mode M --seed S --members 3 --outs 2 --keys 4     one run

Trace format (one JSON object per line). Header: {"v":1,"model":"swapmodel6a.py","sha256_16":..,"flags":<mode>,"members":
[[name,skew],..],"D":..,"field_safe":<bool: no delete and no concurrent same-key writes, so the field policy can be replayed exactly>}. Then one line per top-level model call: {"k": kind, "step", "clock", ..arguments.., "res": result,
"new": [the entries the call minted], "ms": [the state of the acting member after the call, of every member every 20 calls and at the structural events]}. Kinds: local, ldrop, lcreate, out, bind,
take_from, take, hold (the hold route: take_hold), repair_ev (the repair route: one register event handed over as information), take_quiesce, steps_end (the run's steps are over: the replay computes the D10 state counters there), check_inv (the per-step invariants, compact), set_pages (a script's reorder of a join's pages), snap_begin (with the shuffled page order), land_page, relay_tomb (with the chosen physical), settle,
vanish (a scripted vanish), lost (the quiescence's lost set), pre_finish (the counters before `finish`), final (the verdict).

NOT in the traces, Rust-only: the sender-dies-mid-join scenario and its fresh-pull supersession (the model never abandons a join),
and the mutant `m-stale-src` (a Rust-only name; the model's flag `stale_src_bug` exists but the manager ruled it Rust-only).
The model's statistics that the Rust port does not carry are listed in `PY_ONLY_STATS` of the replay and nowhere else."""
import hashlib
import json
import os
import random
import sys

sys.dont_write_bytecode = True
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import swapmodel6a as sm  # noqa: E402

MASK = (1 << 64) - 1
FULL_EVERY = 20


def sj(items):
    """sort structured items by their compact JSON text (the same order as the Rust replay's)"""
    return sorted(items, key=lambda x: json.dumps(x, separators=(",", ":")))


def fnv(ids):
    """FNV-1a over a list of entry ids (each id + 1 is one input), the same function as the Rust replay's."""
    x = 1469598103934665603
    for v in ids:
        x = ((x ^ (v + 1)) * 1099511628211) & MASK
    return x


def stamp_j(s):
    return None if s is None else [s[0], s[1]]


def body_j(b):
    return None if b is None else [":".join(str(x) for x in mk) for mk in b]


def lk_j(k):
    out = [k[0]]
    if len(k) > 1:
        out.append(stamp_j(k[1]))
    if len(k) > 2:
        out.append(k[2])
    return out


def entry_j(e):
    d = dict(idx=e.idx, kind=e.kind, origin=e.origin, hlc=e.hlc, staging=bool(e.staging))
    if e.kind == "loss":
        d.update(lkey=lk_j(e.key), body=body_j(e.body), extra=e.extra, phys="")
    else:
        d.update(phys=e.phys or "", key=e.key if e.kind == "doc" else None, body=body_j(e.body), opk=e.opk)
        d["dl"] = e.dl if e.kind in ("create", "bind") else None
        d["extra"] = e.extra if e.kind == "lcreate" else None
    return d


def member_j(m):
    phys = [[n, dict(kind=p.kind, alive=bool(p.alive), kill=stamp_j(p.kill), kept=bool(p.kept), missing=bool(p.missing),
                     orphan=bool(p.orphan), docs=[[k, stamp_j(v[0]), body_j(v[1])] for k, v in p.docs.items()])]
            for n, p in m.phys.items()]
    j = None
    if m.join is not None:
        jj = m.join
        pl = jj["pl"]
        j = dict(src=jj["src"].name, pages=list(jj["pages"]), granted=bool(jj["granted"]), created=sorted(jj["created"]),
                 events=[e.idx for e in pl["events"]], vec=sj([o, h] for o, h in pl["vec"].items()),
                 held_n=len(pl["held"]), held_h=fnv(sorted(pl["held"])), appended_h=fnv(sorted(pl["appended"])))
    return dict(
        name=m.name, serving=m.serving(), cur=None if m.cur is None else m.cur.idx, vanished=bool(m.vanished), hlc=m.hlc, phys=phys,
        evl=[e.idx for e in m.evl], bmap=sj([n, stamp_j(s)] for n, s in m.bmap.items()), abandoned=sorted(m.abandoned),
        learned=sj([n, s] for n, s in m.learned.items()),
        loss=[[lk_j(k), body_j(b)] for k, b in m.loss.items()], loss_pending=[[lk_j(k), body_j(b)] for k, b in m.loss_pending.items()],
        loss_phys=sj([lk_j(k), p] for k, p in m.loss_phys.items()),
        events_emitted=sj([[k[0], stamp_j(k[1]) if len(k) > 1 else None], n] for k, n in m.events_emitted.items() if n),
        pend_tombs=sj([n, stamp_j(s)] for n, s in m.pend_tombs.items()),
        held_n=len(m.held), held_h=fnv(sorted(m.held)), appended_n=len(m.appended), appended_h=fnv(sorted(m.appended)),
        dochist_n=len(m.dochist), dochist_h=fnv(list(m.dochist)), hist_kept_h=fnv(sorted(m.hist_kept)),
        held_oop=sorted(m.held_oop),
        eps=sj([i, ep["start"], ep["last"], ep["n"], ep["end"], bool(ep["end_q"]), bool(ep["info"]), ep["cover"], bool(ep["cover_q"])] for i, ep in m.refusal_eps.items()),
        recv_entry_h=fnv(sorted(m.recv_entry)), deferred=[e.idx for e in m.deferred], info=sorted(m.info),
        held_records=sj([p, k, stamp_j(s)] for (p, k, s) in m.held_records),
        bare_state=sj([p, k, stamp_j(s)] for (p, k, s) in m.bare_state),
        state_held=sj([o, h] for o, h in m.state_held.items()), join=j, inv=sorted(m.inv), refused=m.refused, flips=m.flips,
        last_served=m.last_served)


class RecRng:
    """a proxy of the world's random generator that records the last shuffle and the last choice (the replay is driven by them)"""

    def __init__(self, r):
        self._r = r
        self.last_shuffle = None
        self.last_choice = None

    def shuffle(self, x):
        self._r.shuffle(x)
        self.last_shuffle = list(x)

    def choice(self, seq):
        v = self._r.choice(seq)
        self.last_choice = v
        return v

    def __getattr__(self, n):
        return getattr(self._r, n)


class Tracer:
    def __init__(self):
        self.out = None
        self.depth = 0
        self.w = None
        self.jobs = []
        self.started = False
        self.header = None
        self.field_safe = False
        self.steps_marked = False
        self.n = 0

    def open(self, path):
        self.out = open(path, "w")
        self.jobs = []
        self.started = False
        self.depth = 0
        self.steps_marked = False
        self.n = 0

    def write(self, obj):
        self.out.write(json.dumps(obj, separators=(",", ":")) + "\n")

    def log(self, kind, args, res, n0, w, extra=None):
        if getattr(w, "quiescing", False) and not self.steps_marked:
            # the run's steps are over (a random run): the replay computes the D10 state counters of the steps' end here
            self.steps_marked = True
            self.write(dict(k="steps_end", step=w.steps_end - 1, clock=w.clock, steps_end=w.steps_end))
        d = dict(k=kind, step=w.step, clock=w.clock)
        d.update(args)
        d["res"] = res
        d["new"] = [entry_j(e) for e in w.all[n0:]]
        if extra:
            d.update(extra)
        # the acting member after every call; every member every FULL_EVERY calls and at the structural events (an event changes
        # at most its acting member, so the others are checked unchanged at the next full snapshot)
        self.n += 1
        acting = args.get("m") or args.get("dst")
        if acting is None or self.n % FULL_EVERY == 0 or kind in ("lost", "pre_finish", "final", "vanish"):
            d["ms"] = [member_j(m) for m in w.members.values()]
        else:
            d["ms"] = [member_j(w.members[acting])]
        self.write(d)

    def close(self):
        self.out.close()
        self.out = None


T = Tracer()

_orig_world_init = sm.World.__init__


def _world_init(self, members, flags, rng, D=20, trace=False):
    rec = RecRng(rng)
    _orig_world_init(self, members, flags, rec, D, trace)
    T.w = self
    if T.out is not None:
        T.write(dict(v=1, model="swapmodel6a.py", sha256_16=hashlib.sha256(open(sm.__file__, "rb").read()).hexdigest()[:16],
                     members=[[n, s] for n, s in members], D=D, flags=T.header, field_safe=T.field_safe,
                     route_f=float(os.environ.get("Q6_F", "0") or 0)))
    T.started = True


sm.World.__init__ = _world_init


def _lost_get(self):
    return self.__dict__.get("_lost", set())


def _lost_set(self, v):
    self.__dict__["_lost"] = v
    if T.started and T.out is not None and T.depth == 0:
        T.log("lost", dict(lost=sorted(v)), None, len(self.all), self)


sm.World.lost = property(_lost_get, _lost_set)


def _wrap(name, kind, argf, resf=lambda r: r, extraf=None):
    orig = getattr(sm.Member, name)

    def f(self, *a, **k):
        if T.depth > 0 or T.out is None:
            return orig(self, *a, **k)
        w = self.w
        n0 = len(w.all)
        args = argf(self, *a, **k)
        if isinstance(w.rng, RecRng):
            w.rng.last_shuffle = None
            w.rng.last_choice = None
        T.depth += 1
        try:
            r = orig(self, *a, **k)
        finally:
            T.depth -= 1
        T.log(kind, args, resf(r), n0, w, extraf(self, r) if extraf else None)
        return r
    setattr(sm.Member, name, f)


_wrap("local_doc", "local", lambda s, key, op: dict(m=s.name, key=key, op=op), lambda r: None if r is None else r.idx)
_wrap("local_ldrop", "ldrop", lambda s: dict(m=s.name), lambda r: None if r is None else r.idx)
_wrap("local_lcreate", "lcreate", lambda s: dict(m=s.name), lambda r: None if r is None else r.idx)


def _out_res(r):
    T.jobs.append(r)
    return dict(job=len(T.jobs) - 1, stage=r.stage)


_wrap("start_out", "out", lambda s, keys, kind, bind_step: dict(m=s.name, keys=list(keys), kind=kind, bind_step=bind_step), _out_res)
_wrap("do_bind", "bind", lambda s, job: dict(m=s.name, job=T.jobs.index(job)), lambda r: None if r is None else r.idx)
_wrap("take_from", "take_from", lambda s, src, e, quiesce=False: dict(dst=s.name, src=src.name, idx=e.idx, quiesce=bool(quiesce)))
_wrap("take", "take", lambda s, e: dict(dst=s.name, idx=e.idx))
_wrap("take_hold", "hold", lambda s, e: dict(dst=s.name, idx=e.idx))  # the hold route (Q6_F > 0)
_wrap("add_event", "repair_ev", lambda s, e: dict(dst=s.name, idx=e.idx))  # the repair route: a register event handed over as information
_wrap("take_quiesce", "take_quiesce", lambda s, e: dict(dst=s.name, idx=e.idx))
_wrap("snapshot_begin", "snap_begin", lambda s, src, lazy: dict(dst=s.name, src=src.name, lazy=bool(lazy)), lambda r: bool(r),
      lambda s, r: dict(pages=list(s.w.rng.last_shuffle) if r and s.w.rng.last_shuffle is not None else None))
_wrap("land_page", "land_page", lambda s: dict(m=s.name), lambda r: bool(r))
_wrap("relay_tomb", "relay_tomb", lambda s, src: dict(dst=s.name, src=src.name), lambda r: bool(r),
      lambda s, r: dict(phys=s.w.rng.last_choice))
_wrap("settle", "settle", lambda s: dict(m=s.name))

_orig_check_inv = sm.Member.check_inv


def _check_inv(self):
    _orig_check_inv(self)
    if T.out is not None and T.depth == 0:
        T.write(dict(k="check_inv", m=self.name, inv=sorted(self.inv)))  # a pure function of the state: logged compactly, no member view


sm.Member.check_inv = _check_inv

_orig_finish = sm.finish


def _finish(w, names):
    if T.out is None or T.depth > 0:
        return _orig_finish(w, names)
    st = {k: v for k, v in w.stats.items() if v}
    T.log("pre_finish", dict(stats=sj([k, v] for k, v in st.items())), None, len(w.all), w)
    T.depth += 1
    try:
        res, w2 = _orig_finish(w, names)
    finally:
        T.depth -= 1
    keep = {k: v for k, v in res.items() if not k.startswith("_") and (k.startswith("inv_") or k in (
        "div_state", "div_loss", "div_replay_stamp_order", "oracle_state", "served_mismatch", "alive_mismatch", "loss_missing", "loss_extra",
        "loss_body_mismatch", "loss_mismatch", "heads_required", "true_late_writes_allowed", "nonheads_unreported",
        "nonheads_unreported_vanished_origin", "nonheads_reported", "vr_replicated_late_writes", "vr_unreplicated_late_writes",
        "loss_missing_f4_class", "loss_missing_unexplained", "invariant"))}
    T.log("final", dict(verdict=sj([k, v] for k, v in keep.items())), None, len(w.all), w)
    return res, w2


sm.finish = _finish


# ----------------------------------------------------------------------------- scenario twins (the Rust `scenarios.rs` and `main.rs` tests)


class Sc:
    def __init__(self, mode, members, D=60):
        T.header = mode
        self.mode = mode
        self.w = sm.World(members, sm.flags_for(mode, "v6"), random.Random(0), D=D)
        self.names = [n for n, _ in members]
        self.m = self.w.members

    def adv(self, t):
        self.w.clock = t
        self.w.step += 1

    def pull(self, dst, src, only=None):
        d, s = self.m[dst], self.m[src]
        while True:
            cand = [e for e in self.w.all if e.idx in s.appended and e.idx not in d.held and (only is None or e.origin in only)]
            if not cand:
                return
            n = len(d.held)
            d.take_from(s, min(cand, key=lambda x: x.stamp))
            if len(d.held) == n:
                return

    def set_pages(self, name, order):
        """the script reorders the pages of a join in progress (a direct edit of the join); logged so the replay does the same"""
        self.m[name].join["pages"] = list(order)
        T.log("set_pages", dict(m=name, pages=list(order)), None, len(self.w.all), self.w)

    def vanish(self, *names):
        for n in names:
            self.m[n].vanished = True
            self.w.stats["writer_vanished"] += 1
            T.log("vanish", dict(m=n), None, len(self.w.all), self.w)

    def quiesce(self):
        w = self.w
        live = [m for m in w.members.values() if not m.vanished]
        for m in live:
            while m.join is not None:
                m.land_page()
        w.lost = {e.idx for e in w.all if not any(e.idx in m.held for m in live)}
        for _ in range(4):
            for m in live:
                while m.join is not None:
                    m.land_page()
            for m in live:
                for e in sorted((x for x in w.all if x.idx not in w.lost), key=lambda x: x.stamp):
                    m.take_quiesce(e)
                m.settle()
        return live

    def finish(self):
        return sm.finish(self.w, self.names)


def sc_flip(mode):
    s = Sc(mode, [("W", 0), ("X", 0), ("Y", 0), ("O", 50)])
    W, X, Y, O = (s.m[n] for n in "WXYO")
    s.adv(1)
    W.local_doc(0, "insert")
    for n in "XYO":
        s.pull(n, "W")
    s.adv(10)
    job = W.start_out([1], "good", 0)
    s.adv(12)
    W.do_bind(job)
    s.adv(14)
    O.local_doc(0, "update")
    s.quiesce()
    s.finish()


def sc_corner(mode, second_vanish=True):
    s = Sc(mode, [("A", 0), ("B", 0), ("C", 50)])
    A, B, C = (s.m[n] for n in "ABC")
    s.adv(1)
    A.local_doc(0, "insert")
    s.pull("B", "A")
    s.pull("C", "A")
    s.adv(10)
    job = B.start_out([1], "good", 0)
    s.adv(12)
    B.do_bind(job)
    s.adv(14)
    late = C.local_doc(0, "update")
    B.take_from(C, late)
    s.pull("A", "B")
    jc = C.start_out([2], "vanish", 0)
    C.do_bind(jc)
    if second_vanish:
        jb = B.start_out([3], "vanish", 0)
        B.do_bind(jb)
    s.quiesce()
    s.finish()


def sc_seed885(mode):
    s = Sc(mode, [("A", 0), ("B", 0), ("C", 0), ("D", 0)])
    A, B, C, D = (s.m[n] for n in "ABCD")
    s.adv(1)
    seed = A.local_doc(6, "insert")
    for m in (B, C, D):
        m.take_from(A, seed)
    s.adv(10)
    jobA = A.start_out([0], "good", 12)
    s.adv(12)
    A.do_bind(jobA)
    s.adv(20)
    C.local_doc(6, "update")
    s.adv(30)
    jobD = D.start_out([1], "good", 31)
    s.adv(31)
    D.do_bind(jobD)
    s.adv(32)
    D.snapshot_begin(C, lazy=False)
    s.adv(40)
    s.vanish("C")
    s.adv(41)
    A.snapshot_begin(D, lazy=False)
    s.adv(42)
    B.snapshot_begin(D, lazy=False)
    for _ in range(3):
        s.pull("D", "A")
        s.pull("B", "A")
        s.pull("A", "B")
    s.quiesce()
    s.finish()


def sc_sole_carrier(mode):
    s = Sc(mode, [(n, 0) for n in "ABCDE"])
    A, B, C, D, E = (s.m[n] for n in "ABCDE")
    s.adv(1)
    seed = A.local_doc(6, "insert")
    for m in (B, C, D):
        m.take_from(A, seed)
    s.adv(10)
    jobA = A.start_out([0], "good", 12)
    s.adv(12)
    A.do_bind(jobA)
    s.adv(14)
    s.pull("B", "A")
    s.pull("D", "A")
    s.adv(20)
    late = C.local_doc(6, "update")
    s.adv(22)
    B.take_from(C, late)
    s.adv(25)
    C.start_out([1], "good", 99)
    s.adv(30)
    s.pull("B", "C")
    s.adv(31)
    s.pull("D", "B")
    s.adv(40)
    s.vanish("B", "C")
    s.adv(41)
    E.snapshot_begin(D, lazy=False)
    s.quiesce()
    s.finish()


def sc_late_delete(mode):
    s = Sc(mode, [("O", 50), ("S", 0), ("W", 0), ("Y", 60), ("N", 0)])
    O, S, W, Y, N = (s.m[n] for n in "OSWYN")
    s.adv(1)
    seed = W.local_doc(0, "insert")
    for m in (O, S, Y, N):
        m.take_from(W, seed)
    s.adv(10)
    jw = W.start_out([1], "good", 0)
    s.adv(12)
    W.do_bind(jw)
    s.adv(14)
    d = O.local_doc(0, "delete")
    S.take_from(O, d)
    N.snapshot_begin(S, lazy=False)
    for dead in (O, S):
        jd = dead.start_out([2], "vanish", 0)
        dead.do_bind(jd)
    s.adv(20)
    g = Y.local_ldrop()
    N.take_from(Y, g)
    s.pull("N", "W")
    s.quiesce()
    s.finish()


def sc_append_in_position(mode):
    s = Sc(mode, [("W", 0), ("W2", 0), ("S", 0), ("C", 0), ("A", 0)])
    W, W2, S, C, A = (s.m[n] for n in ("W", "W2", "S", "C", "A"))
    s.adv(1)
    seed = W.local_doc(5, "insert")
    for n in ("W2", "S", "C", "A"):
        s.pull(n, "W")
    s.adv(10)
    jw = W.start_out([0, 1], "good", 0)
    s.adv(12)
    W.do_bind(jw)
    s.adv(13)
    W2.start_out([2], "good", 0)
    s.pull("S", "W")
    s.pull("S", "W2")
    s.adv(30)
    S.local_doc(0, "update")
    C.snapshot_begin(S, lazy=True)
    s.set_pages("C", [n for n in C.join["pages"] if n != jw.stage] + [jw.stage])
    C.land_page()
    s.pull("C", "W", only={"W"})
    A.take_from(W, seed)
    for e in sorted((x for x in s.w.all if x.origin == "W" and x.idx not in A.held), key=lambda x: x.stamp):
        if e.idx in C.appended:
            A.take_from(C, e)
    s.quiesce()
    s.finish()


def sc_join_other_peer(mode):
    s = Sc(mode, [("W", 0), ("X", 0), ("Y", 0), ("N", 0)])
    W, X, Y, N = (s.m[n] for n in "WXYN")
    s.adv(1)
    seed = W.local_doc(0, "insert")
    for m in (X, Y):
        m.take_from(W, seed)
    s.adv(10)
    job = W.start_out([1], "good", 0)
    s.adv(12)
    W.do_bind(job)
    s.pull("X", "W")
    N.snapshot_begin(X, lazy=True)
    N.snapshot_begin(Y, lazy=True)  # refused: a join in progress is resumed only from its own sending peer
    while N.join is not None:
        N.land_page()
    s.quiesce()
    s.finish()


def sc_rule_b(mode):
    s = Sc(mode, [("A", 0), ("B", 0), ("C", 0)])
    A, B, C = (s.m[n] for n in "ABC")
    s.adv(1)
    seed = A.local_doc(0, "insert")
    for m in (B, C):
        m.take_from(A, seed)
    s.adv(10)
    job = B.start_out([1, 2], "good", 0)
    s.adv(12)
    bind = B.do_bind(job)
    create = [e for e in s.w.all if e.kind == "create" and e.phys == job.stage][0]
    A.take_from(B, create)
    A.take(bind)  # route 3: held out of position, before the rows
    s.adv(14)
    A.local_doc(0, "update")  # refused (D10)
    s.adv(30)
    A.local_ldrop()
    s.pull("A", "B")
    s.pull("C", "A")
    s.quiesce()
    s.finish()


# (name, script, modes, field_safe): `field_safe` marks a scenario with no delete and no concurrent writes to one key, on which the
# model's last-writer-wins and the `field` join agree, so the Rust replay under the `field` policy compares it exactly (design
# note q6-phaseF-design.md, section 3). `late-delete` has a delete: it is replayed under `document` only.
SCRIPTED = [
    ("flip", sc_flip, ["v6"], True),
    ("corner-two-vanish", lambda m: sc_corner(m, True), ["v6"], True),
    ("corner-one-vanish", lambda m: sc_corner(m, False), ["v6"], True),
    ("seed885", sc_seed885, ["v6", "m-dead-page-dropped"], True),
    ("sole-carrier", sc_sole_carrier, ["v6", "m-snap-drop-loss", "m-snap-no-loss-records"], True),
    ("late-delete", sc_late_delete, ["v6", "m-snap-delete-bare"], False),
    ("append-in-position", sc_append_in_position, ["v6", "m-no-append-in-position"], True),
    ("join-other-peer", sc_join_other_peer, ["v6"], True),
    ("rule-b", sc_rule_b, ["v6", "m-rule-b-off"], True),
]


def gen_scripted(outdir):
    n = 0
    for name, fn, modes, field_safe in SCRIPTED:
        for mode in modes:
            T.field_safe = field_safe
            T.open(os.path.join(outdir, f"s-{name}-{mode}.jsonl"))
            fn(mode)
            T.close()
            n += 1
    print(f"{n} scripted traces in {outdir}")


# ----------------------------------------------------------------------------- random runs of the model


def grid36():
    return [(m, o, k) for m in (2, 3, 4, 5) for o in (1, 2, 3) for k in (3, 4, 5)]


def one_run(path, mode, seed, nm, no, nk, steps=None, delete_bias=False, route_f=0.0, routes="hold,repair"):
    T.header = mode
    T.field_safe = False
    if route_f:
        os.environ["Q6_F"], os.environ["Q6_ROUTES"] = str(route_f), routes
    else:
        os.environ.pop("Q6_F", None)
        os.environ.pop("Q6_ROUTES", None)
    if delete_bias:
        os.environ["Q6_DELETE_BIAS"] = "1"
    else:
        os.environ.pop("Q6_DELETE_BIAS", None)
    T.open(path)
    sm.run(mode, seed, nmembers=nm, nkeys=nk, outs=no, base="v6", steps=steps)
    T.close()


def gen_random(outdir, mode, count, seed0, delete_bias, heavy, route_f=0.0, routes="hold,repair"):
    g = grid36()
    for i in range(count):
        seed = seed0 + i
        nm, no, nk = (5, 3, 5) if heavy else g[seed % 36]
        tag = f"-F{route_f}-{routes.replace(',', '+')}" if route_f else ""
        one_run(os.path.join(outdir, f"r-{mode}{tag}-{seed}.jsonl"), mode, seed, nm, no, nk, 600 if heavy else None, delete_bias, route_f, routes)
    print(f"{count} random traces of mode {mode} in {outdir}")


def main(argv):
    a = argv[1:]
    if len(a) < 2:
        raise SystemExit(__doc__)
    cmd, outdir = a[0], a[1]
    os.makedirs(outdir, exist_ok=True)
    opts = {}
    i = 2
    while i < len(a):
        if a[i] in ("--delete-bias", "--heavy"):
            opts[a[i][2:]] = True
            i += 1
        else:
            opts[a[i][2:]] = a[i + 1]
            i += 2
    if cmd == "scripted":
        gen_scripted(outdir)
    elif cmd == "random":
        gen_random(outdir, opts.get("mode", "v6"), int(opts.get("count", 100)), int(opts.get("seed0", 0)), "delete-bias" in opts, "heavy" in opts,
                   float(opts.get("f", 0) or 0), opts.get("routes", "hold,repair"))
    elif cmd == "seed":
        one_run(os.path.join(outdir, f"r-{opts.get('mode', 'v6')}-{opts['seed']}.jsonl"), opts.get("mode", "v6"), int(opts["seed"]),
                int(opts["members"]), int(opts["outs"]), int(opts["keys"]))
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main(sys.argv)
