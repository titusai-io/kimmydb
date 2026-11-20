"""Q7 step-0 model (nested paths of any depth, option A field order) state and join (replicated by pullmodel6.py, which carries the cluster's coverage protocol).

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
  losses[(kind, path, loser_stamp)] = loser value                  grow-only, filtered by the horizon, and capped: the K greatest by rank stay
A life is alive iff every delete in the state is in its `seen`. Cells are keyed by (life, path); cells of
dead lives are dropped. Visible value of p = newest base among alive lives + sum over counted slots
(gen == newest gen over alive lives) of (total - observed by that base).
Horizon H(state) = max(vv) - RETENTION: a pure function of the state; records with loser hlc < H are
filtered on every join.
"""
import copy, itertools, random, struct, sys

GENESIS = None
HORIZON_FIRST = True      # V3M1
CELLS_REPORTED = True     # V6M1: writes into a superseded first insert are reported
SUPERSEDE = True          # Q2: concurrent first inserts, newest whole document wins
BOUND = 2 ** 63 - 1       # Q11: Int64 bound (tests lower it); the lower bound is -BOUND - 1
RET = 10 ** 9
K = 64                    # the loss records one document keeps: the K greatest by `loss_rank` (Rust: MAX_LOSS_CAP is the default)
SNAPS = [0]
# --- Step 0 (Q7 v6): nested paths of any depth, the map marker, typeClash, arrayClash, field order (option A) -----
# Paths are dotted strings ("a.b.c"); field names never contain "." or start with "$". The Rust port uses segment
# lists (INTERFACE.md); a segment list and a dotted string are interchangeable for these names.
MAP = "\x00map"          # the map marker, stored as a register value at a map path (an explicit map value)
MAPI = "\x00mapi"        # the IMPLICIT map marker: written when a leaf write creates the maps above it (never a loser: see join)
MARKERS = (MAP, MAPI)
# mutants (all False = the design):
MARKER_HIDES = False       # mutant: a map marker hides the children
TYPE_IGNORES_RECENCY = False   # mutant: a scalar register at p hides every child, even newer ones
REWRITE_UNCHANGED = False  # mutant: a parent write is not expanded to leaf effects: every leaf of the new value is rewritten
ARRAYS_ELEMENTWISE = False # mutant: concurrent array registers merge element-wise (max) instead of the newest winning
NO_IMPLICIT_MARKER = False  # mutant: a leaf write under an absent map writes no marker (an emptied map vanishes)
# option A (field order). ORDER_STATE False = the model without order state (visible keys sorted by path, as before).
ORDER_STATE = True
ABSORB_BY_WRITING_LIFE = True   # an absorbed pair belongs to the life the base write was made in (it goes when that life is dropped, and when the life of its slot is); False = mutant: the old keying, by the slot's own life
ABSORB_SINGLE_PAIR = False      # mutant: one pair per slot (the highest stamp), tagged with its writing life: a dominated pair of another life is lost with the winner's life
READ_WITHOUT_OBS = False        # run the families on the Rust's read: the register `obs` is not consulted (only the absorbed pairs), so a layout mistake the obs would hide shows
ABSORBED = True            # Q6: a counter cell remembers the highest (stamp, total) of each slot that any base write observed
COPY_DEEP = False          # State.copy by copy.deepcopy (the model5 way) instead of the structural copy
ORD_GC = False             # collect an Ord with no valid candidate and a floor older than the horizon
ORD_COMPACT = False        # variant: a cell's born stamp is omitted on the wire when it equals the cell's register stamp
ORD_NO_LAST = False        # mutant: the register's own write is not a born candidate
ORD_NO_RESET = False       # mutant: a replace does not reset the candidates (fields keep their old position)
ORD_SURVIVOR_CLAMP = True  # a field that exists only through increments an unset never saw is ordered right after the unset
ORD_NOOP_UNSET_FLOOR = "hybrid"  # F1 (manager's ruling): False = a no-op unset leaves no trace; True = even a STANDALONE no-op unset is a write; 'hybrid' (default) = the floor rides on an update another change already stamps (metadata only, no extra entry)
NULL = "\x00null"            # a stored, present null (`$set x null`): a register value, distinct from an unset
ORD_UPDATE_BIT = True       # the u floor remembers whether its write also wrote other fields (survivors go after all of them)
ORD_REPLACE_VV = False     # Q4(b): a replace's `r` floor carries the replace's version vector; candidates it does not cover survive the reset and go after it
ORD_LEVEL_FLOOR = True     # a document-valued write (replace, $set of a document) orders the fields it never saw AFTER its own
ORD_KEEP_ALL = False       # mutant: candidates are not filtered by the unset floor
ORD_LATEST = False         # mutant: born = the LATEST candidate instead of the earliest
ORD_BY_NAME = False        # mutant: the visible order ignores the order state (keys sorted by path)
ORD_NO_ANCESTOR = False    # mutant: a write below a map is not a write to the map (the ancestors get no candidate)
ORD_NO_POS = False         # mutant: the position inside a write is ignored (every field of a write has pos 0)
ORD_COMPACT_LOSSY = False  # mutant: the compact wire form also drops `last` (not just a stamp equal to the register's)


def nn(v):
    """Python None inside a document value means a stored null (the reference), not an unset."""
    if v is None:
        return NULL
    if isinstance(v, dict):
        return {k: nn(x) for k, x in v.items()}
    return v


def parent_of(p):
    return p.rsplit(".", 1)[0] if "." in p else None


def name_of(p):
    return p.rsplit(".", 1)[-1]


def ancestors(p):
    """Proper ancestors, outermost first: 'a.b.c' -> ['a', 'a.b']."""
    s = p.split(".")
    return [".".join(s[:i]) for i in range(1, len(s))]


def is_under(q, p):
    return q.startswith(p + ".")


def is_val(v):
    """A register value that is a plain value: not an unset, not the marker."""
    return v is not None and not (isinstance(v, str) and v in MARKERS)


def getp(doc, p):
    """Value of a (possibly dotted) path in a nested visible document, or None."""
    d = doc
    for s in p.split("."):
        if not isinstance(d, dict) or s not in d:
            return None
        d = d[s]
    return d


def flatten_doc(doc, prefix=""):
    """Nested doc -> {dotted path: value} in document (pre-)order, with MAP at every map: a nested doc is its leaves
    plus a marker at each map. Arrays (tuples) are atomic leaves."""
    out = {}
    for k, v in doc.items():
        q = prefix + k
        if isinstance(v, dict):
            out[q] = MAP
            out.update(flatten_doc(v, q + "."))
        else:
            out[q] = v
    return out


def ordered(v):
    """The order-carrying form of a visible document: nested lists of (key, value) pairs. dict equality ignores
    order; this does not."""
    if isinstance(v, dict):
        return [(k, ordered(x)) for k, x in v.items()]
    return v


def tyrepr(v):
    """A type-strict repr (so 2 and 2.0 differ), for comparing ordered documents."""
    if isinstance(v, list):
        return "[" + ",".join(f"{k}:{tyrepr(x)}" for k, x in v) + "]"
    return f"{type(v).__name__}:{v!r}"


def cov(vv, stamp):
    return vv.get(stamp[1], 0) >= stamp[0]


class Dec128(str):
    """A2.1 (review m5): stand-in for a stored Decimal128. The string is its representation (the Rust diff compares the 16 bytes, so `1.0` and
    `1.00` are different values); two Dec128 are the same only when the text is identical. A distinct type, so it never equals a plain
    string, an int or a double of the same text or numeric value. The model gives it no arithmetic (it is a non-numeric register value)."""
    __slots__ = ()


def _bits(x):
    return struct.pack("<d", x)


def same(a, b):
    """Q4 (ruled) and the diff revision of 2026-11-03: the diff compares TYPE and then VALUE, as the Rust diff does. Doubles compare by BIT
    PATTERN (`-0.0` over `0.0` is a write; a NaN over the same NaN bit pattern is a no-op, a different NaN payload is a write);
    Decimal128 (`Dec128`) by its representation; everything else by value and type (Int32, Int64 and Double of equal value are different;
    in the model one `int` type stands for the two integer widths). An array (tuple) compares element by element with this rule."""
    ta = type(a)
    if ta is not type(b):
        return False
    if ta is float:
        return _bits(a) == _bits(b)
    if ta is tuple or ta is list:
        return len(a) == len(b) and all(same(x, y) for x, y in zip(a, b))
    if ta is dict:
        return a.keys() == b.keys() and all(same(a[k], b[k]) for k in a)
    return a == b


def ordered_same(a, b):
    """Two visible values with the same field order and `same()` leaves (the no-op check of a replace or an order-only `$set`)."""
    if isinstance(a, dict) and isinstance(b, dict):
        return len(a) == len(b) and all(ka == kb and ordered_same(a[ka], b[kb]) for ka, kb in zip(a, b))
    return same(a, b)


def numeric(v):
    return v is None or (isinstance(v, int) and not isinstance(v, bool))


# --- option A: the field-order state (design: D-item revision of 2026-11-01) -----------------------------------------
# For every cell path (L, p) (and every map path, and every ancestor of a written path):
#   Ord(floor, firsts, last)
#     floor  = (stamp, 'u'|'r') or None. 'u' = u*, the largest UNSET stamp seen for the path; 'r' = a REPLACE (or a set of
#              a whole sub-document) that re-assigned the position of the path at that stamp. Candidates must be above
#              a 'u' floor and at or above an 'r' floor. Grow-only (max).
#     firsts = {writer: (stamp, pos)}: the EARLIEST write by that writer to the path that is valid under `floor`
#              (at most one entry per writer: 64 in the design).
#     last   = (stamp, pos) | None: the latest write to the path (this is the register's own born; it is what the
#              approximate case of the ruling (a) falls back to).
#   born(L, p) = min of the valid candidates (firsts values and last). The visible order of a level is its present
#   children sorted by (min over alive lives of born, name).
class Ord:
    __slots__ = ("floor", "firsts", "last", "lvl")

    def __init__(self, floor=None, firsts=None, last=None, lvl=None):
        self.floor, self.firsts, self.last, self.lvl = floor, dict(firsts or {}), last, lvl

    def copy(self):
        return Ord(self.floor, self.firsts, self.last, self.lvl)

    def valid(self, c):
        if c is None:
            return False
        f = self.floor
        if f is None or ORD_KEEP_ALL or c[0] > f[0] or (c[0] == f[0] and f[1] == "r"):
            return True
        if ORD_REPLACE_VV and f[1] == "r" and len(f) > 2:
            return dict(f[2]).get(c[0][1], 0) < c[0][0]          # concurrent with the replace: it survives the reset
        return False

    def normalise(self):
        self.firsts = {w: c for w, c in self.firsts.items() if self.valid(c)}
        if not self.valid(self.last):
            self.last = None
        return self

    def born(self):
        cs = list(self.firsts.values()) + ([] if ORD_NO_LAST or self.last is None else [self.last])
        cs = [c for c in cs if self.valid(c)]
        if not cs:
            return None
        f = self.floor
        if ORD_REPLACE_VV and f is not None and f[1] == "r" and len(f) > 2:
            cs = [(f[0], 10 ** 9, c) if c[0] < f[0] else c for c in cs]       # survivors of the reset go after the replace's own fields
        return max(cs) if ORD_LATEST else min(cs)

    def key(self):
        return (self.floor, tuple(sorted(self.firsts.items())), self.last) + ((self.lvl,) if self.lvl is not None else ())

    def __repr__(self):
        return f"Ord{self.key()!r}"


def ord_join(a, b):
    """Commutative, idempotent; associative on reachable states (tested, not assumed)."""
    if a is None:
        return b.copy().normalise()
    if b is None:
        return a.copy().normalise()
    floor = max([f for f in (a.floor, b.floor) if f is not None], default=None)
    o = Ord(floor)
    for w in set(a.firsts) | set(b.firsts):
        cs = [c for c in (a.firsts.get(w), b.firsts.get(w)) if o.valid(c)]
        if cs:
            o.firsts[w] = min(cs)
    lasts = [c for c in (a.last, b.last) if c is not None]
    o.last = max(lasts) if lasts else None
    lv = [x for x in (a.lvl, b.lvl) if x is not None]
    o.lvl = max(lv) if lv else None
    return o.normalise()



# --- option A: the compact wire form (a born stamp equal to the cell's register stamp is omitted) ---------------------
WIRE_CHECK = True          # assert expand(compact(s)) == s whenever a state is put on the wire in compact form


def _regstamp(state, k):
    """The stamp of the cell (L, p): its register's, or the newest of its counter slots'."""
    r = state.regs.get(k)
    sl = [v[0] for kk, v in state.slots.items() if kk[0] == k[0] and kk[1] == k[1]]
    cs = ([r[0]] if r else []) + sl
    return max(cs) if cs else None


def wire(state):
    """The state as it goes on the wire. With ORD_COMPACT, a first/last candidate whose stamp equals the cell's own
    register stamp carries only its position (the stamp is the string 'R')."""
    if not ORD_COMPACT:
        return state.copy()
    w = state.copy()
    for k, o in w.ords.items():
        rs = _regstamp(state, k)
        if rs is None:
            continue
        o.firsts = {wr: (("R" if c[0] == rs else c[0]), c[1]) for wr, c in o.firsts.items()}
        if o.last is not None and o.last[0] == rs:
            o.last = ("R", o.last[1])
        if ORD_COMPACT_LOSSY:
            o.last = None
    if WIRE_CHECK:
        back = unwire(w)
        assert back.canon() == state.canon(), "compact order state does not round-trip"
    return w


def unwire(state):
    if not ORD_COMPACT:
        return state
    s = state.copy()
    for k, o in s.ords.items():
        rs = _regstamp(s, k)
        o.firsts = {wr: ((rs if c[0] == "R" else c[0]), c[1]) for wr, c in o.firsts.items()}
        if o.last is not None and o.last[0] == "R":
            o.last = (rs, o.last[1])
    return s


def ord_bytes(state, compact):
    """Design-size estimate of the order state in bytes (A2.1 layout, INTERFACE.md section 4): a stamp is 11 B, a writer index 1 B, a position
    1 B (varint). The floor is ONE presence-and-kind byte for every Ord (0 none, 1 `u`, 2 `r`, 3 `u` with the update bit), then, if present,
    its stamp (11 B), then for a `u` floor `prev` as a 10 B HLC; no vv in an `r` floor. `last` and `lvl` are optional (their presence rides in
    spare bits of the floor byte, 0 B); `lvl` is a stamp (11 B). The count of `firsts` is not counted (as before). Returns (bytes, number of
    Ord entries, number of cells)."""
    total = 0
    for k, o in state.ords.items():
        rs = _regstamp(state, k)
        total += 1                                        # floor presence-and-kind byte (the update bit is part of the kind: 0 B)
        if o.floor:
            total += 11 + (10 if o.floor[1] == 'u' else 0)    # stamp, then prev as a 10 B HLC for `u`
        if o.lvl is not None:
            total += 11
        for wr, c in o.firsts.items():
            total += 1 + (0 if (compact and c[0] == rs) else 11) + 1
        if o.last:
            total += (0 if (compact and o.last[0] == rs) else 11) + 1
    cells = len({(k[0], k[1]) for k in state.regs} | {(k[0], k[1]) for k in state.slots})
    return total, len(state.ords), cells



class State:
    def __init__(self):
        self.lives, self.deletes, self.regs, self.slots = {}, {}, {}, {}
        self.gens, self.vv, self.losses = {}, {}, {}
        self.ords = {}             # option A: (life, path) -> Ord (the field-order state)
        self.absorbed = {}         # Q6: (writing life, slot key (life, path, writer, gen)) -> the highest (slot stamp, total) a base write made in that life observed; dropped with the writing life
        self.dead = set()          # lives known dead; grow-only until their stamp passes the horizon
        self.contained = set()      # ORACLE ONLY

    def copy(self):
        """Structural copy. Every nested value (tuples, frozensets, the dicts inside a register tuple) is treated as
        immutable by the code, which always builds a new one; the equivalence with deepcopy is what flatequiv.py checks
        against model5, which still deep-copies. COPY_DEEP switches back."""
        if COPY_DEEP:
            return copy.deepcopy(self)
        n = State.__new__(State)
        n.lives, n.deletes, n.regs, n.slots = dict(self.lives), dict(self.deletes), dict(self.regs), dict(self.slots)
        n.gens, n.vv, n.losses = dict(self.gens), dict(self.vv), dict(self.losses)
        n.ords = {k: o.copy() for k, o in self.ords.items()}
        n.absorbed = dict(self.absorbed)
        n.dead, n.contained = set(self.dead), set(self.contained)
        return n

    def alive(self):
        return [L for L, v in self.lives.items()
                if L not in self.dead and all(d in v[0] for d in self.deletes if d[0] >= L[0] - RET)]

    def horizon(self):
        return (max(self.vv.values()) if self.vv else 0) - RET

    def canon(self, with_ords=True):
        def _r(v):
            if isinstance(v, frozenset):
                return repr(sorted(v))
            if isinstance(v, tuple):
                return "(" + ",".join(_r(x) for x in v) + ")"
            return repr(v)
        f = lambda d: tuple(sorted(((repr(k), _r(v)) for k, v in d.items())))
        base = (f(self.lives), repr(sorted(self.dead)), f(self.deletes), f(self.regs), f(self.slots),
                tuple(sorted(self.vv.items())), f(self.losses))
        if not with_ords:
            return base
        return base + (tuple(sorted((repr(k), repr(o.key())) for k, o in self.ords.items())), tuple(sorted((repr(k), repr(v)) for k, v in self.absorbed.items())))

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

    def eff_obs(self, obs, k):
        """What has been observed of slot k: the winning base's own observation, or (Q6) the highest observation any base write
        on the path made, whichever is later."""
        o = None if READ_WITHOUT_OBS else obs.get(k[0:1] + k[2:])
        if ABSORBED:
            a, akey = None, None
            for (wl, *sk), v in self.absorbed.items():      # only base writes made in a life that is still alive count (they are dropped with it)
                if tuple(sk) == k and (akey is None or (v[0], wl) > akey):      # the highest stamp; equal stamps (not honest) by writing life, never by dict order
                    a, akey = v, (v[0], wl)
            if a is not None and (o is None or a[0] > o[0]):
                return a
        return o

    def value(self, p):
        regs = [(r, k) for k, r in self.live_regs(p)]
        win = max(regs, key=lambda x: x[0][0])[0] if regs else None
        if win is not None and not numeric(win[1]):
            return win[1]
        base = win[1] if win is not None else None
        obs = win[2] if win is not None else {}
        total, seen = (base if base is not None else 0), base is not None
        for k in sorted(self.counted(p), key=repr):
            o = self.eff_obs(obs, k)
            d = self.slots[k][1] - (o[1] if o else 0)
            if d or o is None or o[0] != self.slots[k][0]:    # B1: the slot moved since the base saw it, even if it nets to 0
                total += d
                seen = True
        return max(min(total, BOUND), -BOUND - 1) if seen else None     # Q11: a merge that crosses either bound saturates

    def newest(self, c, al=None):
        """The newest stamp among the cells (register or slots) of path c in alive lives, or None."""
        al = set(self.alive()) if al is None else al
        st = [r[0] for k, r in self.regs.items() if k[1] == c and k[0] in al and not self.suppressed(k, r, al)]
        st += [v[0] for k, v in self.slots.items() if k[1] == c and k[0] in al]
        return max(st) if st else None

    def _survivors(self, p, al, floor):
        """The candidates that content surviving p's unset (the floor) gives p. An unset of p unsets exactly what its writer
        saw, so what is still there below it was not seen and survives (U2, the partial sub-document of Q3): (1) a counted
        slot of p below the floor that still contributes: candidate = the slot's stamp; (2) a descendant q whose winning
        register holds a value stamped below the floor: candidate = q's own born; or whose counted slot below the floor
        contributes: candidate = the slot's stamp. In the serial order that explains the value the unset is replayed
        before them and AFTER everything its author knew (its causal past): so the candidate is not placed before the
        highest stamp the unsetter had seen (`prev`, carried by the floor)."""
        fstamp = floor[0]
        after_known = (floor[2], ("\U0010ffff", 0)) if len(floor) > 2 else None      # just after hlc `prev`
        out = []

        others = len(floor) > 3 and floor[3] and ORD_UPDATE_BIT

        def place(c):
            if others:       # the unsetting write also wrote other fields: what survives it is replayed after ALL of that write
                return (fstamp, 10 ** 9, c)
            return c if after_known is None or c[0] >= after_known else (after_known, 0)

        def slot_survivors(q):
            win = self.winner(q)
            obs = win[2] if win is not None else {}
            r = []
            for k, v in self.counted(q).items():
                o_ = self.eff_obs(obs, k)
                if v[0] < fstamp and ((v[1] - (o_[1] if o_ else 0)) != 0 or o_ is None or o_[0] != v[0]):
                    r.append(place((v[0], 0)))
            return r
        out += slot_survivors(p)
        win_p = self.winner(p)
        if win_p is not None and is_val(win_p[1]) and win_p[0] < fstamp:
            # only reachable with the no-op-unset variant: a floor with no unset register above p's own value
            out.append(place((win_p[0], 0)))
        for q in sorted({k[1] for k in list(self.regs) + list(self.slots) if k[0] in al and is_under(k[1], p)}):
            win = self.winner(q)
            if win is not None and is_val(win[1]) and win[0] < fstamp:
                bq = self.born_of(q, al)
                out.append(place(bq if bq is not None else (win[0], 0)))
            out += slot_survivors(q)
        return out

    def born_of(self, p, al):
        """Option A: the position key of path p over the alive lives (their Ords joined, so an unset in one life also
        raises the floor of the others: the lives are one document), or None."""
        o = None
        for L in sorted(al):                       # the alive lives are one document: their order states join first
            if (L, p) in self.ords:
                o = ord_join(o, self.ords[(L, p)])
        b = o.born() if o is not None else None
        if ORD_SURVIVOR_CLAMP and o is not None and o.floor is not None and o.floor[1] == "u":
            for c in self._survivors(p, al, o.floor):
                b = c if b is None or c < b else b
        if b is not None and ORD_LEVEL_FLOOR:
            # a field the replacing document never saw (it survives the diff, Q4) is ordered after the fields that
            # document names: as in the serial order 'the replace, then the write that made the field'
            par = parent_of(p) or ""
            lv = None
            for L in al:
                po = self.ords.get((L, par))
                if po is not None and po.lvl is not None:
                    lv = po.lvl if lv is None else max(lv, po.lvl)
            if lv is not None and b[0] < lv:
                return (lv, 10 ** 9, b)
        return b

    def visible(self):
        """The nested visible document, keys of every level in the derived field order (option A) or by path when
        ORDER_STATE is off. TYPE RULE (D4), at every depth: p is a map iff some descendant is visible and newer than
        p's own scalar register, or a map marker is visible at p; otherwise p is its register. A descendant is
        hidden by a newer scalar/array at an ancestor level; a marker never hides children."""
        if not self.alive():
            return None
        al = set(self.alive())
        paths = {k[1] for k, r in self.regs.items() if k[0] in al and not self.suppressed(k, r, al)} | {k[1] for k in self.slots if k[0] in al}
        allp = set(paths)
        for q in paths:
            allp.update(ancestors(q))
        kids, tops = {}, []
        for q in allp:
            par = parent_of(q)
            (tops if par is None else kids.setdefault(par, [])).append(q)

        def okey(q):
            b = self.born_of(q, al) if (ORDER_STATE and not ORD_BY_NAME) else None
            return (0, b, q) if b is not None else (1, (), q)

        def node(q):
            """(value, newest stamp of the visible subtree) or None when q is absent."""
            ks = {}
            for c in sorted(kids.get(q, ()), key=okey):
                r = node(c)
                if r is not None:
                    ks[name_of(c)] = r
            own = self.value(q)
            own_n = self.newest(q, al)
            if not ks:
                if own is None:
                    return None
                return ({} if own in MARKERS else own, own_n)
            if own is not None and own not in MARKERS:                 # a scalar, number or array at q
                reg = self.winner(q)
                rs = reg[0] if (reg is not None and is_val(reg[1])) else own_n
                shown = {} if TYPE_IGNORES_RECENCY else {c: v for c, v in ks.items() if v[1] > rs}
                if shown:
                    return ({c: v[0] for c, v in shown.items()}, max([own_n] + [v[1] for v in shown.values()]))
                return (own, own_n)
            if MARKER_HIDES and own in MARKERS:
                return ({}, own_n)
            ns = [v[1] for v in ks.values()] + ([own_n] if own_n is not None else [])
            return ({c: v[0] for c, v in ks.items()}, max(ns))

        out = {}
        for t in sorted(tops, key=okey):
            r = node(t)
            if r is not None:
                out[name_of(t)] = r[0]
        return out

    def order_report(self):
        """The ordered visible document (nested lists of pairs)."""
        v = self.visible()
        return None if v is None else ordered(v)



def obs_key(L, w, gen):
    return (L,) + (w, gen)


def _single_pair(absorbed):
    """MUTANT (ABSORB_SINGLE_PAIR): one pair per slot, the highest stamp, tagged with its writing life."""
    best = {}
    for k, v in absorbed.items():
        sk = k[1:]
        if sk not in best or (v[0], k) > (absorbed[best[sk]][0], best[sk]):
            best[sk] = k
    return {k: absorbed[k] for k in best.values()}


def _prefilter(st, H):
    st = st.copy()
    for d in [d for d in st.deletes if d[0] < H]:
        del st.deletes[d]
    for L in [L for L in st.dead if L[0] < H]:
        st.dead.discard(L)
        st.lives.pop(L, None)
    return st


_LOSS_KIND_ORDER = {"clash": 0, "deleteWon": 1, "insertLost": 2, "retiredWriter": 3, "typeClash": 4, "arrayClash": 5}


def loss_rank(key):
    """The order the cap keeps the greatest of (Rust `LossKey::rank_cmp`): the loser's stamp, then what the record is
    about (a path compared segment by segment, the whole document `_` first among equals), then the kind in the
    order of the Rust enum (clash, deleteWon, insertLost, retiredWriter, typeClash, arrayClash). The model spells the whole
    document and a field named `_` alike (both the path `_`), where Rust puts the document first of the two; the generators
    never draw a field named `_`, so no script tells them apart."""
    kind, path, stamp = key
    return (stamp, (tuple(path.split(".")), 0 if path == "_" else 1), _LOSS_KIND_ORDER[kind])


def cap_losses(losses):
    """The K greatest records by rank stay (`cap(F_h(S u T))`); the others are dropped, whichever side held them."""
    if len(losses) <= K:
        return losses
    keep = sorted(losses, key=loss_rank)[len(losses) - K:]
    return {k: losses[k] for k in keep}


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
        if ARRAYS_ELEMENTWISE and k in out.regs and isinstance(r[1], tuple) and isinstance(out.regs[k][1], tuple) \
                and r[0] != out.regs[k][0] and not cov(s.vv, r[0]) and not cov(t.vv, out.regs[k][0]):
            a_, b_ = out.regs[k], r             # mutant: concurrent arrays merge element-wise
            top = a_ if a_[0] > b_[0] else b_
            va, vb = a_[1], b_[1]
            mv = tuple(max(va[i], vb[i]) if i < min(len(va), len(vb)) else (va if len(va) > len(vb) else vb)[i]
                       for i in range(max(len(va), len(vb))))
            out.regs[k] = (top[0], mv) + tuple(top[2:])
            continue
        if ok(k[0]) and (k not in out.regs or r[0] > out.regs[k][0]):
            out.regs[k] = copy.deepcopy(r)
    for k, v in t.slots.items():
        if ok(k[0]) and (k not in out.slots or v[0] > out.slots[k][0]):
            out.slots[k] = v
    for k, g in t.gens.items():
        if ok(k[0]) and (k not in out.gens or g[0] > out.gens[k][0]):
            out.gens[k] = copy.deepcopy(g)
    for k, v in t.absorbed.items():
        if ok(k[0]) and (k not in out.absorbed or v[0] > out.absorbed[k][0]):
            out.absorbed[k] = v
    if ABSORB_SINGLE_PAIR:           # the mutant picks the winner BEFORE the alive filter below, so the pair of a surviving life is lost with the winner's life
        out.absorbed = _single_pair(out.absorbed)
    for k, o in t.ords.items():
        if ok(k[0]):
            out.ords[k] = ord_join(out.ords.get(k), o)
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
    # (1) same-path register clashes: a register of s against a register of t, concurrent by the VVs.
    # DELTA (c): a creation cell that an alive explicit insert suppresses (`out.suppressed`, the read side's own predicate)
    # is not a competitor of the insert's cells: it takes part in no clash and no typeClash, and the V6m2 block below
    # reports it, at least once, as an insertLost.
    paths = {k[1] for k in list(s.regs) + list(t.regs)}
    for p in paths:
        rs = [r for k, r in s.regs.items() if k[1] == p and k[0] in al and not out.suppressed(k, r, al)]
        rt = [r for k, r in t.regs.items() if k[1] == p and k[0] in al and not out.suppressed(k, r, al)]
        for a in rs:
            for b in rt:
                if a[0] != b[0] and not cov(t.vv, a[0]) and not cov(s.vv, b[0]):
                    lo = a if a[0] < b[0] else b
                    if lo[1] == MAPI:        # an implicit marker carries no user value: it is never a loser to report
                        continue
                    # S1: an explicit map that loses to a concurrent map IS a clash: its cell is gone after this join, so a later
                    # delete or supersession could otherwise leave its write with no record (delta (b) withdrawn)
                    kind = "arrayClash" if isinstance(a[1], tuple) and isinstance(b[1], tuple) else "clash"
                    new[(kind, p, lo[0])] = lo[1]
    # (1b) typeClash: a scalar/array register at p against a descendant cell (register with a value, or counter slot) at
    # p.c, concurrent by the VVs. The loser is the side the type rule hides: the older stamp (a newer register at p
    # hides the descendant; a newer descendant makes p a map and the register at p loses).
    for A, B in ((s, t), (t, s)):
        for k, r in A.regs.items():
            if k[0] not in al or not is_val(r[1]) or cov(B.vv, r[0]) or out.suppressed(k, r, al):
                continue
            cells = [(kb[1], rb[0], rb[1]) for kb, rb in B.regs.items()
                     if kb[0] in al and is_val(rb[1]) and not out.suppressed(kb, rb, al)]
            cells += [(kb[1], vb[0], vb[1]) for kb, vb in B.slots.items() if kb[0] in al]
            for q, qst, qv in cells:
                if q.startswith(k[1] + ".") and qst != r[0] and not cov(A.vv, qst):
                    if r[0] > qst:
                        new[("typeClash", q, qst)] = qv
                    else:
                        new[("typeClash", k[1], r[0])] = r[1]
    # (2) a cell in a dead life that no known delete saw
    def _marker_with_child(k, r):     # DELTA (a): a marker cell whose own write also left a cell below it in the same life
        if r[1] not in MARKERS:
            return False
        for src2 in (s, t):
            for k2, r2 in src2.regs.items():
                if k2[0] == k[0] and r2[0] == r[0] and k2[1].startswith(k[1] + ".") and r2[1] not in MARKERS:
                    return True
            for k2, v2 in src2.slots.items():
                if k2[0] == k[0] and v2[0] == r[0] and k2[1].startswith(k[1] + "."):
                    return True
        return False
    for src in (s, t):
        for k, r in src.regs.items():
            if CELLS_REPORTED and k[0] in reported and r[0] != k[0]:       # V6M1: a write made into the losing life
                if not _marker_with_child(k, r):
                    new[("insertLost", k[1], r[0])] = r[1]
            elif k[0] not in al and k[0] not in reported and not any(cov(vv, r[0]) for vv in out.deletes.values()):
                if not _marker_with_child(k, r):
                    new[("deleteWon", k[1], r[0])] = r[1]
        for k, v in src.slots.items():
            if CELLS_REPORTED and k[0] in reported:
                new[("insertLost", k[1], v[0])] = v[1]
            elif k[0] not in al and k[0] not in reported and not any(cov(vv, v[0]) for vv in out.deletes.values()):
                new[("deleteWon", k[1], v[0])] = v[1]
    # V6m2: creation cells of an upsert life dropped for an alive explicit first insert: the cells `out.suppressed` names
    for src in (s, t):
        for k, r in src.regs.items():
            if k[0] in al and out.suppressed(k, r, al):
                new[("insertLost", k[1], r[0])] = r[1]
    # drop cells of dead lives; filter records by the horizon
    for store in (out.regs, out.slots, out.gens, out.ords, out.absorbed):
        for k in [k for k in store if k[0] not in al]:
            del store[k]
    if ABSORB_BY_WRITING_LIFE:       # a pair goes with the life of its slot as well (k[1]): no pair without its slot (the Rust holds the pair on the slot)
        for k in [k for k in out.absorbed if k[1] not in al]:
            del out.absorbed[k]

    H = out.horizon()
    for d in [d for d in out.deletes if d[0] < H]:
        del out.deletes[d]
    for L in [L for L in out.lives if L[0] < H and L in out.dead]:
        del out.lives[L]
    out.dead = {L for L in out.dead if L[0] >= H}
    if ORD_GC:       # an Ord with no valid candidate and a floor older than the horizon is dead weight, like a delete
        for k in [k for k, o in out.ords.items() if not o.firsts and o.last is None and (o.floor is None or o.floor[0][0] < H)]:
            del out.ords[k]
    out.losses = {}
    for key, val in list(s.losses.items()) + list(t.losses.items()) + list(new.items()):
        if key[2][0] >= H:
            out.losses.setdefault(key, val)
    out.losses = cap_losses(out.losses)
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
        self.known = max(self.state.vv.values(), default=0)      # the highest hlc this write's author knew (option A: Ord floor)
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
        obs = self._obs(p)
        s.regs[(L, p)] = (st, v, obs, gen)
        if ABSORBED:
            # A2.1 (review M2): fold EVERY slot of the path this writer holds in an alive life, of any generation, with its stamp (the set
            # `_seen` computes), not only the counted ones (`obs`): an increment the writer holds but a newer generation hides was
            # still in the writer's causal past, so no other base may count it again. `obs` is a subset, folded first.
            held = {(k[0], k[2], k[3]): (v[0], v[1]) for k, v in s.slots.items() if k[1] == p and k[0] in set(s.alive())}
            for src in (obs, held):
                for (l2, w2, g2), sv in src.items():
                    key = (L if ABSORB_BY_WRITING_LIFE else l2, l2, p, w2, g2)
                    if key not in s.absorbed or sv[0] > s.absorbed[key][0]:
                        s.absorbed[key] = sv
            if ABSORB_SINGLE_PAIR:
                s.absorbed = _single_pair(s.absorbed)
        if not numeric(v):
            s.gens[(L, p)] = (st, dict(vvnow))      # kept only for the V3M3 mutant

    def finish(self, st, log, meta):
        s = self.state
        s.vv[st[1]] = st[0]
        s.contained = s.contained | {st}
        log[st] = (frozenset(s.contained - {st}), meta)
        self.oplog[("w", st)] = ("w", st, wire(s))
        return st

    # --- option A: field-order events ----------------------------------------------------------------------
    @staticmethod
    def _events(items):
        """items: [(path, mode)] in write (document pre-)order; mode 'w' write, 'r' reset-and-write (replace, whole
        sub-document), 'u' unset. -> [(path, mode, pos)] where pos is the index of the path among its siblings that the
        write names, in write order (unsets take no position). Duplicate paths keep their first position."""
        seen, evs, done = {}, [], set()
        for path, mode in items:
            if (path, mode) in done:
                continue
            done.add((path, mode))
            if mode in ("u", "L"):
                evs.append((path, mode, 0))
                continue
            m = seen.setdefault(parent_of(path), {})
            if path not in m:
                m[path] = len(m)
            evs.append((path, mode, 0 if ORD_NO_POS else m[path]))
        return evs

    def _ord_apply(self, events, st, L=None):
        if not ORDER_STATE or not events:
            return
        s = self.state
        L = self._write_life() if L is None else L
        w = self.wid
        for p, mode, pos in events:
            o = s.ords.get((L, p))
            o = o.copy() if o is not None else Ord()
            if mode == "L":
                if ORD_LEVEL_FLOOR:
                    o.lvl = st if o.lvl is None else max(o.lvl, st)
                    s.ords[(L, p)] = o
                continue
            if mode == "u":
                others = any(m_ not in ("u", "L") for _, m_, _ in events)       # the unsetting write also writes other fields
                o.floor = max(f for f in (o.floor, (st, "u", getattr(self, "known", 0), others)) if f is not None)
                o.normalise()
            else:
                if mode == "r" and not ORD_NO_RESET:
                    fl = (st, "r", tuple(sorted(s.vv.items()))) if ORD_REPLACE_VV else (st, "r")
                    o.floor = max(f for f in (o.floor, fl) if f is not None)
                o.normalise()
                if w not in o.firsts:
                    o.firsts[w] = (st, pos)
                o.last = (st, pos)
            s.ords[(L, p)] = o

    # --- writes ----------------------------------------------------------------------------------------------
    def insert(self, fields, log, skew=0):
        s = self.state
        if s.alive():
            return None
        fields = nn(fields)
        orig = fields
        fields = flatten_doc(fields)            # a nested document is its leaves plus a marker at each map
        st = self.tick(skew)
        s.lives[st] = (frozenset(s.deletes), tuple(fields), "insert")
        for p, v in fields.items():
            s.regs[(st, p)] = (st, v, {}, None if numeric(v) else (st, {**s.vv, st[1]: st[0]}, {}))
            if not numeric(v):
                s.gens[(st, p)] = (st, {st[1]: st[0]})
        s.vv[st[1]] = st[0]
        self._ord_apply(self._events([(p, "w") for p in fields]), st, L=st)
        return self.finish(st, log, {"kind": "insert", "life": st, "paths": list(fields), "vals": dict(fields),
                                     "intent": ("insert", None, orig), "items": [(p, "w") for p in fields]})

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
        items = self._upsert_items(vals)
        self._ord_apply(self._events(items), st, L=st)
        self.finish(st, log, {"kind": "insert", "upsert": True, "insert": True, "life": st, "paths": list(vals),
                              "vals": vals, "soi": list(fields), "items": items})
        st2 = self.tick(skew)
        s = self.state                       # finish() normalised the writer's state into a new object
        s.vv[st2[1]] = st2[0]
        s.slots[(st, p, self.wid, None)] = (st2, n)
        items = self._upsert_items([p])
        self._ord_apply(self._events(items), st2, L=st)
        return self.finish(st2, log, {"kind": "inc", "life": st, "paths": [p], "n": n, "gen": None, "items": items})

    def upsert_inc(self, p, n, log, skew=0):
        s = self.state
        if s.alive():
            return None
        st = self.tick(skew)
        s.lives[st] = (frozenset(s.deletes), (), "upsert")
        s.slots[(st, p, self.wid, None)] = (st, n)
        items = self._upsert_items([p])
        self._ord_apply(self._events(items), st, L=st)
        return self.finish(st, log, {"kind": "inc", "life": st, "paths": [p], "n": n, "gen": None, "insert": True, "items": items})

    # leaf effects of a write (D4/D6/D7): the diff against what the writer SAW
    def _unset_subtree(self, path, o, eff, items):
        """Unset a value the writer saw: every leaf and every map marker below it, then the path itself."""
        if isinstance(o, dict):
            for name, ov in o.items():
                self._unset_subtree(path + "." + name, ov, eff, items)
        eff[path] = None
        items.append((path, "u"))

    def _diff(self, path, o, n, eff, items, mode):
        """Leaf effects of changing `path` from the writer's visible value `o` to `n` (None = unset): set the changed
        leaves, unset the leaves the writer saw that the new value lacks, a marker at a map when its type changed or
        anything under it changed. `items` collects the order events (every field NAMED by a new value, changed or not;
        mode 'r' re-assigns the position of the fields of a replacing document)."""
        keep = lambda x, y: (not REWRITE_UNCHANGED) and same(x, y)
        if n is None:
            if o is not None:
                self._unset_subtree(path, o, eff, items)
            return
        if isinstance(n, dict):
            od = o if isinstance(o, dict) else None
            n0 = len(eff)
            items.append((path, mode))
            items.append((path, "L"))
            for name, v in n.items():
                self._diff(path + "." + name, od.get(name) if od is not None else None, v, eff, items, "r")
            if od is not None:
                for name, ov in od.items():
                    if name not in n:
                        self._unset_subtree(path + "." + name, ov, eff, items)
            if od is None or len(eff) > n0 or REWRITE_UNCHANGED:
                eff[path] = MAP
            return
        if isinstance(o, dict):                       # a scalar or an array over a map the writer saw
            for name, ov in o.items():
                self._unset_subtree(path + "." + name, ov, eff, items)
            eff[path] = n
        elif not keep(o, n):
            eff[path] = n
        items.append((path, mode))

    def _write_effects(self, eff, intent, items, st, log):
        s = self.state
        L = self._write_life()
        for q, v in eff.items():
            self._setreg(q, v, st, s.vv)
        self._ord_apply(self._events(items), st)
        return self.finish(st, log, {"kind": "reg", "life": L, "paths": list(eff), "vals": dict(eff), "intent": intent,
                                     "items": list(items)})

    @staticmethod
    def _anc_items(p):
        return [] if ORD_NO_ANCESTOR else [(a, "w") for a in ancestors(p)]

    def _upsert_items(self, paths):
        """The order items of an upsert body (A2.3): each path names its ancestors first, each ancestor once, then itself."""
        items, seen = [], set()
        for q in paths:
            items += [(a, m_) for a, m_ in self._anc_items(q) if a not in seen]
            seen.update(a for a, _ in self._anc_items(q))
            items.append((q, "w"))
        return items

    def _update(self, ops, intent, vis, log, skew):
        """ONE write, ONE stamp, for a list of `$set` / `$unset` / `$inc` operators on pairwise disjoint paths, effects and
        positions in update-document order. Atomic: any refusal refuses the whole update (returns None). A write that
        needs both register effects (a value, a marker for a map it creates, an unset) and increments is kind 'reg' with
        an `incs` dict (the oracle reads both); a lone `$inc` of an existing path stays kind 'inc'."""
        s = self.state
        ps = [p for _, p, _ in ops]
        for i, a_ in enumerate(ps):
            for b_ in ps[i + 1:]:
                if a_ == b_ or is_under(a_, b_) or is_under(b_, a_):
                    return None                       # conflicting paths in one update: the reference refuses
        eff, items, incs, seen_anc, changed, pending_noop = {}, [], {}, set(), False, []

        def ancestors_of(p):
            for a in ancestors(p):
                if a not in seen_anc:
                    seen_anc.add(a)
                    if not ORD_NO_ANCESTOR:
                        items.append((a, "w"))
                    if getp(vis, a) is None and not NO_IMPLICIT_MARKER:
                        eff[a] = MAPI
        for op, p, arg in ops:
            if any(getp(vis, a) is not None and not isinstance(getp(vis, a), dict) for a in ancestors(p)):
                return None                           # the reference cannot create a field in a non-document
            cur = getp(vis, p)
            if op == "unset":
                if cur is None:
                    if ORD_NOOP_UNSET_FLOOR == "hybrid":    # manager's ruling: only metadata, and only inside an update another change already stamps
                        pending_noop.append((p, "u"))
                    elif ORD_NOOP_UNSET_FLOOR:              # variant: a STANDALONE no-op unset is still a write that raises the u floor
                        items.append((p, "u"))
                        changed = True
                    continue
                self._unset_subtree(p, cur, eff, items)
                changed = True
            elif op == "set":
                ancestors_of(p)
                n0 = len(eff)
                self._diff(p, cur, arg, eff, items, "w")
                changed = changed or len(eff) > n0 or (isinstance(arg, dict) and not ordered_same(cur, arg))
            else:                                     # inc
                if isinstance(cur, (dict, tuple)):
                    return None
                if numeric(cur) and not (-BOUND - 1 <= (cur or 0) + arg <= BOUND):
                    return None                       # Q11 (Chris): a local $inc past the bound is refused (400)
                if not numeric(cur):
                    if not isinstance(cur, float):
                        return None                   # a string, a null: 400
                    ancestors_of(p)                   # Nm5: a double base is a register write that starts a generation
                    n0 = len(eff)
                    self._diff(p, cur, cur + arg, eff, items, "w")
                    changed = changed or len(eff) > n0
                    continue
                ancestors_of(p)
                incs[p] = arg
                items.append((p, "w"))
                changed = True
        if not changed:
            return None
        items.extend(pending_noop)                    # the hybrid: the floors of the no-op unsets ride on a write that exists anyway
        st = self.tick(skew)
        s.vv[st[1]] = st[0]
        L = self._write_life()
        for q, v in eff.items():
            self._setreg(q, v, st, s.vv)
        gens = {}
        for q, n in incs.items():
            g = s.gen_of(q)
            G = g[0] if g else None
            old = s.slots.get((L, q, self.wid, G))
            s.slots[(L, q, self.wid, G)] = (st, (old[1] if old else 0) + n)
            gens[q] = G
        self._ord_apply(self._events(items), st)
        if incs and not eff and len(incs) == 1:
            (q, n), = incs.items()
            meta = {"kind": "inc", "life": L, "paths": [q], "n": n, "gen": gens[q], "items": items}
            if len(ops) > 1:
                meta["replay"] = intent          # the update also held operators that were no-ops at its writer (a `$unset` of an absent field): the serial replay must run them
            return self.finish(st, log, meta)
        meta = {"kind": "reg", "life": L, "paths": list(eff), "vals": dict(eff), "intent": intent, "items": list(items)}
        if incs:
            meta["incs"], meta["ipaths"] = dict(incs), list(incs)
        return self.finish(st, log, meta)

    def write(self, op, p, arg, log, skew=0):
        s = self.state
        if not s.alive():
            return None
        if op == "delete":
            st = self.tick(skew)
            s.vv[st[1]] = st[0]
            s.deletes[st] = dict(s.vv)
            return self.finish(st, log, {"kind": "delete", "paths": [], "intent": ("delete", None, None)})
        vis = s.visible()
        if op in ("push", "incqty"):                 # arrays are atomic: the whole array is rewritten
            cur = getp(vis, p)
            if op == "push":
                if cur is not None and not isinstance(cur, tuple):
                    return None
                arg, op = (cur or ()) + (arg,), "set"
            else:
                if not isinstance(cur, tuple) or not cur or not isinstance(cur[0], int) or isinstance(cur[0], bool):
                    return None
                arg, op = (cur[0] + arg,) + cur[1:], "set"
        if op in ("set", "unset", "inc", "setmany", "update"):
            if op == "update":
                ops = [(o_, q, nn(v)) for o_, q, v in arg]                       # a multi-operator update: [("set"|"unset"|"inc", path, arg)] in update-document order
                intent = ("update", None, list(ops))
            elif op == "setmany":
                ops = [("set", q, nn(v)) for q, v in arg]
                intent = ("setmany", None, [(q, v) for _, q, v in ops])
            elif op == "inc":
                ops, intent = [("inc", p, arg)], ("update", None, [("inc", p, arg)])
            elif op == "set" and arg is None:
                ops, intent = [("set", p, NULL)], ("set", p, NULL)      # `$set x null` stores a present null (the reference)
            else:
                arg = nn(arg) if op == "set" else None
                ops, intent = [(op, p, arg)], (op, p, arg if op == "set" else None)
            return self._update(ops, intent, vis, log, skew)
        if op == "replace":
            arg = nn(arg)
            eff, items = {}, [("", "L")]
            for t, v in arg.items():
                self._diff(t, vis.get(t), v, eff, items, "r")
            for t, v in vis.items():
                if t not in arg:
                    self._unset_subtree(t, v, eff, items)
            if not eff and ordered_same(vis, arg):
                return None
            st = self.tick(skew)
            s.vv[st[1]] = st[0]
            return self._write_effects(eff, ("replace", None, arg), items, st, log)
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
            if self._absorb(unwire(ent[2])) and ("w", ent[1]) not in self.oplog:
                self.oplog[("w", ent[1])] = ent
        else:
            key, val = ent[1], ent[2]
            if key[2][0] < self.state.horizon() or key in self.state.losses:
                return
            self.state.losses[key] = val
            self.state.losses = cap_losses(self.state.losses)
            if key not in self.state.losses:      # refused by the cap: the K greater records stay
                return
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


def incs_of(m):
    """The increments a write makes: {path: n}. A pure `$inc` is kind 'inc'; a write that also carries registers (a combined
    update, or an `$inc` that creates the maps above it) is kind 'reg' with an `incs` dict."""
    if m["kind"] == "inc":
        return {m["paths"][0]: m["n"]}
    return m.get("incs", {})


def touched(m):
    """Every path a write names: its registers and its increments."""
    return list(m.get("paths", [])) + [p for p in m.get("ipaths", []) if p not in m.get("paths", [])]


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
        if m["kind"] in ("insert", "reg") and p in m["vals"] and not numeric(m["vals"][p]):
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


def _flat_oracle(log):
    """Per-path values (no type rule): returns (flat doc or None, alive lives, deletes). The value of a path, in words: the winning base (the
    highest-stamped register write in an alive life), plus every increment that no base write on the path had
    seen (Q6, A2.1 wording: a register write in an alive life, `$setOnInsert` cells suppressed; with `ABSORBED` False, the model5 rule: only
    the increments the winner's writer had not seen), except the increments the winner's generation overwrote (an increment made in an OLDER
    generation than the winner's, and not seen by the starter of the winner's generation). Increments made in a NEWER
    generation are kept even when its starter lost."""
    ins = {st: m for st, (ctx, m) in log.items() if m["kind"] == "insert" or m.get("insert")}
    dels = [st for st, (ctx, m) in log.items() if m["kind"] == "delete"]
    alive = _alive_lives(log, ins, dels)
    gen = make_gen(log)

    def writes_on(p, kinds):
        return [(st, ctx, m) for st, (ctx, m) in log.items()
                if m["kind"] in kinds and p in m.get("paths", []) and m.get("life") is not None]

    def incs_on(p):
        return [(st, ctx, m) for st, (ctx, m) in log.items() if p in incs_of(m) and m.get("life") is not None]

    paths = {p for st, (ctx, m) in log.items() for p in touched(m)}
    doc = {}
    for p in sorted(paths):
        regs = [(st, ctx, m) for st, ctx, m in writes_on(p, ("insert", "reg"))
                if m["life"] in alive and not _supp(log, m, p, alive)]
        incs = [(st, ctx, m) for st, ctx, m in incs_on(p) if m["life"] in alive]
        win = max(regs, key=lambda r: r[0]) if regs else None
        if win is not None and not numeric(win[2]["vals"][p]):
            doc[p] = win[2]["vals"][p]
            continue
        gW = gen(win[0], p) if win else None
        counted = [(st, ctx, m) for st, ctx, m in incs
                   if gen(st, p) == gW or (gen(st, p) is not None and (gW is None or gen(st, p) > gW))]
        base = win[2]["vals"][p] if win else None
        wctx = win[1] if win else frozenset()
        if ABSORBED:       # Q6 (A2.1 wording): an increment counts only if NO base write on the path had seen it
            for st_, ctx_, m_ in writes_on(p, ("insert", "reg")):
                if m_["life"] in alive and not _supp(log, m_, p, alive):
                    wctx = wctx | ctx_
        extra = [incs_of(m)[p] for st, ctx, m in counted if st not in wctx]
        if base is None and not extra:
            continue
        doc[p] = max(min((base or 0) + sum(extra), BOUND), -BOUND - 1)
    return (doc if alive else None), alive, dels


def _regs_at(log, alive, p):
    """Register writes (inserts and sets, unsets and markers included) at p in alive lives."""
    return [(st, ctx, m) for st, (ctx, m) in log.items()
            if m["kind"] in ("insert", "reg") and p in m["paths"] and m["life"] in alive and not _supp(log, m, p, alive)]


def _incs_at(log, alive, p):
    return [(st, ctx, m) for st, (ctx, m) in log.items()
            if p in incs_of(m) and m.get("life") in alive]


def oracle(log):
    """Returns (nested visible-doc or None, alive lives, deletes). Per leaf path as before (_flat_oracle); then the TYPE
    RULE of D4 at every depth, in words: a path p is a map iff some descendant is visible and newer than p's own scalar
    register, or a map marker is visible at p; otherwise p is its register. A marker never hides children. A newer
    scalar register at p beats an older marker (p's register is the newest register write at p). An empty map is {}
    when the marker is visible and no child is. 'Newer' compares the newest stamp among all writes to the
    descendant's subtree (a register write or an increment, in an alive life) with the stamp of p's register. The
    ORDER of every level is not graded here (see order_oracle)."""
    flat, alive, dels = _flat_oracle(log)
    if flat is None:
        return None, alive, dels
    paths = {p for st, (ctx, m) in log.items() for p in touched(m)}
    allp = set(paths)
    for q in paths:
        allp.update(ancestors(q))
    kids, tops = {}, []
    for q in sorted(allp):
        par = parent_of(q)
        (tops if par is None else kids.setdefault(par, [])).append(q)

    def node(q):
        ks = {}
        for c in kids.get(q, ()):
            r = node(c)
            if r is not None:
                ks[name_of(c)] = r
        sts = [w[0] for w in _regs_at(log, alive, q)] + [w[0] for w in _incs_at(log, alive, q)]
        own_n = max(sts) if sts else None
        own = flat.get(q)
        if not ks:
            if own is None:
                return None
            return ({} if own in MARKERS else own, own_n)
        if own is not None and own not in MARKERS:
            rs_ = _regs_at(log, alive, q)
            reg = max(rs_, key=lambda r: r[0]) if rs_ else None
            rs = reg[0] if (reg is not None and is_val(reg[2]["vals"][q])) else own_n
            shown = {c: v for c, v in ks.items() if v[1] > rs}
            if shown:
                return ({c: v[0] for c, v in shown.items()}, max([own_n] + [v[1] for v in shown.values()]))
            return (own, own_n)
        ns = [v[1] for v in ks.values()] + ([own_n] if own_n is not None else [])
        return ({c: v[0] for c, v in sorted(ks.items())}, max(ns))

    doc = {}
    for t in sorted(tops):
        r = node(t)
        if r is not None:
            doc[name_of(t)] = r[0]
    return doc, alive, dels


def oracle_at(log, ctx):
    """The visible document a writer with causal past `ctx` could read (the oracle over that part of the log)."""
    sub = {st: log[st] for st in ctx if st in log}
    return oracle(sub)[0]


def _expected_effects(old, intent):
    """The leaf effects a write with `intent` must have, from the writer's visible document `old` (D4/D6/D7)."""
    op, p, arg = intent
    old = old or {}
    ident = lambda x, y: type(x) is type(y) and x == y

    def unset_all(path, o):
        e = {}
        if isinstance(o, dict):
            for k, v in o.items():
                e.update(unset_all(path + "." + k, v))
        e[path] = None
        return e

    def eff_of(path, o, n):
        if n is None:
            return unset_all(path, o) if o is not None else {}
        if isinstance(n, dict):
            od = o if isinstance(o, dict) else None
            e = {}
            for k, v in n.items():
                e.update(eff_of(path + "." + k, od.get(k) if od is not None else None, v))
            if od is not None:
                for k, v in od.items():
                    if k not in n:
                        e.update(unset_all(path + "." + k, v))
            if e or od is None:
                e[path] = MAP
            return e
        e = {}
        if isinstance(o, dict):
            for k, v in o.items():
                e.update(unset_all(path + "." + k, v))
            e[path] = n
        elif not ident(o, n):
            e[path] = n
        return e

    def sets(pairs):
        e = {}
        for q, v in pairs:
            for a in ancestors(q):
                if getp(old, a) is None:
                    e[a] = MAPI
            e.update(eff_of(q, getp(old, q), v))
        return e

    if op == "insert":
        return flatten_doc(arg)
    if op == "replace":
        e = {}
        for t in set(old) | set(arg):
            e.update(eff_of(t, old.get(t), arg.get(t)))
        return e
    if op == "setmany":
        return sets(arg)
    if op == "update":
        e = {}
        for o_, q, v in arg:
            if o_ == "set":
                e.update(sets([(q, v)]))
            elif o_ == "unset":
                e.update(eff_of(q, getp(old, q), None))
            else:
                cur = getp(old, q)
                if isinstance(cur, float):
                    e.update(sets([(q, cur + v)]))
                else:
                    for a in ancestors(q):
                        if getp(old, a) is None:
                            e[a] = MAPI
        return e
    if op == "set":
        return sets([(p, arg)])
    if op == "unset":
        return eff_of(p, getp(old, p), None)
    return {}


def expansion_errors(log):
    """Every write with an intent must have exactly the leaf effects the words give against its writer's document."""
    bad = []
    for st, (ctx, m) in log.items():
        if "intent" in m and m["intent"] is not None and m["intent"][0] in ("set", "unset", "replace", "setmany", "insert", "update") \
                and not m.get("upsert"):
            want = _expected_effects(oracle_at(log, ctx), m["intent"])
            if repr(sorted(want.items(), key=repr)) != repr(sorted(m["vals"].items(), key=repr)):
                bad.append((st, m["intent"], want, m["vals"]))
    return bad


def _type_must(log, alive, flat, doc):
    """typeClash heads that must be reported: (path, stamp). W = the newest register at an ancestor t, a scalar or an
    array. A descendant c that is not visible in the final document (so hidden by W's recency) loses each of its
    value-bearing heads that W's writer had not seen. A descendant that is visible and has such a head NEWER than W
    (not having seen W) makes t a map: W is the loser."""
    must = set()
    gen = make_gen(log)
    allp = {p for st, (ctx, m) in log.items() for p in touched(m)}
    for t in sorted(allp):
        rs = _regs_at(log, alive, t)
        if not rs:
            continue
        W = max(rs, key=lambda r: r[0])
        if not is_val(W[2]["vals"][t]):
            continue
        for c in sorted(q for q in allp if is_under(q, t)):
            cr = _regs_at(log, alive, c)
            ci = _incs_at(log, alive, c)
            if not cr and not ci:
                continue
            newest = max([r[0] for r in cr] + [r[0] for r in ci])
            heads = [(r[0], r[1]) for r in cr if is_val(r[2]["vals"][c]) and not any(r[0] in r2[1] for r2 in cr)]
            groups = {}
            for st, ctx, m in ci:
                groups.setdefault((m["life"], st[1], gen(st, c)), []).append(st)
            heads += [(max(sts), log[max(sts)][0]) for sts in groups.values()]
            visible = doc is not None and getp(doc, c) is not None
            if newest <= W[0] and not visible:
                must |= {(c, h) for h, hctx in heads if h not in W[1]}
            elif visible and any(h > W[0] and W[0] not in hctx for h, hctx in heads):
                must.add((t, W[0]))
    return must


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
            for p in touched(m):
                if m.get("vals", {}).get(p) == MAPI:      # an implicit marker is never a head of its own: its write's leaves are
                    continue
                later = [w for w, (c2, m2) in log.items() if st in c2 and p in touched(m2)
                         and m2.get("life") == m["life"]]
                if not later:
                    must.add((p, st))
    for st, (ctx, m) in log.items():      # V6m2: the dropped $setOnInsert cells are reported
        for p in m.get("soi", []):
            if m.get("life") in alive and _supp(log, m, p, alive):
                later = [w for w, (c2, m2) in log.items() if st in c2 and p in touched(m2)
                         and m2.get("life") == m["life"] and m2["kind"] in ("reg", "insert")]
                if not later:      # a cell overwritten in sequence by its own life's writer is no longer a head
                    must.add((p, st))
    paths = {p for st, (ctx, m) in log.items() for p in touched(m)}
    for p in paths:
        regs = [(st, ctx, m) for st, (ctx, m) in log.items()
                if m["kind"] in ("insert", "reg") and p in m["paths"] and m["life"] in alive
                and not _supp(log, m, p, alive)]
        heads = [r for r in regs if not any(r[0] in r2[1] for r2 in regs)]
        if heads:
            top = max(heads, key=lambda r: r[0])
            for h in heads:
                if h is not top and h[2]["vals"][p] != MAPI:      # an implicit marker is never a reported loser (delta (b) withdrawn)
                    must.add((p, h[0]))
        dead_regs = [(st, ctx, m) for st, (ctx, m) in log.items()
                     if m["kind"] in ("insert", "reg") and p in m["paths"] and m["life"] not in alive and m["life"] not in (sup - killed)
                     and not (m["life"] in sup and st == m["life"])]    # a superseded life's own fields: order-dependent
        for st, ctx, m in dead_regs:
            if m["vals"][p] != MAPI and not any(st in log[d][0] for d in dels) and not any(st in c2 for s2, c2, m2 in dead_regs):
                must.add((p, st))      # an implicit marker is never a head of its own, as in the head loop above
        dead_incs = [(st, ctx, m) for st, (ctx, m) in log.items()
                     if p in incs_of(m) and m["life"] not in alive and m["life"] not in (sup - killed)]
        by_slot = {}
        for st, ctx, m in dead_incs:
            by_slot.setdefault((m["life"], st[1], gen(st, p)), []).append(st)
        for sts in by_slot.values():
            head = max(sts)
            if not any(head in log[d][0] for d in dels):
                must.add((p, head))
    flat = _flat_oracle(log)[0] or {}
    must |= _type_must(log, alive, flat, oracle(log)[0])
    # DELTA (a), as a cover: a marker head stays required; `covered` accepts any record of the same write at or below it
    return must


def marker_head(log, p, st):
    """A required head that is a map marker of its write (explicit or implicit)."""
    return st in log and log[st][1].get("vals", {}).get(p) in MARKERS


def covered(head, have, log):
    """DELTA (a): a head is reported by its own record; a MARKER head also by a record of the same write at a NON-MARKER path
    strictly below it (the join reports a marker's lost write through the write's own child cells, wherever those survive; a
    child is a register with a value that is not a marker, or a counter slot: `_marker_with_child`)."""
    p, st = head
    if head in have:
        return True
    return marker_head(log, p, st) and any(s2 == st and q.startswith(p + ".") and not marker_head(log, q, st) for (q, s2) in have)


def hidden_oracle(log):
    """V4M1: from D5's words. For each path with a winning numeric or non-numeric base: the whole total of every
    (life, writer, generation) group of increments in an OLDER generation than the winner's, whose latest
    increment is not in the causal past of the starter of the winner's generation."""
    ins = {st: m for st, (ctx, m) in log.items() if m["kind"] == "insert" or m.get("insert")}
    dels = [st for st, (ctx, m) in log.items() if m["kind"] == "delete"]
    alive = _alive_lives(log, ins, dels)
    gen = make_gen(log)
    out = {}
    paths = {p for st, (ctx, m) in log.items() for p in touched(m)}
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
            if p in incs_of(m) and m["life"] in alive:
                groups.setdefault((m["life"], st[1], gen(st, p)), []).append(st)
        total = 0
        for (L, w, gi), sts in groups.items():
            if gi is None or gi < gW:
                total += sum(incs_of(log[s_][1])[p] for s_ in sts if s_ not in log[gW][0])
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
        if kind == "typeClash":      # hiding by the type rule is not monotone (a later descendant can revive it): see NOTES
            continue
        if p not in incs_of(m) or m.get("life") not in alive:
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
            out.append({("_", L)} | {(p, L) for p in touched(m)})
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
    vb = lambda m2, q: (m2["kind"] in ("insert", "reg") and q in m2["paths"] and is_val(m2["vals"][q])) or \
        (q in incs_of(m2))
    conc = lambda w, c2: w != st and st not in c2 and w not in ctx
    if kind == "arrayClash":
        return (m["kind"] in ("insert", "reg") and isinstance(m["vals"].get(p), tuple) and
                any(w > st and conc(w, c2) and m2["kind"] in ("insert", "reg") and isinstance(m2["vals"].get(p), tuple)
                    for w, (c2, m2) in log.items() if w != st and p in touched(m2)))
    if kind == "typeClash":
        if vb(m, p) and "." in p:        # a descendant loses to a newer scalar/array register at an ancestor
            for par in ancestors(p):
                if any(w > st and conc(w, c2) and m2["kind"] in ("insert", "reg") and par in m2["paths"] and is_val(m2["vals"][par])
                       and not isinstance(m2["vals"][par], dict) for w, (c2, m2) in log.items()):
                    return True
        if m["kind"] in ("insert", "reg") and p in m["paths"] and is_val(m["vals"][p]):     # the register at p loses to a newer descendant
            return any(w > st and conc(w, c2) and any(q.startswith(p + ".") and vb(m2, q) for q in touched(m2))
                       for w, (c2, m2) in log.items())
        return False
    if kind == "clash":
        return p in touched(m) and any(
            w != st and w > st and st not in c2 and w not in ctx and p in touched(m2)
            and m2["kind"] in ("insert", "reg") and m["kind"] in ("insert", "reg")
            for w, (c2, m2) in log.items() if w != st)
    if kind == "insertLost":
        if p == "_":
            return st in superseded_lives(log)
        return p in touched(m) and (m.get("life") in superseded_lives(log)
                                            or (m["kind"] == "insert" and p in m.get("soi", [])))
    if kind == "deleteWon":
        life = m.get("life")
        return (p in touched(m) and life is not None and life in log and life not in alive
                and any(st not in log[d][0] for d in _killers(log, life, dels)))
    return False


# ---------------------------------------------------------------------------
# Field order: the reference's serial outcome (the D-item of 2026-10-31) and the checks against it
# ---------------------------------------------------------------------------
class Ref:
    """The reference's single primary, for ordered nested documents. `apply` returns False when the reference would
    fail the operation (a duplicate insert, a write to a missing document, a field in a non-document, `$inc` of a
    non-number): a serial order in which an operation of the history fails is not an explanation of that history."""

    def __init__(self):
        self.doc = None

    def _walk(self, p, create):
        """The (dict, key) holding path p; creates absent ancestors (appended) when `create`; None if an ancestor is a
        non-document."""
        d = self.doc
        parts_ = p.split(".")
        for s_ in parts_[:-1]:
            if s_ not in d:
                if not create:
                    return None
                d[s_] = {}
            if not isinstance(d[s_], dict):
                return None
            d = d[s_]
        return d, parts_[-1]

    def apply(self, intent):
        op, p, arg = intent
        if op == "insert":
            if self.doc is not None:
                return False
            self.doc = copy.deepcopy(arg)
            return True
        if self.doc is None:
            return op == "delete"
        if op == "delete":
            self.doc = None
            return True
        if op == "replace":
            self.doc = copy.deepcopy(arg)
            return True
        if op == "setmany":
            return all(self.apply(("set", q, v)) for q, v in arg)
        if op == "update":
            return all(self.apply((o_, q, v)) for o_, q, v in arg)
        if op == "set":
            w = self._walk(p, True)
            if w is None:
                return False
            d, k = w
            d[k] = copy.deepcopy(arg)           # an existing key keeps its position; a new one is appended
            return True
        if op == "unset":
            w = self._walk(p, False)
            if w is not None:
                w[0].pop(w[1], None)
            return True
        if op == "inc":
            w = self._walk(p, True)
            if w is None:
                return False
            d, k = w
            if k not in d:
                d[k] = arg
            elif isinstance(d[k], int) and not isinstance(d[k], bool):
                d[k] = d[k] + arg
            elif isinstance(d[k], float):
                d[k] = d[k] + arg
            else:
                return False
            return True
        return False


def intent_of(m):
    """The operation a log entry stands for, or None for entries the replay does not model."""
    if m.get("upsert") or m.get("soi") is not None or m["kind"] == "insert" and m.get("upsert"):
        return "unsupported"
    if m.get("replay") is not None:
        return m["replay"]
    if m["kind"] == "inc":
        if m.get("insert"):                         # an `$inc` that CREATES the document (an upsert): the insert of its creation document
            doc = {}
            segs = m["paths"][0].split(".")
            d = doc
            for sg in segs[:-1]:
                d = d.setdefault(sg, {})
            d[segs[-1]] = m["n"]
            return ("insert", None, doc)
        return ("inc", m["paths"][0], m["n"])
    it = m.get("intent")
    if it is None or it[0] == "implicit":
        return None
    return it


def serial_outcome(log, order):
    """Replay the log's writes in `order` (a list of stamps) on the reference. Returns the final ordered document,
    None for a deleted document, or the string 'invalid' when some operation fails."""
    r = Ref()
    for st in order:
        it = intent_of(log[st][1])
        if it is None:
            continue
        if it == "unsupported" or not r.apply(it):
            return "invalid"
    return r.doc


def stamp_outcome(log):
    return serial_outcome(log, sorted(log))


def totally_ordered(log):
    """Every write's causal past holds every earlier-stamped write: the history is sequential."""
    ss = sorted(log)
    return all(set(ss[:i]) <= log[s][0] for i, s in enumerate(ss))


def _touches(m, p):
    return any(q == p and mode != "u" for q, mode in m.get("items", []))


def _unsets(m, p):
    return any(q == p and mode == "u" for q, mode in m.get("items", []))


def approx_triggers(log):
    """The narrow case of the ruling (a), as the model can actually hit it: a path p with an unset u and a writer W who
    wrote p both before u (w1 < u) and after u (w2 > u) without having seen u at w2. W's earliest write since the unset
    it knew is w1; the unset drops it, w2 was never retained as W's candidate, and the position of p falls back to
    the latest write to p. (The ruling's wording needs three writes by W; any later write by another writer to p, or an
    ancestor event of one below p, also replaces the fallback, so two writes by W are enough.) Returns the triples."""
    out = set()
    paths = {q for st, (ctx, m) in log.items() for q, mode in m.get("items", [])}
    for p in paths:
        us = [st for st, (ctx, m) in log.items() if _unsets(m, p)]
        for u in us:
            for W in {st[1] for st in log}:
                ws = sorted(st for st, (ctx, m) in log.items() if st[1] == W and _touches(m, p))
                before = [w for w in ws if w < u]
                after = [w for w in ws if w > u and u not in log[w][0]]
                if before and after:
                    out.add((p, u, before[0], after[0]))
    return out


def _cmp_levels(model, ref, path=""):
    """Relative order of the keys both documents hold, level by level; returns [(path, model_order, ref_order)]."""
    bad = []
    if isinstance(model, dict) and isinstance(ref, dict):
        common_m = [k for k in model if k in ref]
        common_r = [k for k in ref if k in model]
        if common_m != common_r:
            bad.append((path, common_m, common_r))
        for k in common_m:
            bad += _cmp_levels(model[k], ref[k], path + ("." if path else "") + k)
    return bad


def _is_docwrite(m):
    """A replace or a document-valued `$set`: both are diffs against what their writer saw (Q4, Q3)."""
    it = m.get("intent")
    if not it:
        return False
    if it[0] == "replace":
        return True
    if it[0] == "set":
        return isinstance(it[2], dict)
    if it[0] in ("setmany", "update"):
        return any(isinstance(x[-1], dict) for x in it[2])
    return False


def concurrent_replace(log):
    """A replace or document-valued `$set` concurrent with another write. Q4/Q3 (ruled): such a write is a diff against what its
    writer saw, so a field it never saw survives it, and a concurrent observed-remove can take part of it away; the serial
    reading ('the write replaces the whole value') does not hold, for values or for positions."""
    reps = [st for st, (ctx, m) in log.items() if _is_docwrite(m)]
    return any(r != w and r not in log[w][0] and w not in log[r][0] and log[w][1]["kind"] != "insert"
               for r in reps for w in log)


def sortdoc(d):
    if isinstance(d, dict):
        return {k: sortdoc(d[k]) for k in sorted(d)}
    return d


def M_TYPES(d):
    """A type-strict, order-free view of a document (so 2 and 2.0 differ)."""
    return tyrepr(ordered(sortdoc(d)))


class _Counter(dict):
    def __missing__(self, k):
        return 0


ORDER_STATS = _Counter()


ORDER_EXT_BOUND = 10000    # complete linear extensions the serial-order walk tries per history; past it the history is decided by the order RULE alone (below)


def serial_orders(log, cap=None):
    """The linear extensions of the log's causal order (every write after everything in its causal past), the stamp order first.
    Yields at most `cap` COMPLETE extensions (the unit is complete extensions, not tree nodes); the returned state says whether more existed."""
    cap = ORDER_EXT_BOUND if cap is None else cap
    ss = sorted(log)
    state = {"n": 0, "cut": False}

    def rec(done, placed, rest):
        if state["cut"]:
            return
        if not rest:
            if state["n"] >= cap:
                state["cut"] = True
                return
            state["n"] += 1
            yield done
            return
        for s_ in rest:
            if log[s_][0] <= placed:
                yield from rec(done + [s_], placed | {s_}, [r for r in rest if r != s_])
                if state["cut"]:
                    return
    return rec([], frozenset(), ss), state


# --- the order RULE restated from the log (closed form), used to test the placement of a mismatching level EXACTLY -----------------------------
# The order check restates the few helpers it shares with the writer, so a defect in one of them cannot hide itself (the writer's insert items, ancestors and path
# lookups are what the items it emitted are judged against).
def _o_parent(p):
    return p[:p.rindex(".")] if "." in p else None


def _o_ancestors(p):
    out, cur = [], ""
    for seg in p.split(".")[:-1]:
        cur = cur + "." + seg if cur else seg
        out.append(cur)
    return out


def _o_get(d, p):
    for seg in p.split("."):
        if not isinstance(d, dict) or seg not in d:
            return None
        d = d[seg]
    return d


def _o_plain(v):
    return v is not None and not (isinstance(v, str) and v in (MAP, MAPI))


def _o_insert_items(doc, prefix=""):
    """An insert names every map and every leaf of its document, in document order (a map before what is in it)."""
    out = []
    for k, v in doc.items():
        q = prefix + k
        out.append((q, "w"))
        if isinstance(v, dict):
            out += _o_insert_items(v, q + ".")
    return out


_BIG = 10 ** 9


class _Rule:
    """The field order the design's rule gives, computed from the log alone (never from a replicated Ord state): per path, the candidates (writes that name
    it), the floors (unsets and resets), the per-writer first valid candidate and the latest write, the survivor clamp, the level floor. Fields of a
    level are shown sorted by (born, path)."""

    def __init__(self, log):
        self.log = log
        self.cands, self.floors, self.lvl, self.pos = {}, {}, {}, {}
        for w, (ctx, m) in log.items():
            seen = {}
            others = any(md not in ("u", "L") for q, md in m.get("items", []))
            for q, md in m.get("items", []):
                if md == "L":
                    self.lvl[q] = max(self.lvl.get(q, w), w)
                elif md == "u":
                    self.floors.setdefault(q, []).append((w, "u", others))
                else:
                    d = seen.setdefault(_o_parent(q), {})
                    if q not in d:
                        d[q] = len(d)
                    self.cands.setdefault(q, []).append((w, d[q]))
                    if md == "r":
                        self.floors.setdefault(q, []).append((w, "r", others))
        self.memo = {}

    @staticmethod
    def _valid(c, f):
        return f is None or c[0] > f[0] or (c[0] == f[0] and f[1] == "r")

    def _floor(self, q, within=None):
        fs = [f for f in self.floors.get(q, []) if within is None or f[0] in within]
        return max(fs, key=lambda f: (f[0], f[1] == "r")) if fs else None

    def _born0(self, q):
        """The candidates of q under its own floors: per writer the first write since the floors that writer KNEW (a later write of its own is not kept
        once the floor dropped the first: the approximate case of ruling (a)), and the latest write overall."""
        cs = self.cands.get(q, [])
        if not cs:
            return None, None
        F = self._floor(q)
        by_w = {}
        for w, pos in cs:
            by_w.setdefault(w[1], []).append((w, pos))
        firsts = []
        for W, lst in by_w.items():
            lst.sort()
            first, timeline = None, []
            for w, pos in lst:
                ctx = self.log[w][0] | {w}
                Fv = self._floor(q, ctx)
                if first is not None and not self._valid(first, Fv):
                    first = None
                if first is None:
                    first = (w, pos)
                    timeline.append(first)
            firsts += [c for c in timeline if self._valid(c, F)]
        last = max(cs)
        cand = firsts + ([last] if self._valid(last, F) else [])
        return (min(cand) if cand else None), F

    def _plain_winner(self, q):
        best = None
        for w, (ctx, m) in self.log.items():
            if q in m.get("paths", []) and m["kind"] in ("insert", "reg"):
                if best is None or w > best[0]:
                    best = (w, m["vals"].get(q))
        return best

    def _slot_cands(self, q, fstamp, place):
        out = []
        incs = {}
        for w, (ctx, m) in self.log.items():
            if q in incs_of(m):
                incs.setdefault(w[1], []).append(w)
        bases = [(w, ctx) for w, (ctx, m) in self.log.items() if q in m.get("paths", []) and m["kind"] in ("insert", "reg")]
        for W, ws in incs.items():
            latest = max(ws)
            if latest < fstamp and not any(latest in ctx for w, ctx in bases):
                out.append(place((latest, 0)))
        return out

    def born(self, q):
        if q in self.memo:
            return self.memo[q]
        self.memo[q] = None                      # guards recursion
        b, F = self._born0(q)
        if F is not None and F[1] == "u":
            fstamp, others = F[0], F[2]
            prev = max([c[0] for c in self.log[fstamp][0]], default=0)
            after_known = (prev, ("\U0010ffff", 0))
            def place(c):
                if others:
                    return (fstamp, _BIG, c)
                return c if c[0] >= after_known else (after_known, 0)
            surv = self._slot_cands(q, fstamp, place)
            win = self._plain_winner(q)
            if win is not None and _o_plain(win[1]) and win[0] < fstamp:
                surv.append(place((win[0], 0)))
            below = {p for p in list(self.cands) + [x for w, (c, m) in self.log.items() for x in incs_of(m)] if p.startswith(q + ".")}
            for r in sorted(below):
                wr = self._plain_winner(r)
                if wr is not None and _o_plain(wr[1]) and wr[0] < fstamp:
                    br = self.born(r)
                    surv.append(place(br if br is not None else (wr[0], 0)))
                surv += self._slot_cands(r, fstamp, place)
            for c in surv:
                b = c if b is None or c < b else b
        par = _o_parent(q) or ""
        lv = self.lvl.get(par)
        if b is not None and lv is not None and b[0] < lv:
            b = (lv, _BIG, b)
        self.memo[q] = b
        return b

    def level_order(self, level, fields):
        """`fields` ordered by (born, path); a field with no born last."""
        def key(f):
            q = (level + "." if level else "") + f
            b = self.born(q)
            return (1, (), q) if b is None else (0, b, q)
        return sorted(fields, key=key)


def _item_summary(items):
    """What an item list says: the fields a write names, per parent, in the order it names them; which of them it resets; the paths it unsets; the levels it marks."""
    names, resets, unsets, levels = {}, set(), set(), set()
    for q, md in items:
        if md == "u":
            unsets.add(q)
        elif md == "L":
            levels.add(q)
        else:
            lst = names.setdefault(_o_parent(q), [])
            if q not in lst:
                lst.append(q)
            if md == "r":
                resets.add(q)
    return names, resets, unsets, levels


def _want_items(intent, view):
    """The order items a write with `intent` must carry, from the document its writer saw (`view`), derived from the design's words and not from the writer's
    code (Rust's `order_oracle::write_ord` does the same): a value written at a path names it; a document names the map, marks the level, names every field it
    holds as a reset and unsets what the writer saw below the path that it lacks; a scalar over a map unsets what was below; an unset removes the leaves and maps
    the writer saw, deepest first; every ancestor of a written path is named once before it; an unset of what the writer did not see is a no-op that leaves a
    floor (the hybrid, ruled 2026-11-03) at the end of an update that stands."""
    op, p, arg = intent
    view = view or {}
    out, anc_seen, pending = [], set(), []

    def below_unset(path, o):
        if isinstance(o, dict):
            for k, v in o.items():
                below_unset(path + "." + k, v)
        out.append((path, "u"))

    def value_at(path, cur, n, mode):
        if n is None:
            if cur is not None:
                below_unset(path, cur)
            return
        if isinstance(n, dict):
            curd = cur if isinstance(cur, dict) else None
            out.append((path, mode))
            out.append((path, "L"))
            for k, v in n.items():
                value_at(path + "." + k, curd.get(k) if curd is not None else None, v, "r")
            if curd is not None:
                for k, ov in curd.items():
                    if k not in n:
                        below_unset(path + "." + k, ov)
            return
        if isinstance(cur, dict):
            for k, ov in cur.items():
                below_unset(path + "." + k, ov)
        out.append((path, mode))

    def anc(q):
        for a in _o_ancestors(q):
            if a not in anc_seen:
                anc_seen.add(a)
                out.append((a, "w"))

    def one(o_, q, v):
        cur = _o_get(view, q)
        if o_ == "set":
            anc(q)
            value_at(q, cur, v, "w")
        elif o_ == "unset":
            if cur is None:
                pending.append((q, "u"))
            else:
                below_unset(q, cur)
        else:                                            # inc
            anc(q)
            if isinstance(cur, float):
                value_at(q, cur, cur + v, "w")
            else:
                out.append((q, "w"))

    if op == "insert":
        return _o_insert_items(arg)
    if op == "replace":
        out.append(("", "L"))
        for t, v in arg.items():
            value_at(t, view.get(t), v, "r")
        for t, ov in view.items():
            if t not in arg:
                below_unset(t, ov)
        return out
    if op in ("set", "unset", "inc"):
        one(op, p, arg)
    elif op == "setmany":
        for q, v in arg:
            one("set", q, v)
    elif op == "update":
        for o_, q, v in arg:
            one(o_, q, v)
    return out + pending


def order_item_errors(log):
    """The order items every write emitted against the items its intent and its writer's document give (`_want_items`). The rule above reads the logged items, so
    a writer that emitted a wrong item would agree with itself and with the state it built: this check is what makes the items independent of the writer.
    A write is compared as a summary (the names per parent in order, the resets, the unsets, the levels)."""
    errs = []
    for w, (ctx, m) in log.items():
        items = m.get("items")
        if items is None or m.get("upsert"):
            continue
        it = m.get("replay") or m.get("intent")
        if it is None and m["kind"] == "inc" and m.get("paths"):
            it = intent_of(m)
        if it is None or it[0] not in ("insert", "replace", "set", "unset", "inc", "setmany", "update"):
            continue
        want = _item_summary(_want_items(it, oracle_at(log, ctx)))
        got = _item_summary(items)
        if want != got:
            names = ["names", "resets", "unsets", "levels"]
            diff = [n for n, a, b in zip(names, want, got) if a != b]
            errs.append((w, "items differ from the intent: " + ",".join(diff), [(a, b) for a, b in zip(want, got) if a != b][:1]))
    return errs


def _levels(d, path=""):
    """(level path, the field names of that level in served order) for every level of a document."""
    if isinstance(d, dict):
        yield path, list(d.keys())
        for k, v in d.items():
            yield from _levels(v, (path + "." if path else "") + k)


def order_errors(state, log):
    """Random-run check of the derived order. Returns (errors, deviations).
    The contract (design, field-order D-item and ruling (a)): the order is the outcome of SOME serial order of the same writes, values and order from ONE
    order. The stamp-ordered serial history is tried first, then every linear extension of the causal order (at most ORDER_EXT_BOUND complete ones).
    A history with more than one alive life (an upsert life included) has no serial oracle and is skipped (ruling of 2026-11-04).
    In a TOTALLY ORDERED history (no two writes concurrent) the stamp order is the only serial order and no mechanism can act: any other order is an error.
    When no serial order explains the order, the history is a deviation only if the order RULE (`_Rule`: the design's rule restated from the log
    alone, never from a replicated Ord state: candidates, floors, per-writer first candidates, the survivor clamp, the level floor, `(born, path)`)
    gives the served order of EVERY level of the document, exactly. That is the placement test of every accepted mechanism at once (the no-op floor,
    Q4(a)'s r floor and level floor, the survivors of an unset, ruling (a), D6 summing); any other order is an error. A history whose values no
    serial order gives is checked the same way (it was skipped before)."""
    if not ORDER_STATE:
        return [], []
    vis = state.visible()
    if vis is None:
        return [], []
    if sum(m["kind"] == "insert" or bool(m.get("insert")) for c, m in log.values()) != 1 or any(m["kind"] == "delete" for c, m in log.values()):
        return [], []                      # one life only (a creating `$inc` is the life's insert): the reference has no merge of concurrent lives
    if len(state.alive()) != 1:
        ORDER_STATS["skipped (more than one alive life: no serial oracle)"] += 1
        return [], []
    ie = order_item_errors(log)
    if ie:
        ORDER_STATS["ERROR: a write's order items do not follow from its intent"] += 1
        return [("order-items",) + e for e in ie[:3]], []
    ref = stamp_outcome(log)                         # 'invalid' when an operation fails in the stamp order (then only the walk and the rule decide)
    stamp_values_ok = ref not in ("invalid", None) and sortdoc(vis) == sortdoc(ref) and M_TYPES(vis) == M_TYPES(ref)
    stamp_bad = _cmp_levels(vis, ref) if stamp_values_ok else None
    if stamp_bad == []:
        ORDER_STATS["compared"] += 1
        return [], []                                # the stamp-ordered serial history gives the document and its order
    if totally_ordered(log):
        # a sequential history has ONE serial order and no concurrency: none of the ruled mechanisms can act in it, so the stamp order is the only answer
        if ref in ("invalid", None):                     # no serial reading to compare with: the order rule decides (an error if it disagrees)
            rule = _Rule(log)
            errs = [("order", level, keys, rule.level_order(level, keys)) for level, keys in _levels(vis) if rule.level_order(level, keys) != keys]
            if errs:
                return errs, []
            ORDER_STATS["sequential history the reference cannot replay: placed by the order rule"] += 1
            return [], []
        bad = _cmp_levels(vis, ref)                      # the relative order of the fields both documents hold (values may differ: they are graded elsewhere)
        if bad:
            ORDER_STATS["ERROR: a sequential history's order is not the serial order"] += 1
            return [("order",) + b for b in bad], []
        ORDER_STATS["compared"] += 1
        return [], []
    gen, st = serial_orders(log)
    first_bad = None
    for order in gen:
        out = serial_outcome(log, order)
        if out in ("invalid", None) or sortdoc(vis) != sortdoc(out) or M_TYPES(vis) != M_TYPES(out):
            continue
        mism = _cmp_levels(vis, out)
        if not mism:
            ORDER_STATS["compared (some serial order, not the stamp order)"] += 1
            return [], []
        if first_bad is None:
            first_bad = mism
    if st["cut"]:
        ORDER_STATS["walk cut at ORDER_EXT_BOUND: decided by the order rule"] += 1
    rule = _Rule(log)
    errs = []
    for level, keys in _levels(vis):
        exp = rule.level_order(level, keys)
        if exp != keys:
            errs.append(("order", level, keys, exp))
    if errs:
        return errs, []
    ORDER_STATS["deviation, placed by the order rule"] += 1
    return [], (first_bad or stamp_bad or [("", [], [])])


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
        haveh = {(k[1], k[2]) for k in have}
        missing = {h for h in must if not covered(h, haveh, log)}
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
