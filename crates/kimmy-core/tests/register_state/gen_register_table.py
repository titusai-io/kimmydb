"""gen_register_table.py (NEW, Q6.2): the differential table of the product's pure register function (`kimmy-core::register_state`, ADR-272 decisions 3, 5, 8) against the model.
Each case is a small scenario of one logical name's register events plus the readiness of the physicals they name; the expected answers are what swapmodel6g.py's `Member` makes of it:
  visible  = the member's `cur` after `settle` (`newest_applicable` and `enter`),
  burials  = the physicals `sweep` buried (D4, bound out; DOOM off) with the stamp of the visible event,
  superseded = `first_superseding` of each physical (D11/D10: the earliest valid event above the physical's birth that is a logical drop or names another physical).
Rule B is off here (every event given is in effect: coverage is the caller's input in the product), DOOM is off (the DOOM commit regenerates its own rows). The events reach the model in a shuffled order and in the given order, and the two answers must agree
(the monotone claim of decision 5), or the generator stops.
usage: gen_register_table.py [cases] > crates/kimmy-core/tests/register_state/table.rs   (run from the directory that holds swapmodel6g.py; the generator imports it from its own directory)     (the header names the model's sha256[:16])"""
import hashlib, os, random, sys
sys.dont_write_bytecode = True
os.environ.update(Q6_RULE_B="0", Q6_DOOM="0")
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import swapmodel6g as M

N = int(sys.argv[1]) if len(sys.argv) > 1 else 400
SHA = hashlib.sha256(open(M.__file__, "rb").read()).hexdigest()[:16]
ORIGINS = "ABC"
rng = random.Random(20261122)


def scenario():
    n_stage = rng.randint(1, 3)
    stages = [f"S{i}" for i in range(1, n_stage + 1)]
    recs = [f"R{i}" for i in range(1, rng.randint(0, 2) + 1)]
    physicals = ["L"] + stages + recs
    ready = {p: rng.choices(["alive", "missing", "buried"], [70, 15, 15])[0] for p in physicals}
    if rng.random() < 0.7:
        ready["L"] = "alive"
    used_hlc = set()
    def hlc():
        # one hlc per event in a case, so that no two events share a stamp (a stamp belongs to one entry); two events at one hlc and two origins
        # are exercised by the unit tests of the timeline
        while True:
            h = rng.randint(10, 70)
            if h not in used_hlc:
                used_hlc.add(h)
                return h
    creation, deadline = {}, {}
    for s in stages:
        c = rng.randint(3, 40)
        creation[s], deadline[s] = c, c + rng.randint(3, 25)
    events = []
    for s in stages:
        if rng.random() < 0.9:
            h = hlc()
            if rng.random() < 0.2:
                h = deadline[s] + rng.randint(1, 6)       # a binding above its stage's deadline: invalid, history
                while h in used_hlc:
                    h += 1
                used_hlc.add(h)
            events.append(("bind", ORIGINS[rng.randrange(3)], h, s))
    for r in recs:
        if rng.random() < 0.5:
            events.append(("lcreate", ORIGINS[rng.randrange(3)], hlc(), r))
    for _ in range(1 if rng.random() < 0.3 else 0):
        events.append(("ldrop", ORIGINS[rng.randrange(3)], hlc(), rng.choice(physicals)))
    return physicals, ready, stages, creation, deadline, events


def run(case, order):
    physicals, ready, stages, creation, deadline, events = case
    flags = M.flags_for("v6", "v6")
    w = M.World([(n, 0) for n in ORIGINS], flags, random.Random(1), 20, False)
    m = w.members["A"]
    for p in physicals:
        if p == "L":
            continue
        m.phys[p] = M.Phys("stage" if p.startswith("S") else "rc")
    for p, r in ready.items():
        ph = m.phys[p]
        ph.alive = r == "alive" or r == "missing"
        ph.missing = r == "missing"
        if not ph.alive:
            ph.kill = (0, "A")
    initially_alive = {p for p in physicals if m.phys[p].alive}
    es = []
    for kind, o, h, ph in events:
        dl = deadline.get(ph) if kind == "bind" else None
        e = M.Entry(o, h, kind, ph, dl=dl)
        e.idx = len(w.all); w.all.append(e)
        es.append(e)
    for e in (es if order == "given" else rng.sample(es, len(es))):
        if w.valid(e):
            m.add_event(e)
    m.settle()
    cur = None if m.cur is None else (m.cur.kind, m.cur.phys, m.cur.hlc, m.cur.origin)
    burials = sorted((p, ph.kill) for p, ph in m.phys.items() if p in initially_alive and not ph.alive)
    sup = {}
    for p in physicals:
        ev = m.first_superseding(p)
        sup[p] = None if ev is None else (ev.hlc, ev.origin)
    return cur, burials, sup


def rs_str(s):
    return '"' + s + '"'


out = []
for i in range(N):
    case = scenario()
    a = run(case, "given")
    b = run(case, "shuffled")
    if a != b:
        sys.exit(f"case {i}: the model answers differ by arrival order: {a} vs {b} for {case}")
    physicals, ready, stages, creation, deadline, events = case
    cur, burials, sup = a
    vis = "Vis::Identity" if cur is None else (f'Vis::Drop({cur[2]}, {ORIGINS.index(cur[3])})' if cur[0] == "ldrop" else f'Vis::{"Bind" if cur[0] == "bind" else "Create"}({rs_str(cur[1])}, {cur[2]}, {ORIGINS.index(cur[3])})')
    out.append(f"""    Case {{
        physicals: &[{", ".join(f'({rs_str(p)}, Ready::{ready[p].capitalize()})' for p in physicals)}],
        stages: &[{", ".join(f'({rs_str(s)}, {creation[s]}, {deadline[s]})' for s in stages)}],
        events: &[{", ".join(f'({rs_str(k)}, {ORIGINS.index(o)}, {h}, {rs_str(p)})' for k, o, h, p in events)}],
        visible: {vis},
        burials: &[{", ".join(f'({rs_str(p)}, {k[0]}, {ORIGINS.index(k[1])})' for p, k in burials)}],
        superseded: &[{", ".join(f'({rs_str(p)}, {"None" if sup[p] is None else f"Some(({sup[p][0]}, {ORIGINS.index(sup[p][1])}))"})' for p in physicals)}],
    }},""")
print(f"""//! The model's answers for the pure register function (generated; do not edit by hand).
//!
//! Generated by `kimmydb-reviews-0470-0480/q6v2-model/gen_register_table.py {N}` from `swapmodel6g.py`, sha256[:16] {SHA}.
//! Each case: the physicals of one logical name and their readiness here, the stages (name, creation hlc, deadline), the register
//! events (kind, origin index, hlc, physical), and what the model's `Member` makes of them: the visible event, the physicals the
//! bound-out burial buries (with the visible event's stamp) and, for every physical, the earliest valid event that supersedes it.
//! The model answered every case identically for the given order of the events and for a shuffled one.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ready {{
    Alive,
    Missing,
    Buried,
}}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Vis {{
    Identity,
    Bind(&'static str, u64, usize),
    Create(&'static str, u64, usize),
    Drop(u64, usize),
}}

pub struct Case {{
    pub physicals: &'static [(&'static str, Ready)],
    pub stages: &'static [(&'static str, u64, u64)],
    pub events: &'static [(&'static str, usize, u64, &'static str)],
    pub visible: Vis,
    pub burials: &'static [(&'static str, u64, usize)],
    pub superseded: &'static [(&'static str, Option<(u64, usize)>)],
}}

pub const MODEL_SHA: &str = "{SHA}";

pub const CASES: &[Case] = &[""")
print("\n".join(out))
print("];")
