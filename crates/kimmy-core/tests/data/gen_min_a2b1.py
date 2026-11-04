"""Q7 Step 0, item MIN-A2B1: the exhaustive condition-(a) class MIN-A2B1 of the model's brute-force checker, as a fixture for a Rust test.

The class (`suite6.brute_pinned`, pins/MIN-A2B1.json): the 18 operations below on writers A (two operations) and B (one), every causal
pattern of who-has-seen-what, every stamp order that extends causality. This script runs `brute.run_class` of the reviewed model tree
UNCHANGED (read only; `model6.py` 72da18eb, `pullmodel6.py` e68fbe04, `brute.py` 0e29ba34) and records, through a hook on
`brute.check_history` and `brute.run_model` that changes no result, for every history the model's final ORDERED document after each
stamp order, as the FNV-1a 64 hash of its text (`doc_text`, written by the Rust test in the same words), and how many of the stamp
orders the checker found "ok" (the model's document is the outcome of some serial order).

The stats the run returns are checked against the pins (`pins/MIN-A2B1.json`) before anything is written: the fixture is only written
when every pinned count is reproduced.

Output (`min_a2b1.txt`), a header of `#` lines and one line per history:
    <op indexes of A0,A1,B0>;<receives>;<the hashes of its stamp orders, 16 hex digits each, in the order of the linear extensions>;<ok>
A receive token `A1:B1` is "before its operation 1, A holds B's first operation and its past". A history with no valid serial order
is not compared (no stamp order is run): its hash field is empty.

Usage:  Q7MODEL_DIR=<q7step0-model> python3 -I -B gen_min_a2b1.py <out-dir>     (about 110 s, single process)
        GEN_LIMIT=<n> limits the histories (for trying the script out).
"""
import hashlib, json, os, sys, time

sys.dont_write_bytecode = True
MODEL = os.environ["Q7MODEL_DIR"]
sys.path.insert(0, MODEL)
import brute as B                                  # noqa: E402
import model6 as M                                 # noqa: E402

OPS = "setm,swap,repl2,seta,unseta,setc,setmr,unsetmp,unsetm,unsetb,inca,incd,setmany,setnull,setmscalar,incnew,upd,upd2".split(",")
SHAPE = {"A": 2, "B": 1}
OK_KEY = "ok: the model's document is the outcome of some serial order"


def fnv(text):
    h = 0xcbf29ce484222325
    for b in text.encode("utf-8"):
        h ^= b
        h = (h * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return "%016x" % h


def doc_text(o):
    """The ordered document as text: `{k:v,...}` in field order, a leaf `i<int>` or `n` (a present null)."""
    if o is None:
        return "-"
    if isinstance(o, list):
        return "{" + ",".join("%s:%s" % (k, doc_text(v)) for k, v in o) + "}"
    if o == M.NULL:
        return "n"
    assert isinstance(o, int) and not isinstance(o, bool), o
    return "i%d" % o


def op_index(op):
    for k, name in enumerate(OPS):
        if B.POOL[name] == op:
            return k
    raise AssertionError(op)


LINES, REC = [], []
_run_model, _check_history = B.run_model, B.check_history


def run_model(h, pi):
    res, log, noop = _run_model(h, pi)
    REC.append(fnv(doc_text(M.ordered(res) if res is not None else None)))
    return res, log, noop


def check_history(h, stats, examples, want_examples=6):
    REC.clear()
    ok0 = stats[OK_KEY]
    _check_history(h, stats, examples, want_examples)
    ops = ",".join(str(op_index(o)) for w in h.writers for o in h.ops[w])
    recv = ",".join("%s%d:%s%d" % (w, j, k, c) for (w, j), v in sorted(h.recv.items()) for k, c in v)
    LINES.append("%s;%s;%s;%d" % (ops, recv, "".join(REC), stats[OK_KEY] - ok0))


B.run_model, B.check_history = run_model, check_history


def main():
    out = sys.argv[1]
    limit = int(os.environ["GEN_LIMIT"]) if os.environ.get("GEN_LIMIT") else None
    t0 = time.time()
    stats, _, dt = B.run_class("MIN", OPS, SHAPE, limit=limit)
    pins = json.load(open(os.path.join(MODEL, "pins", "MIN-A2B1.json")))
    if limit is None:
        diff = {k: (pins.get(k), stats.get(k)) for k in set(pins) | set(stats) if pins.get(k) != stats.get(k)}
        assert not diff, ("the pinned counts of MIN-A2B1 are not reproduced", diff)
    hashes = {f: hashlib.sha256(open(os.path.join(MODEL, f), "rb").read()).hexdigest() for f in ("model6.py", "pullmodel6.py", "brute.py")}
    head = ["# MIN-A2B1: the model's stamp orders of the exhaustive class A2B1, one line per history (see gen_min_a2b1.py)",
            "# model6.py " + hashes["model6.py"], "# pullmodel6.py " + hashes["pullmodel6.py"], "# brute.py " + hashes["brute.py"],
            "# ops " + ",".join(OPS)]
    head += ["# stat %s = %d" % (k, v) for k, v in sorted(stats.items()) if not k.startswith("counterexample")]
    open(os.path.join(out, "min_a2b1.txt"), "w").write("\n".join(head + LINES) + "\n")
    print("histories", len(LINES), "seconds", round(time.time() - t0, 1), "limit", limit)
    for k, v in sorted(stats.items()):
        if not k.startswith("counterexample"):
            print("  ", v, k)


main()
