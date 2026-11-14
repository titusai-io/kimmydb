"""[swapmodel6e.py: a COPY of swapmodel6d.py (sha256[:16] 409eb9d10f35682c, untouched) with exactly two changes, no draw added or moved on any existing
  path. (1) Build item (a): a released held entry (the release path of `take_from`) takes effect (`settle`) in the same step it becomes covered and served
  (the 6d-delta reviewer's swapmodel6dps2.py, `Q6_PS_RELEASE_SETTLE=1`); the DEFAULT here, off-switch `Q6_RELEASE_SETTLE=0` (bridge/mutant rows only). The release
  path only exists when `World.held_uncovered` is set (the doc routes, or `Q6_PS=1` = product semantics on the model's own routes, as in swapmodel6dps.py:
  held entry not covered, FIX-1, release rules, no witness of a held entry at quiescence or in replay, no held entry in a snapshot's held set).
  (2) O8, D9's orphan AGE fallback (manager's ruling 2026-11-12): a member whose own clock (the wall component of its HLC: world clock + skew) >= the stage's DEADLINE stamp + `Q6_AGE` buries an UNBOUND
  stage by age, only at a member with NO valid binding for it, in effect or known (events, parked information, held out of position); abandon and the
  coverage safety net act first. `Q6_AGE` is the named parameter, in model clock units = steps (1 step = 1 clock unit); the model has no tombstone retention,
  so it is STATED here: the model's retention R is 300 clock units (= half of the longest run, the 600-step heavy rows; the model claims convergence only
  within retention), so the "2 x tombstone_retention" setting is Q6_AGE=600 (the DEFAULT: the clock never reaches a deadline + 600 in any run, so it cannot fire)
  and the short settings that fire in the grid are set by Q6_AGE (e.g. 120 = 0.4 R). A first choice, R = 120 (2 x = 240), was WRONG: the 600-step in-order
  heavy row has the fallback fire (710 of 1,500 runs diverge) because the model's delivery backlog grows with the run length (one entry moves per step).
  Q6_AGE=0 turns the fallback off. Mutants: `m-age-ignores-binding` (buries by age even where a valid binding is known or in effect),
  `m-age-ignores-held-binding` (ignores only a binding known but not in effect). The oracle is age-aware for lingering stages only (see `finish`).]

[swapmodel6d.py: a COPY of swapmodel6b.py (sha256 4343f520..., untouched) with (a) the product-shaped Document routes of the B1 overlay
  (q6rev/df/b1port.py, rule for rule) in the model proper, OFF by default: `Q6_ROUTES` may name `doc_hold` (a document entry the source holds
  is delivered out of position: applied to state, held, not appended) and `doc_repair` (a scoped repair: the source's pages land as held
  state, nothing completes, nothing is granted). Either one sets `World.held_uncovered` (an entry held out of position is not covered,
  is released by an in-window re-delivery or by `promote_oop`, and is not in a snapshot's held set). HB-1 is REMOVED: a held entry applied
  later by `retry_deferred` is not appended while it is still held out of position (`apply_doc`). The model's own routes and the doc routes
  are not mixed in one run. (b) The v2.6 staged-row rule (`staged_row`, ON in base v6, off-switch `Q6_STAGEDROW=0`, mutant
  `m-staged-row-off`): a staging entry that arrives IN POSITION is appended and served onward even when a version already held here
  supersedes it, whatever that version's origin or coverage; never a held (or held-then-deferred) entry.]

[swapmodel6.py: a COPY of swapmodel5.py (sha256 6254857e9c16392e, untouched) with RULE B (the design change of v2.4, after the
review q6ooo-rev.md). Base `v6` = `v5` + rule B + the product's grant + the interleaved replay + the split oracle.
  Rule B: a register event (bind, logical drop, recreate) takes effect on a member only once the member covers the event's origin
    up to its stamp (the coverage predicate). One that arrives ahead of its position is kept as INFORMATION (`Member.info`): it does
    not enter the register, set its physical's birth (`bmap`), make anything visible, bury anything or make a write late until it is
    covered; its taking effect is evaluated as an arrival (`activate_info`, with D11's rescan). D10's refusal counts it
    (`refusal_cause`). In order it never fires (`info_parked` = 0). Off-switch: environment `Q6_RULE_B=0` (default ON), or the mutant
    mode `m-rule-b-off`; only meaningful with the route switches below.
  Invariant (i), the grant invariant: no page and no grant names an entry above its sender's contiguous coverage. The grant is the
    sender's contiguous coverage per origin and only that prefix of the sender's held set is granted (`snapshot_begin`); the counters
    `grant_above_coverage` / `page_above_coverage` check it (must be 0; 6e: page_above_coverage is INFORMATION `cnt_page_carries_held_state` under held_uncovered).
  Route probes (default OFF; the base is the in-order model): `Q6_F` (probability, per window step, of a route step instead of the
    window; the random draw is made only when `Q6_F` > 0, so `Q6_F`=0 is REPORT5's random sequence exactly) and `Q6_ROUTES` (a comma
    list drawn from `hold`, `repair`; default `hold,repair`). `hold`: a non-document entry of the source's held set is delivered
    out of position (held, not appended, becomes servable once its position is reached). `repair`: the source's register events are
    handed over as information only (no coverage). The restore route is not modelled.
  Replay checks in `finish`: `div_replay` is the interleaved replay (each replica repeatedly picks a random origin with entries left,
    keeps that origin's order, takes its next entry from a random live member that appended it, or witnesses it if none did, then
    sweeps once in stamp order); `div_replay_stamp_order` is the old check (ONE replica taking every surviving entry in stamp order
    through the product relay). The oracle's allowed set is the split oracle of the manager's seed-530 ruling (`oracle`).
  Seed 885 rule (`dead_page_retained`, ON in base v6, off-switch `Q6_DEADPAGE=0`, mutant `m-dead-page-dropped`): a snapshot page's document
    versions of a physical already buried on the receiver are history on arrival: retained as entries (F4), evaluated by D11 at once and on every
    later register event, carried onward by the receiver's own snapshots (`land_page`).
  Refusal durations (`Member.refusal_eps`, `refusal_episode_stats`): see the function.
 swapmodel5.py is untouched.]

[swapmodel5.py: a COPY of swapmodel4.py that models the PAGED snapshot's register route (delta at v2.3): the grant is the sender's
vector as served with the FIRST page (the payload's held set); the register events are read from the sender when the LAST page
lands. Base `v5` applies the D3 rule: a snapshot carries only the register events its grant covers (stamped at or below the first
page's vector for their origin); later events arrive by windows. `v5-late-register` / mutant `m-snap-register-late`: the events are
read at the last page WITHOUT the filter (the reviewer's q6rev/v23late.py variant). swapmodel4.py is untouched.]

[swapmodel4.py: a COPY of swapmodel3b.py whose BASE `v4` is v3red2 (v3red + the F5 dedup) with the PRODUCT'S relay semantics
(after q6rev/v22relay.py): a member relays only the entries it APPENDED; register events, creates, abandons and loss
entries are always appended; a receiver WITNESSES, without receiving, the earlier entries its source skipped; windows
are served in stamp order; ADR-224 refuses a window across history a COMPLETED snapshot holds as state; LWW losers are not
kept; F4 history is kept but not relayed; NO prefix closure at death. Gap-1 fix (append in position), gap-2 fix (snapshot
deletes carry the _id as an entry) and the per-head oracle (D11) are in; mutants m-no-append-in-position,
m-snap-delete-bare, m-ignored-event-not-served, m-no-defer-received, m-no-pending-dedup. swapmodel3b.py is untouched.]

[swapmodel3b.py: a COPY of swapmodel3.py with (1) a dead-vanish semantic, (2) per-write event counters and the
"vanish after replication" check, (3) measurement hooks (hist_kept, snap_carried), (4) mutant m-record-per-minter.
swapmodel3.py is the v2.2 evidence of REPORT3.md and is untouched.]

Q6 design v2.2 model: the atomic `$out` swap, rules R1-R10 as corrected by the v2.1 re-review
(V21-M1..M5, V21-m1..m5), under the `document` merge policy (whole-document last writer wins by stamp).

This is a COPY of swapmodel2.py (the v2.1 evidence, untouched) with these changes, tagged in the code:

  M1  every recreate makes a NEW physical named by its own event, birth = the event's stamp, no id
      reuse (`local_lcreate`, `add_event`). Mutant m-recreate-reuse reuses the head's identity.
  M3  a member refuses local writes (503, `refused` counted by cause) whenever ANY known valid
      register event (bind, ldrop, lcreate) supersedes the physical it serves (`refusal_cause`).
      Mutant m-pending-narrow is v2.1's narrower pending. New invariant write_accepted_while_superseded.
  M4  loss records are ordinary entries inside their appender's coverage prefix (`report`: cover=True,
      minted under the reporter's own origin above everything it has witnessed). Mutant
      m-loss-outside-prefix is the v2.1 rule.
  M5  a snapshot carries STATE (physicals with docs, tombstone/orphan information, register events, loss
      records, abandoned stages, a vector grant), not `dochist` and not oplog entries. The grant is
      recorded when the LAST page lands, in one step with the register events and the receiver's own
      D4 evaluation (`snapshot_begin`, `land_page`, `complete_join`). Mutant m-grant-before-pages applies
      the grant at the start.
  m1  no retained late-entry set: a member keeps (`dochist`) its own writes and the entries it applied
      while the physical was alive; an entry judged history on arrival is evaluated ONCE and dropped
      (`apply_doc`). D11 is evaluated over that oplog (`check_late`, `rescan`).
  m3  a write is reported only once its physical is buried on the reporting member, re-evaluated after
      every burial (`check_late`, `bury`). Invariant record_for_served_write_at_member, every step.
      Mutant m-report-before-burial is v2.1's rule.
  m4  no heartbeat. The orphan witness is the writer's own `abandon(stage)` entry; the
      coverage-past-deadline rule stays as a safety net (with a witness entry if one exists). Writer
      modes: good, late, crash-abandon, vanish (never abandons, never writes again: its stage lingers).
  m5  definitions across a logical drop: defmodel3.py.

Delivery is adversarial as in swapmodel2.py (any member forwards any entry it holds to any other, in any
order; relays; clock skew); snapshot joins are random, lazy ones land their pages over later steps.

Entries each member holds in this model: `held` = ids of the entries it has taken (the coverage claim,
including whatever a snapshot grant adds); `dochist` = its own writes plus the entries it applied while the
physical was alive (retained oplog); entries judged history on arrival are not retained; `deferred` =
entries addressed to a physical not known yet. No GC is modelled: convergence is claimed only within oplog
retention.

Usage:
  python3 swapmodel3.py --seeds 1500 --members 2,3,4,5 --outs 1,2,3 --keys 3,4,5 --grid full --modes v3
  python3 swapmodel3.py --show SEED --members 3 --outs 2 --keys 4 --mode v3
Modes can be combined with '+', e.g. m-orphan-age+m-snap-no-orphan-burials.
"""

import argparse
import os
import statistics
import multiprocessing
import random
from collections import Counter, defaultdict

L = "L"
NEG = (-1, "")  # the identity physical counts as bound at minus infinity


# ----------------------------------------------------------------------------- flags

DEFAULTS = dict(
    keep=False,
    digest=False,
    pend_accepts=False,
    pend_narrow=False,
    no_loss=False,
    visible_newest=False,
    bury_without_alive=False,
    stage_buried_early=False,
    bt_early=False,
    ldrop_identity_only=False,
    no_recreate_floor=False,
    recreate_reuse=False,
    orphan_age=None,
    s_trigger="staging",  # 6e addendum: rule S's trigger: staging (e.staging, 6d exactly) | bind (S-bind) | bindorigin (S-bind-origin); env Q6_TRIGGER
    age=0,  # 6e: the orphan age fallback, clock units after the deadline (0 = off); set from Q6_AGE in flags_for
    age_ignores_binding=False,
    age_ignores_held=False,
    age_tomb=True,  # 6f: an AGE tombstone is applied by a snapshot receiver only if its own age rule would bury the stage now (WHEN and WHERE)
    orphan_no_deadline=False,
    loss_key_collapse=False,
    loss_outside_prefix=False,
    report_before_burial=False,
    snap_drop_loss=False,
    snap_no_loss=False,
    snap_no_events=False,
    snap_no_orphan_burials=False,
    grant_before_pages=False,
    snap_with_dochist=False,
    defer_received=False,
    state_scan=False,
    retain_history=False,
    snap_late_entries=False,
    record_per_minter=False,
    pending_dedup=False,
    product_relay=False,
    append_in_position=False,
    snap_versions_as_entries=False,
    snap_delete_bare=False,
    ignored_not_served=False,
    paged_register_read=False,
    register_grant_filter=False,
    stale_src_bug=False,
    rule_b=False,  # v6: a register event takes effect only once covered; one ahead of its position is information (see the docstring)
    grant_prefix=False,  # v6: the product's grant: the sender's contiguous coverage, and only that prefix of its held set
    loss_in_position=False,  # 6r (seed 1434): a loss entry enters the member's state only in position (rule B for records)
    loss_grant_filter=False,  # 6r (seed 1434): a snapshot carries a record only if an entry of it is within the grant
    paged_loss_read=False,  # 6b (companion clause): the sender's loss records are read when the LAST page lands (as the register events are), not at the first page
    held_strict=False,  # 6d (FIX-1, D9): on the doc routes a held entry raises no vector: it is marked held before it is applied, and D9's deadline witness must be covered
    replay_skips_held=False,  # 6d: on the doc routes the interleaved replay does not witness away an entry a live member holds out of position
    staged_row=False,  # 6d (v2.6, DF-1): a staging entry arriving in position is appended even when superseded on arrival
    dead_page_retained=False,  # v6 (seed 885): a snapshot page's versions of a physical buried on the receiver are history on arrival: retained (F4)
)

BASES = {
    "v3": {},
    # the fix for finding F3: a loss record RECEIVED for a write this member still serves is held and forwarded
    # but enters the member's state (and so emits its outReplaced event) only when the write is no longer served
    # F3 + F4: also, at the burial of a physical, report the document versions in its STATE that are late (the
    # heads), so a late write is reported by any live member that holds it as state, not only by a member that holds
    # its entry (a snapshot carries state, not entries)
    "v3fix": {"defer_received": True, "state_scan": True},
    # F4 (the residual): a late write whose origin is gone for good and which no live member applied while its
    # physical served is lost for the record set. Redundancy fix (the cost V21-m1 named): members also keep the
    # entries judged history on arrival, and a snapshot carries the sender's retained entries of physicals the
    # sender has buried.
    # F5 (found by the per-write event counter): a member that holds a record DEFERRED (received, its physical not
    # buried here yet) must not mint the same record itself when it buries the physical, or the event is emitted twice
    "v3fix2": {"defer_received": True, "state_scan": True, "pending_dedup": True},
    "v3red2": {"defer_received": True, "state_scan": True, "retain_history": True, "snap_late_entries": True,
               "pending_dedup": True},
    "v3red": {"defer_received": True, "state_scan": True, "retain_history": True, "snap_late_entries": True},
    # v4: v3red2 + the product's relay + gap-1 fix + gap-2 fix; the per-head oracle applies whenever product_relay is on
    # v5: v4 + the paged snapshot reads its register events when the last page lands and the D3 rule filters them to the grant
    "v5": {"defer_received": True, "state_scan": True, "retain_history": True, "snap_late_entries": True,
           "pending_dedup": True, "product_relay": True, "append_in_position": True, "snap_versions_as_entries": True,
           "paged_register_read": True, "register_grant_filter": True},
    # the reviewer's variant (q6rev/v23late.py): events read at the last page, grant = the first page's held set, no filter
    "v5-late-register": {"defer_received": True, "state_scan": True, "retain_history": True, "snap_late_entries": True,
                         "pending_dedup": True, "product_relay": True, "append_in_position": True,
                         "snap_versions_as_entries": True, "paged_register_read": True, "register_grant_filter": False},
    # the reviewer's patch AS WRITTEN: it remembers the last snapshot_begin() call's source even when that call is refused
    # (the joiner already has a join in progress), so the pending join's last page reads the register of a member that is NOT its sender
    "v5-late-register-literal": {"defer_received": True, "state_scan": True, "retain_history": True, "snap_late_entries": True,
                                 "pending_dedup": True, "product_relay": True, "append_in_position": True,
                                 "snap_versions_as_entries": True, "paged_register_read": True,
                                 "register_grant_filter": False, "stale_src_bug": True},
    # the reviewer's patch as written, WITH the D3 rule applied: the events are limited to the grant (reading them from the wrong
    # member is then harmless: whatever is stamped at or below the grant's vector is covered)
    "v5-literal-with-rule": {"defer_received": True, "state_scan": True, "retain_history": True, "snap_late_entries": True,
                             "pending_dedup": True, "product_relay": True, "append_in_position": True,
                             "snap_versions_as_entries": True, "paged_register_read": True,
                             "register_grant_filter": True, "stale_src_bug": True},
    "v4": {"defer_received": True, "state_scan": True, "retain_history": True, "snap_late_entries": True,
           "pending_dedup": True, "product_relay": True, "append_in_position": True, "snap_versions_as_entries": True},
    # v6: v5 + rule B + the product's grant (the oracle split and the interleaved replay are in `finish` for every base)
    "v6": {"defer_received": True, "state_scan": True, "retain_history": True, "snap_late_entries": True,
           "pending_dedup": True, "product_relay": True, "append_in_position": True, "snap_versions_as_entries": True,
           "paged_register_read": True, "register_grant_filter": True, "rule_b": True, "grant_prefix": True,
           "dead_page_retained": True, "loss_in_position": True, "loss_grant_filter": True, "staged_row": True,
           "held_strict": True, "replay_skips_held": True},
    # an experiment, not a rule: the v2.1 snapshot that also carried the sender's retained entries
    "v3-snap-with-oplog": {"snap_with_dochist": True},
}

MUTANTS = {
    "m-recreate-reuse": {"recreate_reuse": True},
    "m-pending-narrow": {"pend_narrow": True},
    "m-loss-outside-prefix": {"loss_outside_prefix": True},
    "m-snap-drop-loss": {"snap_drop_loss": True},
    "m-snap-no-loss-records": {"snap_no_loss": True},
    "m-grant-before-pages": {"grant_before_pages": True},
    "m-snap-no-orphan-burials": {"snap_no_orphan_burials": True},
    "m-report-before-burial": {"report_before_burial": True},
    "m-keep": {"keep": True},
    "m-digest": {"digest": True},
    "m-pending-accepts": {"pend_accepts": True},
    "m-no-loss-records": {"no_loss": True},
    "m-visible-newest": {"visible_newest": True},
    "m-bury-without-alive": {"bury_without_alive": True},
    "m-stage-buried-early": {"stage_buried_early": True},
    "m-bound-out-tombstone-early": {"bt_early": True},
    "m-ldrop-identity-only": {"ldrop_identity_only": True},
    "m-no-recreate-floor": {"no_recreate_floor": True},
    "m-orphan-age": {"orphan_age": 25},
    "m-orphan-no-deadline": {"orphan_no_deadline": True},
    "m-loss-key-collapse": {"loss_key_collapse": True},
    "m-snap-no-events": {"snap_no_events": True},
    "m-record-per-minter": {"record_per_minter": True},
    "m-snap-register-late": {"paged_register_read": True, "register_grant_filter": False},
    "m-no-append-in-position": {"append_in_position": False},
    "m-snap-delete-bare": {"snap_delete_bare": True},
    "m-ignored-event-not-served": {"ignored_not_served": True},
    "m-no-defer-received": {"defer_received": False},
    "m-no-pending-dedup": {"pending_dedup": False},
    # v6: rule B off. Equivalent in order (the rule never fires there); meant for the route probes (Q6_F > 0)
    "m-rule-b-off": {"rule_b": False},
    # v6: the seed-885 rule off (a page version of a physical already buried on the receiver is evaluated once and dropped, as in v5)
    "m-dead-page-dropped": {"dead_page_retained": False},
    # 6r: the seed-1434 rule off (a loss entry received out of position enters the state at once, as in swapmodel6a)
    "m-loss-out-of-position": {"loss_in_position": False, "loss_grant_filter": False},
    # 6b: the companion clause's control and its mutant. Both read the sender's records at the LAST page (a paged join); `v6-paged-loss` applies the
    # clause (a record is carried only if an entry of it is within the grant), `m-snap-record-no-grant-filter` does not.
    "v6-paged-loss": {"paged_loss_read": True},
    "m-snap-record-no-grant-filter": {"paged_loss_read": True, "loss_grant_filter": False},
    # 6d: the v2.6 staged-row rule off (= Q6_STAGEDROW=0)
    "m-staged-row-off": {"staged_row": False},
    # 6d: FIX-1 off on the doc routes: a held entry is applied before it is marked held and may be D9's deadline witness (the overlay's behaviour)
    "m-held-entry-witnesses-the-deadline": {"held_strict": False},
    # 6e: the age fallback buries even where a valid binding is known (in effect, parked or held): must be KILLED (needs Q6_AGE short enough to fire)
    "m-age-ignores-binding": {"age_ignores_binding": True},
    "m-age-ignores-held-binding": {"age_ignores_held": True},
    # 6f: the sender's AGE tombstone is honoured unconditionally at a snapshot receiver (= 6e's behaviour = Q6_AGE_TOMB=0): must be KILLED (serve_not_alive by age > 0)
    "m-age-tombstone-unconditional": {"age_tomb": False},
}


def flags_for(mode, base="v3"):
    d = dict(DEFAULTS)
    d.update(BASES[base])
    for part in mode.split("+"):
        name, _, arg = part.partition(":")
        if name in BASES:
            d.update(BASES[name])
        elif name in MUTANTS:
            d.update(MUTANTS[name])
            if name == "m-orphan-age" and arg:
                d["orphan_age"] = int(arg)
        else:
            raise SystemExit(f"unknown mode {mode}")
    if os.environ.get("Q6_RULE_B", "1") == "0":
        d["rule_b"] = False  # the off-switch of rule B (default ON); only the v6 base has it on
    if os.environ.get("Q6_LOSSPOS", "1") == "0":
        d["loss_in_position"] = False  # 6r: the off-switch of the seed-1434 rule (both parts)
        d["loss_grant_filter"] = False
    if os.environ.get("Q6_DEADPAGE", "1") == "0":
        d["dead_page_retained"] = False  # the off-switch of the seed-885 rule (default ON); only the v6 base has it on
    if os.environ.get("Q6_STAGEDROW", "1") == "0":
        d["staged_row"] = False  # 6d: the off-switch of the staged-row rule (default ON); only the v6 base has it on
    if os.environ.get("Q6_HELDSTRICT", "1") == "0":
        d["held_strict"] = False  # 6d: the off-switch of FIX-1 on the doc routes (= m-held-entry-witnesses-the-deadline)
    d["s_trigger"] = os.environ.get("Q6_TRIGGER", "staging")
    assert d["s_trigger"] in ("staging", "bind", "bindorigin"), "Q6_TRIGGER: staging, bind, bindorigin"
    d["age_tomb"] = os.environ.get("Q6_AGE_TOMB", "1") != "0"  # 6f: the cause-tagged tombstone (default ON); =0 is 6e's behaviour
    d["age"] = int(os.environ.get("Q6_AGE", "600") or 0)  # 6e: 600 = 2 x the model's retention (300); 0 = off
    if os.environ.get("Q6_REPLAYSKIP", "1") == "0":
        d["replay_skips_held"] = False  # 6d: the interleaved replay witnesses held entries away (the overlay's behaviour)
    return type("Flags", (), d)()


# ----------------------------------------------------------------------------- entries


class Entry:
    __slots__ = ("idx", "origin", "hlc", "kind", "phys", "key", "body", "opk", "staging", "dl", "extra", "stamp")

    def __init__(self, origin, hlc, kind, phys=None, key=None, body=None, opk=None, staging=False, dl=None, extra=None):
        self.idx = -1
        self.origin, self.hlc, self.kind = origin, hlc, kind
        self.phys, self.key, self.body, self.opk, self.staging = phys, key, body, opk, staging
        self.dl, self.extra = dl, extra
        self.stamp = (hlc, origin)

    def __repr__(self):
        return f"{self.kind}{self.stamp}:{self.phys}:{self.key}:{self.opk}"


class Phys:
    __slots__ = ("kind", "alive", "docs", "kill", "kept", "missing", "orphan", "ghost", "age")

    def __init__(self, kind):
        self.kind = kind  # id | stage | rc
        self.alive = True
        self.docs = {}  # key -> (stamp, body or None)
        self.kill = None  # stamp recorded at burial
        self.kept = False  # m-keep: re-dated and kept by the ADR-231 path
        self.missing = False  # created by a page of a join whose grant has not landed yet
        self.orphan = False
        self.ghost = None
        self.age = False  # 6e: buried by the age fallback (here or by a snapshot that carried such a burial)


class Job:
    def __init__(self, writer, keys, kind, bind_step):
        self.writer, self.keys, self.kind, self.bind_step = writer, keys, kind, bind_step
        self.stage = None
        self.dl = None
        self.digest = None


# ----------------------------------------------------------------------------- world


class World:
    def __init__(self, members, flags, rng, D=20, trace=False):
        self.flags, self.rng, self.D = flags, rng, D
        self.members = {}
        self.all = []
        self.ent_step = {}
        self.by_origin = defaultdict(list)
        self.clock = 0
        self.step = 0
        self.stage_docs = defaultdict(list)  # stage -> staging entries (global truth)
        self.stats = Counter()
        self.rec_index = {}  # (phys, key, stamp) -> the document entry
        self.lost = set()  # entries held only by vanished (dead) members: lost with them
        self.lost_struct = None  # 6e: the oracle's lost set (see `run`); None = `lost` (scripted scenarios that set only `lost`)
        self.lost_late = set()
        self.snap_carried = []  # per snapshot join: retained entries carried
        self.pend_acked = set()  # idx of writes acknowledged while a pending binding was held
        self.sup_acked = set()  # idx of writes acknowledged while a known valid event superseded the served physical
        self.trace = trace
        self.quiescing = False  # set when the run's steps are over and the remaining entries are delivered
        self.steps_end = None  # the step number at which the run's steps ended
        self.held_uncovered = os.environ.get("Q6_PS") == "1"  # 6d: the doc routes are on (an entry held out of position is not covered; `run`); 6e: Q6_PS=1 = product semantics on every route
        self.binds = defaultdict(list)  # 6e: phys -> the bind entries naming it (check-only index for the age rule)
        self.age_final = False  # 6e: set while a replica of `finish` makes its final settle (the age rule is applied to the replayed state)
        self.bd = {}  # 6e: (valid bind idx, member name) -> (the member's wall clock when it first KNEW the binding: in effect, parked as information or held out of position, first known only at quiescence)
        self.age_log = []  # 6e: (member, stage, step, bound elsewhere at burial, via snapshot)
        self.sr_done = set()  # 6d: (member, idx) of the staged-row appends (to count the ones a hold withdraws at once)
        self.log = []
        for name, skew in members:
            self.members[name] = Member(self, name, skew)

    def record(self, e, cover=True):
        e.idx = len(self.all)
        self.all.append(e)
        self.ent_step[e.idx] = self.step  # 6f: the creation step (the bounded-delivery option)
        if cover:
            self.by_origin[e.origin].append(e)
        if e.kind == "bind":
            self.binds[e.phys].append(e)
        if e.kind == "doc":
            self.rec_index[(e.phys, e.key, e.stamp)] = e

    def bd_scan(self, final=False):
        """6e (manager's ruling A): the BINDING DELAY. Records, read-only and draw-free, the first moment each live member knows each valid binding. Delay = that moment's wall clock minus the stage's deadline."""
        for blist in self.binds.values():
            for b in blist:
                if not self.valid(b):
                    continue
                for m in self.members.values():
                    if m.vanished or (b.idx, m.name) in self.bd:
                        continue
                    if b.idx in m.events or b.idx in m.info or b.idx in m.held_oop:
                        self.bd[(b.idx, m.name)] = (m.age_wall(), final)

    def prefix_close(self):
        """Called when a member vanishes. In the real system an origin's entries travel in sequence, so a member that
        holds entry n of an origin holds every earlier one; the model's random delivery is any-order, which could leave
        a PERMANENT gap once the only holder of an earlier entry is dead. At the moment of death every live member
        receives the earlier entries of each origin up to the highest one it holds; what no live member holds beyond
        that is the unreplicated suffix, lost with the dead member."""
        live = [m for m in self.members.values() if not m.vanished]
        for m in live:
            for origin, lst in list(self.by_origin.items()):
                top = -1
                for i, e in enumerate(lst):
                    if e.idx in m.held:
                        top = i
                for e in lst[:top + 1]:
                    if e.idx not in m.held:
                        m.take(e)
                        self.stats["prefix_closure_entries"] += 1

    def valid(self, e):
        """R9: a binding stamped above its stage's deadline is invalid; pure function of the stamps."""
        if e.kind == "bind":
            return e.hlc <= e.dl
        return True

    def note(self, msg):
        if self.trace:
            self.log.append(f"[{self.step:3d} c={self.clock}] {msg}")


# ----------------------------------------------------------------------------- member


class Member:
    def __init__(self, world, name, skew):
        self.w, self.name, self.skew = world, name, skew
        self.replica = name[0] in "ZI" and name[1:].isdigit()  # 6e: a check-only replica of `finish` (see age_due)
        self.hlc = 0
        self.held = set()
        self.phys = {L: Phys("id")}
        self.events = {}  # idx -> entry: valid bind / lcreate / ldrop entries held
        self.evl = []  # the same, sorted by stamp
        self.bmap = {L: NEG}  # physical -> birth stamp (M1: one birth per physical, the stamp of its own event)
        self.creates = {}
        self.abandoned = set()  # m4: stages whose writer abandoned them
        self.cur = None  # the visible event, None = the identity
        self.loss = {}  # key -> body: the records in this member's state (their events are emitted)
        self.loss_pending = {}  # v3fix: received records whose write's physical is not buried here yet
        self.loss_phys = {}  # record key -> physical of the lost write
        self.appended = set()  # entries this member APPENDED: the only ones it serves onward (product relay)
        self.held_records = set()  # (phys, key, stamp) of page records held as an unfinished join's state, out of position
        self.state_held = {}  # ADR-224: origin -> hlc below which this member holds history only as a completed snapshot's state
        self.bare_state = set()  # m-snap-delete-bare: snapshot tombstones held without an entry
        self.recv_entry = set()  # document entries this member took as entries (not as snapshot state)
        self.events_emitted = Counter()  # (key, stamp) of a lost write -> events emitted at this member
        self.hist_kept = set()  # idx of entries kept by retain_history (judged history on arrival or carried by a snapshot)
        self.dochist = {}  # idx -> client doc entry (m1: own writes + entries applied while the physical was alive)
        self.pend_tombs = {}  # R4: bound-out tombstone information received before the member's own flip
        self.deferred = []
        self.learned = {}
        self.cov = {}
        self.join = None  # a snapshot join in progress
        self.vanished = False  # m4: a writer that vanished never writes again
        self.refused = 0
        self.inv = {}
        self.flips = 0
        self.last_served = L
        self.info = {}  # rule B: idx -> a register event known but not yet covered (information: it takes no effect)
        self.refusal_eps = {}  # event idx -> dict(start, last, n, end, end_q, info, cover, cover_q): a D10 refusal episode (see `refusal_cause`)
        self.held_oop = set()  # the hold route: non-document entries held out of position (known, not appended)
        self.loss_info = {}  # 6r: idx -> a loss entry held ahead of its position: information, not in the state, until covered

    # ---- clocks
    def next_hlc(self):
        self.hlc = max(self.hlc + 1, self.w.clock + self.skew)
        return self.hlc

    def peek_hlc(self):
        return max(self.hlc + 1, self.w.clock + self.skew)

    def witness(self, e):
        self.hlc = max(self.hlc, e.hlc)

    # ---- R2: coverage, the contiguous prefix of the origin's entries held here
    def covers(self, origin, hlc):
        if self.held_oop and self.w.held_uncovered:
            # 6d (the overlay's covers_servable): an entry held out of position is not covered
            for e in self.w.by_origin[origin]:
                if e.hlc > hlc:
                    return True
                if e.idx not in self.held or e.idx in self.held_oop:
                    return False
            return True
        lst = self.w.by_origin[origin]
        i = self.cov.get(origin, 0)
        n = len(lst)
        held = self.held
        while i < n and lst[i].idx in held:
            i += 1
        self.cov[origin] = i
        return i >= n or lst[i].hlc > hlc

    def covers_past(self, origin, hlc):
        """R9 safety net: the whole prefix up to `hlc` is held and some held entry of the origin is above it."""
        if self.w.held_uncovered and self.w.flags.held_strict:
            # 6d (FIX-1, D9): the deadline witness must be COVERED: the servable prefix (no entry held out of position) reaches past `hlc`
            for e in self.w.by_origin[origin]:
                if e.idx not in self.held or e.idx in self.held_oop:
                    return False
                if e.hlc > hlc:
                    return True
            return False
        if not self.covers(origin, hlc):
            return False
        return any(e.hlc > hlc and e.idx in self.held for e in self.w.by_origin[origin])

    # ---- the register
    def member_valid(self, e):
        return self.w.flags.orphan_no_deadline or self.w.valid(e)

    def add_event(self, e):
        if e.idx in self.events:
            return False
        if self.w.flags.rule_b and not self.covers(e.origin, e.hlc):
            # rule B: the event is ahead of its position here: information, no effect until the member covers it
            if e.idx not in self.info:
                self.info[e.idx] = e
                self.w.stats["info_parked"] += 1
            return False
        if self.info.pop(e.idx, None) is not None:
            self.w.stats["info_taken_effect"] += 1
            ep = self.refusal_eps.get(e.idx)
            if ep is not None and ep["end"] is None and ep["info"] and ep["cover"] is None:
                ep["cover"], ep["cover_q"] = self.w.step, self.w.quiescing  # the member covers the event: it takes effect
        self.events[e.idx] = e
        self.evl = sorted(self.events.values(), key=lambda x: x.stamp)
        if e.kind in ("bind", "lcreate"):
            if not (e.kind == "lcreate" and self.w.flags.recreate_reuse):
                # M1: the birth of a physical is the stamp of the event that made it, set once
                self.bmap.setdefault(e.phys, e.stamp)
        return True

    def serving(self):
        if self.cur is None:
            return L
        if self.cur.kind == "ldrop":
            return None
        return self.cur.phys

    def first_superseding(self, name):
        """D11: the earliest known valid register event that supersedes the physical (stamp above its birth,
        an ldrop or an event naming another physical), or None."""
        b = self.bmap.get(name)
        if b is None:
            return None
        for ev in self.evl:
            if ev.stamp > b and (ev.kind == "ldrop" or ev.phys != name):
                return ev
        return None

    def late_stamp(self, name):
        ev = self.first_superseding(name)
        return ev.stamp if ev else None

    def pending(self):
        """v2.1's R5 (kept for m-pending-narrow): a valid binding above the visible event whose stage is not buried."""
        curst = self.cur.stamp if self.cur else NEG
        for ev in self.evl:
            if ev.kind == "bind" and ev.stamp > curst:
                p = self.phys.get(ev.phys)
                if p is None or p.alive:
                    return True
        return False

    def refusal_cause(self, s):
        """M3 (D10 as corrected): refuse whenever any known valid register event supersedes the served physical
        (D11 evaluated for a write at the member's next stamp: birth(p) < E.stamp < next stamp)."""
        f = self.w.flags
        if f.pend_accepts:
            return None
        if f.pend_narrow:
            return "bind" if self.pending() else None
        ev = self.first_superseding(s)
        x = ev if ev is not None else self.info_superseding(s)  # rule B / D10: "known" includes an event held as information ahead of its position
        if x is None:
            return None
        # a D10 refusal episode: a member refusing writes because of the event x, from its first refused write until its served physical is no
        # longer superseded by x (`close_refusal_eps`, end of `settle`). `info`: the first refusal was on an event held as information
        # (known, not covered); otherwise the event was covered and not yet visible (e.g. a join in progress holds the stage as a page).
        ep = self.refusal_eps.get(x.idx)
        if ep is None:
            self.refusal_eps[x.idx] = dict(start=self.w.step, last=self.w.step, n=1, end=None, end_q=False, info=ev is None, cover=None, cover_q=False)
            self.w.stats["refusal_episodes"] += 1
            if ev is None:
                self.w.stats["refusal_episodes_info"] += 1
        else:
            ep["last"], ep["n"] = self.w.step, ep["n"] + 1
        return x.kind

    def close_refusal_eps(self):
        """end the open D10 refusal episodes whose event no longer supersedes the physical the member serves (no side effect on the model)"""
        s = self.serving()
        for idx, ep in self.refusal_eps.items():
            if ep["end"] is not None:
                continue
            x = self.w.all[idx]
            b = self.bmap.get(s) if s is not None else None
            if b is not None and x.stamp > b and (x.kind == "ldrop" or x.phys != s) and (idx in self.events or idx in self.info):
                continue  # still refusing on it
            ep["end"], ep["end_q"] = self.w.step, self.w.quiescing

    def info_superseding(self, name):
        """the earliest valid register event held as information that would supersede the physical (rule B), or None"""
        b = self.bmap.get(name)
        if b is None or not self.info:
            return None
        for x in sorted(self.info.values(), key=lambda x: x.stamp):
            if x.stamp > b and (x.kind == "ldrop" or x.phys != name) and self.w.valid(x):
                return x
        return None

    # ---- local operations (the clients' view)
    def local_doc(self, key, op):
        if self.vanished:
            return None
        s = self.serving()
        if s is None:
            return None
        cause = self.refusal_cause(s)
        if cause is not None:
            self.refused += 1
            self.w.stats["refused"] += 1
            self.w.stats["refused_" + cause] += 1
            return None
        p = self.phys[s]
        if not p.alive:
            return None
        cur = p.docs.get(key)
        live = cur is not None and cur[1] is not None
        mark = ((self.name, self.w.clock),)
        if op == "insert":
            if live:
                return None
            body, opk = mark, "Insert"
        elif op == "update":
            if not live:
                return None
            body, opk = cur[1] + mark, "Replace"
        else:
            if not live:
                return None
            body, opk = None, "Delete"
        pend = self.pending()
        sup = self.first_superseding(s) is not None
        if self.info_superseding(s) is not None:
            self.w.stats["accepted_while_info_superseded"] += 1  # (a sanity counter: D10 refuses before this point)
        e = Entry(self.name, self.next_hlc(), "doc", s, key, body, opk)
        self.w.record(e)
        if pend:
            self.w.pend_acked.add(e.idx)
            self.w.stats["accepted_while_pending"] += 1
        if sup:
            self.w.sup_acked.add(e.idx)
            self.w.stats["accepted_while_superseded"] += 1
            self.inv["write_accepted_while_superseded"] = True
        self.w.stats["client_writes"] += 1
        self.w.note(f"{self.name} write {e}{' (SUPERSEDED, known)' if sup else ''}")
        self.take(e)
        self.dochist[e.idx] = e  # the origin holds every write it made
        return e

    def local_ldrop(self):
        if self.vanished:
            return None
        s = self.serving()
        if s is None:
            return None
        e = Entry(self.name, self.next_hlc(), "ldrop", s)
        self.w.record(e)
        self.w.stats["ldrop"] += 1
        self.w.note(f"{self.name} ldrop {e.stamp} (viewing {s})")
        self.take(e)
        return e

    def local_lcreate(self):
        """M1: a recreate makes a NEW physical named by its own event, birth = the event's stamp, no id reuse.
        (m-recreate-reuse: the event names the head physical the creator last served.)"""
        if self.vanished or self.serving() is not None:
            return None
        h = self.next_hlc()
        name = self.last_served if self.w.flags.recreate_reuse else f"R.{self.name}.{h}"
        e = Entry(self.name, h, "lcreate", name, extra=self.last_served)
        self.w.record(e)
        self.w.stats["lcreate"] += 1
        self.w.note(f"{self.name} lcreate {e.stamp} -> {name}")
        self.take(e)
        return e

    # ---- the $out writer
    def start_out(self, keys, kind, bind_step):
        job = Job(self, keys, kind, bind_step)
        h0 = self.next_hlc()
        job.stage = f"S.{self.name}.{h0}"
        job.dl = h0 + self.w.D
        c = Entry(self.name, h0, "create", job.stage, dl=job.dl)
        self.w.record(c)
        self.take(c)
        recs = []
        for k in keys:
            e = Entry(self.name, self.next_hlc(), "doc", job.stage, k, (("out", self.name, h0),), "Insert", staging=True)
            self.w.record(e)
            self.w.stage_docs[job.stage].append(e)
            recs.append((k, e.stamp))
            self.take(e)
        job.digest = (len(recs), frozenset(recs))
        self.w.note(f"{self.name} stages {job.stage} dl={job.dl} writer={kind}")
        return job

    def abandon_stage(self, stage):
        """m4: the writer's abandonment of its unbound stage (request failure / restart scan): an ordinary entry."""
        e = Entry(self.name, self.next_hlc(), "abandon", stage)
        self.w.record(e)
        self.w.stats["abandon_entries"] += 1
        self.w.note(f"{self.name} abandons {stage}")
        self.take(e)

    def do_bind(self, job):
        if self.vanished:
            return None
        if job.kind == "vanish" and sum(1 for m in self.w.members.values() if not m.vanished) <= 1:
            job.kind = "crash-abandon"  # model constraint: at least one member stays alive (a cluster with no live member has no one to report)
        if job.kind == "vanish":
            self.vanished = True
            if not self.w.flags.product_relay:
                self.w.prefix_close()  # (v4: a dead member relays nothing and nobody completes its prefix)
            self.w.stats["writer_vanished"] += 1
            self.w.note(f"{self.name} VANISHES after staging {job.stage} (no abandon, never writes again)")
            return None
        if job.kind == "crash-abandon":
            self.w.stats["writer_crashed"] += 1
            self.abandon_stage(job.stage)
            return None
        if job.kind == "good" and self.peek_hlc() > job.dl:
            self.w.stats["writer_refused_late_bind"] += 1
            self.w.note(f"{self.name} refuses to bind {job.stage}: past its deadline")
            self.abandon_stage(job.stage)
            return None
        b = Entry(self.name, self.next_hlc(), "bind", job.stage, dl=job.dl, extra=job.digest)
        self.w.record(b)
        if not self.w.valid(b):
            self.w.stats["invalid_binding"] += 1
        self.w.note(f"{self.name} binds {job.stage} at {b.stamp} valid={self.w.valid(b)}")
        self.take(b)
        return b

    # ---- apply (incremental)
    def take(self, e):
        if e.idx in self.held:
            return
        f = self.w.flags
        ignored = f.product_relay and f.ignored_not_served and e.kind != "doc" and e.origin != self.name and self.would_be_ignored(e)
        self.held.add(e.idx)
        self.witness(e)
        if f.product_relay and e.kind != "doc" and not ignored:
            self.appended.add(e.idx)  # register events, creates, abandons and loss entries are always appended and served onward
        if e.kind == "doc":
            self.recv_entry.add(e.idx)
        k = e.kind
        is_event = False
        if k == "create":
            self.creates[e.phys] = e
            self.learned.setdefault(e.phys, self.w.step)
            if e.phys not in self.phys:
                self.phys[e.phys] = Phys("stage")
            else:
                self.phys[e.phys].kind = "stage"
        elif k == "abandon":
            self.abandoned.add(e.phys)
        elif k == "lcreate":
            is_event = self.add_event(e)
            if e.phys not in self.phys and (is_event or not self.w.flags.rule_b):
                self.phys[e.phys] = Phys("rc")  # (rule B: made when the event takes effect, `activate_info`)
        elif k == "bind":
            if self.member_valid(e):
                is_event = self.add_event(e)
                if self.w.step - self.learned.get(e.phys, self.w.step) > 12:
                    self.w.stats["valid_bind_arrives_late"] += 1
        elif k == "ldrop":
            if self.w.flags.ldrop_identity_only:
                self.apply_ldrop_identity_only(e)
            else:
                is_event = self.add_event(e)
        elif k == "loss":
            if self.w.flags.loss_in_position and not self.covers(e.origin, e.hlc):
                # 6r (seed 1434): a record ahead of its position is information, like a register event under rule B: it does not enter the
                # state (no event, no snapshot carry, no dedup of this member's own mint) until the member covers its origin up to it
                if e.idx not in self.loss_info:
                    self.loss_info[e.idx] = e
                    self.w.stats["loss_info_parked"] += 1
            else:
                self.merge_loss(e.key, e.body, e.extra)
        elif k == "doc":
            self.apply_doc(e)
        if is_event:
            self.rescan()
        self.settle()

    def would_be_ignored(self, e):
        """an entry that takes no effect at this member (m-ignored-event-not-served: such an entry is not appended)"""
        k = e.kind
        if k == "loss":
            return e.key in self.loss or e.key in self.loss_pending
        if k == "create":
            return e.phys in self.phys
        if k == "abandon":
            p = self.phys.get(e.phys)
            return p is None or not p.alive or e.phys in self.bmap
        if k == "bind":
            return not self.member_valid(e) or (self.cur is not None and e.stamp < self.cur.stamp)
        if k in ("ldrop", "lcreate"):
            return self.cur is not None and e.stamp < self.cur.stamp
        return False

    def apply_ldrop_identity_only(self, e):
        """Mutant (R7): a logical drop addresses only the physical it names."""
        p = self.phys.get(e.phys)
        if p is not None and p.alive:
            self.bury(e.phys, p, e.stamp, "ldrop-identity")
        elif p is not None and (p.kill is None or e.stamp < p.kill):
            p.kill = e.stamp

    def retry_deferred(self):
        prog = False
        again = True
        while again:
            again = False
            for e in list(self.deferred):
                if e.phys in self.phys:
                    self.deferred.remove(e)
                    self.apply_doc(e)
                    again = prog = True
        return prog

    def lww(self, p, key, stamp, body):
        cur = p.docs.get(key)
        if cur is None or stamp > cur[0]:
            p.docs[key] = (stamp, body)

    def apply_doc(self, e):
        self._apply_doc_relay(e)
        if self.w.held_uncovered and e.idx in self.held_oop and e.idx in self.appended:
            # 6d, HB-1 removed: an entry held out of position (e.g. applied later by `retry_deferred`) is not servable before its release
            self.appended.discard(e.idx)
            if e.idx != getattr(self, "_holding", None):  # (not the doc_hold arrival itself under FIX-1)
                self.w.stats["hb1_held_not_appended"] += 1

    def _apply_doc_relay(self, e):
        f = self.w.flags
        p = self.phys.get(e.phys)
        if not f.product_relay or p is None or not p.alive:
            return self._apply_doc_orig(e)
        cur = p.docs.get(e.key)
        wins = cur is None or e.stamp >= cur[0]  # an equal stamp is the same entry, held as a snapshot's state
        held_state = p.missing or (cur is not None and (e.phys, e.key, cur[0]) in self.held_records)
        self._apply_doc_orig(e)
        if wins:
            self.appended.add(e.idx)
        elif f.append_in_position and held_state:
            # gap-1 fix: superseded ONLY by a record held as an unfinished snapshot's state: appended in position,
            # servable, no change to the document
            self.appended.add(e.idx)
            self.w.stats["appended_in_position"] += 1
        elif f.staged_row and self.s_trigger(e, p) and e.idx not in self.held_oop:
            # 6d, the v2.6 staged-row rule: a staging entry arriving in position, superseded on arrival by a version held here (whatever its
            # origin or coverage), is appended and served onward; no change to the document. Never a held (or held-then-deferred) entry.
            self.appended.add(e.idx)
            self.w.stats["staged_row_appended"] += 1
            self.w.sr_done.add((self.name, e.idx))
        elif e.origin != self.name:
            self.dochist.pop(e.idx, None)  # superseded on arrival: witnessed, not in the product's oplog, never served
            self.w.stats["lww_losers_not_kept"] += 1

    def known_bind_stamp(self, name):
        """6e addendum: the lowest stamp of a valid binding of the stage known here (in effect, parked as information, or held out of position), or None"""
        best = None
        for b in self.w.binds.get(name, ()):
            if self.member_valid(b) and (b.idx in self.events or b.idx in self.info or b.idx in self.held_oop):
                if best is None or b.stamp < best:
                    best = b.stamp
        return best

    def s_trigger(self, e, p):
        """rule S's trigger. `staging` (default, 6d exactly): e.staging, the model's mark. `bind` (S-bind): addressed to a stage physical AND (its stamp is below
        the stage's known valid binding, or no valid binding is known here). `bindorigin` (S-bind-origin): S-bind AND the entry's origin is the origin of the
        stage's creation entry (the stage's own job). No draw."""
        mode = self.w.flags.s_trigger
        if mode == "staging":
            return e.staging
        if p.kind != "stage":
            return False
        bs = self.known_bind_stamp(e.phys)
        if not (bs is None or e.stamp < bs):
            return False
        if mode == "bindorigin":
            cr = self.creates.get(e.phys)
            return cr is not None and cr.origin == e.origin
        return True

    def _apply_doc_orig(self, e):
        p = self.phys.get(e.phys)
        if p is None:
            if e not in self.deferred:
                self.deferred.append(e)
            return
        if e.staging:
            if p.alive:
                self.lww(p, e.key, e.stamp, e.body)
            return
        if p.alive:
            self.lww(p, e.key, e.stamp, e.body)
            self.dochist[e.idx] = e  # m1: retained while the physical is alive here
            self.check_late(e)
            return
        # R4/R6/m1: addressed to a buried (bound-out) physical: history on arrival. Evaluated ONCE, not retained.
        if self.w.flags.retain_history:
            self.dochist[e.idx] = e
            self.hist_kept.add(e.idx)
        else:
            self.w.stats["history_arrivals_dropped"] += 1
        self.check_late(e)

    # ---- R6 / D11: loss records
    def report(self, key, stamp, body, phys):
        f = self.w.flags
        if f.no_loss:
            return
        k = (key,) if f.loss_key_collapse else (key, stamp)
        if f.record_per_minter:
            k = (key, stamp, self.name)  # MUTANT: the minter is part of the key
        if k in self.loss or (f.pending_dedup and k in self.loss_pending):
            return
        if self.vanished:
            return  # a vanished writer never writes again: a record IS an entry (Q7 D10), so it makes none
        self.enter_loss(k, body)
        self.loss_phys[k] = phys
        s = self.serving()
        if s is not None:
            sp = self.phys.get(s)
            c = sp.docs.get(key) if sp is not None and sp.alive else None
            if c is not None and c[0] == stamp:
                self.inv["report_minted_for_served_write"] = True
        # M4: an ordinary entry of the reporter's own origin, above everything it has witnessed, appended to
        # the end of its own sequence, so inside its coverage prefix (m-loss-outside-prefix: v2.1)
        e = Entry(self.name, self.next_hlc(), "loss", key=k, body=body, extra=phys)
        self.w.record(e, cover=not f.loss_outside_prefix)
        self.held.add(e.idx)
        if f.product_relay:
            self.appended.add(e.idx)
        self.w.stats["loss_records_made"] += 1
        if not f.loss_outside_prefix:
            self.w.stats["loss_records_inside_prefix"] += 1
        self.w.note(f"{self.name} records loss {k}")

    def merge_loss(self, k, body, phys):
        """v3fix (F3): a record is an ordinary entry and replicates, but it enters this member's state (and
        emits its outReplaced event) only once the physical of the lost write is buried HERE, so a member
        never reports a write it may still serve (the record can reach it before the write, or before its
        own flip)."""
        if k in self.loss:
            return
        self.loss_phys[k] = phys
        if self.w.flags.defer_received and not self.buried_here(phys):
            self.loss_pending.setdefault(k, body)
            self.w.stats["records_deferred_while_served"] += 1
            return
        self.enter_loss(k, body)
        self.loss_pending.pop(k, None)

    def enter_loss(self, k, body):
        """a record first enters this member's state: its outReplaced event is emitted (Q7 D10), once per record key"""
        self.loss[k] = body
        self.events_emitted[k[:2]] += 1

    def buried_here(self, phys):
        p = self.phys.get(phys)
        return p is not None and not p.alive

    def promote_pending(self):
        for k in list(self.loss_pending):
            if self.buried_here(self.loss_phys[k]):
                self.enter_loss(k, self.loss_pending.pop(k))

    def check_late(self, e):
        """D11, existential form, with m3: the write is late iff a known valid register event supersedes its
        physical with a stamp between the physical's birth and the write's stamp; it is REPORTED only once the
        physical is buried on this member (m-report-before-burial: whenever known)."""
        ks = self.late_stamp(e.phys)
        if ks is None or e.stamp <= ks:
            return
        p = self.phys.get(e.phys)
        if not self.w.flags.report_before_burial and (p is None or p.alive):
            self.w.stats["report_delayed_until_burial"] += 1
            return
        self.report(e.key, e.stamp, e.body, e.phys)

    def rescan(self, name=None):
        for e in list(self.dochist.values()):
            if name is None or e.phys == name:
                self.check_late(e)

    # ---- R3, R4, R7, R9: the flip machinery
    def newest_applicable(self):
        f = self.w.flags
        curst = self.cur.stamp if self.cur else NEG
        for ev in reversed(self.evl):
            if ev.stamp <= curst:
                break
            if not f.visible_newest and not self.covers(ev.origin, ev.hlc):
                continue
            if ev.kind in ("bind", "lcreate"):
                p = self.phys.get(ev.phys)
                if ev.kind == "lcreate" and f.recreate_reuse and p is not None:
                    return ev  # m-recreate-reuse: revives the head physical
                if p is None or not p.alive:
                    continue
                if p.missing and not f.bury_without_alive:
                    self.w.stats["swap_not_here_yet"] += 1
                    continue
                if f.digest and ev.kind == "bind":
                    live = frozenset((k, v[0]) for k, v in p.docs.items() if v[1] is not None)
                    if (len(live), live) != ev.extra:
                        self.w.stats["digest_pending"] += 1
                        continue
            return ev
        return None

    def enter(self, V):
        f = self.w.flags
        self.cur = V
        if V.kind in ("bind", "lcreate"):
            self.last_served = V.phys
            p = self.phys[V.phys]
            if V.kind == "lcreate" and f.recreate_reuse:
                # revive the head physical, floor = the latest drop known below the recreate
                floor = max((e.stamp for e in self.evl if e.kind == "ldrop" and e.stamp < V.stamp), default=NEG)
                p.alive, p.kill, p.missing = True, None, False
                p.docs = {k: v for k, v in p.docs.items() if v[0] > floor}
                self.w.stats["lcreate_entered"] += 1
                return
            if p.missing or not p.alive:
                self.inv["serve_not_alive"] = True
            if V.kind == "bind":
                self.flips += 1
                self.w.stats["flips"] += 1
                for se in self.w.stage_docs[V.phys]:
                    c = p.docs.get(se.key)
                    if c is None or c[0] < se.stamp:
                        self.inv["partial_flip"] = True
                        break
            else:
                self.w.stats["lcreate_entered"] += 1
                if f.no_recreate_floor:
                    h = self.phys.get(V.extra)
                    if h is not None:
                        src = h.ghost if h.ghost is not None else h.docs
                        hb = self.bmap.get(V.extra, NEG)
                        for k, v in src.items():
                            if v[0] > hb and v[1] is not None:
                                p.docs[k] = v
        else:
            self.w.stats["ldrop_entered"] += 1
            if self.flips:
                self.w.stats["ldrop_after_flip"] += 1

    def bury(self, name, p, fst, reason):
        """R4: unconditional burial of a bound-out physical (never kept, never re-dated). m3: the retained
        writes addressed to it are re-evaluated now that it is buried."""
        f = self.w.flags
        if f.keep and any(s > fst for (s, _) in p.docs.values()):
            p.docs = {k: v for k, v in p.docs.items() if v[0] > fst}
            p.kept = True
            p.kill = fst
            self.w.stats["kept_by_adr231_path"] += 1
            return
        if f.no_recreate_floor:
            p.ghost = dict(p.docs)
        old_docs = p.docs
        p.docs = {}
        p.alive = False
        p.missing = False
        p.kill = fst
        if f.state_scan:
            ks = self.late_stamp(name)
            if ks is not None:
                for k, (s, body) in old_docs.items():
                    if s > ks and not (f.snap_delete_bare and (name, k, s) in self.bare_state):
                        self.report(k, s, body, name)
        self.w.stats["buried_" + reason] += 1
        if p.kind == "stage" and reason == "sweep":
            self.w.stats["loser_stage_buried"] += 1
        self.rescan(name)

    def sweep(self):
        cur = self.cur
        if cur is None:
            return False
        f = self.w.flags
        served = cur.phys if cur.kind in ("bind", "lcreate") else None
        prog = False
        for name, p in list(self.phys.items()):
            if not p.alive or p.kept or name == served:
                continue
            if f.stage_buried_early and p.kind == "stage" and cur.kind == "bind":
                self.bury(name, p, cur.stamp, "early")
                prog = True
                continue
            b = self.bmap.get(name)
            if b is None:
                continue  # R3: a stage with no known binding is never buried by the loser rule
            if b < cur.stamp:
                self.bury(name, p, cur.stamp, "sweep")
                prog = True
        return prog

    # ---- 6e: the orphan age fallback (D9, O8)
    def age_wall(self):
        """this member's own clock: the physical (wall) component of its HLC = the world clock + its skew. NOT `self.hlc`: the model's HLC is one counter that
        every event increments by a whole unit, so it runs AHEAD of the wall clock (at the end of a run a replica that has witnessed every entry reads
        257 against a clock of 240); using it made the check-only replicas of `finish` bury lingering stages at the '2 x' setting (5 div_replay_stamp_order
        runs in order, found by the first 6e in-order grid, sha 7c5cb102dccc8271)."""
        return self.w.clock + self.skew

    def age_due(self, cr):
        if self.replica and not self.w.age_final:
            return False  # a replica replays entries without their times: it applies the age rule ONCE, after the replay (`finish`), at the final clock
        return bool(self.w.flags.age) and self.age_wall() >= cr.dl + self.w.flags.age

    def knows_valid_bind(self, name):
        """a valid binding of the stage is known here: in effect (events), parked as information (rule B), or held out of position (received, not appended)"""
        for b in self.w.binds.get(name, ()):
            if not self.member_valid(b):
                continue
            if b.idx in self.events or b.idx in self.info or b.idx in self.held_oop:
                return True
        return False

    def log_age_burial(self, name, p, via_snapshot):
        w = self.w
        p.age = True
        if self.replica:
            return  # a check-only replica of `finish` (replay oracle): obeys the rule, is not counted
        w.stats["age_buried"] += 1
        elsewhere = any(m is not self and not m.vanished and m.knows_valid_bind(name) for m in w.members.values())
        w.age_log.append((self.name, name, w.step, elsewhere, via_snapshot, self.knows_valid_bind(name)))

    def age_tick(self):
        """time passes without an event: the age fallback is evaluated for this member (no draw); `settle` then buries"""
        f = self.w.flags
        for name, p in self.phys.items():
            if p.kind != "stage" or not p.alive:
                continue
            cr = self.creates.get(name)
            if cr is None or not self.age_due(cr):
                continue
            if name in self.bmap and not f.age_ignores_binding:
                continue
            if not (f.age_ignores_binding or f.age_ignores_held) and self.knows_valid_bind(name):
                continue
            self.settle()
            return

    def orphans(self):
        """R9/m4: a stage with no known valid binding is buried when its writer abandoned it (the witness), or,
        as a safety net, when the member covers the writer up to the deadline with a witness entry beyond it."""
        f = self.w.flags
        prog = False
        for name, p in list(self.phys.items()):
            if p.kind != "stage" or not p.alive:
                continue
            if name in self.bmap and not (f.age and f.age_ignores_binding):
                continue
            cr = self.creates.get(name)
            if cr is None:
                continue
            by_age = False
            if f.orphan_age is not None:
                bury = self.w.step - self.learned.get(name, 0) > f.orphan_age
            elif name in self.bmap:
                bury = False  # 6e mutant m-age-ignores-binding only: a stage with a binding in effect is touched by the age rule alone
                if self.age_due(cr):
                    bury = by_age = True
            else:
                bury = name in self.abandoned or self.covers_past(cr.origin, cr.dl)
                if not bury and f.age and self.age_due(cr) and (f.age_ignores_binding or f.age_ignores_held or not self.knows_valid_bind(name)):
                    bury = by_age = True  # 6e: abandon and the coverage safety net act first; the age fallback only where no valid binding is known
            if bury:
                if by_age:
                    self.log_age_burial(name, p, via_snapshot=False)
                p.docs = {}
                p.alive = False
                p.missing = False
                p.orphan = True
                self.w.stats["orphans_buried"] += 1
                if name in self.abandoned:
                    self.w.stats["orphans_buried_by_abandon"] += 1
                prog = True
        return prog

    def promote_oop(self):
        """the hold route: an entry held out of position becomes servable (`appended`) once every earlier entry of its origin is held"""
        ho = self.held_oop
        if not ho:
            return
        for i in sorted(ho):
            e = self.w.all[i]
            ok = True
            for x in self.w.by_origin[e.origin]:
                if x.hlc >= e.hlc:
                    break
                if x.idx not in self.held or (self.w.held_uncovered and x.idx in ho):
                    ok = False
                    break
            if ok:
                ho.discard(i)
                self.appended.add(i)

    def take_hold(self, e):
        """the hold route: an entry held out of position: known (the register and the state see it), servable only once it is in position"""
        was = e.idx in self.held
        if not was and self.w.held_uncovered and self.w.flags.held_strict:
            self.held_oop.add(e.idx)  # 6d (FIX-1): marked held BEFORE it is applied, so the settle inside `take` never sees it covered
        self._holding = e.idx
        self.take(e)
        self._holding = None
        if not was and (e.kind != "doc" or self.w.held_uncovered):  # 6d: doc_hold holds a document entry
            if (self.name, e.idx) in self.w.sr_done:
                self.w.stats["staged_row_withdrawn_by_hold"] += 1  # (the staged-row append made inside `take` does not survive the hold)
            self.appended.discard(e.idx)
            self.held_oop.add(e.idx)
        self.promote_oop()

    def activate_info(self):
        """rule B: an event held as information whose origin the member now covers takes effect, as an arrival (D11's rescan)"""
        if not self.info:
            return False
        prog = False
        for i, e in sorted(self.info.items(), key=lambda kv: kv[1].stamp):
            if self.covers(e.origin, e.hlc):
                if self.add_event(e):  # (removes it from `info`, counts it, closes its refusal episode)
                    if e.kind == "lcreate" and e.phys not in self.phys:
                        self.phys[e.phys] = Phys("rc")
                    self.rescan()
                    prog = True
                else:
                    self.info.pop(i, None)
        return prog

    def activate_loss_info(self):
        """6r: a record held as information whose origin the member now covers enters the state (an ordinary arrival, `merge_loss`)"""
        for i, e in sorted(self.loss_info.items(), key=lambda kv: kv[1].stamp):
            if self.covers(e.origin, e.hlc):
                del self.loss_info[i]
                self.w.stats["loss_info_taken_effect"] += 1
                self.merge_loss(e.key, e.body, e.extra)

    def settle(self):
        self.promote_oop()
        if self.loss_info:
            self.activate_loss_info()
        for _ in range(60):
            prog = False
            if self.activate_info():
                prog = True
            if self.retry_deferred():  # held entries are applied before visibility is judged
                prog = True
            V = self.newest_applicable()
            if V is not None and (self.cur is None or V.stamp > self.cur.stamp):
                self.enter(V)
                prog = True
            if self.sweep():
                prog = True
            if self.orphans():
                prog = True
            if self.retry_deferred():
                prog = True
            if not prog:
                break
        if self.loss_pending:
            self.promote_pending()
        if self.refusal_eps:
            self.close_refusal_eps()

    # ---- the product's relay: windows, witnessing, ADR-224
    def take_from(self, src, e, quiesce=False):
        """This member receives entry e in a window served by src (src APPENDED it). The receiver WITNESSES, without
        receiving them, the earlier entries of e's origin that src holds but did not append (its witnessed vector
        rises past them). ADR-224: src refuses a window below a row it holds only as a COMPLETED snapshot's state."""
        w = self.w
        release = w.held_uncovered and e.idx in self.held_oop  # 6d: an in-window re-delivery releases a held entry
        if e.idx in self.held and not release:
            return
        row = src.state_held.get(e.origin)
        if not quiesce and row is not None and not self.covers(e.origin, row):
            w.stats["state_held_refusals"] += 1
            return
        if src.join is not None:
            w.stats["windows_served_by_a_join_in_progress"] += 1
        for x in w.by_origin[e.origin]:
            if x.hlc >= e.hlc:
                break
            if x.idx in src.held and x.idx not in src.appended and x.idx not in self.held:
                self.held.add(x.idx)
                self.witness(x)
                w.stats["witnessed_without_receiving"] += 1
        if release:
            self.held_oop.discard(e.idx)
            self.appended.add(e.idx)
            w.stats["held_released_in_window"] += 1
            if os.environ.get("Q6_RELEASE_SETTLE", "1") != "0":
                self.settle()  # 6e build item (a): a released entry takes effect (rule B) in the same step it becomes covered and served
            return
        self.take(e)
        if w.held_uncovered and any(x.idx in self.held_oop for x in w.by_origin[e.origin] if x.hlc < e.hlc):
            self.appended.discard(e.idx)  # 6d: behind a held entry of its origin: held too
            self.held_oop.add(e.idx)
            w.stats["held_behind_held"] += 1

    def take_quiesce(self, e):
        if e.idx in self.held:
            return
        srcs = [m for m in self.w.members.values() if not m.vanished and m is not self and e.idx in m.appended]
        if srcs:
            self.take_from(srcs[0], e, quiesce=True)
        elif self.w.held_uncovered and any(e.idx in m.held_oop for m in self.w.members.values() if not m.vanished):
            return  # 6d: held out of position by a live member: not witnessed away
        else:
            self.held.add(e.idx)  # no live member appended it: it is only witnessed
            self.witness(e)
            self.w.stats["witnessed_at_quiescence"] += 1

    # ---- M5: snapshot joins carry STATE; the grant lands with the last page
    def eligible_source(self):
        return (self.join is None and not self.deferred and not any(p.missing for p in self.phys.values()))

    def make_payload(self):
        f = self.w.flags
        return dict(
            phys={n: dict(kind=p.kind, alive=p.alive, docs=dict(p.docs), kill=p.kill, orphan=p.orphan, age=p.age)
                  for n, p in self.phys.items()},
            events=[] if f.snap_no_events else list(self.evl),
            loss={} if (f.snap_no_loss) else {k: (b, self.loss_phys[k]) for k, b in {**self.loss_pending, **self.loss}.items()},
            held=(set(self.held) - self.held_oop) if self.w.held_uncovered else set(self.held),
            creates=dict(self.creates),
            learned=dict(self.learned),
            pend=dict(self.pend_tombs),
            abandoned=set(self.abandoned),
            hlc=self.hlc,
            appended=set(self.appended),
            dochist=(dict(self.dochist) if f.snap_with_dochist else
                     {i: e for i, e in self.dochist.items() if e.phys in self.phys and not self.phys[e.phys].alive}
                     if f.snap_late_entries else {}),
        )

    def snapshot_begin(self, src, lazy):
        if self.w.flags.stale_src_bug:
            self._snap_src = src
        w, f = self.w, self.w.flags
        if self.join is not None or not src.eligible_source() or src is self:
            return False
        pl = src.make_payload()
        w.stats["snap"] += 1
        w.snap_carried.append(len(pl["dochist"]))
        if pl["loss"]:
            w.stats["snap_carrying_records"] += 1
        w.stats["snap_entries_not_carried"] += len(src.dochist) - len(pl["dochist"])
        if src.cur is not None and src.cur.kind == "bind":
            sp = src.phys.get(src.cur.phys)
            if sp is not None and any(s > src.cur.stamp for (s, _) in sp.docs.values()):
                w.stats["snap_after_postflip_write"] += 1
        pages = [n for n, pd in pl["phys"].items() if pd["alive"]]
        w.rng.shuffle(pages)
        vec = {}
        if f.grant_prefix:
            # the product's grant (invariant (i)): the sender's CONTIGUOUS coverage per origin, and only that prefix of its held set
            # is granted; an entry the sender holds out of position (above its coverage) is neither claimed nor served by the receiver
            for o, lst in w.by_origin.items():
                top = -1
                for x in lst:
                    if x.idx not in pl["held"]:
                        break
                    top = x.hlc
                if top >= 0:
                    vec[o] = top
            g = {i for i in pl["held"] if w.all[i].hlc <= vec.get(w.all[i].origin, -1)}
            w.stats["grant_entries_withheld_above_coverage"] += len(pl["held"]) - len(g)
            pl["held"] = g
            pl["appended"] = pl["appended"] & g
            if f.loss_grant_filter and pl["loss"]:
                # 6r: a snapshot carries a record only if some entry of that record is within the grant (D3's register-event rule, for records)
                keep = {}
                for k, v in pl["loss"].items():
                    if any(x.kind == "loss" and x.key == k and x.idx in g for x in w.all):
                        keep[k] = v
                    else:
                        w.stats["snap_records_not_carried_above_grant"] += 1
                pl["loss"] = keep
        else:
            for i in pl["held"]:
                x = w.all[i]
                if x.hlc > vec.get(x.origin, -1):
                    vec[x.origin] = x.hlc
        pl["vec"] = vec  # the grant: the sender's vector as served with the FIRST page (ADR-152)
        # invariant (i), checked against the sender's own coverage (`covers`, not the prefix loop above): no grant and no page names an
        # entry above the sender's contiguous coverage
        if any(not src.covers(w.all[i].origin, w.all[i].hlc) for i in pl["held"]) or any(not src.covers(o, h) for o, h in vec.items()):
            w.stats["grant_above_coverage"] += 1
        if any(not src.covers(ent.origin, ent.hlc) for n, pd in pl["phys"].items() for k, v in pd["docs"].items()
               for ent in [w.rec_index.get((n, k, v[0]))] if ent is not None):
            w.stats["page_above_coverage"] += 1
        self.join = dict(pl=pl, pages=pages, created=set(), granted=False, src=src, begin_events={e.idx for e in pl["events"]})
        w.note(f"{self.name} begins a snapshot join from {src.name} lazy={lazy} pages={len(pages)}")
        if f.grant_before_pages:
            self.apply_grant()  # MUTANT: the grant lands before the pages
            self.settle()
        if not lazy:
            while self.join and self.join["pages"]:
                self.land_page()
        if self.join is not None and not self.join["pages"]:
            self.complete_join()
        return True

    def land_page(self):
        j = self.join
        if j is None:
            return False
        if not j["pages"]:
            if not j.get("repair"):  # 6d: a scoped repair lands pages and completes nothing
                self.complete_join()
            return True
        name = j["pages"].pop()
        pd = j["pl"]["phys"][name]
        dp = self.phys.get(name)
        f = self.w.flags
        ungranted = not j["granted"]
        if dp is None:
            dp = self.phys[name] = Phys(pd["kind"])
            dp.docs = dict(pd["docs"])
            if ungranted:
                for k, v in pd["docs"].items():
                    self.held_records.add((name, k, v[0]))  # a page's record: state, out of position
            if j["granted"]:
                pass  # (mutant: grant already applied) placeholder is complete once its page landed
            else:
                dp.missing = True
                j["created"].add(name)
            self.page_entries(name, pd["docs"], j)
        elif dp.alive:
            for k, v in pd["docs"].items():
                cur = dp.docs.get(k)
                if ungranted and (cur is None or v[0] > cur[0]):
                    self.held_records.add((name, k, v[0]))
                self.lww(dp, k, v[0], v[1])
            self.page_entries(name, pd["docs"], j)
        elif f.state_scan:
            j.setdefault("dead_pages", []).append((name, dict(pd["docs"])))  # evaluated when the grant lands
            if f.dead_page_retained:
                # seed 885: a page of a physical BURIED HERE is history on arrival, like an entry addressed to a bound-out physical: each
                # non-staging version (a delete with its _id, gap 2) is retained (F4: `dochist`, `hist_kept`), so D11 evaluates it at once
                # (`complete_join`'s rescan) and on every later register event (`rescan`), and this member's own snapshots carry it onward
                # (`make_payload`, snap_late_entries). A page version of a physical alive here is untouched (it is rebuilt as an entry, `page_entries`).
                for k, v in pd["docs"].items():
                    ent = self.w.rec_index.get((name, k, v[0]))
                    if ent is None or ent.staging:
                        continue
                    if v[1] is None and f.snap_delete_bare:
                        continue  # (m-snap-delete-bare: ADR-228's bare tombstone has no _id and no entry)
                    if ent.idx not in self.dochist:
                        self.dochist[ent.idx] = ent
                        self.hist_kept.add(ent.idx)
                        self.w.stats["dead_page_versions_retained"] += 1
        if not j["pages"] and not j.get("repair"):
            self.complete_join()
        return True

    def scoped_repair(self, src):
        """6d, the doc_repair route: a scoped repair. The source's pages of physicals alive on both land as held state (`held_records`); no
        register events, no records, no grant, and the join never completes (snapshot.rs `done = pages.is_empty() && !j.repair`)."""
        if self.join is not None or not src.eligible_source() or src is self:
            return False
        pl = src.make_payload()
        pages = sorted(n for n, pd in pl["phys"].items() if pd["alive"] and n in self.phys and self.phys[n].alive and not self.phys[n].missing)
        if not pages:
            return False
        pl.update(vec={}, held=set(), appended=set(), events=[], creates={}, abandoned=set(), loss={}, dochist={})
        self.join = dict(pl=pl, pages=pages, created=set(), granted=False, src=src, begin_events=set(), repair=True)
        for _ in range(len(pages)):
            self.land_page()
        self.join = None
        return True

    def page_entries(self, name, docs, j):
        """gap 2: the product rebuilds a snapshot's current versions as held entries (Position::Hold); for a physical of a
        once-swapped name a DELETED document carries its _id and the delete's stamp as an entry too, so D11's burial scan
        and rescans find it. m-snap-delete-bare: ADR-228's bare tombstone, no _id, no entry."""
        f = self.w.flags
        if not (f.product_relay and f.snap_versions_as_entries):
            return
        for k, v in docs.items():
            ent = self.w.rec_index.get((name, k, v[0]))
            if ent is None or ent.staging:
                continue
            if v[1] is None and f.snap_delete_bare:
                if ent.idx not in self.held and ent.idx not in self.dochist:
                    j.setdefault("bare_ids", set()).add(ent.idx)
                    self.bare_state.add((name, k, v[0]))
                continue
            if ent.idx not in self.dochist:
                self.dochist[ent.idx] = ent
                self.w.stats["snap_versions_as_entries"] += 1
                if v[1] is None:
                    self.w.stats["snap_deletes_carried_with_id"] += 1

    def apply_grant(self):
        j = self.join
        pl, f = j["pl"], self.w.flags
        if j["granted"]:
            return
        j["granted"] = True
        if f.paged_register_read and not f.snap_no_events and j.get("src") is not None and not f.grant_before_pages:
            # the register state is read from the sender when the LAST page lands. D3 rule: only the events the grant covers
            # (stamped at or below the first page's vector for their origin) are carried; a later event reaches this member
            # by windows, in order. Without the rule (m-snap-register-late) the member knows events above its grant.
            src, vec = (getattr(self, "_snap_src", None) or j["src"]) if f.stale_src_bug else j["src"], pl["vec"]
            if f.stale_src_bug and src is not j["src"]:
                self.w.stats["register_read_from_a_member_that_is_not_the_sender"] += 1
                self.w.note(f"{self.name} reads the register of {src.name}, NOT its sender {j['src'].name}")
            cand = list(src.evl)
            if f.register_grant_filter:
                carried = [e for e in cand if e.hlc <= vec.get(e.origin, -1)]
            else:
                carried = cand
            above = [e for e in cand if e.hlc > vec.get(e.origin, -1)]
            if above:
                self.w.stats["register_events_above_grant"] += len(above)
                if f.register_grant_filter:
                    self.w.stats["register_events_not_carried_above_grant"] += len(above)
                else:
                    self.w.stats["register_events_read_above_grant"] += len(above)
                    self.w.note(f"{self.name} reads at the last page register events ABOVE its grant: {above}")
            if any(e.kind == "bind" and self.w.valid(e) and e.idx not in j["begin_events"] for e in cand):
                self.w.stats["bind_between_first_and_last_page"] += 1
            pl["events"] = carried
        if f.paged_loss_read and not f.snap_no_loss and j.get("src") is not None and not f.grant_before_pages:
            # 6b (companion clause): the sender's loss records are read when the LAST page lands. Clause: a record is carried only if some entry of it
            # is within the grant (`pl["held"]`, the first page's granted set); a record whose entries are all above it reaches this member by windows.
            src, w = j["src"], self.w
            cand = {k: (b, src.loss_phys[k]) for k, b in {**src.loss_pending, **src.loss}.items()}
            w.stats["loss_records_read_late"] += len(cand)
            ok = {k: v for k, v in cand.items() if any(x.kind == "loss" and x.key == k and x.idx in pl["held"] for x in w.all)}
            w.stats["loss_records_above_grant"] += len(cand) - len(ok)
            if f.loss_grant_filter:
                pl["loss"] = ok
            else:
                w.stats["loss_records_read_above_grant"] += len(cand) - len(ok)
                pl["loss"] = cand
        if f.product_relay:
            rows = {}
            for i in pl["held"] - self.held:
                x = self.w.all[i]
                if x.kind != "loss":
                    rows[x.origin] = max(rows.get(x.origin, -1), x.hlc)
            for o, h in rows.items():
                self.state_held[o] = max(self.state_held.get(o, -1), h)  # ADR-224's state_held row, written at completion
            self.appended |= pl["appended"] - j.get("bare_ids", set())
        self.held |= pl["held"]
        self.hlc = max(self.hlc, pl["hlc"])
        for k, v in pl["creates"].items():
            self.creates.setdefault(k, v)
        for k, v in pl["learned"].items():
            self.learned.setdefault(k, v)
        for e in pl["events"]:
            self.add_event(e)
        if not f.snap_drop_loss:
            for k, (b, ph) in pl["loss"].items():
                self.merge_loss(k, b, ph)
        for i, e in pl["dochist"].items():
            if i not in self.dochist:
                self.hist_kept.add(i)
            self.dochist.setdefault(i, e)
        for k, v in pl["pend"].items():
            self.pend_tombs[k] = min(v, self.pend_tombs.get(k, v))
        if not f.snap_no_orphan_burials:
            self.abandoned |= pl["abandoned"]
        for name, pd in pl["phys"].items():
            if f.snap_no_orphan_burials and pd["orphan"]:
                continue  # MUTANT: the pages do not carry the sender's orphan burials
            if not pd["alive"] and name not in self.phys and pd["age"] and pd["orphan"] and f.age_tomb:
                # 6f (manager's ruling): an AGE tombstone is a local decision; a receiver that does not know the stage makes no stub (its own rule decides if the create reaches it later)
                self.w.stats["age_tomb_ignored_unknown_stage"] += 1
                continue
            if not pd["alive"] and name not in self.phys:
                # the pages carry tombstone information for the sender's buried physicals; the receiver
                # buries by its own D4 evaluation, a stub only records that the physical is gone
                st = self.phys[name] = Phys(pd["kind"])
                st.alive, st.kill, st.orphan = False, pd["kill"], pd["orphan"]
                st.age = pd["age"]
            elif not pd["alive"] and pd["orphan"] and name in self.phys and self.phys[name].alive:
                # the sender's orphan burial is honoured
                p = self.phys[name]
                if pd["age"] and f.age_tomb:
                    # 6f (manager's ruling): an AGE tombstone is applied only if the receiver's OWN age rule would bury the stage now: WHEN (its own wall clock >= deadline + age)
                    # and WHERE (no valid binding known in any form, none in effect); otherwise it is ignored and the receiver buries later, by its own rule, or never
                    cr = self.creates.get(name)
                    if cr is None or not self.age_due(cr):
                        self.w.stats["age_tomb_ignored_when"] += 1
                        continue
                    if name in self.bmap or self.knows_valid_bind(name):
                        self.w.stats["age_tomb_ignored_where"] += 1
                        continue
                    self.w.stats["age_tomb_applied"] += 1
                if pd["age"]:
                    self.log_age_burial(name, p, via_snapshot=True)  # 6e: the sender's age burial is honoured like any orphan burial (counted)
                p.docs, p.alive, p.missing, p.orphan = {}, False, False, True
                self.w.stats["orphans_buried"] += 1

    def complete_join(self):
        """The grant is recorded when the LAST page lands, in one step with the register events and the
        receiver's own D4 evaluation."""
        j = self.join
        self.apply_grant()
        for n in j["created"]:
            p = self.phys.get(n)
            if p is not None:
                p.missing = False
        for name, docs in j.get("dead_pages", []):
            # the sender still holds this physical, here it is buried: its late document versions are evaluated
            # (once) like entries judged history on arrival
            ks = self.late_stamp(name)
            if ks is not None:
                for k, (s, body) in docs.items():
                    if s > ks:
                        if self.w.flags.snap_delete_bare and body is None:
                            ent = self.w.rec_index.get((name, k, s))
                            if ent is not None and ent.idx not in self.held:
                                continue  # a bare tombstone: no _id, no entry
                        self.report(k, s, body, name)
        self.held_records = set()  # the grant has landed: ADR-224's state_held row governs serving from here
        self.join = None
        self.w.stats["snap_completed"] += 1
        self.rescan()
        self.settle()

    def relay_tomb(self, src):
        """R4: src relays the tombstone of a bound-out physical to this member."""
        w, f = self.w, self.w.flags
        cands = [n for n, p in src.phys.items() if not p.alive and not p.orphan and p.kill is not None
                 and n in self.phys and self.phys[n].alive and (self.cur is None or self.cur.stamp < p.kill)]
        if not cands:
            return False
        n = w.rng.choice(sorted(cands))
        p = self.phys[n]
        k = src.phys[n].kill
        w.stats["relay_tomb"] += 1
        if f.bt_early:
            w.note(f"{self.name}: MUTANT buries {n} on a relayed tombstone before its own flip")
            self.bury(n, p, k, "relay")
        else:
            self.pend_tombs[n] = k
            w.note(f"{self.name}: keeps tombstone info for {n} ({k}), buries at its own flip")
        self.settle()
        return True

    # ---- views
    def served(self):
        s = self.serving()
        if s is None:
            return {}
        p = self.phys.get(s)
        if p is None or not p.alive:
            return {}
        return {k: v for k, v in p.docs.items() if v[1] is not None}

    def state(self):
        alive = tuple(sorted((n, tuple(sorted((k, v) for k, v in p.docs.items() if v[1] is not None)))
                             for n, p in self.phys.items() if p.alive))
        return (self.serving(), alive)

    def check_inv(self):
        s = self.serving()
        if s is not None:
            p = self.phys.get(s)
            if p is None or not p.alive or p.missing:
                self.inv["serve_not_alive"] = True
        for name, p in self.phys.items():
            if p.alive or p.orphan or p.kept:
                continue
            b = self.bmap.get(name)
            if self.cur is None or b is None or b >= self.cur.stamp:
                self.inv["bury_without_newer_visible"] = True
        # m3: no member holds a loss record for a write that it still serves
        if self.loss:
            for k, (st, body) in self.served().items():
                if (k, st) in self.loss or (k,) in self.loss:
                    self.inv["record_for_served_write_at_member"] = True
                    break


# ----------------------------------------------------------------------------- oracle


def oracle(world):
    """The oracle. Entries held by no live member (lost with a vanished member: a write that never left it) are
    treated as never having reached the cluster: they are excluded, and so is any register event of the same
    origin stamped after one of them (its coverage can never complete)."""
    lost = world.lost  # COVERAGE-lost: held by no live member at all (witnessing counts: a witnessed prefix is covered, so a register event behind it is reachable)
    # 6e: for DOCUMENT entries (loss records, late writes, heads) an entry counts as held by a live member only if that member applied it or holds it out of position,
    # never if only witnessed (`slost`, the structural set). Events, reachability and the D9 witness keep the coverage-lost set: seed 2219 (3/3/3, hold 0.3, Q6_PS=1,
    # paged) showed that using the structural set for them drops a reachable bind and turns a real record into a "false" one.
    slost = world.lost if world.lost_struct is None else world.lost_struct
    allx = [e for e in world.all if e.idx not in lost and not (e.kind == "doc" and e.idx in slost)]

    def unreachable(e):
        return any(x.idx in lost and x.hlc < e.hlc for x in world.by_origin[e.origin])

    ev = sorted((e for e in allx if e.kind in ("bind", "lcreate", "ldrop") and world.valid(e) and not unreachable(e)),
                key=lambda x: x.stamp)
    birth = {L: NEG}
    for e in ev:
        if e.kind in ("bind", "lcreate"):
            birth.setdefault(e.phys, e.stamp)
    top = ev[-1] if ev else None
    if top is None:
        head, alive = L, {L}
    elif top.kind == "ldrop":
        head, alive = None, set()
    else:
        head, alive = top.phys, {top.phys}
    docs = {}
    if head is not None:
        for e in allx:
            if e.kind == "doc" and e.phys == head:
                c = docs.get(e.key)
                if c is None or e.stamp > c[0]:
                    docs[e.key] = (e.stamp, e.body)
    served = {k: v for k, v in docs.items() if v[1] is not None}

    def late(e, events):
        b = birth.get(e.phys)
        if b is None:
            return False
        return any(b < E.stamp < e.stamp and (E.kind == "ldrop" or E.phys != e.phys) for E in events)

    loss = {}
    for e in allx:
        if e.kind == "doc" and not e.staging and late(e, ev):
            loss[(e.key, e.stamp)] = e.body
    # the converse: late writes (by the same rule) that were lost with a vanished member
    world.lost_late = {(e.key, e.stamp) for e in world.all
                       if e.idx in slost and e.kind == "doc" and not e.staging and late(e, ev)}
    # the split oracle (the manager's seed-530 ruling): the REQUIRED set is `loss` (heads of the late writes whose entries a live
    # member holds); the ALLOWED set is every write of the whole write log, lost entries included, that is late by D11 judged over the
    # reachable valid events `ev` (events some live member can have in effect: a covered one, or one whose origin's prefix survives)
    world.allowed = {(e.key, e.stamp) for e in world.all if e.kind == "doc" and not e.staging and late(e, ev)}
    # 6e: "writes lost with their only holders" (manager's seed-858 addendum): late writes (D11, over the reachable valid events) that no live member applied or holds
    # out of position. Split: (a) a vanished member OTHER than the origin had applied it (replicated, then every holder vanished); (b) only the origin ever held it;
    # (c) of (a)+(b), the ones the 6d oracle counted as held by a live member because it had only WITNESSED them (the ones the structural rule moves).
    _ll = [e for e in world.all if e.idx in slost and e.kind == "doc" and not e.staging and late(e, ev)]
    world.lwolh = dict(
        total=len(_ll),
        replicated=sum(1 for e in _ll if any((e.idx in m.appended or e.idx in m.recv_entry) for m in world.members.values() if m.vanished and m.name != e.origin)),
        origin_only=sum(1 for e in _ll if not any((e.idx in m.appended or e.idx in m.recv_entry) for m in world.members.values() if m.vanished and m.name != e.origin)),
        witnessed_only=sum(1 for e in _ll if e.idx not in world.lost))
    lingering = set()
    for c in allx:
        if c.kind != "create":
            continue
        if any(b.kind == "bind" and b.phys == c.phys and world.valid(b) and not unreachable(b) for b in allx):
            continue
        if any(a.kind == "abandon" and a.phys == c.phys for a in allx):
            continue
        blocked = any(x.idx in lost and x.hlc <= c.dl for x in world.by_origin[c.origin])
        if not blocked and any(x.hlc > c.dl and x.idx not in lost for x in world.by_origin[c.origin]):
            continue
        lingering.add(c.phys)
    return top, alive | lingering, served, loss, lingering


# ----------------------------------------------------------------------------- driver


def run(mode, seed, nmembers=4, nkeys=4, outs=2, steps=None, base="v3", trace=False):
    flags = flags_for(mode, base)
    rng = random.Random(seed)
    names = [chr(ord("A") + i) for i in range(nmembers)]
    D = rng.choice([nkeys + 8, nkeys + 25, nkeys + 60])
    w = World([(n, rng.choice([-6, -3, 0, 0, 3, 6])) for n in names], flags, rng, D, trace)
    steps = steps or (120 + 40 * outs)
    seedw = w.members[names[0]]
    for k in range(nkeys):
        seedw.local_doc(k, "insert")
    for m in w.members.values():
        for e in list(w.all):
            m.take(e)
    route_f = float(os.environ.get("Q6_F", "0") or 0)
    route_kinds = [x for x in os.environ.get("Q6_ROUTES", "hold,repair").split(",") if x]
    assert all(x in ("hold", "repair", "doc_hold", "doc_repair") for x in route_kinds), "Q6_ROUTES: hold, repair, doc_hold, doc_repair"
    w.held_uncovered = any(x in ("doc_hold", "doc_repair") for x in route_kinds) or os.environ.get("Q6_PS") == "1"  # 6d: the doc routes (b1port.py); 6e: Q6_PS=1 product semantics
    assert os.environ.get("Q6_PS") == "1" or not (w.held_uncovered and any(x in ("hold", "repair") for x in route_kinds)), "6d: the model's routes and the doc routes are not mixed"
    jobs, outs_left = [], outs
    dbound = int(os.environ.get("Q6_DELIVERY_BOUND", "0") or 0)  # 6f: 0 = off
    novanish = os.environ.get("Q6_NO_VANISH", "0") == "1"
    ldrop_p = rng.choice([0.0, 0.0, 0.5, 1.2])  # per-run rate of logical drops (weight out of ~100 per step)

    def active():
        return [n for n in names if not w.members[n].vanished]  # a vanished member is dead: it acts, relays and joins no more

    for step in range(steps):
        w.step = step
        w.clock += 1
        if flags.age:
            for _m in w.members.values():
                if not _m.vanished:
                    _m.age_tick()
        w.bd_scan()
        for j in list(jobs):
            if j.bind_step <= step:
                jobs.remove(j)
                j.writer.do_bind(j)
        act = rng.choices(["write", "out", "ldrop", "lcreate", "snap", "jpage", "relay", "fwd"],
                          [26, 5 if outs_left else 0, ldrop_p, 8, 4, 4, 2, 52])[0]
        act_names = active()
        if not act_names and act in ("write", "out", "ldrop", "lcreate"):
            act = "fwd"
        if act == "write":
            m = w.members[rng.choice(act_names)]
            m.local_doc(rng.randrange(nkeys + 2), rng.choice(
                ["insert", "update", "delete", "delete"] if os.environ.get("Q6_DELETE_BIAS") else ["insert", "update", "update", "delete"]))
        elif act == "out":
            outs_left -= 1
            m = w.members[rng.choice(act_names)]
            kind = rng.choices(["good", "late", "crash-abandon", "vanish"], [62, 12, 16, 0 if novanish else 10])[0]  # 6f: Q6_NO_VANISH=1 removes the writers that vanish silently (the same draw, weight 0)
            if kind == "late":
                delay = D + rng.randint(4, 25)
            else:
                delay = rng.randint(1, min(D, 30))
            jobs.append(m.start_out(rng.sample(range(nkeys + 2), rng.randint(1, nkeys)), kind, step + delay))
        elif act == "ldrop":
            w.members[rng.choice(act_names)].local_ldrop()
        elif act == "lcreate":
            absent = [w.members[n] for n in act_names if w.members[n].serving() is None]
            if absent:
                rng.choice(absent).local_lcreate()
        elif act == "snap":
            if not act_names:
                continue
            dst = w.members[rng.choice(act_names)]
            cands = [m for m in w.members.values() if m is not dst and not m.vanished and m.eligible_source()]
            post = [m for m in cands if m.cur is not None and m.cur.kind == "bind"
                    and any(s > m.cur.stamp for (s, _) in m.phys[m.cur.phys].docs.values())]
            if cands:
                src = rng.choice(post) if post and rng.random() < 0.6 else rng.choice(cands)
                dst.snapshot_begin(src, lazy=rng.random() < 0.5)
        elif act == "jpage":
            joining = [m for m in w.members.values() if m.join is not None and not m.vanished]
            if joining:
                rng.choice(joining).land_page()
        elif act == "relay":
            if len(act_names) < 2:
                continue
            src, dst = rng.sample(act_names, 2)
            w.members[dst].relay_tomb(w.members[src])
        else:
            if len(act_names) < 2:
                continue
            src, dst = rng.sample(act_names, 2)
            s, d = w.members[src], w.members[dst]
            if flags.product_relay:
                # a window from the receiver's single threshold: the lowest-stamp entry the source APPENDED and the receiver lacks
                cand = [e for e in w.all if e.idx in s.appended and (e.idx not in d.held or (w.held_uncovered and e.idx in d.held_oop))]
                # route probes (off by default; the random draw is made only when Q6_F > 0, so Q6_F = 0 is the in-order model exactly)
                r = rng.random() if route_f else 1.0
                if route_f and r < route_f:
                    kind = rng.choice(route_kinds)
                    if kind == "hold":
                        # the hold route: a non-document entry the source holds (any stamp, any origin, any kind) is delivered out of position
                        nd = [e for e in w.all if e.idx in s.held and e.kind != "doc" and e.idx not in d.held]
                        if nd:
                            _e = rng.choice(nd)
                            w.note(f"HOLD {d.name} <- {s.name}: {_e}")
                            w.stats["hold_taken"] += 1
                            d.take_hold(_e)
                    elif kind == "repair":
                        # the repair route: the source's register events are handed over as information (a scoped repair that
                        # carries the register and grants no coverage): no entry, no coverage, no clock witness
                        new = [e for e in s.evl if e.idx not in d.events]
                        if new:
                            w.note(f"REPAIR {d.name} <- {s.name}: register events {new}")
                            for e in new:
                                d.add_event(e)
                            w.stats["repair_events"] += len(new)
                            d.settle()
                    elif kind == "doc_hold":
                        # 6d: the product's doc_hold: a document entry the source holds is delivered out of position (held, not appended)
                        nd = [e for e in w.all if e.kind == "doc" and e.idx in s.held and e.idx not in d.held]
                        if nd:
                            _e = rng.choice(nd)
                            w.note(f"DOC-HOLD {d.name} <- {s.name}: {_e}")
                            w.stats["doc_hold_taken"] += 1
                            d.take_hold(_e)
                    else:
                        # 6d: the product's doc_repair: a scoped repair from the source
                        w.note(f"DOC-REPAIR {d.name} <- {s.name}")
                        if d.scoped_repair(s):
                            w.stats["doc_repair"] += 1
                elif cand:
                    d.take_from(s, min(cand, key=lambda x: x.stamp))
            else:
                cand = [e for e in w.all if e.idx in s.held and e.idx not in d.held]
                if cand:
                    d.take(rng.choice(cand))
        if dbound and flags.product_relay:
            # 6f bounded delivery (Q6_DELIVERY_BOUND=D, ordinary delivery only; the end-of-run delivery below is unchanged): an entry that has existed for D steps is
            # delivered to every live member that lacks it, through the ordinary window (the lowest-stamp entry the source APPENDED and the receiver lacks), no random draw
            for d in w.members.values():
                if d.vanished:
                    continue
                for _ in range(256):
                    over = [e for e in w.all if step - w.ent_step[e.idx] >= dbound and e.idx not in d.held]
                    moved = False
                    for e in over:
                        srcs = [x for x in w.members.values() if x is not d and not x.vanished and e.idx in x.appended]
                        if not srcs:
                            continue
                        for s_ in srcs:  # a source may refuse the window (ADR-224 state_held row): try the others
                            cand_ = [x for x in w.all if x.idx in s_.appended and x.idx not in d.held]
                            before = len(d.held)
                            d.take_from(s_, min(cand_, key=lambda x: x.stamp))
                            if len(d.held) != before:
                                moved = True
                                w.stats["bounded_delivery_moves"] += 1
                                break
                        if moved:
                            break
                    if not moved:
                        break
        for m in w.members.values():
            m.check_inv()
    # quiescence: pending $out jobs run, joins complete; entries held by no live member are lost with the vanished
    # members; every other entry is delivered to every live member in random orders
    for m in w.members.values():  # D10 measured without side effects when the steps end (member-runs still refusing), every cause
        if not m.vanished:
            _s = m.serving()
            if _s is not None and (m.first_superseding(_s) is not None or m.info_superseding(_s) is not None):
                w.stats["refusal_open_at_steps_end"] += 1
            if _s is not None and m.info_superseding(_s) is not None:
                w.stats["refusal_open_at_steps_end_info"] += 1
    w.step += 1
    w.steps_end = w.step
    w.quiescing = True
    for j in jobs:
        j.writer.do_bind(j)
    live = [m for m in w.members.values() if not m.vanished]
    for m in live:
        while m.join is not None:
            m.land_page()
    w.lost = {e.idx for e in w.all if not any(e.idx in m.held for m in live)}
    # 6e (manager, seed 858): the ORACLE's lost set is structural: an entry counts as held by a live member only if that member APPLIED it (it is in `appended`) or holds
    # it out of position (`held_oop`), never if it is only WITNESSED (`take_from` adds omitted earlier entries to `held` without receiving them). `w.lost` (the
    # trajectory: which entries the end-of-run delivery moves, the replicas' input) is unchanged. Q6_LOSTSTRUCT=1 is the DEFAULT (manager's ruling: the 6d oracle counted a witnessed-only member as a holder; in order it moves 4 late writes in 4 runs, seeds 147 (4/3/4, 4/3/5) and 1320 (3/2/3, 3/3/3), no state or failure column moves: REPORT9 'oracle corrections'); =0 is 6d's oracle (bridge rows), =2 is a misreading (appended or held_oop only: breaks 37 in-order runs).
    # Q6_LOSTSTRUCT=1 (applied or held out of position): `appended`, `held_oop` or `recv_entry` (a document entry the member took as an entry and applied, even when it
    # lost last-writer-wins on arrival and was not kept); =2 (the manager's wording literally): `appended` or `held_oop` only; =0: 6d's oracle (`held`).
    _ls = os.environ.get("Q6_LOSTSTRUCT", "1")
    if _ls == "0":
        w.lost_struct = set(w.lost)
    elif _ls == "2":
        w.lost_struct = {e.idx for e in w.all if not any(e.idx in m.appended or e.idx in m.held_oop for m in live)}
    else:
        w.lost_struct = {e.idx for e in w.all if not any(e.idx in m.appended or e.idx in m.held_oop or e.idx in m.recv_entry for m in live)}
    for _ in range(12):
        for m in live:
            while m.join is not None:
                m.land_page()
        for m in live:
            order = [e for e in w.all if e.idx not in w.lost]
            rng.shuffle(order)
            if flags.product_relay:
                order.sort(key=lambda x: x.stamp)
                for e in order:
                    m.take_quiesce(e)
            else:
                for e in order:
                    m.take(e)
        for m in live:
            m.settle()
        if all(all(e.idx in m.held for e in w.all if e.idx not in w.lost) for m in live):
            break
    for m in live:
        m.settle()
        m.check_inv()
    w.bd_scan(final=True)
    for m in live:  # ... and at the end of the run, after the end-of-run delivery
        _s = m.serving()
        if _s is not None and (m.first_superseding(_s) is not None or m.info_superseding(_s) is not None):
            w.stats["refusal_open_at_run_end"] += 1
        if _s is not None and m.info_superseding(_s) is not None:
            w.stats["refusal_open_at_run_end_info"] += 1
    return finish(w, names)


def finish(w, names):
    top, alive, served, loss, lingering = oracle(w)
    res = Counter()
    ms = [m for m in w.members.values() if not m.vanished]  # the live members; a vanished member is dead
    states = {m.state() for m in ms}
    losssets = {frozenset(m.loss) for m in ms}
    # replay checks: fresh replicas that receive every surviving entry (no snapshots), compared with the members by the per-head test
    st_run = Counter(w.stats)  # the run's own counters, before any replica (the v6 counters are read from here)
    base_entries = [e for e in w.all if e.idx not in w.lost]
    replays = []  # (state, records) of the shuffled replicas (v3* bases, no product relay) or of the interleaved replicas
    stamp_replays = []  # product relay: the stamp-order replay (see below)
    stamp_seq = [e.idx for e in sorted(base_entries, key=lambda x: x.stamp)]
    interleave_differs = 0
    if w.flags.product_relay:
        # (1) the stamp-order replay (`replay_stamp_order`): every surviving entry, in stamp order, through the product relay (an entry
        # is taken from the first live member that appended it, otherwise only witnessed). The stamp sort discards any shuffle, so
        # this is ONE replay, not independent random orders. It is made twice, as REPORT5's `finish` did, only so that the mechanism
        # counts, which include the replicas' own window and witness counts, reproduce REPORT5's exactly; `stamp_replicas_identical`
        # counts the runs in which the two ended identical.
        for i in range(2):
            Z = Member(w, f"Z{i}", 0)
            order = base_entries[:]
            w.rng.shuffle(order)
            order.sort(key=lambda x: x.stamp)
            for e in order:
                Z.take_quiesce(e)
            w.age_final = True
            Z.settle()
            w.age_final = False
            stamp_replays.append((Z.state(), frozenset(Z.loss)))
        # (2) the interleaved replay (`div_replay`): each replica repeatedly picks a random origin with entries left (each origin's
        # order kept), takes that origin's next entry from a random live member that appended it (or witnesses it if none did), then
        # sweeps once in stamp order. Its counters are not added to the run's (`w.stats` is restored after it).
        saved_stats = Counter(w.stats)
        live_ms = [m for m in w.members.values() if not m.vanished]
        for i in range(2):
            Z = Member(w, f"I{i}", 0)
            queues = defaultdict(list)
            for e in sorted(base_entries, key=lambda x: x.stamp):
                queues[e.origin].append(e)
            seq = []
            while True:
                ks = sorted(o for o, q in queues.items() if q)
                if not ks:
                    break
                e = queues[w.rng.choice(ks)].pop(0)
                seq.append(e.idx)
                if e.idx in Z.held:
                    continue
                srcs = [m for m in live_ms if e.idx in m.appended]
                if srcs:
                    Z.take_from(w.rng.choice(srcs), e, quiesce=True)
                elif not (w.held_uncovered and w.flags.replay_skips_held and any(e.idx in m.held_oop for m in live_ms)):
                    Z.held.add(e.idx)  # no live member appended it: it is only witnessed
                    Z.witness(e)
            for e in sorted(base_entries, key=lambda x: x.stamp):
                Z.take_quiesce(e)
            w.age_final = True
            Z.settle()
            w.age_final = False
            replays.append((Z.state(), frozenset(Z.loss)))
            interleave_differs |= int(seq != stamp_seq)
        w.stats = saved_stats
    else:
        for i in range(2):
            Z = Member(w, f"Z{i}", 0)
            order = base_entries[:]
            w.rng.shuffle(order)
            for e in order:
                Z.take(e)
            w.age_final = True
            Z.settle()
            w.age_final = False
            replays.append((Z.state(), frozenset(Z.loss)))
    PH = w.flags.product_relay  # the per-head oracle (D11): heads required, any true late write allowed
    okeys0 = set(loss)
    newest = {}
    for x in w.all:
        if x.kind == "doc" and not x.staging and x.idx not in (w.lost if w.lost_struct is None else w.lost_struct):
            kk = (x.phys, x.key)
            if kk not in newest or x.stamp > newest[kk]:
                newest[kk] = x.stamp
    late_by_key = {(x.key, x.stamp): x for x in w.all if x.kind == "doc" and not x.staging and (x.key, x.stamp) in okeys0}
    heads = {k for k, x in late_by_key.items() if newest.get((x.phys, x.key)) == x.stamp}
    bk = lambda recs: {k[:2] for k in recs}
    allowed = w.allowed  # the split oracle's allowed set (see `oracle`)
    replay_bad = lambda r: r[0] != ms[0].state() or (heads - bk(r[1])) or (bk(r[1]) - allowed)
    if PH:
        res["div_replay"] = int(any(replay_bad(r) for r in replays))  # the interleaved replay
        res["div_replay_stamp_order"] = int(any(replay_bad(r) for r in stamp_replays))
        res["stamp_replicas_identical"] = int(stamp_replays[0] == stamp_replays[1])
        res["replay_interleave_order_differs"] = interleave_differs
    else:
        res["div_replay"] = int(any(r[0] != ms[0].state() or r[1] != frozenset(ms[0].loss) for r in replays))
        res["div_replay_stamp_order"] = 0
    res["diverged"] = int(len(states) > 1 or len(losssets) > 1 or res["div_replay"] or res["div_replay_stamp_order"])
    res["div_state"] = int(len(states) > 1)
    res["div_loss"] = int(len(losssets) > 1)
    res["served_mismatch"] = int(any(m.served() != served for m in ms))
    if w.flags.age:
        # 6e: the oracle is age-aware for LINGERING stages only (a vanished writer's never-validly-bound stage that the oracle keeps alive): a member whose
        # own clock is past the stage's deadline + age, and that knows no valid binding, is expected to have buried it. A stage buried by the oracle
        # (O8) or bound is judged as before; an age burial of a stage that IS bound elsewhere is a mismatch (the documented cost).
        def _exp(m):
            dead = set()
            for n in lingering:
                cr = m.creates.get(n)
                if cr is not None and m.age_due(cr) and not m.knows_valid_bind(n):
                    dead.add(n)
            return alive - dead
        res["alive_mismatch"] = int(any({n for n, p in m.phys.items() if p.alive} != _exp(m) for m in ms))
        for n in lingering:  # members straddling the age threshold at the end of the run: a timing artefact of the end, counted apart
            _crs = [(m, m.creates.get(n)) for m in ms if m.creates.get(n) is not None]
            _st = {bool(m.age_due(cr) and not m.knows_valid_bind(n)) for m, cr in _crs}
            if len(_st) > 1:
                res["age_straddle_stages"] += 1
                res["age_straddle_run"] = 1
    else:
        res["alive_mismatch"] = int(any({n for n, p in m.phys.items() if p.alive} != alive for m in ms))
    res["oracle_state"] = int(res["served_mismatch"] or res["alive_mismatch"])
    okeys = set(loss)
    if PH:
        res["loss_missing"] = int(any(heads - bk(m.loss) for m in ms))  # a HEAD without a record
        res["loss_extra"] = int(any(bk(m.loss) - allowed for m in ms))  # a record outside the split oracle's allowed set
        res["loss_extra_base_oracle"] = int(any(bk(m.loss) - okeys for m in ms))  # (the former test: outside the held late writes)
        res["split_allowed_extra_over_base"] = len(allowed - okeys)
        res["loss_mismatch"] = int(res["loss_missing"] or res["loss_extra"])
        res["heads_required"] = len(heads)
        res["true_late_writes_allowed"] = len(okeys)
        nonheads = okeys - heads
        unrep = {k for k in nonheads if any(k not in bk(m.loss) for m in ms)}
        res["nonheads_unreported"] = len(unrep)
        res["nonheads_unreported_vanished_origin"] = sum(1 for k in unrep if w.members[late_by_key[k].origin].vanished)
        res["nonheads_reported"] = len(nonheads) - len(unrep)
        res["nv_nonheads_unreported"] = int(bool(unrep))
        res["nv_late_delete_head_held_only_as_state"] = int(any(
            late_by_key[k].body is None and not any(late_by_key[k].idx in m.recv_entry for m in ms)
            and any(late_by_key[k].idx in m.dochist for m in ms) for k in heads))
        res["nv_late_delete_heads"] = int(any(late_by_key[k].body is None for k in heads))
        bad = 0
        for k in heads:
            for m in ms:
                if sum(1 for kk in m.loss if kk[:2] == k) != 1 or m.events_emitted[k] != 1:
                    bad = 1
        res["head_not_exactly_one_record_and_event"] = bad
    else:
        res["loss_mismatch"] = int(any(set(m.loss) != okeys for m in ms))
        res["loss_missing"] = int(any(okeys - set(m.loss) for m in ms))
        res["loss_extra"] = int(any(set(m.loss) - okeys for m in ms))
    missing_keys = set()
    for m in ms:
        missing_keys |= (heads if PH else okeys) - bk(m.loss) if PH else okeys - set(m.loss)
    if missing_keys:
        # F4 class: the write's origin is gone for good and no live member holds its entry in its retained oplog
        f4 = True
        for k in missing_keys:
            wr = [x for x in w.all if x.kind == "doc" and (x.key, x.stamp) == k]
            if not wr or not w.members[wr[0].origin].vanished or any(
                    wr[0].idx in m.dochist for m in ms if not m.vanished):
                f4 = False
        res["loss_missing_f4_class"] = int(f4)
        res["loss_missing_unexplained"] = int(not f4)
    res["loss_body_mismatch"] = int(any(m.loss.get(k) != v for m in ms for k, v in loss.items() if k in m.loss))
    # vanish after replication: a late write of a vanished origin that reached a live member must have exactly ONE
    # record on every live member and have emitted exactly ONE event there; one that never left the origin is lost
    # with it (the oracle excludes it)
    late_entries = [e for e in w.all if e.kind == "doc" and not e.staging and (e.key, e.stamp) in loss]
    vr = [e for e in late_entries if w.members[e.origin].vanished and (not PH or (e.key, e.stamp) in heads)]
    res["vr_replicated_late_writes"] = len(vr)
    res["vr_unreplicated_late_writes"] = len(w.lost_late)
    if True:
        for _k, _v in w.lwolh.items():
            res["cnt_lwolh_" + _k] = _v
        res["nzrun_lwolh_total"] = int(w.lwolh["total"] > 0)
        res["nzrun_lwolh_replicated"] = int(w.lwolh["replicated"] > 0)
        res["nzrun_lwolh_witnessed_only"] = int(w.lwolh["witnessed_only"] > 0)
    vr_bad = 0
    for e in vr:
        for m in ms:
            if sum(1 for k in m.loss if k[:2] == (e.key, e.stamp)) != 1 or m.events_emitted[(e.key, e.stamp)] != 1:
                vr_bad = 1
    minters = Counter()
    for x in w.all:
        if x.kind == "loss":
            minters[x.key[:2]] += 1
    res["vr_several_minters_collapsed"] = int(any(minters[(e.key, e.stamp)] > 1 for e in vr))
    dup_events = int(any(c > 1 for m in ms for c in m.events_emitted.values()))
    inv = {}
    if PH and res["head_not_exactly_one_record_and_event"]:
        inv["head_not_exactly_one_record_and_event"] = 1
    if vr_bad:
        inv["vanish_replicated_late_write_not_exactly_one_record_and_event"] = 1
    if dup_events:
        inv["duplicate_event_for_one_write"] = 1
    for m in ms:
        for k in m.inv:
            inv[k] = 1
        if (m.cur is None) != (top is None) or (m.cur is not None and top is not None and m.cur.idx != top.idx):
            inv["not_serving_newest_valid_event"] = 1
        for n, p in m.phys.items():
            if p.orphan and any(e.kind == "bind" and e.phys == n and w.valid(e) for e in w.all):
                inv["orphan_with_valid_binding"] = 1
    st = w.stats
    if st["accepted_while_pending"]:
        inv["write_accepted_while_pending"] = 1
        res["pending_acked_lost"] = sum(1 for i in w.pend_acked if (w.all[i].key, w.all[i].stamp) in loss)
    if st["accepted_while_superseded"]:
        res["superseded_acked_lost"] = sum(1 for i in w.sup_acked if (w.all[i].key, w.all[i].stamp) in loss)
    # swapmodel6a.py: CHECK-ONLY addition. In an in-order run (no route step: Q6_F unset or 0; the routes break invariant (ii) by construction) no live
    # member may still be refusing writes at the end of the run (D10: a known register event supersedes the physical it serves, covered or held as
    # information). It reads state only: it draws nothing from the random generator, changes no state, and can only add a failure.
    if not float(os.environ.get("Q6_F", "0") or 0):
        for m in ms:
            _s = m.serving()
            if _s is not None and (m.first_superseding(_s) is not None or m.info_superseding(_s) is not None):
                inv["open_refusal_at_run_end"] = 1
    if st_run["grant_above_coverage"]:
        inv["grant_above_coverage"] = 1  # invariant (i): a grant names an entry above its sender's coverage
    if st_run["page_above_coverage"] and not w.held_uncovered:
        inv["page_above_coverage"] = 1  # invariant (i): a page names an entry above its sender's coverage (6e: an invariant only WITHOUT held_uncovered, as in 6d)
    if w.held_uncovered:
        # 6e (manager's ruling): under product semantics a snapshot page may legitimately carry state its sender holds above its vector
        # (docs/operations.md:1423; the Rust harness counts it as cnt_page_carries_held_state): INFORMATION, counted, not an invariant, not in first_bad_seed
        res["cnt_page_carries_held_state"] = st_run["page_above_coverage"]
        res["nzrun_page_carries_held_state"] = int(st_run["page_above_coverage"] > 0)
    for k in inv:
        res["inv_" + k] = 1
    res["invariant"] = int(bool(inv))
    nv = {
        "flips": st["flips"] > 0,
        "refusals": st["refused"] > 0,
        "refusals_by_pending_binding": st["refused_bind"] > 0,
        "refusals_by_known_drop": st["refused_ldrop"] > 0,
        "refusals_by_known_recreate": st["refused_lcreate"] > 0,
        "snapshot_joins": st["snap"] > 0,
        "snapshot_after_postflip_write": st["snap_after_postflip_write"] > 0,
        "snapshots_carrying_loss_records": st["snap_carrying_records"] > 0,
        "snapshot_entries_not_carried": st["snap_entries_not_carried"] > 0,
        "late_writes(oracle)": len(loss) > 0,
        "loss_records": any(m.loss for m in ms),
        "loss_records_minted_inside_prefix": st["loss_records_inside_prefix"] > 0,
        "reports_delayed_until_burial": st["report_delayed_until_burial"] > 0,
        "history_arrivals_dropped": st["history_arrivals_dropped"] > 0,
        "ldrop": st["ldrop"] > 0,
        "lcreate": st["lcreate"] > 0,
        "ldrop_or_lcreate_after_a_flip": st["ldrop_after_flip"] > 0 or any(
            e.kind in ("ldrop", "lcreate") and any(b.kind == "bind" and w.valid(b) and b.stamp < e.stamp for b in w.all)
            for e in w.all),
        "ldrop_stamped_after_f_made_without_it": any(
            e.kind == "ldrop" and any(b.kind == "bind" and w.valid(b) and b.stamp < e.stamp and e.phys != b.phys
                                      for b in w.all) for e in w.all),
        "orphans_detected": st["orphans_buried"] > 0,
        "abandon_entries": st["abandon_entries"] > 0,
        "orphans_buried_by_abandon": st["orphans_buried_by_abandon"] > 0,
        "vanished_writer": st["writer_vanished"] > 0,
        "vanished_stage_lingering": len(lingering) > 0,
        "invalid_late_binding": st["invalid_binding"] > 0,
        "valid_bind_arrives_late": st["valid_bind_arrives_late"] > 0,
        "concurrent_outs_loser_buried": st["loser_stage_buried"] > 0,
        "swap_not_here_yet(R3 retry)": st["swap_not_here_yet"] > 0,
        "relayed_tombstone": st["relay_tomb"] > 0,
        "kept_path_taken": st["kept_by_adr231_path"] > 0,
        "write_accepted_while_pending": st["accepted_while_pending"] > 0,
        "write_accepted_while_superseded": st["accepted_while_superseded"] > 0,
    }
    for k, v in nv.items():
        res["nv_" + k] = int(v)
    res["accepted_while_pending_count"] = st["accepted_while_pending"]
    res["accepted_while_superseded_count"] = st["accepted_while_superseded"]
    res["nv_vanish_origin_replicated_late_write"] = int(len(vr) > 0)
    res["nv_vanish_origin_unreplicated_late_write"] = int(len(w.lost_late) > 0)
    res["nv_vanish_several_minters_collapsed"] = res["vr_several_minters_collapsed"]
    res["nv_entries_lost_with_a_vanished_member"] = int(len(w.lost) > 0)
    for kk in ("register_events_above_grant", "register_events_not_carried_above_grant", "register_events_read_above_grant",
               "bind_between_first_and_last_page", "register_read_from_a_member_that_is_not_the_sender", "appended_in_position", "witnessed_without_receiving", "state_held_refusals", "snap_deletes_carried_with_id",
               "windows_served_by_a_join_in_progress", "lww_losers_not_kept", "witnessed_at_quiescence", "snap_versions_as_entries"):
        res["nv_" + kk] = int(st[kk] > 0)
        res["cnt_" + kk] = st[kk]
    for kk in V6_COUNTERS:
        res["cnt_" + kk] = st_run[kk]
        res["nzrun_" + kk] = int(st_run[kk] > 0)
    # D10's refusal duration (item 4): one episode per (member, register event held as information) in which the member refused a
    # write on that event; it ends when the member covers the event, or at the run's end
    eps = []
    for m in w.members.values():
        for idx, ep in m.refusal_eps.items():
            e = w.all[idx]
            eps.append(dict(member_vanished=m.vanished, start=ep["start"], last=ep["last"], n=ep["n"], end=ep["end"], end_q=ep["end_q"],
                            info=ep["info"], cover=ep["cover"], cover_q=ep["cover_q"], run_end=w.step, steps_end=w.steps_end,
                            origin_vanished=w.members[e.origin].vanished,
                            unreachable=any(x.idx in w.lost and x.hlc <= e.hlc for x in w.by_origin[e.origin]), kind=e.kind))
    res["_eps"] = eps
    res["late_write_count"] = len(loss)
    res["refused_count"] = st["refused"]
    res["refused_bind_count"] = st["refused_bind"]
    res["refused_ldrop_count"] = st["refused_ldrop"]
    res["refused_lcreate_count"] = st["refused_lcreate"]
    # 6e: the binding delay (see World.bd_scan): per valid binding and live member, the wall clock at which the member first knew the binding minus the stage's deadline (>= 0)
    _bins = 72
    _sd = {}
    res["cnt_bd_never"] = 0
    for blist in w.binds.values():
        for b in blist:
            if not w.valid(b):
                continue
            for m in ms:
                got = w.bd.get((b.idx, m.name))
                if got is None:
                    res["cnt_bd_never"] += 1
                    continue
                d = max(0, got[0] - b.dl)
                res["cnt_bdp_total"] += 1
                res["cnt_bdph_%d" % min(d // 10, _bins - 1)] += 1
                if got[1]:
                    res["cnt_bd_censored"] += 1
                for A in BD_AGES:
                    if d > A:
                        res["cnt_bdp_gt_%d" % A] += 1
                _sd[b.idx] = max(_sd.get(b.idx, 0), d)
    for d in _sd.values():
        res["cnt_bds_total"] += 1
        res["cnt_bdsh_%d" % min(d // 10, _bins - 1)] += 1
        res["mx_bd_stage_max"] = max(res["mx_bd_stage_max"], d)
        for A in BD_AGES:
            if d > A:
                res["cnt_bds_gt_%d" % A] += 1
    if w.flags.age:
        log = w.age_log
        res["age_buried_pairs"] = len(log)
        res["age_snapshot_pairs"] = sum(1 for x in log if x[4])
        stages = {x[1] for x in log}
        res["age_stages"] = len(stages)
        res["age_fired_run"] = int(bool(log))
        # 6f: a member that serves a stage whose physical is dead BECAUSE of an age burial (the 6e snapshot route); must be 0 under the cause-tagged tombstone
        _sna = 0
        for _m in ms:
            if _m.vanished or _m.replica:
                continue
            _s = _m.serving()
            _p = _m.phys.get(_s) if _s is not None else None
            if _p is not None and (not _p.alive or _p.missing) and _p.age:
                _sna += 1
        res["age_sna_pairs"] = _sna
        res["age_sna_run"] = int(_sna > 0)
        res["age_tomb_ignored_when"] = w.stats["age_tomb_ignored_when"]
        res["age_tomb_ignored_where"] = w.stats["age_tomb_ignored_where"]
        res["age_tomb_ignored_unknown_stage"] = w.stats["age_tomb_ignored_unknown_stage"]
        res["age_tomb_applied"] = w.stats["age_tomb_applied"]
        at_burial = {x[1] for x in log if x[3]}
        at_end = {n for n in stages if any(m.knows_valid_bind(n) for m in ms)}
        cost = at_burial | at_end
        valid_any = {n for n in stages if any(w.valid(b) for b in w.binds.get(n, ()))}
        res["age_cost_burial_stages"] = len(at_burial)
        res["age_cost_end_stages"] = len(at_end)
        res["age_cost_stages"] = len(cost)
        res["age_orphan_never_bound"] = len(stages - cost - valid_any)
        res["age_orphan_bind_lost"] = len((stages - cost) & valid_any)
        topn = top.phys if (top is not None and top.kind in ("bind", "lcreate")) else None
        res["age_never_flip_pairs"] = sum(1 for x in log if x[1] in cost and x[1] == topn)
        if cost:
            res["age_cost_run"] = 1
            for kk in ("diverged", "div_state", "div_loss", "div_replay", "alive_mismatch", "served_mismatch", "oracle_state", "invariant",
                       "inv_partial_flip", "inv_orphan_with_valid_binding", "loss_mismatch", "loss_missing", "loss_extra"):
                if res.get(kk):
                    res["age_cost_" + kk] = 1
    return res, w


# ----------------------------------------------------------------------------- batch

BD_AGES = (30, 60, 120, 240, 360, 480, 600)
V6_COUNTERS = ("loss_records_read_late", "loss_records_above_grant", "loss_records_read_above_grant", "loss_info_parked", "loss_info_taken_effect", "snap_records_not_carried_above_grant", "info_parked", "info_taken_effect", "accepted_while_info_superseded", "grant_above_coverage", "page_above_coverage",
               "grant_entries_withheld_above_coverage", "hold_taken", "repair_events", "refusal_episodes", "refusal_episodes_info",
               "dead_page_versions_retained", "refusal_open_at_steps_end", "refusal_open_at_steps_end_info", "refusal_open_at_run_end",
               "refusal_open_at_run_end_info", "staged_row_appended", "staged_row_withdrawn_by_hold", "hb1_held_not_appended", "doc_hold_taken",
               "doc_repair", "held_released_in_window", "held_behind_held")

SUMS = tuple("cnt_" + k for k in V6_COUNTERS) + ("cnt_lwolh_total", "cnt_lwolh_replicated", "cnt_lwolh_origin_only", "cnt_lwolh_witnessed_only", "cnt_page_carries_held_state", "age_buried_pairs", "age_snapshot_pairs", "age_sna_pairs", "age_sna_run", "age_tomb_ignored_when", "age_tomb_ignored_where", "age_tomb_ignored_unknown_stage", "age_tomb_applied", "age_stages", "age_cost_burial_stages", "age_cost_end_stages", "age_cost_stages",
        "age_orphan_never_bound", "age_orphan_bind_lost", "age_never_flip_pairs", "age_straddle_stages", "split_allowed_extra_over_base", "cnt_register_events_above_grant", "cnt_register_events_not_carried_above_grant", "cnt_register_events_read_above_grant",
        "cnt_bind_between_first_and_last_page", "heads_required", "true_late_writes_allowed", "nonheads_unreported", "nonheads_unreported_vanished_origin", "nonheads_reported",
        "cnt_appended_in_position", "cnt_witnessed_without_receiving", "cnt_state_held_refusals", "cnt_snap_deletes_carried_with_id",
        "cnt_windows_served_by_a_join_in_progress", "cnt_lww_losers_not_kept", "cnt_witnessed_at_quiescence", "cnt_snap_versions_as_entries",
        "vr_replicated_late_writes", "vr_unreplicated_late_writes", "vr_several_minters_collapsed_x", "late_write_count", "refused_count", "refused_bind_count", "refused_ldrop_count", "refused_lcreate_count",
        "pending_acked_lost", "accepted_while_pending_count", "accepted_while_superseded_count", "superseded_acked_lost")


def one(args):
    mode, seed, nm, nk, outs, base, steps = args
    res, _ = run(mode, seed, nmembers=nm, nkeys=nk, outs=outs, base=base, steps=steps)
    return seed, dict(res)


def configs(members, outs, keys, grid):
    if grid == "full":
        return [(m, o, k) for m in members for o in outs for k in keys]
    out = []
    i = 0
    for m in members:
        for o in outs:
            out.append((m, o, keys[i % len(keys)]))
            i += 1
    return out


def refusal_episode_stats(eps):
    """D10's refusal durations. `eps`: the episodes of a set of runs: one per member and register event on which the member refused a write
    (`Member.refusal_eps`): from its first refused write to the step its served physical is no longer superseded by the event; `info` = the first
    refusal was on an event held as information (known, not covered, rule B), otherwise on a covered event not yet visible (e.g. a member mid-join
    whose stage is a page of the unfinished join). Length in steps: end - start when the refusal ended during the run's steps (`closed_in_run`);
    otherwise run_end - start (`open_at_steps_end` = closed only by the end-of-run delivery + `open_at_run_end`, the ones never closed).
    For the information episodes a second measure: steps from the first refusal until the member COVERS the event (it takes effect). Episodes of
    members that vanished are counted apart. No interpretation: the numbers only."""
    live = [e for e in eps if not e["member_vanished"]]

    def length(e):
        return (e["end"] - e["start"]) if (e["end"] is not None and not e["end_q"]) else (e["run_end"] - e["start"])

    def row(sel):
        n = len(sel)
        if not n:
            return "episodes=0"
        lens = sorted(length(e) for e in sel)
        return (f"episodes={n} max_len={lens[-1]} median_len={statistics.median(lens)} closed_in_run={sum(1 for e in sel if e['end'] is not None and not e['end_q'])} "
                f"open_at_steps_end={sum(1 for e in sel if e['end'] is None or e['end_q'])} (closed by the end-of-run delivery {sum(1 for e in sel if e['end_q'])}, "
                f"still open at run end {sum(1 for e in sel if e['end'] is None)}) refusals={sum(e['n'] for e in sel)}")

    def to_cover(sel):
        sel = [e for e in sel if e["info"]]
        if not sel:
            return "episodes=0"
        lens = sorted((e["cover"] - e["start"]) if (e["cover"] is not None and not e["cover_q"]) else (e["run_end"] - e["start"]) for e in sel)
        return (f"episodes={len(sel)} max_len={lens[-1]} median_len={statistics.median(lens)} covered_in_run={sum(1 for e in sel if e['cover'] is not None and not e['cover_q'])} "
                f"covered_only_by_the_end_of_run_delivery={sum(1 for e in sel if e['cover'] is not None and e['cover_q'])} never_covered={sum(1 for e in sel if e['cover'] is None)}")
    out = [("all", row(live)),
           ("information (known, not covered)", row([e for e in live if e["info"]])),
           ("covered, not yet visible", row([e for e in live if not e["info"]])),
           ("origin not vanished", row([e for e in live if not e["origin_vanished"]])),
           ("origin vanished", row([e for e in live if e["origin_vanished"]])),
           ("event reachable", row([e for e in live if not e["unreachable"]])),
           ("event unreachable (its origin's prefix died)", row([e for e in live if e["unreachable"]])),
           ("information episodes, to coverage: all", to_cover(live)),
           ("information episodes, to coverage: origin not vanished", to_cover([e for e in live if not e["origin_vanished"]])),
           ("information episodes, to coverage: origin vanished", to_cover([e for e in live if e["origin_vanished"]]))]
    return out, len(eps) - len(live)


def summarise(rows):
    tot = Counter()
    first = {}
    for seed, r in rows:
        for k, v in r.items():
            if k.startswith("mx_"):
                tot[k] = max(tot[k], v)
                continue
            if k in SUMS or k.startswith("cnt_bd"):
                tot[k] += v
                continue
            if v:
                tot[k] += 1
                first.setdefault(k, seed)
    return tot, first


# keys that are information, not failures: kept out of `first_bad_seed` (so its value is REPORT5's)
INFO_KEYS = ("replay_interleave_order_differs", "loss_extra_base_oracle", "stamp_replicas_identical")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=200)
    ap.add_argument("--seed0", type=int, default=0)
    ap.add_argument("--members", default="3")
    ap.add_argument("--outs", default="2")
    ap.add_argument("--keys", default="4")
    ap.add_argument("--grid", default="diag")
    ap.add_argument("--modes", default="v3")
    ap.add_argument("--base", default="v3")
    ap.add_argument("--workers", type=int, default=4)
    ap.add_argument("--steps", type=int, default=None, help="actions per run (default 120 + 40 * outs)")
    ap.add_argument("--show", type=int, default=None)
    ap.add_argument("--mode", default="v3")
    ap.add_argument("--quiet-nv", action="store_true")
    ap.add_argument("--list-fail", action="store_true", help="print the failing seeds per check for every configuration")
    ap.add_argument("--delete-bias", action="store_true", help="a delete-heavy write mix (2 of 4 writes are deletes) for non-vacuity of the delete routes")
    a = ap.parse_args()
    ints = lambda s: [int(x) for x in s.split(",")]
    if a.show is not None:
        nm, no, nk = ints(a.members)[0], ints(a.outs)[0], ints(a.keys)[0]
        res, w = run(a.mode, a.show, nmembers=nm, nkeys=nk, outs=no, base=a.base, steps=a.steps, trace=True)
        for line in w.log:
            print(line)
        print("-- final")
        top, alive, served, loss, lingering = oracle(w)
        print("oracle top event:", top, "alive:", alive, "served:", served)
        print("oracle loss:", sorted(loss))
        for m in w.members.values():
            print(f"member {m.name} cur={m.cur} serving={m.serving()} served={m.served()}")
            print(f"   alive={sorted(n for n, p in m.phys.items() if p.alive)} loss={sorted(m.loss)} inv={sorted(m.inv)}")
        print({k: v for k, v in res.items() if v and not k.startswith("_")})
        for _e in res["_eps"]:
            print("refusal episode:", _e)
        return
    if a.delete_bias:
        os.environ["Q6_DELETE_BIAS"] = "1"
    cfgs = configs(ints(a.members), ints(a.outs), ints(a.keys), a.grid)
    modes = a.modes.split(",")
    pool = multiprocessing.Pool(a.workers) if a.workers > 1 else None
    for mode in modes:
        allrows = []
        alleps = []
        print(f"=== mode {mode} (base {a.base}) seeds={a.seeds} configs={len(cfgs)}", flush=True)
        for (nm, no, nk) in cfgs:
            jobs = [(mode, s, nm, nk, no, a.base, a.steps) for s in range(a.seed0, a.seed0 + a.seeds)]
            rows = pool.map(one, jobs, chunksize=20) if pool else [one(j) for j in jobs]
            for _s, _r in rows:
                alleps.extend(_r.pop("_eps", []))
            allrows.extend(rows)
            if a.list_fail:
                cats = {}
                for s, r in rows:
                    for kk in ("diverged", "oracle_state", "loss_mismatch", "inv_partial_flip", "inv_orphan_with_valid_binding",
                               "inv_not_serving_newest_valid_event", "inv_head_not_exactly_one_record_and_event",
                               "inv_duplicate_event_for_one_write", "inv_serve_not_alive", "inv_bury_without_newer_visible"):
                        if r.get(kk):
                            cats.setdefault(kk, []).append(s)
                for kk, lst in sorted(cats.items()):
                    print(f"    FAIL members={nm} outs={no} keys={nk} {kk}: seeds {lst[:12]}{' ...' if len(lst) > 12 else ''} (n={len(lst)})", flush=True)
            tot, first = summarise(rows)
            invs = ",".join(f"{k[4:]}={v}" for k, v in sorted(tot.items()) if k.startswith("inv_"))
            print(f"  members={nm} outs={no} keys={nk} runs={len(rows)} diverged={tot['diverged']} "
                  f"(state={tot['div_state']} loss={tot['div_loss']} replay={tot['div_replay']}) oracle_state_mismatch={tot['oracle_state']} "
                  f"loss_mismatch={tot['loss_mismatch']} (missing={tot['loss_missing']} extra={tot['loss_extra']} "
                  f"body={tot['loss_body_mismatch']}) invariant_runs={tot['invariant']} [{invs}] "
                  f"first_bad_seed={min((v for k, v in first.items() if not k.startswith('nv_') and not k.startswith('nzrun_') and not k.startswith('age_') and not k.startswith('mx_') and not k.startswith('cnt_bd') and k not in SUMS and k not in INFO_KEYS and k != 'div_replay_stamp_order'), default=None)} "
                  f"replay_stamp_order={tot['div_replay_stamp_order']}", flush=True)
        tot, first = summarise(allrows)
        print(f"  TOTAL {mode}: runs={len(allrows)} diverged={tot['diverged']} oracle_state_mismatch={tot['oracle_state']} "
              f"loss_mismatch={tot['loss_mismatch']} invariant_runs={tot['invariant']}", flush=True)
        invs = {k[4:]: v for k, v in sorted(tot.items()) if k.startswith("inv_")}
        print(f"  TOTAL {mode} invariant runs by check: {invs}", flush=True)
        print(f"  TOTAL {mode} first seeds: " + str({k: v for k, v in sorted(first.items()) if k in (
            'diverged', 'oracle_state', 'loss_mismatch', 'invariant') or k.startswith('inv_')}), flush=True)
        print(f"  TOTAL {mode} detail: div_state={tot['div_state']} div_loss={tot['div_loss']} div_replay={tot['div_replay']} "
              f"served_mismatch={tot['served_mismatch']} alive_mismatch={tot['alive_mismatch']} loss_missing={tot['loss_missing']} "
              f"loss_extra={tot['loss_extra']} missing_in_f4_class={tot['loss_missing_f4_class']} "
              f"missing_unexplained={tot['loss_missing_unexplained']}", flush=True)
        print(f"  TOTAL {mode} per-head: heads_required={tot['heads_required']} true_late_writes_allowed={tot['true_late_writes_allowed']} "
              f"nonheads_unreported={tot['nonheads_unreported']} (vanished origin {tot['nonheads_unreported_vanished_origin']}) "
              f"nonheads_reported={tot['nonheads_reported']} | head_missing_runs={tot['loss_missing']} false_record_runs={tot['loss_extra']} "
              f"head_not_exactly_one_runs={tot['head_not_exactly_one_record_and_event']}", flush=True)
        print(f"  TOTAL {mode} mechanism counts: appended_in_position={tot['cnt_appended_in_position']} "
              f"witnessed_without_receiving={tot['cnt_witnessed_without_receiving']} state_held_refusals={tot['cnt_state_held_refusals']} "
              f"snap_deletes_carried_with_id={tot['cnt_snap_deletes_carried_with_id']} snap_versions_as_entries={tot['cnt_snap_versions_as_entries']} "
              f"windows_served_by_a_join_in_progress={tot['cnt_windows_served_by_a_join_in_progress']} "
              f"lww_losers_not_kept={tot['cnt_lww_losers_not_kept']} witnessed_at_quiescence={tot['cnt_witnessed_at_quiescence']}", flush=True)
        print(f"  TOTAL {mode} replay checks: div_replay(interleaved)={tot['div_replay']} div_replay_stamp_order={tot['div_replay_stamp_order']} "
              f"runs_where_the_interleaved_order_differs_from_stamp_order={tot['replay_interleave_order_differs']} "
              f"runs_where_the_two_stamp_order_replicas_are_identical={tot['stamp_replicas_identical']} "
              f"loss_extra_runs_under_the_former_oracle={tot['loss_extra_base_oracle']} "
              f"writes_allowed_by_the_split_oracle_beyond_the_former_set={tot['split_allowed_extra_over_base']}", flush=True)
        print(f"  TOTAL {mode} rule B and grant counters: info_parked={tot['cnt_info_parked']} (runs {tot['nzrun_info_parked']}) "
              f"info_taken_effect={tot['cnt_info_taken_effect']} accepted_while_info_superseded={tot['cnt_accepted_while_info_superseded']} "
              f"grant_above_coverage={tot['cnt_grant_above_coverage']} page_above_coverage={tot['cnt_page_above_coverage']} "
              f"grant_entries_withheld_above_coverage={tot['cnt_grant_entries_withheld_above_coverage']} "
              f"hold_taken={tot['cnt_hold_taken']} repair_events={tot['cnt_repair_events']} refusal_episodes={tot['cnt_refusal_episodes']} "
              f"(information {tot['cnt_refusal_episodes_info']})", flush=True)
        print(f"  LOSSRULE {mode} (6b, not a TOTAL line): loss_info_parked={tot['cnt_loss_info_parked']} (runs {tot['nzrun_loss_info_parked']}) "
              f"loss_info_taken_effect={tot['cnt_loss_info_taken_effect']} snap_records_not_carried_above_grant={tot['cnt_snap_records_not_carried_above_grant']} "
              f"(runs {tot['nzrun_snap_records_not_carried_above_grant']}) loss_records_read_late={tot['cnt_loss_records_read_late']} "
              f"loss_records_above_grant={tot['cnt_loss_records_above_grant']} (runs {tot['nzrun_loss_records_above_grant']}) "
              f"loss_records_read_above_grant={tot['cnt_loss_records_read_above_grant']} (runs {tot['nzrun_loss_records_read_above_grant']})", flush=True)
        print(f"  TOTAL {mode} seed-885 rule and D10 state counters: dead_page_versions_retained={tot['cnt_dead_page_versions_retained']} (runs {tot['nzrun_dead_page_versions_retained']}) "
              f"member-runs refusing (state, any cause) when the steps end={tot['cnt_refusal_open_at_steps_end']} (information {tot['cnt_refusal_open_at_steps_end_info']}), "
              f"at the end of the run={tot['cnt_refusal_open_at_run_end']} (information {tot['cnt_refusal_open_at_run_end_info']})", flush=True)
        _cl = sorted(e["end"] - e["start"] for e in alleps if e["end"] is not None)
        print(f"  STAGEDROW {mode} (6d, not a TOTAL line): staged_row_appended={tot['cnt_staged_row_appended']} (runs {tot['nzrun_staged_row_appended']}) "
              f"withdrawn_by_hold={tot['cnt_staged_row_withdrawn_by_hold']} hb1_held_not_appended={tot['cnt_hb1_held_not_appended']} "
              f"doc_hold_taken={tot['cnt_doc_hold_taken']} doc_repair={tot['cnt_doc_repair']} held_released_in_window={tot['cnt_held_released_in_window']} "
              f"held_behind_held={tot['cnt_held_behind_held']} | refusal episodes closed (b1rand measure, every member): n={len(_cl)}"
              + (f" median={_cl[len(_cl) // 2]} p99={_cl[int(len(_cl) * 0.99)]} max={_cl[-1]}" if _cl else ""), flush=True)
        if flags_for(mode, a.base).age:
            print(f"  AGE {mode} (6e, not a TOTAL line): Q6_AGE={flags_for(mode, a.base).age} fired_runs={tot['age_fired_run']} buried_pairs={tot['age_buried_pairs']} "
                  f"(via snapshot {tot['age_snapshot_pairs']}) stages={tot['age_stages']} | COST: stages_with_valid_binding_at_a_live_member_at_burial={tot['age_cost_burial_stages']} "
                  f"at_run_end={tot['age_cost_end_stages']} either={tot['age_cost_stages']} cost_runs={tot['age_cost_run']} never_flip_member_stage_pairs={tot['age_never_flip_pairs']} | "
                  f"cost_runs_with: diverged={tot['age_cost_diverged']} div_state={tot['age_cost_div_state']} div_loss={tot['age_cost_div_loss']} div_replay={tot['age_cost_div_replay']} "
                  f"alive_mismatch={tot['age_cost_alive_mismatch']} served_mismatch={tot['age_cost_served_mismatch']} partial_flip={tot['age_cost_inv_partial_flip']} "
                  f"orphan_with_valid_binding={tot['age_cost_inv_orphan_with_valid_binding']} invariant={tot['age_cost_invariant']} loss_mismatch={tot['age_cost_loss_mismatch']} "
                  f"(missing={tot['age_cost_loss_missing']} extra={tot['age_cost_loss_extra']}) | TRULY ORPHAN stages: never_validly_bound={tot['age_orphan_never_bound']} "
                  f"bind_lost_with_its_origin={tot['age_orphan_bind_lost']} | end-of-run straddle: stages={tot['age_straddle_stages']} runs={tot['age_straddle_run']}", flush=True)
        if flags_for(mode, a.base).age:
            print(f"  AGE6F {mode} (6f, not a TOTAL line): Q6_AGE_TOMB={int(flags_for(mode, a.base).age_tomb)} serve_not_alive_by_age: runs={tot['age_sna_run']} member_stage_pairs={tot['age_sna_pairs']} | "
                  f"snapshot age tombstones: applied={tot['age_tomb_applied']} ignored: WHERE_fails={tot['age_tomb_ignored_where']} WHEN_fails={tot['age_tomb_ignored_when']} unknown_stage={tot['age_tomb_ignored_unknown_stage']}", flush=True)
        print(f"  PAGESTATE {mode} (6e, not a TOTAL line): page_carries_held_state (information under held_uncovered)={tot['cnt_page_carries_held_state']} (runs {tot['nzrun_page_carries_held_state']}) | "
              f"page_above_coverage as an INVARIANT (without held_uncovered, as in 6d): runs={tot['inv_page_above_coverage']}", flush=True)
        _h = [tot["cnt_bdsh_%d" % i] for i in range(72)]
        _n = sum(_h)

        def _q(p):
            acc = 0
            for i, c in enumerate(_h):
                acc += c
                if _n and acc >= p * _n:
                    return (i + 1) * 10
            return None
        _hp = [tot["cnt_bdph_%d" % i] for i in range(72)]
        print(f"  BINDDELAY {mode} (6e, not a TOTAL line): delay = wall clock at which a live member first knew a valid binding minus the stage's deadline, in model clock units (steps). "
              f"stages bound={tot['cnt_bds_total']} (member-binding pairs {tot['cnt_bdp_total']}, known only at the end-of-run delivery {tot['cnt_bd_censored']}, never known by a live member {tot['cnt_bd_never']}) | "
              f"LONGEST delay per stage: max={tot['mx_bd_stage_max']} p50<={_q(0.5)} p99<={_q(0.99)} p99.9<={_q(0.999)} (bin upper edges, width 10) | stages whose longest delay exceeds the age: "
              + " ".join(f"{A}:{tot['cnt_bds_gt_%d' % A]}" for A in BD_AGES) + " | member-binding pairs whose delay exceeds the age: " + " ".join(f"{A}:{tot['cnt_bdp_gt_%d' % A]}" for A in BD_AGES), flush=True)
        print("  BINDDELAYHIST " + mode + " stage-longest-delay histogram (bin = delay // 10, last bin open): " + str({i: c for i, c in enumerate(_h) if c}), flush=True)
        print(f"  LWOLH {mode} (6e, not a TOTAL line): writes lost with their only holders (late writes no live member applied or holds out of position): "
              f"{tot['cnt_lwolh_total']} writes in {tot['nzrun_lwolh_total']} runs | replicated then every holder vanished {tot['cnt_lwolh_replicated']} (runs {tot['nzrun_lwolh_replicated']}) | "
              f"origin only {tot['cnt_lwolh_origin_only']} | of these, held by a live member as WITNESSED-ONLY under 6d's oracle {tot['cnt_lwolh_witnessed_only']} (runs {tot['nzrun_lwolh_witnessed_only']})", flush=True)
        _lines, _vanished_member = refusal_episode_stats(alleps)
        for _name, _txt in _lines:
            print(f"  TOTAL {mode} refusal durations [{_name}]: {_txt}", flush=True)
        print(f"  TOTAL {mode} refusal durations: episodes of members that vanished (not in the lines above)={_vanished_member}", flush=True)
        if not a.quiet_nv:
            nv = {k[3:]: v for k, v in sorted(tot.items()) if k.startswith("nv_")}
            print(f"  NON-VACUITY {mode} (runs of {len(allrows)} exercising): {nv}", flush=True)
            print(f"  COUNTS {mode}: late writes (oracle)={tot['late_write_count']} refusals={tot['refused_count']} "
                  f"(pending binding={tot['refused_bind_count']} known drop={tot['refused_ldrop_count']} "
                  f"known recreate={tot['refused_lcreate_count']}) "
                  f"accepted while a binding was pending={tot['accepted_while_pending_count']} (lost={tot['pending_acked_lost']}) "
                  f"accepted while superseded={tot['accepted_while_superseded_count']} (lost={tot['superseded_acked_lost']})", flush=True)
    if pool:
        pool.close()


if __name__ == "__main__":
    main()
