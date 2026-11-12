//! The model's flags, bases and mutants (`swapmodel5.py` 73-206). Only the `v5` family is ported; the
//! generous-relay bases and the flags only they read (`product_relay` is always on, `snap_with_dochist`) are not.
//! A mutant is a runtime switch of this struct, never product code.

#[derive(Clone, Copy, Debug, Default)]
pub struct Flags {
    pub keep: bool,
    pub digest: bool,
    pub pend_accepts: bool,
    pub pend_narrow: bool,
    pub no_loss: bool,
    pub visible_newest: bool,
    pub bury_without_alive: bool,
    pub stage_buried_early: bool,
    pub bt_early: bool,
    pub ldrop_identity_only: bool,
    pub no_recreate_floor: bool,
    pub recreate_reuse: bool,
    pub orphan_age: Option<i64>,
    pub orphan_no_deadline: bool,
    pub loss_key_collapse: bool,
    pub loss_outside_prefix: bool,
    pub report_before_burial: bool,
    pub snap_drop_loss: bool,
    pub snap_no_loss: bool,
    pub snap_no_events: bool,
    pub snap_no_orphan_burials: bool,
    pub grant_before_pages: bool,
    pub defer_received: bool,
    pub state_scan: bool,
    pub retain_history: bool,
    pub snap_late_entries: bool,
    pub record_per_minter: bool,
    pub pending_dedup: bool,
    pub append_in_position: bool,
    pub snap_versions_as_entries: bool,
    pub snap_delete_bare: bool,
    pub ignored_not_served: bool,
    pub paged_register_read: bool,
    pub register_grant_filter: bool,
    pub stale_src_bug: bool,
    /// v6: a register event takes effect only once covered; one ahead of its position is information
    pub rule_b: bool,
    /// v6: the product's grant: the sender's contiguous coverage, and only that prefix of its held set
    pub grant_prefix: bool,
    /// v6 (seed 885): a snapshot page's versions of a physical buried on the receiver are history on arrival: retained
    pub dead_page_retained: bool,
    /// 6r (seed 1434, rule B for loss records): a loss entry enters the member's state only in position; ahead of it, it is
    /// information (`loss_info`) until the member covers its origin up to it
    pub loss_in_position: bool,
    /// 6r: a snapshot carries a record only if some entry of that record is within the grant
    pub loss_grant_filter: bool,
    /// 6b (the companion clause): the sender's loss records are read when the LAST page lands, not at the first page
    pub paged_loss_read: bool,
    /// Rust-only mutant of the step check: a loss entry received from another member enters the state but is not appended
    pub fm_loss_not_appended: bool,
    /// Rust-only mutants of the `field` policy's apply and loss path (point 4; read only under `Policy::Field`, no-ops otherwise)
    pub fm_always_append: bool,
    pub fm_stale_body: bool,
    pub fm_loss_superseder_stamp: bool,
    pub fm_no_page_loss: bool,
    pub fm_stale_version_stamp: bool,
    pub fm_witness_off: bool,
    /// Rust-only mutants of `K` (read only with `Cfg::k` set)
    pub fm_k_drop_newest: bool,
    pub fm_k_plus_one: bool,
    pub fm_k_ignore_q7: bool,
    /// Rust-only mutant of the product routes: `doc_hold` also delivers register events out of position
    pub fm_register_ahead: bool,
}

/// The Rust-only mutant of the product routes (invariant (ii) broken on purpose; graded on the product-route rows).
pub const ROUTE_MUTANTS: [&str; 1] = ["m-register-ahead"];

/// The Rust-only mutants of the `K` bound (graded on the `K = 4` grid).
pub const K_MUTANTS: [&str; 3] = [
    // the window keeps the oldest `K` records, drops the newest
    "m-field-k-drop-newest",
    // the window keeps `K + 1`
    "m-field-k-plus-one",
    // the window counts `outReplaced`'s records only, not Q7's beside them
    "m-field-k-ignore-q7",
];

/// The Rust-only mutants of the `field` policy (a deliberate fault in its apply or loss path); the Document-vs-Field
/// metamorphic check and the field grid are graded on them.
pub const FIELD_MUTANTS: [&str; 6] = [
    // every entry is appended, also one the join left unchanged or the covered-entry skip only witnessed
    "m-field-always-append",
    // the body is not refreshed when the join changed the state: the document keeps its old body
    "m-field-stale-body",
    // a loss record is keyed by the superseding event's stamp, not by the loser's
    "m-field-loss-superseder-stamp",
    // a retained page version's late cells are not reported
    "m-field-no-page-loss",
    // after a join the version keeps its old stamp (the stamp of the latest write merged is not taken)
    "m-field-stale-version-stamp",
    // the HLC rule's witness of a joined page state's version vector is off
    "m-field-witness-off",
];

/// Every base the harness knows.
pub const BASES: [&str; 4] =
    ["v5", "v5-late-register", "v5-late-register-literal", "v5-literal-with-rule"];

/// The mutants of `MUTANTS` (`swapmodel5.py` 160-190, plus the two of `swapmodel6a.py`), in the model's order.
pub const MUTANTS: [&str; 35] = [
    "m-recreate-reuse",
    "m-pending-narrow",
    "m-loss-outside-prefix",
    "m-snap-drop-loss",
    "m-snap-no-loss-records",
    "m-grant-before-pages",
    "m-snap-no-orphan-burials",
    "m-report-before-burial",
    "m-keep",
    "m-digest",
    "m-pending-accepts",
    "m-no-loss-records",
    "m-visible-newest",
    "m-bury-without-alive",
    "m-stage-buried-early",
    "m-bound-out-tombstone-early",
    "m-ldrop-identity-only",
    "m-no-recreate-floor",
    "m-orphan-age",
    "m-orphan-no-deadline",
    "m-loss-key-collapse",
    "m-snap-no-events",
    "m-record-per-minter",
    "m-snap-register-late",
    "m-no-append-in-position",
    "m-snap-delete-bare",
    "m-ignored-event-not-served",
    "m-no-defer-received",
    "m-no-pending-dedup",
    "m-rule-b-off",
    "m-dead-page-dropped",
    // 6r: rule B for loss records off (a loss entry received ahead of its position enters the state at once), with the grant filter
    "m-loss-out-of-position",
    // 6b: the paged read of the records at the last page without the grant clause
    "m-snap-record-no-grant-filter",
    // Rust-only: the step check's own mutant: a received loss entry is in the state but not appended
    "m-loss-received-not-appended",
    // Rust-only: the reviewer's source-overwrite defect of a join (`v5-literal-with-rule`) as a mutant of the v6 base
    "m-stale-src",
];

fn v5() -> Flags {
    Flags {
        defer_received: true,
        state_scan: true,
        retain_history: true,
        snap_late_entries: true,
        pending_dedup: true,
        append_in_position: true,
        snap_versions_as_entries: true,
        paged_register_read: true,
        register_grant_filter: true,
        ..Flags::default()
    }
}

/// `BASES[name]` as the model builds it on top of the defaults.
pub fn base(name: &str) -> Option<Flags> {
    let mut f = v5();
    match name {
        "v5" => {}
        "v6" => {
            f.rule_b = true;
            f.grant_prefix = true;
            f.dead_page_retained = true;
            f.loss_in_position = true;
            f.loss_grant_filter = true;
        }
        "v5-late-register" => f.register_grant_filter = false,
        "v5-late-register-literal" => {
            f.register_grant_filter = false;
            f.stale_src_bug = true;
        }
        "v5-literal-with-rule" => f.stale_src_bug = true,
        _ => return None,
    }
    Some(f)
}

fn apply_mutant(f: &mut Flags, name: &str, arg: Option<i64>) -> bool {
    match name {
        "m-recreate-reuse" => f.recreate_reuse = true,
        "m-pending-narrow" => f.pend_narrow = true,
        "m-loss-outside-prefix" => f.loss_outside_prefix = true,
        "m-snap-drop-loss" => f.snap_drop_loss = true,
        "m-snap-no-loss-records" => f.snap_no_loss = true,
        "m-grant-before-pages" => f.grant_before_pages = true,
        "m-snap-no-orphan-burials" => f.snap_no_orphan_burials = true,
        "m-report-before-burial" => f.report_before_burial = true,
        "m-keep" => f.keep = true,
        "m-digest" => f.digest = true,
        "m-pending-accepts" => f.pend_accepts = true,
        "m-no-loss-records" => f.no_loss = true,
        "m-visible-newest" => f.visible_newest = true,
        "m-bury-without-alive" => f.bury_without_alive = true,
        "m-stage-buried-early" => f.stage_buried_early = true,
        "m-bound-out-tombstone-early" => f.bt_early = true,
        "m-ldrop-identity-only" => f.ldrop_identity_only = true,
        "m-no-recreate-floor" => f.no_recreate_floor = true,
        "m-orphan-age" => f.orphan_age = Some(arg.unwrap_or(25)),
        "m-orphan-no-deadline" => f.orphan_no_deadline = true,
        "m-loss-key-collapse" => f.loss_key_collapse = true,
        "m-snap-no-events" => f.snap_no_events = true,
        "m-record-per-minter" => f.record_per_minter = true,
        "m-snap-register-late" => {
            f.paged_register_read = true;
            f.register_grant_filter = false;
        }
        "m-no-append-in-position" => f.append_in_position = false,
        "m-snap-delete-bare" => f.snap_delete_bare = true,
        "m-ignored-event-not-served" => f.ignored_not_served = true,
        "m-no-defer-received" => f.defer_received = false,
        "m-no-pending-dedup" => f.pending_dedup = false,
        "m-rule-b-off" => f.rule_b = false,
        "m-dead-page-dropped" => f.dead_page_retained = false,
        "m-loss-out-of-position" => {
            f.loss_in_position = false;
            f.loss_grant_filter = false;
        }
        // 6b: the paged read of the sender's records at the last page: `v6-paged-loss` is the control (the clause applies),
        // `m-snap-record-no-grant-filter` reads them without the clause
        "v6-paged-loss" => f.paged_loss_read = true,
        "m-snap-record-no-grant-filter" => {
            f.paged_loss_read = true;
            f.loss_grant_filter = false;
        }
        "m-loss-received-not-appended" => f.fm_loss_not_appended = true,
        "m-stale-src" => f.stale_src_bug = true,
        "m-field-always-append" => f.fm_always_append = true,
        "m-field-stale-body" => f.fm_stale_body = true,
        "m-field-loss-superseder-stamp" => f.fm_loss_superseder_stamp = true,
        "m-field-no-page-loss" => f.fm_no_page_loss = true,
        "m-field-stale-version-stamp" => f.fm_stale_version_stamp = true,
        "m-field-witness-off" => f.fm_witness_off = true,
        "m-field-k-drop-newest" => f.fm_k_drop_newest = true,
        "m-field-k-plus-one" => f.fm_k_plus_one = true,
        "m-field-k-ignore-q7" => f.fm_k_ignore_q7 = true,
        "m-register-ahead" => f.fm_register_ahead = true,
        _ => return false,
    }
    true
}

/// `flags_for(mode, base)`: the base's flags, then each `+`-joined part of the mode (a base name replaces the
/// flags it sets, a mutant name switches its flag; `m-orphan-age:N` takes the age).
pub fn flags_for(mode: &str, base_name: &str) -> Flags {
    let mut f = base(base_name).unwrap_or_else(|| panic!("unknown base {base_name}"));
    for part in mode.split('+') {
        let (name, arg) = match part.split_once(':') {
            Some((n, a)) => (n, Some(a.parse::<i64>().expect("a mutant argument"))),
            None => (part, None),
        };
        if let Some(b) = base(name) {
            // a base in the mode overrides what it sets; the bases differ from `v5` only in these flags
            f.register_grant_filter = b.register_grant_filter;
            f.stale_src_bug = b.stale_src_bug;
            f.rule_b = b.rule_b;
            f.grant_prefix = b.grant_prefix;
            f.dead_page_retained = b.dead_page_retained;
            f.loss_in_position = b.loss_in_position;
            f.loss_grant_filter = b.loss_grant_filter;
        } else if !apply_mutant(&mut f, name, arg) {
            panic!("unknown mode {mode}");
        }
    }
    f
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_mutant_name_is_known_and_changes_a_flag() {
        for m in MUTANTS {
            let base = flags_for("v6", "v6");
            let f = flags_for(m, "v6");
            assert_ne!(format!("{base:?}"), format!("{f:?}"), "{m} changes nothing");
        }
    }

    #[test]
    fn every_field_mutant_name_is_known_and_changes_a_flag() {
        for m in FIELD_MUTANTS.into_iter().chain(K_MUTANTS).chain(ROUTE_MUTANTS) {
            let base = flags_for("v6", "v6");
            let f = flags_for(m, "v6");
            assert_ne!(format!("{base:?}"), format!("{f:?}"), "{m} changes nothing");
        }
    }

    #[test]
    fn modes_combine_with_plus_and_age_takes_an_argument() {
        let f = flags_for("m-orphan-age:10+m-snap-no-orphan-burials", "v5");
        assert_eq!(f.orphan_age, Some(10));
        assert!(f.snap_no_orphan_burials);
        let lit = flags_for("v5-late-register-literal", "v5");
        assert!(lit.stale_src_bug && !lit.register_grant_filter);
        let rule = flags_for("v5-literal-with-rule", "v5");
        assert!(rule.stale_src_bug && rule.register_grant_filter);
    }
}
