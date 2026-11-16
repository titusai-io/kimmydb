//! The aggregation rules of the reference's recorded sweep (q7s4a to q7s4f and the q8 sets,
//! reference 6.0.5), one module per root cause of `q7s4-aggregation-triage.md`.
//!
//! Every case names the recorded line it is checked against: `d:33` is line 33 of
//! `q7s4d.jsonl`, `b:788` line 788 of `q7s4b.jsonl`. A case here is the reference's answer, not
//! ours. A behaviour that was not recorded is marked UNRECORDED where it is asserted.

mod common;

mod a12_divide_mod_null;
mod a14_if_null;
mod a17_array_elem_at;
mod a18_lookup_empty_local;
mod a1_stage_writer;
mod a20_read_paths;
mod a21_edges;
mod a24_match_before_sort;
mod a25_edges_two;
mod a4_date_shift;
mod a5_group_key_missing;
mod a6_field_paths;
mod a7_substr_cp;
mod a8_unwind_null;
mod a9_group_key_sort;
