#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| kimmy_fuzz_harness::key_encoding(data));
