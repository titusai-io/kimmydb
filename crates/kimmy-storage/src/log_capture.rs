//! Capturing what a test body logs, without the callsite race of a scoped
//! subscriber.
//!
//! `tracing` caches, per callsite, whether any subscriber wants it. A callsite
//! first hit by a thread with no subscriber, while another test thread is
//! installing its own with `with_default`, can be cached as "never", and the
//! capturing test then sees no line at all, though its thread did log. That
//! needs only two test threads reaching one `warn!` (a test that captures it
//! and another that merely opens a store), so it shows under load and never
//! alone.
//!
//! One global subscriber, installed once for the test binary, closes it: every
//! callsite is registered against a subscriber that wants it, whichever thread
//! gets there first. It writes into a buffer owned by the thread that asked to
//! capture, and discards the output of every other thread.

use std::cell::RefCell;
use std::io::Write;
use std::sync::Once;

thread_local! {
    static CAPTURE: RefCell<Option<Vec<u8>>> = const { RefCell::new(None) };
}

struct Sink;

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        CAPTURE.with(|c| {
            if let Some(out) = c.borrow_mut().as_mut() {
                out.extend_from_slice(buf);
            }
        });
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Ends a capture on this thread, also when the body panics.
struct Capturing;

impl Drop for Capturing {
    fn drop(&mut self) {
        CAPTURE.with(|c| *c.borrow_mut() = None);
    }
}

fn install() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        let subscriber = tracing_subscriber::fmt().with_writer(|| Sink).with_ansi(false).finish();
        tracing::subscriber::set_global_default(subscriber)
            .expect("no other test installs a global subscriber");
    });
}

/// What `body` logs on this thread, at INFO and above.
pub(crate) fn logs_of(body: impl FnOnce()) -> String {
    install();
    CAPTURE.with(|c| {
        assert!(c.borrow().is_none(), "logs_of does not nest");
        *c.borrow_mut() = Some(Vec::new());
    });
    let _capturing = Capturing;
    body();
    CAPTURE.with(|c| String::from_utf8(c.borrow_mut().take().unwrap_or_default()).unwrap())
}

/// What `body` returns, and what it logged on this thread.
pub(crate) fn logged<T>(body: impl FnOnce() -> T) -> (T, String) {
    let mut out = None;
    let logs = logs_of(|| out = Some(body()));
    (out.expect("the body ran"), logs)
}
