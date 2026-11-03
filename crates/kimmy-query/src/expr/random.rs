//! `$rand`, and the random draw `$sampleRate` makes: a double in `[0, 1)`.
//!
//! The source is a function, [`unit`], that a test replaces for the duration of a closure with
//! [`with_source`] (a scripted sequence of values), as `stages::sample` takes its `below`.
//! There is no seed in the API: the system's source is a generator seeded from the operating
//! system's, one for each thread, and nothing a caller can read or set. Every call is a fresh
//! draw, so `$rand` is not the same twice in a document, and a pipeline-style update computes
//! it once, on the member that accepts the write, as it computes `$$NOW` (the document that
//! results is what replicates).

use std::cell::RefCell;

use bson::Bson;
use kimmy_core::{Error, Result};

type Source = Box<dyn FnMut() -> f64>;

thread_local! {
    /// The scripted source of a test, in place while [`with_source`] runs.
    static SCRIPTED: RefCell<Option<Source>> = const { RefCell::new(None) };
    /// The system's source for this thread, made the first time it is read.
    static SYSTEM: RefCell<Option<rand::rngs::StdRng>> = const { RefCell::new(None) };
}

/// The next number in `[0, 1)`.
pub(crate) fn unit() -> f64 {
    if let Some(value) = SCRIPTED.with(|s| s.borrow_mut().as_mut().map(|source| source())) {
        return value;
    }
    system()
}

fn system() -> f64 {
    use rand::RngExt;
    SYSTEM.with(|s| s.borrow_mut().get_or_insert_with(rand::make_rng).random::<f64>())
}

/// Run `run` with `source` answering every [`unit`] on this thread, and the system's source
/// again afterwards (also when `run` panics).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn with_source<R>(source: impl FnMut() -> f64 + 'static, run: impl FnOnce() -> R) -> R {
    struct Restore(Option<Source>);
    impl Drop for Restore {
        fn drop(&mut self) {
            SCRIPTED.with(|s| *s.borrow_mut() = self.0.take());
        }
    }
    let previous = SCRIPTED.with(|s| s.borrow_mut().replace(Box::new(source)));
    let _restore = Restore(previous);
    run()
}

/// The argument of `$rand`: an empty document, or an empty array, and nothing else.
pub(super) fn check_argument(raw: &Bson) -> Result<()> {
    match raw {
        Bson::Document(d) if d.is_empty() => Ok(()),
        Bson::Array(a) if a.is_empty() => Ok(()),
        Bson::Document(_) | Bson::Array(_) => {
            Err(Error::InvalidQuery("$rand does not currently accept arguments".into()))
        }
        _ => Err(Error::InvalidQuery("invalid parameter: expected an object ($rand)".into())),
    }
}
