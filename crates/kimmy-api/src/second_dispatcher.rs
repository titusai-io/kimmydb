//! What a test that records log lines on its own thread holds beside its capture.

/// A second registered dispatcher, for the caller to hold as long as its scoped capture records.
///
/// While only one dispatcher is registered, tracing asks the thread that first hits a callsite which events it wants, and caches
/// the answer for the whole process (tracing-core, `Rebuilder::JustOne`). A parallel test's thread, which records nothing, can so
/// switch a line off for the thread that records it. With two registered, every registered dispatcher is asked, the capture among
/// them.
pub(crate) fn a_second_dispatcher() -> tracing::Dispatch {
    tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default())
}
