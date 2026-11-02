//! A request that the node's stop ended is answered as the shutdown answers any request
//! it cut (ADR-266): `503 node_stopping`, and not the `503 timeout` of a deadline that passed.
//!
//! The stop is a flag for the whole process that nothing clears, so this is a binary of its
//! own with one test.

use std::time::Duration;

use bson::{Bson, doc};
use kimmy_api::error::{ApiError, ErrorCode};
use kimmy_core::Error as CoreError;
use kimmy_core::deadline::{self, Deadline};
use kimmy_query::expr::Expr;
use kimmy_storage::StorageError;

fn long_expression() -> Expr {
    // Ten thousand nodes over a value of a thousand elements: far more than a stride.
    let eqs: Vec<Bson> =
        (0..10_000).map(|_| Bson::Document(doc! { "$eq": ["$p", "$p"] })).collect();
    Expr::parse(&Bson::Document(doc! { "$and": eqs })).unwrap()
}

#[test]
fn a_request_ended_by_the_stop_is_answered_node_stopping_and_one_ended_by_its_deadline_is_a_timeout()
 {
    let document = doc! { "p": { "q": (0..1_000).collect::<Vec<i32>>() } };
    let expr = long_expression();

    // Before the stop, a passed deadline is the timeout of the middleware.
    let timeout = ApiError::from(CoreError::DeadlineExceeded);
    assert_eq!(timeout.status.as_u16(), 503);
    assert_eq!(timeout.code, ErrorCode::Timeout);
    // And an hour away, the expression runs to its end.
    {
        let _hour = Deadline::scope(Some(Duration::from_secs(3600)));
        assert_eq!(expr.eval(&document).unwrap(), Bson::Boolean(true));
    }

    // The node stops while the expression runs: it ends at its next check, long before its
    // ten thousand nodes are done, and its error is answered as the node stopping.
    let stopper = std::thread::spawn(|| {
        std::thread::sleep(Duration::from_millis(20));
        deadline::stop_requests();
    });
    let started = std::time::Instant::now();
    let outcome = {
        let _hour = Deadline::scope(Some(Duration::from_secs(3600)));
        loop {
            // Repeated until the stop lands, so a fast machine cannot finish first.
            match expr.eval(&document) {
                Ok(_) if started.elapsed() < Duration::from_secs(20) => continue,
                other => break other,
            }
        }
    };
    stopper.join().unwrap();
    let error = outcome.expect_err("the stop ended the expression");
    assert!(matches!(error, CoreError::DeadlineExceeded));
    for answered in
        [ApiError::from(error), ApiError::from(StorageError::Core(CoreError::DeadlineExceeded))]
    {
        assert_eq!(answered.status.as_u16(), 503);
        assert_eq!(answered.code, ErrorCode::NodeStopping);
        assert!(answered.message.contains("shutting down"), "{}", answered.message);
    }
}
