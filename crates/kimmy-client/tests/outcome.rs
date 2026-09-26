//! What the client makes of a failure, against a server that fails on purpose.
//!
//! A real node cannot be made to close a connection at a chosen moment, or to
//! answer a chosen envelope, on demand. Each fake here does one thing the wire
//! allows and nothing the client could have told it to.

use kimmy_client::{Client, Error, ErrorCode, Method, Retry, Safety};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

/// Serve each connection with `handle`; return the base URL.
async fn fake_server<F, Fut>(handle: F) -> String
where
    F: Fn(TcpStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((socket, _)) = listener.accept().await {
            tokio::spawn(handle(socket));
        }
    });
    base
}

/// Read one whole request, its head and a `Content-Length` body.
async fn read_request(socket: &mut TcpStream) -> std::io::Result<()> {
    let mut reader = BufReader::new(socket);
    let mut length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        let line = line.trim_end();
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':')
            && name.eq_ignore_ascii_case("content-length")
        {
            length = value.trim().parse().unwrap_or(0);
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).await?;
    Ok(())
}

/// Reads the whole request, then closes the connection without a word.
async fn unanswered() -> String {
    fake_server(|mut socket| async move {
        let _ = read_request(&mut socket).await;
    })
    .await
}

async fn with_token(base: &str) -> Client {
    Client::builder(base).token("a-token").connect().await.expect("connecting")
}

#[tokio::test]
async fn a_write_whose_connection_closes_after_it_was_sent_has_an_unknown_outcome() {
    let base = unanswered().await;
    let client = with_token(&base).await;

    let error = client.insert("shop", "orders", &json!({ "_id": 1 })).await.unwrap_err();

    assert!(
        error.is_outcome_unknown(),
        "a write sent and never answered may have happened: {error}"
    );
    assert_eq!(error.retry(), Retry::Verify);
    let Error::OutcomeUnknown { endpoint, source } = &error else { unreachable!() };
    assert_eq!(endpoint, &base, "the error names the node");
    assert!(matches!(**source, Error::Transport { .. }), "and carries the transport's failure");
}

#[tokio::test]
async fn a_read_whose_connection_closes_after_it_was_sent_is_a_transport_failure() {
    let base = unanswered().await;
    let client = with_token(&base).await;

    let error = client.version().await.unwrap_err();

    assert!(
        matches!(error, Error::Transport { .. }),
        "a read has no outcome to be unknown: {error}"
    );
}

#[tokio::test]
async fn a_write_to_a_node_that_refuses_the_connection_was_not_sent() {
    let client = with_token("http://127.0.0.1:1").await;

    let error = client.insert("shop", "orders", &json!({ "_id": 1 })).await.unwrap_err();

    assert!(
        matches!(error, Error::Transport { .. }),
        "a refused connection carried nothing: {error}"
    );
    assert_eq!(error.retry(), Retry::Elsewhere);
}

#[tokio::test]
async fn a_write_whose_body_could_not_be_written_is_still_unknown() {
    // Larger than any socket buffer, so the write really does fail partway when
    // the server stops reading and closes. reqwest does not say how far a
    // request got once the connection is made, so this is no evidence that
    // nothing was sent, and without evidence a write is unknown: the
    // conservative answer, as the rule is positive evidence only.
    let base = fake_server(|mut socket| async move {
        let mut buffer = [0; 1024];
        let _ = socket.read(&mut buffer).await;
    })
    .await;
    let client = with_token(&base).await;

    let big = "x".repeat(32 << 20);
    let error =
        client.insert("shop", "orders", &json!({ "_id": 1, "big": big })).await.unwrap_err();

    assert!(error.is_outcome_unknown(), "{error}");
}

#[tokio::test]
async fn a_write_whose_answer_is_cut_off_has_an_unknown_outcome() {
    // The status arrived and the body did not: the node received the write.
    let base = fake_server(|mut socket| async move {
        if read_request(&mut socket).await.is_ok() {
            let _ = socket
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                      Content-Length: 100\r\n\r\n{\"inser",
                )
                .await;
        }
    })
    .await;
    let client = with_token(&base).await;

    let error = client.insert("shop", "orders", &json!({ "_id": 1 })).await.unwrap_err();

    assert!(
        error.is_outcome_unknown(),
        "a write whose answer was cut off may have happened: {error}"
    );
}

#[tokio::test]
async fn the_servers_outcome_unknown_is_typed() {
    let base = fake_server(|mut socket| async move {
        if read_request(&mut socket).await.is_ok() {
            let body = r#"{"error":"outcome_unknown","message":"m","retry":"verify"}"#;
            let head = format!(
                "HTTP/1.1 500 Internal Server Error\r\nContent-Type: application/json\r\n\
                 Content-Length: {}\r\n\r\n",
                body.len()
            );
            let _ = socket.write_all(head.as_bytes()).await;
            let _ = socket.write_all(body.as_bytes()).await;
        }
    })
    .await;
    let client = with_token(&base).await;

    let error = client.insert("shop", "orders", &json!({ "_id": 1 })).await.unwrap_err();

    assert!(error.is_outcome_unknown(), "{error}");
    assert_eq!(error.retry(), Retry::Verify);
    assert_eq!(
        error.code(),
        Some(ErrorCode::OutcomeUnknown),
        "the node's own code, still readable"
    );
    assert_eq!(error.status(), Some(500));
}

#[tokio::test]
async fn a_caller_that_says_a_write_is_idempotent_gets_a_transport_failure() {
    // The caller's claim is that repeating it cannot change the outcome, so
    // there is no outcome to be unknown about: it fails over as a read does.
    let base = unanswered().await;
    let client = with_token(&base).await;

    let error = client
        .request(
            Method::Put,
            "/v1/db/shop/coll/orders/docs/1",
            Some(json!({ "qty": 1 })),
            Safety::Idempotent,
        )
        .await
        .unwrap_err();

    assert!(matches!(error, Error::Transport { .. }), "{error}");
}
