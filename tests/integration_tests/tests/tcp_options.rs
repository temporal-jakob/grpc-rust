/*
 *
 * Copyright 2025 gRPC authors.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 *
 */

use integration_tests::pb::{test_client, test_server, Input, Output};
use socket2::{SockRef, Socket};
use std::time::Duration;
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
};
use tokio_stream::StreamExt;
use tonic::{
    transport::{server::TcpIncoming, Endpoint, Server},
    Request, Response, Status,
};

struct Svc;

#[tonic::async_trait]
impl test_server::Test for Svc {
    async fn unary_call(&self, _: Request<Input>) -> Result<Response<Output>, Status> {
        Ok(Response::new(Output {}))
    }
}

/// Serves `incoming` with `server`, makes one RPC, and returns a duplicate of
/// the server side of the accepted connection. The duplicate shares socket
/// options with the socket tonic served, so it reflects whatever tonic set.
async fn accepted_socket(mut server: Server, incoming: TcpIncoming) -> Socket {
    let addr = incoming.local_addr().unwrap();

    let (socket_tx, mut socket_rx) = mpsc::unbounded_channel();
    let incoming = incoming.map(move |conn| {
        if let Ok(stream) = &conn {
            let _ = socket_tx.send(SockRef::from(stream).try_clone().unwrap());
        }
        conn
    });

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let jh = tokio::spawn(
        server
            .add_service(test_server::TestServer::new(Svc))
            .serve_with_incoming_shutdown(incoming, async { drop(shutdown_rx.await) }),
    );

    let channel = Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .unwrap();

    // Once an RPC has completed, the server has finished setting up the connection.
    test_client::TestClient::new(channel)
        .unary_call(Input {})
        .await
        .unwrap();

    let socket = socket_rx.recv().await.unwrap();

    shutdown_tx.send(()).unwrap();
    jh.await.unwrap().unwrap();

    socket
}

async fn incoming() -> TcpIncoming {
    TcpIncoming::from(TcpListener::bind("127.0.0.1:0").await.unwrap())
}

#[tokio::test]
async fn serve_with_incoming_sets_tcp_nodelay_by_default() {
    let socket = accepted_socket(Server::builder(), incoming().await).await;

    assert!(socket.tcp_nodelay().unwrap());
}

#[tokio::test]
async fn serve_with_incoming_respects_tcp_nodelay_false() {
    let socket = accepted_socket(
        Server::builder().tcp_nodelay(false),
        incoming().await.with_nodelay(Some(true)),
    )
    .await;

    assert!(!socket.tcp_nodelay().unwrap());
}

#[tokio::test]
async fn serve_with_incoming_sets_tcp_keepalive() {
    const KEEPALIVE: Duration = Duration::from_secs(37);

    let socket = accepted_socket(
        Server::builder().tcp_keepalive(Some(KEEPALIVE)),
        incoming().await,
    )
    .await;

    assert!(socket.keepalive().unwrap());
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert_eq!(socket.tcp_keepalive_time().unwrap(), KEEPALIVE);
}
