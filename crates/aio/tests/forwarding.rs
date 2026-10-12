use aio::{
    model::{Endpoint, Route, Snapshot},
    router::{self, Options},
    state::State,
};
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_util::sync::CancellationToken;

fn options(listen: SocketAddr) -> Options {
    Options {
        listen,
        handshake_timeout: Duration::from_secs(1),
        connect_timeout: Duration::from_millis(200),
        connect_deadline: Duration::from_secs(1),
        max_frame: 4096,
        max_connections: 32,
        drain_timeout: Duration::from_secs(1),
        idle_timeout: Duration::from_secs(60),
    }
}
fn packet() -> Vec<u8> {
    let host = b"play.asorda.net";
    let mut frame = vec![0, 47, host.len() as u8];
    frame.extend(host);
    frame.extend([0x63, 0xdd, 2]);
    let mut packet = vec![frame.len() as u8];
    packet.extend(frame);
    packet
}
fn endpoint(id: &str, address: SocketAddr, node: &str) -> Endpoint {
    Endpoint {
        id: id.into(),
        address,
        node_name: Some(node.into()),
    }
}
fn state(endpoints: Vec<Endpoint>, proxy_protocol: bool) -> Arc<State> {
    let state = State::new("node-a".into(), "test".into());
    state.publish(
        Snapshot {
            routes: vec![Route {
                id: "test/main".into(),
                hostnames: vec!["play.asorda.net".into()],
                proxy_protocol,
                endpoints,
                errors: vec![],
            }],
        }
        .validate(),
    );
    state
}

#[tokio::test]
async fn failed_local_connect_falls_back_and_plain_stream_is_unchanged() {
    let local = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dead = local.local_addr().unwrap();
    drop(local);
    let global = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(
        vec![
            endpoint("dead", dead, "node-a"),
            endpoint("global", global.local_addr().unwrap(), "node-b"),
        ],
        false,
    );
    let ingress = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = ingress.local_addr().unwrap();
    let forward_state = state.clone();
    let forward = tokio::spawn(async move {
        let (stream, _) = ingress.accept().await.unwrap();
        router::forward(stream, forward_state, &options(address))
            .await
            .unwrap();
    });
    let expected = packet();
    let expected_backend = expected.clone();
    let backend = tokio::spawn(async move {
        let (mut stream, _) = global.accept().await.unwrap();
        let mut bytes = vec![];
        stream.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, expected_backend);
        stream.write_all(b"response").await.unwrap();
        stream.shutdown().await.unwrap();
    });
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&expected).await.unwrap();
    client.shutdown().await.unwrap();
    let mut reply = vec![];
    timeout(Duration::from_secs(3), client.read_to_end(&mut reply))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(reply, b"response");
    backend.await.unwrap();
    forward.await.unwrap();
    assert_eq!(state.global.load(Ordering::Relaxed), 1);
    assert_eq!(state.local.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn local_wins_and_proxy_v2_precedes_original_handshake() {
    let local = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let global = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(
        vec![
            endpoint("global", global.local_addr().unwrap(), "node-b"),
            endpoint("local", local.local_addr().unwrap(), "node-a"),
        ],
        true,
    );
    let ingress = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = ingress.local_addr().unwrap();
    let forward = tokio::spawn(async move {
        let (stream, _) = ingress.accept().await.unwrap();
        router::forward(stream, state, &options(address))
            .await
            .unwrap();
    });
    let mut client = TcpStream::connect(address).await.unwrap();
    let source = client.local_addr().unwrap();
    let expected = packet();
    client.write_all(&expected).await.unwrap();
    client.shutdown().await.unwrap();
    let (mut backend, _) = timeout(Duration::from_secs(2), local.accept())
        .await
        .unwrap()
        .unwrap();
    let mut bytes = vec![];
    backend.read_to_end(&mut bytes).await.unwrap();
    let mut header = aio::protocol::proxy_v2(source, address).unwrap();
    header.extend(expected);
    assert_eq!(bytes, header);
    backend.shutdown().await.unwrap();
    forward.await.unwrap();
    assert!(
        timeout(Duration::from_millis(50), global.accept())
            .await
            .is_err()
    );
}

async fn wait_accepting(state: &State, value: bool) {
    timeout(Duration::from_secs(2), async {
        while state.accepting.load(Ordering::Acquire) != value {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn overlapping_listener_takes_new_connections_while_old_stream_survives_pause() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(
        vec![endpoint("local", backend.local_addr().unwrap(), "node-a")],
        false,
    );
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(router::serve(
        state.clone(),
        options(address),
        cancel.clone(),
    ));
    wait_accepting(&state, true).await;
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&packet()).await.unwrap();
    let (mut upstream, _) = backend.accept().await.unwrap();
    let mut initial = vec![0; packet().len()];
    upstream.read_exact(&mut initial).await.unwrap();
    let replacement = router::bind(address).unwrap();
    state.admission.send_replace(false);
    wait_accepting(&state, false).await;
    let mut new_client = TcpStream::connect(address).await.unwrap();
    let (mut replacement_client, _) = timeout(Duration::from_secs(2), replacement.accept())
        .await
        .unwrap()
        .unwrap();
    replacement_client.write_all(b"new").await.unwrap();
    let mut new = [0; 3];
    new_client.read_exact(&mut new).await.unwrap();
    assert_eq!(&new, b"new");
    client.write_all(b"still-alive").await.unwrap();
    let mut data = [0; 11];
    upstream.read_exact(&mut data).await.unwrap();
    assert_eq!(&data, b"still-alive");
    upstream.write_all(b"ok").await.unwrap();
    let mut reply = [0; 2];
    client.read_exact(&mut reply).await.unwrap();
    assert_eq!(&reply, b"ok");
    drop(upstream);
    drop(client);
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn management_requires_auth_and_rejects_wrong_instance() {
    let state = state(vec![], false);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        axum::serve(listener, aio::api::routes(state, "secret".into()))
            .await
            .unwrap();
    });
    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{url}/api/v1/status"))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(
        client
            .get(format!("{url}/api/v1/status"))
            .bearer_auth("secret")
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        client
            .post(format!("{url}/api/v1/admission"))
            .bearer_auth("secret")
            .json(&serde_json::json!({"instanceId":"old","accepting":false,"reason":"test"}))
            .send()
            .await
            .unwrap()
            .status(),
        409
    );
    task.abort();
}

#[tokio::test]
async fn three_failed_local_backends_cannot_starve_global_fallback() {
    let global = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut reservations = vec![];
    let mut endpoints = vec![];
    for n in 0..3 {
        let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
        endpoints.push(endpoint(
            &format!("dead-{n}"),
            socket.local_addr().unwrap(),
            "node-a",
        ));
        reservations.push(socket);
    }
    drop(reservations);
    endpoints.push(endpoint("global", global.local_addr().unwrap(), "node-b"));
    let state = state(endpoints, false);
    let ingress = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = ingress.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (stream, _) = ingress.accept().await.unwrap();
        router::forward(stream, state, &options(address))
            .await
            .unwrap();
    });
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&packet()).await.unwrap();
    client.shutdown().await.unwrap();
    let (mut backend, _) = timeout(Duration::from_secs(2), global.accept())
        .await
        .unwrap()
        .unwrap();
    let mut received = vec![];
    backend.read_to_end(&mut received).await.unwrap();
    assert_eq!(received, packet());
    backend.shutdown().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn failed_resume_keeps_existing_stream_and_can_be_retried() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(
        vec![endpoint("local", backend.local_addr().unwrap(), "node-a")],
        false,
    );
    let reservation = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = reservation.local_addr().unwrap();
    drop(reservation);
    let cancel = CancellationToken::new();
    let task = tokio::spawn(router::serve(
        state.clone(),
        options(address),
        cancel.clone(),
    ));
    wait_accepting(&state, true).await;
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&packet()).await.unwrap();
    let (mut upstream, _) = backend.accept().await.unwrap();
    let mut frame = vec![0; packet().len()];
    upstream.read_exact(&mut frame).await.unwrap();
    state.admission.send_replace(false);
    wait_accepting(&state, false).await;
    let blocker = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )
    .unwrap();
    blocker.set_reuse_address(true).unwrap();
    blocker.bind(&address.into()).unwrap();
    blocker.listen(16).unwrap();
    state.admission.send_replace(true);
    timeout(Duration::from_secs(2), async {
        while state.listener_error.read().unwrap().is_none() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!task.is_finished());
    assert!(!state.accepting.load(Ordering::Acquire));
    client.write_all(b"alive").await.unwrap();
    let mut data = [0; 5];
    upstream.read_exact(&mut data).await.unwrap();
    assert_eq!(&data, b"alive");
    assert!(state.bytes.load(Ordering::Relaxed) >= packet().len() as u64 + 5);
    drop(blocker);
    state.admission.send_replace(true);
    wait_accepting(&state, true).await;
    assert!(state.listener_error.read().unwrap().is_none());
    drop(client);
    drop(upstream);
    cancel.cancel();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn idle_stream_expires_and_bytes_are_visible_before_close() {
    let backend = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let state = state(
        vec![endpoint("local", backend.local_addr().unwrap(), "node-a")],
        false,
    );
    let ingress = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = ingress.local_addr().unwrap();
    let mut opts = options(address);
    opts.idle_timeout = Duration::from_millis(100);
    let forward_state = state.clone();
    let task = tokio::spawn(async move {
        let (stream, _) = ingress.accept().await.unwrap();
        router::forward(stream, forward_state, &opts).await
    });
    let mut client = TcpStream::connect(address).await.unwrap();
    client.write_all(&packet()).await.unwrap();
    let (mut stream, _) = backend.accept().await.unwrap();
    let mut initial = vec![0; packet().len()];
    stream.read_exact(&mut initial).await.unwrap();
    assert_eq!(state.bytes.load(Ordering::Relaxed), packet().len() as u64);
    stream.write_all(b"response").await.unwrap();
    let mut reply = [0; 8];
    client.read_exact(&mut reply).await.unwrap();
    assert_eq!(
        state.bytes.load(Ordering::Relaxed),
        packet().len() as u64 + 8
    );
    let result = timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    assert!(result.unwrap_err().to_string().contains("inactivity"));
    let mut byte = [0; 1];
    assert_eq!(client.read(&mut byte).await.unwrap(), 0);
}
