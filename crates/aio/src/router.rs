use crate::{model::Endpoint, protocol, state::State};
use anyhow::Context;
use rand::seq::SliceRandom;
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    net::SocketAddr,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinSet,
    time::{Instant, timeout, timeout_at},
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
pub struct Options {
    pub listen: SocketAddr,
    pub handshake_timeout: Duration,
    pub connect_timeout: Duration,
    pub connect_deadline: Duration,
    pub max_frame: usize,
    pub max_connections: usize,
    pub drain_timeout: Duration,
}

pub fn bind(address: SocketAddr) -> anyhow::Result<TcpListener> {
    let socket = Socket::new(
        Domain::for_address(address),
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    if address.is_ipv6() {
        socket.set_only_v6(false)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&address.into())?;
    socket.listen(1024)?;
    Ok(TcpListener::from_std(socket.into())?)
}

pub async fn serve(
    state: Arc<State>,
    options: Options,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let permits = Arc::new(Semaphore::new(options.max_connections));
    let mut admission = state.admission.subscribe();
    let mut listener = None;
    let mut tasks = JoinSet::new();
    loop {
        if cancel.is_cancelled() {
            break;
        }
        if *admission.borrow_and_update() && listener.is_none() {
            listener = Some(bind(options.listen)?);
            state.accepting.store(true, Ordering::Release);
            state.listener_changed.notify_waiters();
            state.event("listener accepting");
        } else if !*admission.borrow() && listener.is_some() {
            listener = None;
            state.accepting.store(false, Ordering::Release);
            state.listener_changed.notify_waiters();
            state.event("listener paused; established streams retained");
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            changed = admission.changed() => { if changed.is_err() { break; } },
            _ = tasks.join_next(), if !tasks.is_empty() => {},
            accepted = async { match &listener { Some(l) => l.accept().await, None => std::future::pending().await } } => {
                let (stream, _) = accepted?;
                let Ok(permit) = permits.clone().try_acquire_owned() else { state.failures.fetch_add(1,Ordering::Relaxed); continue; };
                let state = state.clone(); let options = options.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    state.active.fetch_add(1,Ordering::Relaxed);
                    let _active = ActiveConnection(state.clone());
                    state.total.fetch_add(1,Ordering::Relaxed);
                    if let Err(error) = forward(stream, state.clone(), &options).await {
                        state.failures.fetch_add(1,Ordering::Relaxed);
                        tracing::debug!(%error,"connection ended with error");
                    }
                });
            }
        }
    }
    drop(listener);
    state.accepting.store(false, Ordering::Release);
    state.terminating.store(true, Ordering::Release);
    state.listener_changed.notify_waiters();
    state.event("listener closed; draining");
    if timeout(options.drain_timeout, async {
        while tasks.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        state.event("drain deadline expired");
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
    Ok(())
}

struct Attempt {
    state: Arc<State>,
    id: String,
    completed: bool,
}
struct ActiveConnection(Arc<State>);
impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}
impl Drop for Attempt {
    fn drop(&mut self) {
        if !self.completed
            && let Some(c) = self.state.circuits.lock().unwrap().get_mut(&self.id)
        {
            c.release();
        }
    }
}

pub async fn forward(
    mut client: TcpStream,
    state: Arc<State>,
    options: &Options,
) -> anyhow::Result<()> {
    let source = client.peer_addr()?;
    let destination = client.local_addr()?;
    let (host, original) = timeout(
        options.handshake_timeout,
        protocol::handshake(&mut client, options.max_frame),
    )
    .await
    .context("handshake timeout")??;
    let snapshot = state.snapshot.read().unwrap().clone();
    let route = snapshot.resolve(&host).context("no accepted route")?;
    let (mut local, mut global): (Vec<_>, Vec<_>) = route
        .endpoints
        .iter()
        .filter(|e| state.usable(e))
        .cloned()
        .partition(|e| e.node_name.as_deref() == Some(state.node.as_str()));
    {
        let mut rng = rand::rng();
        local.shuffle(&mut rng);
        global.shuffle(&mut rng);
    }
    let deadline = Instant::now() + options.connect_deadline;
    let mut attempts = 0;
    let mut upstream = None;
    for endpoint in local.into_iter().chain(global) {
        if attempts >= 3 || Instant::now() >= deadline {
            break;
        }
        if !state
            .circuits
            .lock()
            .unwrap()
            .entry(endpoint.id.clone())
            .or_default()
            .acquire()
        {
            continue;
        }
        let mut guard = Attempt {
            state: state.clone(),
            id: endpoint.id.clone(),
            completed: false,
        };
        attempts += 1;
        match timeout_at(
            deadline.min(Instant::now() + options.connect_timeout),
            TcpStream::connect(endpoint.address),
        )
        .await
        {
            Ok(Ok(stream)) => {
                state
                    .circuits
                    .lock()
                    .unwrap()
                    .entry(endpoint.id.clone())
                    .or_default()
                    .success();
                guard.completed = true;
                selected(&state, &endpoint);
                upstream = Some(stream);
                break;
            }
            _ => {
                state
                    .circuits
                    .lock()
                    .unwrap()
                    .entry(endpoint.id.clone())
                    .or_default()
                    .failure();
                guard.completed = true;
                state.event(format!("upstream connect failed: {}", endpoint.id));
            }
        }
    }
    let mut upstream = upstream.context("no reachable backend")?;
    upstream.set_nodelay(true)?;
    client.set_nodelay(true)?;
    let prelude = async {
        if route.proxy_protocol {
            upstream
                .write_all(&protocol::proxy_v2(source, destination)?)
                .await?;
        }
        upstream.write_all(&original).await?;
        anyhow::Ok(())
    };
    timeout(options.connect_timeout, prelude)
        .await
        .context("upstream prelude timeout")??;
    let (a, b) = tokio::io::copy_bidirectional(&mut client, &mut upstream).await?;
    state.bytes.fetch_add(a + b, Ordering::Relaxed);
    Ok(())
}

fn selected(state: &State, endpoint: &Endpoint) {
    if endpoint.node_name.as_deref() == Some(state.node.as_str()) {
        state.local.fetch_add(1, Ordering::Relaxed);
    } else {
        state.global.fetch_add(1, Ordering::Relaxed);
    }
}
