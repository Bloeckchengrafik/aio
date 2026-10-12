use crate::{model::Endpoint, protocol, state::State};
use anyhow::Context;
use rand::seq::SliceRandom;
use socket2::{Domain, Protocol, SockRef, Socket, TcpKeepalive, Type};
use std::{
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex, atomic::Ordering},
    task::{Context as TaskContext, Poll},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf},
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
    /// Zero disables the inactivity deadline; TCP keepalive remains enabled.
    pub idle_timeout: Duration,
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
    let mut accept_backoff = Duration::from_millis(100);
    let mut retry_at = Instant::now();
    loop {
        if cancel.is_cancelled() {
            break;
        }
        if *admission.borrow_and_update() && listener.is_none() {
            match bind(options.listen) {
                Ok(bound) => {
                    listener = Some(bound);
                    state.listener_bound.store(true, Ordering::Release);
                    *state.listener_error.write().unwrap() = None;
                    state.accepting.store(true, Ordering::Release);
                    retry_at = Instant::now();
                    state.event("listener accepting");
                }
                Err(error) => {
                    // A failed resume must not destroy streams belonging to the old listener.
                    *state.listener_error.write().unwrap() = Some(error.to_string());
                    state.admission.send_replace(false);
                    state.accepting.store(false, Ordering::Release);
                    state.event(format!("listener bind failed; paused: {error}"));
                }
            }
            state.listener_changed.notify_waiters();
        } else if !*admission.borrow() && listener.is_some() {
            listener = None;
            state.listener_bound.store(false, Ordering::Release);
            state.accepting.store(false, Ordering::Release);
            state.listener_changed.notify_waiters();
            state.event("listener paused; established streams retained");
        }
        tokio::select! {
            _ = cancel.cancelled() => break,
            changed = admission.changed() => { if changed.is_err() { break; } },
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(Err(error)) = result { state.event(format!("connection task failed: {error}")); }
            },
            accepted = async {
                match &listener {
                    Some(l) => { tokio::time::sleep_until(retry_at).await; l.accept().await },
                    None => std::future::pending().await,
                }
            } => {
                let (stream, _) = match accepted {
                    Ok(connection) => {
                        accept_backoff=Duration::from_millis(100);
                        *state.listener_error.write().unwrap()=None;
                        state.accepting.store(true,Ordering::Release);
                        connection
                    }
                    Err(error) => {
                        state.accept_errors.fetch_add(1,Ordering::Relaxed);
                        if error.kind()==io::ErrorKind::Interrupted { tokio::task::yield_now().await; continue; }
                        *state.listener_error.write().unwrap()=Some(error.to_string());
                        state.accepting.store(false,Ordering::Release);
                        state.event(format!("accept failed; retrying without dropping streams: {error}"));
                        retry_at=Instant::now()+accept_backoff;
                        accept_backoff=(accept_backoff*2).min(Duration::from_secs(5));
                        continue;
                    }
                };
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
    state.listener_bound.store(false, Ordering::Release);
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
    let proxy_protocol = route.proxy_protocol;
    let (mut local, mut global): (Vec<_>, Vec<_>) = route
        .endpoints
        .iter()
        .filter(|e| state.usable(e))
        .cloned()
        .partition(|e| e.node_name.as_deref() == Some(state.node.as_str()));
    drop(snapshot);
    {
        let mut rng = rand::rng();
        local.shuffle(&mut rng);
        global.shuffle(&mut rng);
    }
    // Reserve both a slot and connect time for global fallback when it exists.
    let candidates = candidate_order(local, global);
    let deadline = Instant::now() + options.connect_deadline;
    let mut attempts = 0;
    let mut upstream = None;
    for (index, endpoint) in candidates.iter().enumerate() {
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
        let later_global = candidates[index + 1..]
            .iter()
            .any(|e| e.node_name.as_deref() != Some(state.node.as_str()));
        let attempt_deadline =
            if endpoint.node_name.as_deref() == Some(state.node.as_str()) && later_global {
                deadline
                    .checked_sub(options.connect_timeout.min(options.connect_deadline / 2))
                    .unwrap_or(deadline)
            } else {
                deadline
            };
        if Instant::now() >= attempt_deadline {
            continue;
        }
        match timeout_at(
            attempt_deadline.min(Instant::now() + options.connect_timeout),
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
                selected(&state, endpoint);
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
    let upstream = upstream.context("no reachable backend")?;
    upstream.set_nodelay(true)?;
    client.set_nodelay(true)?;
    configure_keepalive(&upstream)?;
    configure_keepalive(&client)?;
    let activity = Arc::new(Mutex::new(Instant::now()));
    let mut upstream = MeteredStream {
        stream: upstream,
        state: state.clone(),
        activity: activity.clone(),
    };
    let mut client = MeteredStream {
        stream: client,
        state: state.clone(),
        activity: activity.clone(),
    };
    let prelude = async {
        if proxy_protocol {
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
    let relay = tokio::io::copy_bidirectional(&mut client, &mut upstream);
    if options.idle_timeout.is_zero() {
        relay.await?;
    } else {
        tokio::select! {
            result=relay=>{result?;},
            _=idle_deadline(activity,options.idle_timeout)=>anyhow::bail!("stream inactivity deadline expired"),
        }
    }
    Ok(())
}

fn candidate_order(mut local: Vec<Endpoint>, mut global: Vec<Endpoint>) -> Vec<Endpoint> {
    // Two local failures cannot consume the final global attempt.
    if !global.is_empty() {
        local.truncate(2);
        global.truncate(3 - local.len());
    } else {
        local.truncate(3);
    }
    local.extend(global);
    local
}

fn configure_keepalive(stream: &TcpStream) -> io::Result<()> {
    SockRef::from(stream).set_tcp_keepalive(
        &TcpKeepalive::new()
            .with_time(Duration::from_secs(60))
            .with_interval(Duration::from_secs(20))
            .with_retries(3),
    )
}

async fn idle_deadline(activity: Arc<Mutex<Instant>>, timeout: Duration) {
    loop {
        let deadline = *activity.lock().unwrap() + timeout;
        tokio::time::sleep_until(deadline).await;
        if activity.lock().unwrap().elapsed() >= timeout {
            return;
        }
    }
}

struct MeteredStream {
    stream: TcpStream,
    state: Arc<State>,
    activity: Arc<Mutex<Instant>>,
}
impl AsyncRead for MeteredStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(cx, buf);
        if buf.filled().len() > before {
            *self.activity.lock().unwrap() = Instant::now();
        }
        result
    }
}
impl AsyncWrite for MeteredStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let result = Pin::new(&mut self.stream).poll_write(cx, buf);
        if let Poll::Ready(Ok(n)) = result {
            self.state.bytes.fetch_add(n as u64, Ordering::Relaxed);
            if n > 0 {
                *self.activity.lock().unwrap() = Instant::now();
            }
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}

fn selected(state: &State, endpoint: &Endpoint) {
    if endpoint.node_name.as_deref() == Some(state.node.as_str()) {
        state.local.fetch_add(1, Ordering::Relaxed);
    } else {
        state.global.fetch_add(1, Ordering::Relaxed);
    }
}
