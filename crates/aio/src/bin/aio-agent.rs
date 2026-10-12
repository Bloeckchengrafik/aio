use aio::{
    api, discovery,
    router::{self, Options},
    state::State,
};
use clap::Parser;
use std::{
    future::IntoFuture,
    net::SocketAddr,
    path::PathBuf,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(
    version,
    about = "Minecraft hostname ingress with node-local preference"
)]
struct Args {
    #[arg(long, env = "AIO_LISTEN", default_value = "[::]:25565")]
    listen: SocketAddr,
    #[arg(long, env = "AIO_MANAGEMENT_LISTEN", default_value = "127.0.0.1:0")]
    management_listen: SocketAddr,
    #[arg(long, env = "AIO_NODE_NAME", default_value = "local")]
    node_name: String,
    #[arg(long, env = "AIO_INSTANCE_ID")]
    instance_id: Option<String>,
    #[arg(long, env = "AIO_NAMESPACE", default_value = "asorda")]
    namespace: String,
    #[arg(long, env = "AIO_CONFIG")]
    config: Option<PathBuf>,
    #[arg(long, env = "AIO_API_TOKEN", hide_env_values = true)]
    api_token: String,
    #[arg(long, default_value_t = 5)]
    handshake_timeout_seconds: u64,
    #[arg(long, default_value_t = 2)]
    connect_timeout_seconds: u64,
    #[arg(long, default_value_t = 1800)]
    drain_timeout_seconds: u64,
    #[arg(long, default_value_t = 300)]
    idle_timeout_seconds: u64,
    #[arg(long, default_value_t = 4096)]
    max_frame: usize,
    #[arg(long, default_value_t = 4096)]
    max_connections: usize,
    #[arg(long, env = "AIO_MANAGEMENT_ADDRESS_FILE")]
    management_address_file: Option<PathBuf>,
    #[arg(long, env = "AIO_REGISTRATION_NAMESPACE")]
    registration_namespace: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "aio=info".into()),
        )
        .init();
    let args = Args::parse();
    anyhow::ensure!(!args.api_token.is_empty(), "API token cannot be empty");
    anyhow::ensure!(
        (1..=4096).contains(&args.max_frame) && args.max_connections > 0,
        "invalid resource limits"
    );
    anyhow::ensure!(
        (1..=60).contains(&args.handshake_timeout_seconds)
            && (1..=30).contains(&args.connect_timeout_seconds)
            && args.idle_timeout_seconds <= 86400
            && args.drain_timeout_seconds <= 86400,
        "timeouts must be bounded: handshake 1–60s, connect 1–30s, idle/drain 0–86400s"
    );
    let state = State::new(
        args.node_name,
        args.instance_id
            .unwrap_or_else(|| format!("local-{}", std::process::id())),
    );
    #[cfg(unix)]
    state.fd_limit.store(
        aio::resources::ensure_descriptors(args.max_connections)?,
        Ordering::Relaxed,
    );
    let cancel = CancellationToken::new();
    let discovery_state = state.clone();
    let discovery_cancel = cancel.clone();
    let config = args.config.clone();
    let namespace = args.namespace.clone();
    let discovery_task = tokio::spawn(aio::supervision::supervise(
        state.clone(),
        "discovery",
        move || {
            let state = discovery_state.clone();
            let stop = discovery_cancel.clone();
            let config = config.clone();
            let namespace = namespace.clone();
            async move {
                if let Some(path) = config {
                    discovery::local_file(state, path, stop).await
                } else {
                    discovery::kubernetes(state, namespace, stop).await
                }
            }
        },
        cancel.clone(),
    ));
    let management = tokio::net::TcpListener::bind(args.management_listen).await?;
    let management_address = management.local_addr()?;
    if let Some(path) = args.management_address_file {
        tokio::fs::write(path, management_address.to_string()).await?;
    }
    tracing::info!(%management_address,"management API listening");
    let registration_cancel = CancellationToken::new();
    let registration_task = if let Some(namespace) = args.registration_namespace {
        anyhow::ensure!(
            !management_address.ip().is_unspecified() && !management_address.ip().is_loopback(),
            "registration requires a concrete reachable private management address"
        );
        let register_state = state.clone();
        let stop = registration_cancel.clone();
        Some(tokio::spawn(aio::supervision::supervise(
            state.clone(),
            "registration",
            move || {
                let state = register_state.clone();
                let stop = stop.clone();
                let namespace = namespace.clone();
                async move { aio::registration::run(state, management_address, namespace, stop).await }
            },
            registration_cancel.clone(),
        )))
    } else {
        None
    };
    let api_cancel = CancellationToken::new();
    let management = Arc::new(management.into_std()?);
    let api_state = state.clone();
    let api_stop = api_cancel.clone();
    let token = args.api_token.clone();
    let api_task = tokio::spawn(aio::supervision::supervise(
        state.clone(),
        "management",
        move || {
            let listener = management.try_clone();
            let state = api_state.clone();
            let stop = api_stop.clone();
            let token = token.clone();
            async move {
                let listener = tokio::net::TcpListener::from_std(listener?)?;
                state.service_errors.write().unwrap().remove("management");
                axum::serve(listener, api::routes(state, token))
                    .with_graceful_shutdown(stop.cancelled_owned())
                    .into_future()
                    .await?;
                Ok(())
            }
        },
        api_cancel.clone(),
    ));
    let stop = cancel.clone();
    let signal_state = state.clone();
    tokio::spawn(async move {
        shutdown_signal().await;
        signal_state.terminating.store(true, Ordering::Release);
        stop.cancel();
    });
    while !state.synced.load(Ordering::Acquire) {
        tokio::select! { _=cancel.cancelled()=>break, _=tokio::time::sleep(Duration::from_millis(50))=>{} }
    }
    let result = if cancel.is_cancelled() {
        Ok(())
    } else {
        router::serve(
            state,
            Options {
                listen: args.listen,
                handshake_timeout: Duration::from_secs(args.handshake_timeout_seconds),
                connect_timeout: Duration::from_secs(args.connect_timeout_seconds),
                connect_deadline: Duration::from_secs(5),
                max_frame: args.max_frame,
                max_connections: args.max_connections,
                drain_timeout: Duration::from_secs(args.drain_timeout_seconds),
                idle_timeout: Duration::from_secs(args.idle_timeout_seconds),
            },
            cancel.clone(),
        )
        .await
    };
    cancel.cancel();
    api_cancel.cancel();
    registration_cancel.cancel();
    api_task.await?;
    discovery_task.await?;
    if let Some(task) = registration_task {
        task.await?;
    }
    result
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {_=tokio::signal::ctrl_c()=>{},_=term.recv()=>{}}
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
