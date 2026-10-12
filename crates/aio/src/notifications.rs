//! Optional, best-effort Discord delivery. No network I/O on the routing path.
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tokio_util::sync::CancellationToken;

const QUEUE_SIZE: usize = 128;
const EVENT_TTL: Duration = Duration::from_secs(60);
const ATTEMPTS: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    CircuitOpened,
    CircuitRecovered,
    RouteStopped,
    RouteAllowed,
    RouteKilled,
}
impl Kind {
    fn title(self) -> &'static str {
        match self {
            Self::CircuitOpened => "Backend circuit opened",
            Self::CircuitRecovered => "Backend circuit recovered",
            Self::RouteStopped => "Route: new connections stopped",
            Self::RouteAllowed => "Route: new connections allowed",
            Self::RouteKilled => "Route: connections stopped and sessions cancelled",
        }
    }
}
pub struct Notification {
    pub kind: Kind,
    pub route: String,
    pub node: String,
    pub instance: String,
    pub backend: Option<String>,
    pub failures: u32,
    pub cooldown: u64,
    pub affected_sessions: usize,
    created: Instant,
    timestamp: u64,
}
impl Notification {
    pub fn new(kind: Kind, route: &str, node: &str, instance: &str) -> Self {
        Self {
            kind,
            route: route.into(),
            node: node.into(),
            instance: instance.into(),
            backend: None,
            failures: 0,
            cooldown: 0,
            affected_sessions: 0,
            created: Instant::now(),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
    fn payload(&self) -> serde_json::Value {
        let field = |name: &str, value: &str| serde_json::json!({"name":name,"value":value.chars().take(1000).collect::<String>(),"inline":false});
        let mut fields = vec![
            field("Route", &self.route),
            field("Node", &self.node),
            field("Instance", &self.instance),
        ];
        if let Some(backend) = &self.backend {
            fields.push(field("Backend", backend));
            fields.push(field("Consecutive failures", &self.failures.to_string()));
            fields.push(field("Cooldown", &format!("{} seconds", self.cooldown)));
        } else {
            fields.push(field(
                if self.kind == Kind::RouteKilled {
                    "Disconnect requested"
                } else {
                    "Existing sessions preserved"
                },
                &self.affected_sessions.to_string(),
            ));
        }
        serde_json::json!({"allowed_mentions":{"parse":[]},"embeds":[{"title":self.kind.title(),"color":match self.kind { Kind::CircuitOpened|Kind::RouteKilled=>0xd75b5b,Kind::CircuitRecovered|Kind::RouteAllowed=>0x72c6b1,Kind::RouteStopped=>0xcbb37c },"fields":fields,"footer":{"text":format!("AIO · event time (Unix): {}",self.timestamp)}}]})
    }
}
#[derive(Default)]
pub struct Stats {
    pub delivered: AtomicU64,
    pub failed: AtomicU64,
    pub dropped: AtomicU64,
}
#[derive(Default)]
pub struct Notifier {
    sender: Mutex<Option<mpsc::Sender<Notification>>>,
    pub stats: Arc<Stats>,
}
impl Notifier {
    pub fn start(&self, url: &str, cancel: CancellationToken) -> anyhow::Result<JoinHandle<()>> {
        let mut url =
            reqwest::Url::parse(url).map_err(|_| anyhow::anyhow!("invalid Discord webhook URL"))?;
        anyhow::ensure!(
            url.scheme() == "https"
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.fragment().is_none(),
            "Discord webhook URL must use HTTPS without credentials or a fragment"
        );
        let query: Vec<_> = url
            .query_pairs()
            .filter(|(key, _)| key != "wait")
            .map(|(k, v)| (k.into_owned(), v.into_owned()))
            .collect();
        url.set_query(None);
        url.query_pairs_mut()
            .extend_pairs(query)
            .append_pair("wait", "true");
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(self.spawn(client, url, cancel))
    }
    fn spawn(
        &self,
        client: reqwest::Client,
        url: reqwest::Url,
        cancel: CancellationToken,
    ) -> JoinHandle<()> {
        let (tx, rx) = mpsc::channel(QUEUE_SIZE);
        *self.sender.lock().unwrap() = Some(tx);
        tokio::spawn(worker(client, url, rx, self.stats.clone(), cancel))
    }
    pub fn send(&self, notification: Notification) {
        if let Some(sender) = self.sender.lock().unwrap().as_ref()
            && sender.try_send(notification).is_err()
        {
            self.stats.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
    pub fn metrics(&self) -> String {
        let sender = self.sender.lock().unwrap();
        let depth = sender
            .as_ref()
            .map_or(0, |s| s.max_capacity() - s.capacity());
        format!(
            "# TYPE aio_discord_enabled gauge\naio_discord_enabled {}\n# TYPE aio_discord_notification_queue_depth gauge\naio_discord_notification_queue_depth {depth}\n# TYPE aio_discord_notifications_total counter\naio_discord_notifications_total{{result=\"delivered\"}} {}\naio_discord_notifications_total{{result=\"failed\"}} {}\naio_discord_notifications_total{{result=\"dropped\"}} {}\n",
            u8::from(sender.is_some()),
            self.stats.delivered.load(Ordering::Relaxed),
            self.stats.failed.load(Ordering::Relaxed),
            self.stats.dropped.load(Ordering::Relaxed)
        )
    }
}
enum Delivery {
    Delivered(Duration),
    Retry(Duration),
    Permanent,
}
fn delay(value: &str) -> Option<Duration> {
    let seconds: f64 = value.parse().ok()?;
    (seconds.is_finite() && (0.0..=86400.0).contains(&seconds))
        .then(|| Duration::from_secs_f64(seconds.max(0.1)))
}
async fn deliver(
    client: &reqwest::Client,
    url: &reqwest::Url,
    payload: &serde_json::Value,
    attempt: usize,
) -> Delivery {
    // Never print request errors or response bodies: either may contain the secret URL.
    let backoff = Duration::from_secs(1 << attempt);
    let Ok(mut response) = client.post(url.clone()).json(payload).send().await else {
        return Delivery::Retry(backoff);
    };
    let status = response.status();
    if status.is_success() {
        let pause = if response
            .headers()
            .get("x-ratelimit-remaining")
            .is_some_and(|v| v == "0")
        {
            response
                .headers()
                .get("x-ratelimit-reset-after")
                .and_then(|v| v.to_str().ok())
                .and_then(delay)
                .unwrap_or(Duration::from_secs(1))
        } else {
            Duration::ZERO
        };
        return Delivery::Delivered(pause);
    }
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let header = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(delay);
        let mut bytes = Vec::new();
        while let Ok(Some(chunk)) = response.chunk().await {
            if bytes.len() + chunk.len() > 4096 {
                break;
            }
            bytes.extend(chunk);
        }
        let body = serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|v| {
                v.get("retry_after")
                    .and_then(|v| v.as_f64())
                    .and_then(|v| delay(&v.to_string()))
            });
        return Delivery::Retry(header.or(body).unwrap_or(backoff));
    }
    if status.is_server_error() {
        Delivery::Retry(backoff)
    } else {
        Delivery::Permanent
    }
}
async fn worker(
    client: reqwest::Client,
    url: reqwest::Url,
    mut rx: mpsc::Receiver<Notification>,
    stats: Arc<Stats>,
    cancel: CancellationToken,
) {
    let mut next_allowed = Instant::now();
    loop {
        let notification = tokio::select! { _=cancel.cancelled()=>break, n=rx.recv()=>match n {Some(n)=>n,None=>break} };
        let deadline = notification.created + EVENT_TTL;
        let mut attempted = false;
        let payload = notification.payload();
        for attempt in 0..ATTEMPTS {
            if Instant::now() >= deadline {
                if attempted {
                    stats.failed.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.dropped.fetch_add(1, Ordering::Relaxed);
                }
                break;
            }
            let ready = tokio::select! {
                _=cancel.cancelled()=>return,
                _=tokio::time::sleep_until(deadline)=>false,
                _=tokio::time::sleep_until(next_allowed)=>true,
            };
            if !ready {
                if attempted {
                    stats.failed.fetch_add(1, Ordering::Relaxed);
                } else {
                    stats.dropped.fetch_add(1, Ordering::Relaxed);
                }
                break;
            }
            attempted = true;
            let result = tokio::select! { _=cancel.cancelled()=>return, result=tokio::time::timeout_at(deadline,deliver(&client,&url,&payload,attempt))=>result };
            match result {
                Ok(Delivery::Delivered(pause)) => {
                    stats.delivered.fetch_add(1, Ordering::Relaxed);
                    next_allowed = Instant::now() + pause;
                    break;
                }
                Ok(Delivery::Retry(pause)) if attempt + 1 < ATTEMPTS => {
                    next_allowed = Instant::now() + pause;
                }
                other => {
                    if let Ok(Delivery::Retry(pause)) = other {
                        next_allowed = Instant::now() + pause;
                    }
                    stats.failed.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        "Discord notification could not be delivered; see notification metrics"
                    );
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        model::{Endpoint, Route, Snapshot},
        state::State,
    };
    use axum::{Json, Router, http::StatusCode, routing::post};
    use tokio::net::TcpListener;

    fn notification() -> Notification {
        Notification::new(Kind::RouteStopped, "test/main", "node-a", "instance-a")
    }
    fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap()
    }
    fn state() -> (Arc<State>, Endpoint) {
        let endpoint = Endpoint {
            id: "pod/127.0.0.1:25566".into(),
            address: "127.0.0.1:25566".parse().unwrap(),
            node_name: Some("node-a".into()),
        };
        let state = State::new("node-a".into(), "instance-a".into());
        state.publish(
            Snapshot {
                routes: vec![Route {
                    id: "test/main".into(),
                    hostnames: vec!["example.test".into()],
                    proxy_protocol: false,
                    allow_connections: true,
                    fallback_status: Default::default(),
                    endpoints: vec![endpoint.clone()],
                    errors: vec![],
                    backend_errors: vec![],
                }],
            }
            .validate(),
        );
        (state, endpoint)
    }
    #[test]
    fn transitions_suppress_failed_trials_and_reset_waits_for_real_recovery() {
        let (state, endpoint) = state();
        let (tx, mut rx) = mpsc::channel(16);
        *state.notifications.sender.lock().unwrap() = Some(tx);
        for _ in 0..3 {
            state.circuit_result("test/main", &endpoint, false);
        }
        let opened = rx.try_recv().unwrap();
        assert_eq!(opened.kind, Kind::CircuitOpened);
        assert_eq!(opened.failures, 3);
        assert_eq!(opened.cooldown, 10);
        state.circuit_result("test/main", &endpoint, false);
        state
            .circuits
            .lock()
            .unwrap()
            .get_mut(&endpoint.id)
            .unwrap()
            .reset();
        assert!(rx.try_recv().is_err());
        for _ in 0..3 {
            state.circuit_result("test/main", &endpoint, false);
        }
        assert!(rx.try_recv().is_err());
        state.circuit_result("test/main", &endpoint, true);
        assert_eq!(rx.try_recv().unwrap().kind, Kind::CircuitRecovered);
        state.circuit_result("test/main", &endpoint, true);
        assert!(rx.try_recv().is_err());
        for _ in 0..3 {
            state.circuit_result("test/main", &endpoint, false);
        }
        assert_eq!(rx.try_recv().unwrap().kind, Kind::CircuitOpened);
    }
    #[test]
    fn full_queue_drops_without_blocking_and_mentions_are_disabled() {
        let notifier = Notifier::default();
        let (tx, _rx) = mpsc::channel(QUEUE_SIZE);
        *notifier.sender.lock().unwrap() = Some(tx);
        for _ in 0..QUEUE_SIZE + 1 {
            notifier.send(notification());
        }
        assert_eq!(notifier.stats.dropped.load(Ordering::Relaxed), 1);
        assert!(
            notifier
                .metrics()
                .contains("aio_discord_notification_queue_depth 128")
        );
        let mut event = notification();
        event.node = "@everyone".repeat(200);
        let payload = event.payload();
        assert_eq!(payload["allowed_mentions"]["parse"], serde_json::json!([]));
        assert!(
            payload["embeds"][0]["fields"][1]["value"]
                .as_str()
                .unwrap()
                .chars()
                .count()
                <= 1000
        );
    }
    #[tokio::test]
    async fn retries_rate_limit_and_server_error_then_delivers_and_stops_on_4xx() {
        let calls = Arc::new(AtomicU64::new(0));
        let observed = calls.clone();
        let (bodies, mut received) = mpsc::channel(8);
        let app = Router::new().route(
            "/webhook",
            post(move |Json(body): Json<serde_json::Value>| {
                let calls = observed.clone();
                let bodies = bodies.clone();
                async move {
                    bodies.send(body).await.unwrap();
                    match calls.fetch_add(1, Ordering::Relaxed) {
                        0 => (
                            StatusCode::TOO_MANY_REQUESTS,
                            Json(serde_json::json!({"retry_after":0.1})),
                        ),
                        1 => (StatusCode::SERVICE_UNAVAILABLE, Json(serde_json::json!({}))),
                        2 => (StatusCode::OK, Json(serde_json::json!({}))),
                        _ => (StatusCode::BAD_REQUEST, Json(serde_json::json!({}))),
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/webhook", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let notifier = Notifier::default();
        let cancel = CancellationToken::new();
        let worker = notifier.spawn(client(), url.parse().unwrap(), cancel.clone());
        notifier.send(notification());
        for _ in 0..3 {
            let body = tokio::time::timeout(Duration::from_secs(4), received.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(body["embeds"][0]["title"], "Route: new connections stopped");
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while notifier.stats.delivered.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        notifier.send(notification());
        received.recv().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while notifier.stats.failed.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(calls.load(Ordering::Relaxed), 4);
        cancel.cancel();
        worker.await.unwrap();
        server.abort();
    }
    #[tokio::test]
    async fn expired_events_are_dropped_and_shutdown_cancels_inflight_request() {
        let (calls, mut received) = mpsc::channel(1);
        let app = Router::new().route(
            "/webhook",
            post(move || {
                let calls = calls.clone();
                async move {
                    calls.send(()).await.unwrap();
                    std::future::pending::<StatusCode>().await
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/webhook", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let notifier = Notifier::default();
        let cancel = CancellationToken::new();
        let worker = notifier.spawn(client(), url.parse().unwrap(), cancel.clone());
        let mut expired = notification();
        expired.created -= EVENT_TTL;
        notifier.send(expired);
        notifier.send(notification());
        tokio::time::timeout(Duration::from_secs(1), received.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(notifier.stats.dropped.load(Ordering::Relaxed), 1);
        cancel.cancel();
        tokio::time::timeout(Duration::from_millis(200), worker)
            .await
            .unwrap()
            .unwrap();
        server.abort();
    }
    #[tokio::test]
    async fn route_actions_notify_without_reason_and_include_session_counts() {
        let (state, _) = state();
        let (tx, mut rx) = mpsc::channel(8);
        *state.notifications.sender.lock().unwrap() = Some(tx);
        let session = state.register_route("test/main").unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/api/v1/routes/control",
            listener.local_addr().unwrap()
        );
        let api_state = state.clone();
        let server = tokio::spawn(async move {
            axum::serve(listener, crate::api::routes(api_state, "secret".into()))
                .await
                .unwrap();
        });
        for (action, kind) in [
            ("stop", Kind::RouteStopped),
            ("allow", Kind::RouteAllowed),
            ("kill", Kind::RouteKilled),
        ] {
            let result=client().post(&url).bearer_auth("secret").json(&serde_json::json!({"instanceId":"instance-a","routeId":"test/main","action":action})).send().await.unwrap();
            assert!(result.status().is_success());
            let n = rx.recv().await.unwrap();
            assert_eq!(n.kind, kind);
            assert_eq!(n.affected_sessions, 1);
            assert_eq!(n.node, "node-a");
            assert_eq!(n.instance, "instance-a");
        }
        assert!(session.cancel.is_cancelled());
        let repeated = client().post(&url).bearer_auth("secret").json(&serde_json::json!({"instanceId":"instance-a","routeId":"test/main","action":"kill"})).send().await.unwrap();
        assert_eq!(
            repeated.json::<serde_json::Value>().await.unwrap()["disconnectRequested"],
            0
        );
        assert!(rx.try_recv().is_err());
        server.abort();
    }
    #[tokio::test]
    async fn successful_bucket_exhaustion_delays_next_event_and_retries_are_bounded() {
        let count = Arc::new(AtomicU64::new(0));
        let observed = count.clone();
        let (tx, mut calls) = mpsc::channel(8);
        let app = Router::new().route(
            "/webhook",
            post(move || {
                let tx = tx.clone();
                let observed = observed.clone();
                async move {
                    tx.send(Instant::now()).await.unwrap();
                    if observed.fetch_add(1, Ordering::Relaxed) == 0 {
                        (
                            StatusCode::NO_CONTENT,
                            [
                                ("x-ratelimit-remaining", "0"),
                                ("x-ratelimit-reset-after", "0.2"),
                            ],
                        )
                    } else {
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            [
                                ("x-ratelimit-remaining", "1"),
                                ("x-ratelimit-reset-after", "0"),
                            ],
                        )
                    }
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/webhook", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let notifier = Notifier::default();
        let cancel = CancellationToken::new();
        let worker = notifier.spawn(client(), url.parse().unwrap(), cancel.clone());
        notifier.send(notification());
        notifier.send(notification());
        let first = calls.recv().await.unwrap();
        let second = calls.recv().await.unwrap();
        assert!(second.duration_since(first) >= Duration::from_millis(190));
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(4), calls.recv())
                .await
                .unwrap()
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while notifier.stats.failed.load(Ordering::Relaxed) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(count.load(Ordering::Relaxed), 4);
        assert_eq!(notifier.stats.delivered.load(Ordering::Relaxed), 1);
        cancel.cancel();
        worker.await.unwrap();
        server.abort();
    }
    #[tokio::test]
    async fn invalid_webhook_errors_do_not_expose_secrets() {
        let notifier = Notifier::default();
        for url in [
            "https://user:top-secret@discord.com/api/webhooks/1/token",
            "http://discord.com/api/webhooks/1/top-secret",
        ] {
            let error = notifier.start(url, CancellationToken::new()).unwrap_err();
            assert!(!error.to_string().contains("top-secret"));
        }
        assert!(notifier.metrics().contains("aio_discord_enabled 0"));
    }
}
