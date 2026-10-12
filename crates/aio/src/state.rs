use crate::{
    model::{Endpoint, Snapshot},
    notifications::{Kind, Notification, Notifier},
};
use serde::Serialize;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Notify, watch};

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub timestamp: u64,
    pub message: String,
}

#[derive(Default)]
pub struct Circuit {
    failures: u32,
    // Remains latched across cooldowns and manual reset until a real success.
    outage: bool,
    opened_until: Option<Instant>,
    probe: bool,
    cooldown: u64,
    exclusion: Option<(Instant, String)>,
}

impl Circuit {
    pub fn available(&self) -> bool {
        let now = Instant::now();
        !self
            .exclusion
            .as_ref()
            .is_some_and(|(until, _)| *until > now)
            && !self.probe
            && !self.opened_until.is_some_and(|until| until > now)
    }
    pub fn acquire(&mut self) -> bool {
        if !self.available() {
            return false;
        }
        if self.opened_until.is_some() {
            self.probe = true;
        }
        true
    }
    pub fn success(&mut self) -> bool {
        let recovered = self.outage;
        self.outage = false;
        self.failures = 0;
        self.opened_until = None;
        self.probe = false;
        self.cooldown = 0;
        recovered
    }
    pub fn failure(&mut self) -> bool {
        let mut opened = false;
        self.failures += 1;
        if self.probe || self.failures >= 3 {
            self.cooldown = if self.cooldown == 0 {
                10
            } else {
                (self.cooldown * 2).min(60)
            };
            opened = !self.outage;
            self.outage = true;
            self.opened_until = Some(Instant::now() + Duration::from_secs(self.cooldown));
        }
        self.probe = false;
        opened
    }
    pub fn release(&mut self) {
        self.probe = false;
    }
    pub fn reset(&mut self) {
        let outage = self.outage;
        self.success();
        self.outage = outage;
    }
    pub fn exclude(&mut self, seconds: u64, reason: String) {
        self.exclusion =
            (seconds > 0).then(|| (Instant::now() + Duration::from_secs(seconds), reason));
    }
    pub fn view(&self) -> serde_json::Value {
        let now = Instant::now();
        serde_json::json!({"state": if self.probe { "halfOpen" } else if self.opened_until.is_some_and(|t| t > now) { "open" } else if self.opened_until.is_some() { "halfOpen" } else { "closed" }, "failures": self.failures, "cooldownSeconds": self.opened_until.map(|t| t.saturating_duration_since(now).as_secs()).unwrap_or(0), "exclusion": self.exclusion.as_ref().filter(|(t,_)| *t > now).map(|(t,reason)| serde_json::json!({"remainingSeconds":t.saturating_duration_since(now).as_secs(),"reason":reason}))})
    }
}

#[derive(Default)]
pub struct RouteControl {
    pub blocked: bool,
    pub sessions: HashMap<u64, tokio_util::sync::CancellationToken>,
}
pub struct RouteSession {
    pub state: Arc<State>,
    pub route: String,
    pub id: u64,
    pub cancel: tokio_util::sync::CancellationToken,
}
impl Drop for RouteSession {
    fn drop(&mut self) {
        if let Some(c) = self
            .state
            .route_controls
            .lock()
            .unwrap()
            .get_mut(&self.route)
        {
            c.sessions.remove(&self.id);
        }
    }
}

pub struct State {
    pub notifications: Notifier,
    pub route_controls: Mutex<HashMap<String, RouteControl>>,
    session_sequence: AtomicU64,
    pub started: Instant,
    pub node: String,
    pub instance: String,
    pub snapshot: RwLock<Arc<Snapshot>>,
    pub circuits: Mutex<HashMap<String, Circuit>>,
    pub events: Mutex<VecDeque<Event>>,
    pub generation: AtomicU64,
    pub synced: AtomicBool,
    pub terminating: AtomicBool,
    pub accepting: AtomicBool,
    pub listener_bound: AtomicBool,
    pub admission_operation: tokio::sync::Mutex<()>,
    pub watch_error: RwLock<Option<String>>,
    pub service_errors: RwLock<HashMap<String, String>>,
    pub listener_error: RwLock<Option<String>>,
    pub config_errors: RwLock<HashMap<String, String>>,
    pub accept_errors: AtomicU64,
    pub fd_limit: AtomicU64,
    pub active: AtomicU64,
    pub total: AtomicU64,
    pub failures: AtomicU64,
    pub local: AtomicU64,
    pub global: AtomicU64,
    pub bytes: AtomicU64,
    pub admission: watch::Sender<bool>,
    pub listener_changed: Notify,
}

impl State {
    pub fn new(node: String, instance: String) -> Arc<Self> {
        let (admission, _) = watch::channel(true);
        Arc::new(Self {
            notifications: Notifier::default(),
            route_controls: Mutex::new(HashMap::new()),
            session_sequence: AtomicU64::new(0),
            started: Instant::now(),
            node,
            instance,
            snapshot: RwLock::new(Arc::new(Snapshot::default())),
            circuits: Mutex::new(HashMap::new()),
            events: Mutex::new(VecDeque::new()),
            generation: AtomicU64::new(0),
            synced: AtomicBool::new(false),
            terminating: AtomicBool::new(false),
            accepting: AtomicBool::new(false),
            listener_bound: AtomicBool::new(false),
            admission_operation: tokio::sync::Mutex::new(()),
            watch_error: RwLock::new(None),
            service_errors: RwLock::new(HashMap::new()),
            listener_error: RwLock::new(None),
            config_errors: RwLock::new(HashMap::new()),
            accept_errors: AtomicU64::new(0),
            fd_limit: AtomicU64::new(0),
            active: AtomicU64::new(0),
            total: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            local: AtomicU64::new(0),
            global: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            admission,
            listener_changed: Notify::new(),
        })
    }
    pub fn register_route(self: &Arc<Self>, id: &str) -> anyhow::Result<RouteSession> {
        let mut controls = self.route_controls.lock().unwrap();
        let snapshot = self.snapshot.read().unwrap();
        let route = snapshot
            .routes
            .iter()
            .find(|r| r.id == id)
            .ok_or_else(|| anyhow::anyhow!("route removed"))?;
        anyhow::ensure!(route.errors.is_empty(), "route configuration is invalid");
        let control = controls.entry(id.into()).or_default();
        anyhow::ensure!(
            route.allow_connections && !control.blocked,
            "route is in maintenance"
        );
        let sequence = self.session_sequence.fetch_add(1, Ordering::Relaxed);
        let cancel = tokio_util::sync::CancellationToken::new();
        control.sessions.insert(sequence, cancel.clone());
        Ok(RouteSession {
            state: self.clone(),
            route: id.into(),
            id: sequence,
            cancel,
        })
    }
    pub fn maintenance(&self, route: &crate::model::Route) -> bool {
        !route.allow_connections
            || self
                .route_controls
                .lock()
                .unwrap()
                .get(&route.id)
                .is_some_and(|c| c.blocked)
    }
    pub fn route_view(&self, route: &crate::model::Route) -> serde_json::Value {
        let mut value = serde_json::to_value(route).unwrap();
        let controls = self.route_controls.lock().unwrap();
        let c = controls.get(&route.id);
        let maintenance = !route.allow_connections || c.is_some_and(|c| c.blocked);
        let usable = route
            .endpoints
            .iter()
            .filter(|e| self.usable(e))
            .collect::<Vec<_>>();
        value["admission"] = serde_json::json!(if maintenance {
            "maintenance"
        } else if usable.is_empty() || !route.errors.is_empty() {
            "unavailable"
        } else {
            "normal"
        });
        value["activeConnections"] = serde_json::json!(c.map_or(0, |c| c.sessions.len()));
        value["usableLocal"] = serde_json::json!(
            usable
                .iter()
                .filter(|e| e.node_name.as_deref() == Some(&self.node))
                .count()
        );
        value["usableGlobal"] = serde_json::json!(
            usable
                .iter()
                .filter(|e| e.node_name.as_deref() != Some(&self.node))
                .count()
        );
        value
    }
    pub fn publish(&self, snapshot: Snapshot) {
        let mut controls = self.route_controls.lock().unwrap();
        controls
            .retain(|id, c| !c.sessions.is_empty() || snapshot.routes.iter().any(|r| &r.id == id));
        let ids: std::collections::HashSet<_> = snapshot
            .routes
            .iter()
            .flat_map(|r| &r.endpoints)
            .map(|e| &e.id)
            .collect();
        self.circuits
            .lock()
            .unwrap()
            .retain(|id, _| ids.contains(id));
        *self.snapshot.write().unwrap() = Arc::new(snapshot);
        self.generation.fetch_add(1, Ordering::Relaxed);
        self.synced.store(true, Ordering::Release);
    }
    pub fn event(&self, message: impl Into<String>) {
        let message = message.into();
        tracing::info!(%message);
        let mut events = self.events.lock().unwrap();
        if events.len() == 100 {
            events.pop_front();
        }
        events.push_back(Event {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            message,
        });
    }
    pub fn circuit_result(&self, route: &str, endpoint: &Endpoint, success: bool) {
        let mut circuits = self.circuits.lock().unwrap();
        let circuit = circuits.entry(endpoint.id.clone()).or_default();
        let changed = if success {
            circuit.success()
        } else {
            circuit.failure()
        };
        if changed {
            let kind = if success {
                Kind::CircuitRecovered
            } else {
                Kind::CircuitOpened
            };
            let mut notification = Notification::new(kind, route, &self.node, &self.instance);
            notification.backend = Some(endpoint.id.clone());
            notification.failures = circuit.failures;
            notification.cooldown = circuit.cooldown;
            self.event(format!(
                "backend circuit {} route={route} endpoint={} cooldown={}s",
                if success { "recovered" } else { "opened" },
                endpoint.id,
                circuit.cooldown
            ));
            self.notifications.send(notification);
        }
    }
    pub fn usable(&self, endpoint: &Endpoint) -> bool {
        self.circuits
            .lock()
            .unwrap()
            .get(&endpoint.id)
            .is_none_or(Circuit::available)
    }
    pub fn circuit_metrics(&self) -> String {
        use std::fmt::Write;
        let snapshot = self.snapshot.read().unwrap().clone();
        let circuits = self.circuits.lock().unwrap();
        let now = Instant::now();
        let mut output = String::from(
            "# HELP aio_backend_circuit_state One-hot circuit state; half_open includes waiting for a trial.\n# TYPE aio_backend_circuit_state gauge\n# TYPE aio_backend_circuit_cooldown_seconds gauge\n# TYPE aio_backend_circuit_failures gauge\n# TYPE aio_backend_excluded gauge\n# TYPE aio_backend_exclusion_remaining_seconds gauge\n",
        );
        let endpoints: std::collections::BTreeMap<_, _> = snapshot
            .routes
            .iter()
            .flat_map(|r| r.endpoints.iter().map(move |e| ((&r.id, &e.id), e)))
            .collect();
        for ((route, id), _) in endpoints {
            let default = Circuit::default();
            let circuit = circuits.get(id).unwrap_or(&default);
            let state = if circuit.probe {
                "half_open"
            } else if circuit.opened_until.is_some_and(|t| t > now) {
                "open"
            } else if circuit.opened_until.is_some() {
                "half_open"
            } else {
                "closed"
            };
            let labels = format!(
                "route=\"{}\",endpoint=\"{}\"",
                metric_label(route),
                metric_label(id)
            );
            for name in ["closed", "open", "half_open"] {
                writeln!(
                    output,
                    "aio_backend_circuit_state{{{labels},state=\"{name}\"}} {}",
                    u8::from(state == name)
                )
                .unwrap();
            }
            let cooldown = circuit
                .opened_until
                .map_or(0.0, |t| t.saturating_duration_since(now).as_secs_f64());
            let exclusion = circuit
                .exclusion
                .as_ref()
                .map_or(0.0, |(t, _)| t.saturating_duration_since(now).as_secs_f64());
            writeln!(output, "aio_backend_circuit_cooldown_seconds{{{labels}}} {cooldown}\naio_backend_circuit_failures{{{labels}}} {}\naio_backend_excluded{{{labels}}} {}\naio_backend_exclusion_remaining_seconds{{{labels}}} {exclusion}", circuit.failures, u8::from(exclusion > 0.0)).unwrap();
        }
        output
    }
    pub fn status(&self) -> serde_json::Value {
        serde_json::json!({"instanceId":self.instance,"nodeName":self.node,"version":env!("CARGO_PKG_VERSION"),"synced":self.synced.load(Ordering::Acquire),"accepting":self.accepting.load(Ordering::Acquire),"terminating":self.terminating.load(Ordering::Acquire),"generation":self.generation.load(Ordering::Relaxed),"activeConnections":self.active.load(Ordering::Relaxed),"watchError":*self.watch_error.read().unwrap(),"serviceErrors":*self.service_errors.read().unwrap(),"listenerError":*self.listener_error.read().unwrap(),"configErrors":*self.config_errors.read().unwrap(),"fileDescriptorLimit":self.fd_limit.load(Ordering::Relaxed),"uptimeSeconds":self.started.elapsed().as_secs(),"counters":{"localSelections":self.local.load(Ordering::Relaxed),"globalSelections":self.global.load(Ordering::Relaxed),"connections":self.total.load(Ordering::Relaxed),"errors":self.failures.load(Ordering::Relaxed),"bytes":self.bytes.load(Ordering::Relaxed)}})
    }
}

fn metric_label(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn circuit_opens_and_exclusion_survives_reset() {
        let mut c = Circuit::default();
        for _ in 0..2 {
            c.failure();
            assert!(c.available());
        }
        c.failure();
        assert!(!c.available());
        c.exclude(30, "maintenance".into());
        c.reset();
        assert!(!c.available());
        c.exclude(0, String::new());
        assert!(c.available());
    }
    #[test]
    fn metrics_track_state_exclusion_and_removed_endpoints() {
        use crate::model::Route;
        let state = State::new("node".into(), "instance".into());
        let endpoint = Endpoint {
            id: "pod\"\\\nendpoint".into(),
            address: "127.0.0.1:25566".parse().unwrap(),
            node_name: None,
        };
        state.publish(Snapshot {
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
        });
        let closed = state.circuit_metrics();
        assert!(closed.contains("state=\"closed\"} 1"));
        assert!(closed.contains("endpoint=\"pod\\\"\\\\\\nendpoint\""));
        for _ in 0..3 {
            state.circuit_result("test/main", &endpoint, false);
        }
        assert!(state.circuit_metrics().contains("state=\"open\"} 1"));
        {
            let mut circuits = state.circuits.lock().unwrap();
            let c = circuits.get_mut(&endpoint.id).unwrap();
            c.opened_until = Some(Instant::now());
            c.exclude(60, String::new());
        }
        let trial = state.circuit_metrics();
        assert!(trial.contains("state=\"half_open\"} 1"));
        assert!(trial.contains(
            "aio_backend_excluded{route=\"test/main\",endpoint=\"pod\\\"\\\\\\nendpoint\"} 1"
        ));
        state.circuit_result("test/main", &endpoint, true);
        assert!(state.circuit_metrics().contains("state=\"closed\"} 1"));
        assert!(!state.usable(&endpoint));
        state.publish(Snapshot::default());
        assert!(!state.circuit_metrics().contains("test/main"));
    }
    #[test]
    fn half_open_has_only_one_trial() {
        let mut c = Circuit {
            opened_until: Some(Instant::now()),
            cooldown: 10,
            ..Default::default()
        };
        assert!(c.acquire());
        assert!(!c.acquire());
        c.failure();
        assert!(!c.available());
        assert_eq!(c.cooldown, 20);
    }
}
