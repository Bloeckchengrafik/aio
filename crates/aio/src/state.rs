use crate::model::{Endpoint, Snapshot};
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
    pub fn success(&mut self) {
        self.failures = 0;
        self.opened_until = None;
        self.probe = false;
        self.cooldown = 0;
    }
    pub fn failure(&mut self) {
        self.failures += 1;
        if self.probe || self.failures >= 3 {
            self.cooldown = if self.cooldown == 0 {
                10
            } else {
                (self.cooldown * 2).min(60)
            };
            self.opened_until = Some(Instant::now() + Duration::from_secs(self.cooldown));
        }
        self.probe = false;
    }
    pub fn release(&mut self) {
        self.probe = false;
    }
    pub fn reset(&mut self) {
        self.success();
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

pub struct State {
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
    pub fn publish(&self, snapshot: Snapshot) {
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
    pub fn usable(&self, endpoint: &Endpoint) -> bool {
        self.circuits
            .lock()
            .unwrap()
            .get(&endpoint.id)
            .is_none_or(Circuit::available)
    }
    pub fn status(&self) -> serde_json::Value {
        serde_json::json!({"instanceId":self.instance,"nodeName":self.node,"version":env!("CARGO_PKG_VERSION"),"synced":self.synced.load(Ordering::Acquire),"accepting":self.accepting.load(Ordering::Acquire),"terminating":self.terminating.load(Ordering::Acquire),"generation":self.generation.load(Ordering::Relaxed),"activeConnections":self.active.load(Ordering::Relaxed),"watchError":*self.watch_error.read().unwrap(),"serviceErrors":*self.service_errors.read().unwrap(),"listenerError":*self.listener_error.read().unwrap(),"configErrors":*self.config_errors.read().unwrap(),"fileDescriptorLimit":self.fd_limit.load(Ordering::Relaxed)})
    }
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
