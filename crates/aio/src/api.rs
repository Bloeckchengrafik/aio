use crate::{model::normalize_host, state::State as AgentState};
use axum::{
    Json, Router,
    extract::{Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
};
use serde::Deserialize;
use std::{
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

type Error = (StatusCode, String);

pub fn routes(state: Arc<AgentState>, token: String) -> Router {
    let protected = Router::new()
        .route("/api/v1/status", get(status))
        .route("/api/v1/routes", get(routes_view))
        .route("/api/v1/endpoints", get(endpoints))
        .route("/api/v1/events", get(events))
        .route("/api/v1/debug/resolve", post(resolve))
        .route("/api/v1/admission", post(admission))
        .route("/api/v1/routes/control", post(route_control))
        .route("/api/v1/endpoints/exclude", post(exclude))
        .route("/api/v1/endpoints/reset", post(reset))
        .route("/metrics", get(metrics))
        .layer(middleware::from_fn_with_state(token, auth));
    Router::new()
        .route("/health/live", get(|| async { StatusCode::OK }))
        .route("/health/ready", get(ready))
        .merge(protected)
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(10),
        ))
        .layer(tower_http::limit::RequestBodyLimitLayer::new(16 * 1024))
        .with_state(state)
}

pub async fn auth(
    State(token): State<String>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    use subtle::ConstantTimeEq;
    let provided = request
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or_default();
    if !bool::from(provided.as_bytes().ct_eq(token.as_bytes())) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(next.run(request).await)
}
async fn ready(State(s): State<Arc<AgentState>>) -> StatusCode {
    if s.synced.load(Ordering::Acquire)
        && s.accepting.load(Ordering::Acquire)
        && !s.terminating.load(Ordering::Acquire)
        && !s
            .service_errors
            .read()
            .unwrap()
            .keys()
            .any(|name| matches!(name.as_str(), "discovery" | "management"))
    {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    }
}
async fn status(State(s): State<Arc<AgentState>>) -> Json<serde_json::Value> {
    Json(s.status())
}
async fn routes_view(State(s): State<Arc<AgentState>>) -> Json<serde_json::Value> {
    let snapshot = s.snapshot.read().unwrap().clone();
    Json(
        serde_json::json!({"generation":s.generation.load(Ordering::Relaxed),"routes":snapshot.routes.iter().map(|r| s.route_view(r)).collect::<Vec<_>>()}),
    )
}
async fn endpoints(State(s): State<Arc<AgentState>>) -> Json<serde_json::Value> {
    let snapshot = s.snapshot.read().unwrap().clone();
    let circuits = s.circuits.lock().unwrap();
    Json(serde_json::json!(snapshot.routes.iter().flat_map(|r|r.endpoints.iter().map(|e| serde_json::json!({"routeId":r.id,"endpoint":e,"local":e.node_name.as_deref()==Some(&s.node),"circuit":circuits.get(&e.id).map(|c|c.view()).unwrap_or_else(||serde_json::json!({"state":"closed","failures":0,"cooldownSeconds":0,"exclusion":null}))}))).collect::<Vec<_>>()))
}
async fn events(State(s): State<Arc<AgentState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!(*s.events.lock().unwrap()))
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Resolve {
    hostname: String,
}
async fn resolve(
    State(s): State<Arc<AgentState>>,
    Json(body): Json<Resolve>,
) -> Result<Json<serde_json::Value>, Error> {
    let host =
        normalize_host(&body.hostname).map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let snapshot = s.snapshot.read().unwrap().clone();
    Ok(Json(match snapshot.resolve(&host) {
        Some(r) => {
            serde_json::json!({"hostname":host,"routeId":r.id,"admission":s.route_view(r)["admission"],"proxyProtocol":r.proxy_protocol,"candidates":r.endpoints.iter().map(|e|serde_json::json!({"id":e.id,"local":e.node_name.as_deref()==Some(&s.node),"usable":s.usable(e)})).collect::<Vec<_>>() })
        }
        None => serde_json::json!({"hostname":host,"routeId":null,"candidates":[]}),
    }))
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Admission {
    instance_id: String,
    accepting: bool,
}
fn validate(s: &AgentState, instance: &str) -> Result<(), Error> {
    if instance != s.instance {
        return Err((StatusCode::CONFLICT, "instance changed".into()));
    }
    Ok(())
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RouteAction {
    instance_id: String,
    route_id: String,
    action: String,
}
async fn route_control(
    State(s): State<Arc<AgentState>>,
    Json(body): Json<RouteAction>,
) -> Result<Json<serde_json::Value>, Error> {
    validate(&s, &body.instance_id)?;
    if !matches!(body.action.as_str(), "allow" | "stop" | "kill") {
        return Err((StatusCode::BAD_REQUEST, "unknown route action".into()));
    }
    let mut controls = s.route_controls.lock().unwrap();
    let snapshot = s.snapshot.read().unwrap();
    let route = snapshot
        .routes
        .iter()
        .find(|r| r.id == body.route_id)
        .ok_or((StatusCode::NOT_FOUND, "route not found".into()))?;
    if body.action == "allow" && !route.allow_connections {
        return Err((
            StatusCode::CONFLICT,
            "MinecraftRoute disables connections; change allowConnections first".into(),
        ));
    }
    let c = controls.entry(body.route_id.clone()).or_default();
    c.blocked = body.action != "allow";
    let disconnected = if body.action == "kill" {
        for token in c.sessions.values() {
            token.cancel();
        }
        c.sessions.len()
    } else {
        0
    };
    drop(snapshot);
    drop(controls);
    s.event(format!(
        "operator route={} action={} disconnectRequested={disconnected}",
        body.route_id, body.action
    ));
    Ok(Json(
        serde_json::json!({"ok":true,"disconnectRequested":disconnected}),
    ))
}
async fn admission(
    State(s): State<Arc<AgentState>>,
    Json(body): Json<Admission>,
) -> Result<Json<serde_json::Value>, Error> {
    validate(&s, &body.instance_id)?;
    let _operation = s.admission_operation.lock().await;
    if s.terminating.load(Ordering::Acquire) || !s.synced.load(Ordering::Acquire) {
        return Err((
            StatusCode::CONFLICT,
            "instance is terminating or not synchronized".into(),
        ));
    }
    s.admission.send_replace(body.accepting);
    // Wait for the accept loop to close/rebind, rather than reporting intent as success.
    let confirmed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if !body.accepting && !s.listener_bound.load(Ordering::Acquire) {
                return Ok(());
            }
            if body.accepting
                && s.listener_bound.load(Ordering::Acquire)
                && s.accepting.load(Ordering::Acquire)
            {
                return Ok(());
            }
            if body.accepting && !*s.admission.borrow() {
                return Err(s
                    .listener_error
                    .read()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| "resume rejected".into()));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let confirmed = matches!(confirmed, Ok(Ok(())));
    s.event(format!(
        "operator admission={} confirmed={confirmed}",
        body.accepting
    ));
    if !confirmed {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            "listener transition not confirmed".into(),
        ));
    }
    Ok(Json(s.status()))
}
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EndpointAction {
    instance_id: String,
    endpoint_id: String,
    #[serde(default)]
    ttl_seconds: u64,
}
fn endpoint_exists(s: &AgentState, id: &str) -> bool {
    s.snapshot
        .read()
        .unwrap()
        .routes
        .iter()
        .any(|r| r.endpoints.iter().any(|e| e.id == id))
}
async fn exclude(
    State(s): State<Arc<AgentState>>,
    Json(body): Json<EndpointAction>,
) -> Result<Json<serde_json::Value>, Error> {
    validate(&s, &body.instance_id)?;
    if body.ttl_seconds > 86400 {
        return Err((StatusCode::BAD_REQUEST, "TTL exceeds 24h".into()));
    }
    if !endpoint_exists(&s, &body.endpoint_id) {
        return Err((StatusCode::NOT_FOUND, "endpoint not found".into()));
    }
    s.circuits
        .lock()
        .unwrap()
        .entry(body.endpoint_id.clone())
        .or_default()
        .exclude(body.ttl_seconds, String::new());
    s.event(format!(
        "operator exclusion endpoint={} ttl={}",
        body.endpoint_id, body.ttl_seconds
    ));
    Ok(Json(serde_json::json!({"ok":true})))
}
async fn reset(
    State(s): State<Arc<AgentState>>,
    Json(body): Json<EndpointAction>,
) -> Result<Json<serde_json::Value>, Error> {
    validate(&s, &body.instance_id)?;
    if !endpoint_exists(&s, &body.endpoint_id) {
        return Err((StatusCode::NOT_FOUND, "endpoint not found".into()));
    }
    s.circuits
        .lock()
        .unwrap()
        .entry(body.endpoint_id.clone())
        .or_default()
        .reset();
    s.event(format!("operator reset endpoint={}", body.endpoint_id));
    Ok(Json(serde_json::json!({"ok":true})))
}
async fn metrics(State(s): State<Arc<AgentState>>) -> String {
    [
        ("aio_connections_active", s.active.load(Ordering::Relaxed)),
        ("aio_connections_total", s.total.load(Ordering::Relaxed)),
        (
            "aio_connection_errors_total",
            s.failures.load(Ordering::Relaxed),
        ),
        (
            "aio_local_selections_total",
            s.local.load(Ordering::Relaxed),
        ),
        (
            "aio_global_selections_total",
            s.global.load(Ordering::Relaxed),
        ),
        ("aio_forwarded_bytes_total", s.bytes.load(Ordering::Relaxed)),
        (
            "aio_accept_errors_total",
            s.accept_errors.load(Ordering::Relaxed),
        ),
    ]
    .into_iter()
    .map(|(name, value)| format!("{name} {value}\n"))
    .collect()
}
