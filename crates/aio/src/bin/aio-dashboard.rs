use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    middleware,
    routing::{get, post},
};
use clap::Parser;
use k8s_openapi::api::coordination::v1::Lease;
use kube::{Api, Client, api::ListParams};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::RwLock;
use tower_http::services::ServeDir;

#[derive(Parser)]
struct Args {
    #[arg(long, env = "AIO_DASHBOARD_LISTEN", default_value = "127.0.0.1:8080")]
    listen: SocketAddr,
    #[arg(long, env = "AIO_API_TOKEN", hide_env_values = true)]
    agent_token: String,
    #[arg(long, env = "AIO_DASHBOARD_TOKEN", hide_env_values = true)]
    dashboard_token: String,
    #[arg(long, env = "AIO_AGENTS")]
    agents: Option<PathBuf>,
    #[arg(long, env = "AIO_REGISTRATION_NAMESPACE", default_value = "aio-system")]
    namespace: String,
    #[arg(long, env = "AIO_WEB_DIR", default_value = "web/dist")]
    web_dir: PathBuf,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Agent {
    instance_id: String,
    url: String,
    #[serde(default)]
    last_heartbeat: Option<u64>,
    #[serde(default)]
    missing: bool,
}
struct Dashboard {
    agents: RwLock<HashMap<String, Agent>>,
    client: reqwest::Client,
    token: String,
}
type Error = (StatusCode, String);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().init();
    let args = Args::parse();
    anyhow::ensure!(
        !args.agent_token.is_empty() && !args.dashboard_token.is_empty(),
        "tokens cannot be empty"
    );
    let state = Arc::new(Dashboard {
        agents: RwLock::new(HashMap::new()),
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(3))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        token: args.agent_token,
    });
    if let Some(path) = args.agents {
        let agents: Vec<Agent> = serde_yaml::from_str(&tokio::fs::read_to_string(path).await?)?;
        *state.agents.write().await = agents
            .into_iter()
            .map(|a| (a.instance_id.clone(), a))
            .collect();
    } else {
        let client = Client::try_default().await?;
        let api: Api<Lease> = Api::namespaced(client, &args.namespace);
        let state = state.clone();
        tokio::spawn(async move {
            loop {
                match api
                    .list(&ListParams::default().labels("app.kubernetes.io/name=aio-agent"))
                    .await
                {
                    Ok(list) => {
                        let now = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let mut agents = HashMap::new();
                        for lease in list {
                            let Some(a) = lease.metadata.annotations else {
                                continue;
                            };
                            let Some(updated) = a
                                .get("aio.akasha.asorda.net/updated-at")
                                .and_then(|s| s.parse::<u64>().ok())
                            else {
                                continue;
                            };
                            if let (Some(id), Some(url)) = (
                                a.get("aio.akasha.asorda.net/instance-id"),
                                a.get("aio.akasha.asorda.net/url"),
                            ) {
                                agents.insert(
                                    id.clone(),
                                    Agent {
                                        instance_id: id.clone(),
                                        url: url.clone(),
                                        last_heartbeat: Some(updated),
                                        missing: false,
                                    },
                                );
                            }
                        }
                        let mut inventory = state.agents.write().await;
                        merge_inventory(&mut inventory, agents, now);
                    }
                    Err(error) => {
                        tracing::warn!(%error,"registration discovery failed; retaining prior registrations")
                    }
                }
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });
    }
    let app = Router::new()
        .route("/api/instances", get(instances))
        .route("/api/instances/{id}/{action}", post(action))
        .layer(middleware::from_fn_with_state(
            args.dashboard_token,
            aio::api::auth,
        ))
        .layer(tower_http::limit::RequestBodyLimitLayer::new(16 * 1024))
        .layer(tower_http::timeout::TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(30),
        ))
        .with_state(state)
        .fallback_service(ServeDir::new(args.web_dir));
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    tracing::info!(address=%listener.local_addr()?,"dashboard listening");
    axum::serve(listener, app).await?;
    Ok(())
}

async fn instances(State(s): State<Arc<Dashboard>>) -> Json<serde_json::Value> {
    use futures::{StreamExt, stream};
    let agents: Vec<_> = s.agents.read().await.values().cloned().collect();
    let items:Vec<_>=stream::iter(agents).map(|agent| {let s=s.clone(); async move {
        if stale(&agent,now()) {
            return serde_json::json!({"instanceId":agent.instance_id,"reachable":false,"stale":true,"lastHeartbeat":agent.last_heartbeat,"observedAt":now(),"error":"registration expired or removed"});
        }
        let result=async {
            let mut values=serde_json::Map::new();
            for name in ["status","routes","endpoints","events"] {
                let value=s.client.get(format!("{}/api/v1/{name}",agent.url.trim_end_matches('/'))).bearer_auth(&s.token).send().await?.error_for_status()?.json::<serde_json::Value>().await?;
                values.insert(name.into(),value);
            }
            anyhow::Ok(serde_json::Value::Object(values))
        }.await;
        match result { Ok(data)=>serde_json::json!({"instanceId":agent.instance_id,"reachable":true,"stale":false,"lastHeartbeat":agent.last_heartbeat,"observedAt":now(),"data":data}),Err(error)=>serde_json::json!({"instanceId":agent.instance_id,"reachable":false,"stale":false,"lastHeartbeat":agent.last_heartbeat,"observedAt":now(),"error":error.to_string()}) }
    }}).buffer_unordered(8).collect().await;
    Json(serde_json::json!({"instances":items,"observedAt":now()}))
}

fn now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
async fn action(
    State(s): State<Arc<Dashboard>>,
    Path((id, action)): Path<(String, String)>,
    Json(mut body): Json<serde_json::Value>,
) -> Result<Json<serde_json::Value>, Error> {
    let path = match action.as_str() {
        "resolve" => "debug/resolve",
        "admission" => "admission",
        "exclude" => "endpoints/exclude",
        "reset" => "endpoints/reset",
        _ => return Err((StatusCode::NOT_FOUND, "unknown action".into())),
    };
    let agent = s
        .agents
        .read()
        .await
        .get(&id)
        .cloned()
        .ok_or((StatusCode::NOT_FOUND, "unknown instance".into()))?;
    if stale(&agent, now()) {
        return Err((
            StatusCode::CONFLICT,
            "instance registration is stale; action refused".into(),
        ));
    }
    if action != "resolve" {
        let object = body
            .as_object_mut()
            .ok_or((StatusCode::BAD_REQUEST, "expected JSON object".into()))?;
        object.insert("instanceId".into(), serde_json::json!(id));
    }
    let response = s
        .client
        .post(format!("{}/api/v1/{path}", agent.url.trim_end_matches('/')))
        .bearer_auth(&s.token)
        .json(&body)
        .send()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?;
    if !status.is_success() {
        return Err((status, text));
    }
    Ok(Json(
        serde_json::from_str(&text).map_err(|e| (StatusCode::BAD_GATEWAY, e.to_string()))?,
    ))
}

fn stale(agent: &Agent, now: u64) -> bool {
    agent.missing
        || agent
            .last_heartbeat
            .is_some_and(|heartbeat| now.saturating_sub(heartbeat) > 30)
}

fn merge_inventory(
    inventory: &mut HashMap<String, Agent>,
    current: HashMap<String, Agent>,
    now: u64,
) {
    for (id, agent) in inventory.iter_mut() {
        if !current.contains_key(id) {
            agent.missing = true;
        }
    }
    inventory.extend(current);
    // Bound historical registrations while retaining a full day of failed/drained instances.
    inventory.retain(|_, agent| {
        agent
            .last_heartbeat
            .is_none_or(|heartbeat| now.saturating_sub(heartbeat) <= 86400)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_and_expired_instances_remain_visible_but_cannot_receive_commands() {
        let old = Agent {
            instance_id: "old".into(),
            url: "http://127.0.0.1:1".into(),
            last_heartbeat: Some(100),
            missing: false,
        };
        let mut inventory = HashMap::from([("old".into(), old)]);
        merge_inventory(&mut inventory, HashMap::new(), 110);
        assert!(inventory.contains_key("old"));
        assert!(stale(&inventory["old"], 110));
        inventory.get_mut("old").unwrap().missing = false;
        assert!(stale(&inventory["old"], 131));
        merge_inventory(&mut inventory, HashMap::new(), 86501);
        assert!(inventory.is_empty());
    }
}
