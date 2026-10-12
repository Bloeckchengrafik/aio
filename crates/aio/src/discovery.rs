use crate::{
    model::{Endpoint, MinecraftRoute, Route, Snapshot},
    state::State,
};
use futures::StreamExt;
use kube::{
    Api, Client, ResourceExt,
    api::{ApiResource, DynamicObject, GroupVersionKind},
    runtime::{WatchStreamExt, watcher},
};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

type Objects = HashMap<String, Value>;
enum Update {
    Replace(usize, Objects),
    Error(usize, String),
}

pub async fn kubernetes(
    state: Arc<State>,
    namespace: String,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let client = Client::try_default().await?;
    let resources = [
        (
            "akasha.asorda.net",
            "v1alpha1",
            "MinecraftRoute",
            "minecraftroutes",
        ),
        ("", "v1", "Service", "services"),
        ("discovery.k8s.io", "v1", "EndpointSlice", "endpointslices"),
    ];
    let (tx, mut rx) = mpsc::channel(32);
    let mut workers = tokio::task::JoinSet::new();
    for (index, (group, version, kind, plural)) in resources.into_iter().enumerate() {
        let resource = ApiResource::from_gvk(&GroupVersionKind::gvk(group, version, kind));
        let resource = ApiResource {
            plural: plural.into(),
            ..resource
        };
        let api: Api<DynamicObject> = if namespace == "*" {
            Api::all_with(client.clone(), &resource)
        } else {
            Api::namespaced_with(client.clone(), &namespace, &resource)
        };
        let tx = tx.clone();
        let cancel = cancel.clone();
        workers.spawn(async move {
            let stream = watcher(api, watcher::Config::default()).default_backoff();
            tokio::pin!(stream);
            let mut objects = Objects::new();
            let mut staged = Objects::new();
            loop {
                let event = tokio::select! { _ = cancel.cancelled() => break, event = stream.next() => event };
                let Some(event) = event else {
                    anyhow::bail!("watch {index} ended unexpectedly");
                };
                let update = match event {
                    Ok(watcher::Event::Init) => {
                        staged.clear();
                        continue;
                    }
                    Ok(watcher::Event::InitApply(object)) => {
                        staged.insert(key(&object), serde_json::to_value(object).unwrap());
                        continue;
                    }
                    Ok(watcher::Event::InitDone) => {
                        objects = std::mem::take(&mut staged);
                        Update::Replace(index, objects.clone())
                    }
                    Ok(watcher::Event::Apply(object)) => {
                        objects.insert(key(&object), serde_json::to_value(object).unwrap());
                        Update::Replace(index, objects.clone())
                    }
                    Ok(watcher::Event::Delete(object)) => {
                        objects.remove(&key(&object));
                        Update::Replace(index, objects.clone())
                    }
                    Err(error) => Update::Error(index, error.to_string()),
                };
                if tx.send(update).await.is_err() {
                    break;
                }
            }
            anyhow::Ok(())
        });
    }
    drop(tx);
    let mut caches: [Objects; 3] = Default::default();
    let mut synced = [false; 3];
    let mut errors: [Option<String>; 3] = Default::default();
    let mut valid_routes = HashMap::new();
    loop {
        let update = tokio::select! {
            _ = cancel.cancelled() => break,
            result=workers.join_next()=> {
                match result {Some(Ok(Err(error)))=>return Err(error),Some(Err(error))=>anyhow::bail!("discovery worker failed: {error}"),_=>anyhow::bail!("discovery worker stopped")}
            },
            update = rx.recv() => update
        };
        let Some(update) = update else {
            break;
        };
        match update {
            Update::Replace(i, objects) => {
                caches[i] = objects;
                synced[i] = true;
                errors[i] = None;
                if synced.iter().all(|s| *s) {
                    let (routes, config_errors) =
                        retain_valid_routes(&caches[0], &mut valid_routes);
                    *state.config_errors.write().unwrap() = config_errors;
                    state.publish(build(&[&routes, &caches[1], &caches[2]]));
                    state.service_errors.write().unwrap().remove("discovery");
                }
            }
            Update::Error(i, error) => {
                state.event(format!("watch {i}: {error}"));
                errors[i] = Some(error);
            }
        }
        *state.watch_error.write().unwrap() = errors.iter().flatten().next().cloned();
    }
    Ok(())
}

fn key(object: &DynamicObject) -> String {
    format!(
        "{}/{}",
        object.namespace().unwrap_or_default(),
        object.name_any()
    )
}

fn build(caches: &[&Objects; 3]) -> Snapshot {
    let mut routes = Vec::new();
    for (id, value) in caches[0] {
        let object: MinecraftRoute = match serde_json::from_value(value.clone()) {
            Ok(o) => o,
            Err(error) => {
                routes.push(Route {
                    id: id.clone(),
                    hostnames: vec![],
                    proxy_protocol: false,
                    endpoints: vec![],
                    errors: vec![error.to_string()],
                });
                continue;
            }
        };
        let namespace = object.namespace().unwrap_or_default();
        let spec = object.spec;
        let mut route = Route {
            id: id.clone(),
            hostnames: spec.hostnames,
            proxy_protocol: spec.proxy_protocol,
            endpoints: vec![],
            errors: vec![],
        };
        let Some(service) = caches[1].get(&format!("{namespace}/{}", spec.backend.service)) else {
            route.errors.push("backend Service missing".into());
            routes.push(route);
            continue;
        };
        if service.pointer("/spec/clusterIP").and_then(Value::as_str) != Some("None") {
            route.errors.push("backend Service must be headless".into());
        }
        let ports = service
            .pointer("/spec/ports")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let ports: Vec<_> = ports
            .iter()
            .filter(|p| {
                p["port"].as_u64() == Some(spec.backend.port.into())
                    && p["protocol"].as_str().unwrap_or("TCP") == "TCP"
            })
            .collect();
        if ports.len() != 1 {
            route
                .errors
                .push("Service port missing or ambiguous".into());
            routes.push(route);
            continue;
        }
        let name = ports[0]["name"].as_str().unwrap_or("");
        let mut seen = std::collections::HashSet::new();
        for slice in caches[2].values().filter(|s| {
            s["metadata"]["namespace"].as_str() == Some(&namespace)
                && s["metadata"]["labels"]["kubernetes.io/service-name"].as_str()
                    == Some(&spec.backend.service)
        }) {
            let ports = slice["ports"].as_array().cloned().unwrap_or_default();
            let ports: Vec<_> = ports
                .iter()
                .filter(|p| {
                    p["name"].as_str().unwrap_or("") == name
                        && p["protocol"].as_str().unwrap_or("TCP") == "TCP"
                })
                .collect();
            let Some(port) = ports
                .first()
                .filter(|_| ports.len() == 1)
                .and_then(|p| p["port"].as_u64())
                .filter(|p| *p > 0 && *p <= 65535)
            else {
                route
                    .errors
                    .push("EndpointSlice port missing or ambiguous".into());
                continue;
            };
            for endpoint in slice["endpoints"].as_array().into_iter().flatten() {
                if endpoint["conditions"]["ready"].as_bool() != Some(true)
                    || endpoint["conditions"]["terminating"].as_bool() == Some(true)
                {
                    continue;
                }
                for address in endpoint["addresses"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(Value::as_str)
                {
                    let Ok(ip) = address.parse() else {
                        continue;
                    };
                    let address = std::net::SocketAddr::new(ip, port as u16);
                    if !seen.insert(address) {
                        continue;
                    }
                    route.endpoints.push(Endpoint {
                        id: format!(
                            "{}/{}",
                            endpoint["targetRef"]["uid"].as_str().unwrap_or(id),
                            address
                        ),
                        address,
                        node_name: endpoint["nodeName"].as_str().map(str::to_owned),
                    });
                }
            }
        }
        routes.push(route);
    }
    Snapshot { routes }.validate()
}

type ValidRoutes = HashMap<String, (String, Value)>;
fn retain_valid_routes(
    current: &Objects,
    valid: &mut ValidRoutes,
) -> (Objects, HashMap<String, String>) {
    valid.retain(|id, _| current.contains_key(id));
    let mut effective = Objects::new();
    let mut errors = HashMap::new();
    for (id, value) in current {
        let uid = value["metadata"]["uid"].as_str().unwrap_or_default();
        let validation = (|| -> anyhow::Result<()> {
            let object: MinecraftRoute = serde_json::from_value(value.clone())?;
            anyhow::ensure!(
                object.spec.backend.port > 0 && !object.spec.backend.service.is_empty(),
                "invalid backend reference"
            );
            let route = Route {
                id: id.clone(),
                hostnames: object.spec.hostnames,
                proxy_protocol: object.spec.proxy_protocol,
                endpoints: vec![],
                errors: vec![],
            };
            let checked = Snapshot {
                routes: vec![route],
            }
            .validate();
            anyhow::ensure!(
                checked.routes[0].errors.is_empty(),
                "{}",
                checked.routes[0].errors.join("; ")
            );
            Ok(())
        })();
        match validation {
            Ok(()) => {
                valid.insert(id.clone(), (uid.into(), value.clone()));
                effective.insert(id.clone(), value.clone());
            }
            Err(error) => {
                errors.insert(id.clone(), error.to_string());
                if let Some((_, previous)) = valid.get(id).filter(|(old_uid, _)| old_uid == uid) {
                    effective.insert(id.clone(), previous.clone());
                } else {
                    valid.remove(id);
                    effective.insert(id.clone(), value.clone());
                }
            }
        }
    }
    (effective, errors)
}

pub async fn local_file(
    state: Arc<State>,
    path: std::path::PathBuf,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let mut previous = String::new();
    loop {
        match tokio::fs::read_to_string(&path).await {
            Ok(contents) if contents != previous => {
                match serde_yaml::from_str::<Snapshot>(&contents) {
                    Ok(snapshot) => {
                        let snapshot = snapshot.validate();
                        if snapshot.routes.iter().any(|r| !r.errors.is_empty()) {
                            *state.watch_error.write().unwrap() = Some(
                                "invalid local configuration; retaining previous snapshot".into(),
                            );
                        } else {
                            state.publish(snapshot);
                            state.service_errors.write().unwrap().remove("discovery");
                            *state.watch_error.write().unwrap() = None;
                            previous = contents;
                        }
                    }
                    Err(error) => {
                        *state.watch_error.write().unwrap() = Some(error.to_string());
                    }
                }
            }
            Err(error) => {
                *state.watch_error.write().unwrap() = Some(error.to_string());
            }
            _ => {}
        }
        tokio::select! { _=cancel.cancelled()=>break, _=tokio::time::sleep(Duration::from_secs(2))=>{} }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn malformed_update_retains_only_same_resources_last_valid_spec() {
        let mut current = Objects::new();
        let mut valid = ValidRoutes::new();
        let original = serde_json::json!({"apiVersion":"akasha.asorda.net/v1alpha1","kind":"MinecraftRoute","metadata":{"name":"main","namespace":"asorda","uid":"first"},"spec":{"hostnames":["asorda.net"],"backend":{"service":"proxy","port":25565},"proxyProtocol":true}});
        current.insert("asorda/main".into(), original.clone());
        assert!(retain_valid_routes(&current, &mut valid).1.is_empty());
        let mut malformed = original.clone();
        malformed["spec"]["hostnames"] = serde_json::json!(["bad hostname"]);
        current.insert("asorda/main".into(), malformed.clone());
        let (effective, errors) = retain_valid_routes(&current, &mut valid);
        assert_eq!(effective["asorda/main"], original);
        assert!(errors.contains_key("asorda/main"));
        malformed["metadata"]["uid"] = serde_json::json!("replacement");
        current.insert("asorda/main".into(), malformed.clone());
        assert_eq!(
            retain_valid_routes(&current, &mut valid).0["asorda/main"],
            malformed
        );
        assert!(!valid.contains_key("asorda/main"));
        current.clear();
        assert!(retain_valid_routes(&current, &mut valid).0.is_empty());
    }
    #[test]
    fn endpoint_slice_target_port_and_readiness() {
        let mut c: [Objects; 3] = Default::default();
        c[0].insert("asorda/main".into(),serde_json::json!({"apiVersion":"akasha.asorda.net/v1alpha1","kind":"MinecraftRoute","metadata":{"name":"main","namespace":"asorda"},"spec":{"hostnames":["asorda.net"],"backend":{"service":"proxy","port":25565},"proxyProtocol":true}}));
        c[1].insert("asorda/proxy".into(),serde_json::json!({"spec":{"clusterIP":"None","ports":[{"name":"mc","port":25565,"targetPort":25566}]}}));
        c[2].insert("slice".into(),serde_json::json!({"metadata":{"namespace":"asorda","labels":{"kubernetes.io/service-name":"proxy"}},"ports":[{"name":"mc","port":25566}],"endpoints":[{"addresses":["127.0.0.1"],"conditions":{"ready":true},"nodeName":"n1","targetRef":{"uid":"p1"}},{"addresses":["127.0.0.2"],"conditions":{"ready":false}}]}));
        let snapshot = build(&[&c[0], &c[1], &c[2]]);
        assert!(snapshot.routes[0].errors.is_empty());
        assert_eq!(snapshot.routes[0].endpoints.len(), 1);
        assert_eq!(snapshot.routes[0].endpoints[0].address.port(), 25566);
    }
}
