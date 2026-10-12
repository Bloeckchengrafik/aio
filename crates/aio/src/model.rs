use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(CustomResource, Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[kube(
    group = "akasha.asorda.net",
    version = "v1alpha1",
    kind = "MinecraftRoute",
    namespaced
)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RouteSpec {
    pub hostnames: Vec<String>,
    pub backend: Backend,
    pub proxy_protocol: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Backend {
    pub service: String,
    pub port: u16,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Endpoint {
    pub id: String,
    pub address: SocketAddr,
    pub node_name: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Route {
    pub id: String,
    pub hostnames: Vec<String>,
    pub proxy_protocol: bool,
    #[serde(default)]
    pub endpoints: Vec<Endpoint>,
    #[serde(default)]
    pub errors: Vec<String>,
}

#[derive(Clone, Default, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub routes: Vec<Route>,
}

pub fn normalize_host(input: &str) -> anyhow::Result<String> {
    let host = input
        .strip_suffix('.')
        .unwrap_or(input)
        .to_ascii_lowercase();
    anyhow::ensure!(
        !host.is_empty() && host.len() <= 253 && host.is_ascii(),
        "invalid hostname"
    );
    for label in host.split('.') {
        anyhow::ensure!(
            !label.is_empty() && label.len() <= 63,
            "invalid hostname label"
        );
        anyhow::ensure!(
            !label.starts_with('-') && !label.ends_with('-'),
            "invalid hostname label"
        );
        anyhow::ensure!(
            label
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
            "invalid hostname character"
        );
    }
    Ok(host)
}

pub fn normalize_pattern(input: &str) -> anyhow::Result<String> {
    match input.strip_prefix("*.") {
        Some(suffix) => Ok(format!("*.{}", normalize_host(suffix)?)),
        None => normalize_host(input),
    }
}

impl Snapshot {
    pub fn validate(mut self) -> Self {
        let mut claims = std::collections::HashMap::<String, Vec<usize>>::new();
        for (i, route) in self.routes.iter_mut().enumerate() {
            if route.hostnames.is_empty() {
                route.errors.push("no hostnames".into());
            }
            let mut seen = std::collections::HashSet::new();
            for host in &mut route.hostnames {
                match normalize_pattern(host) {
                    Ok(normalized) => {
                        *host = normalized;
                        if !seen.insert(host.clone()) {
                            route.errors.push(format!("duplicate hostname {host}"));
                        }
                        claims.entry(host.clone()).or_default().push(i);
                    }
                    Err(error) => route.errors.push(error.to_string()),
                }
            }
        }
        for (host, owners) in claims {
            let owners: std::collections::HashSet<_> = owners.into_iter().collect();
            if owners.len() > 1 {
                for i in owners {
                    self.routes[i]
                        .errors
                        .push(format!("conflicting hostname {host}"));
                }
            }
        }
        self
    }

    pub fn resolve(&self, host: &str) -> Option<&Route> {
        self.routes
            .iter()
            .filter(|r| r.errors.is_empty())
            .flat_map(|route| {
                route.hostnames.iter().filter_map(move |pattern| {
                    if pattern == host {
                        Some((usize::MAX, route))
                    } else if let Some(suffix) = pattern.strip_prefix("*.") {
                        (host.len() > suffix.len() + 1 && host.ends_with(&format!(".{suffix}")))
                            .then_some((suffix.len(), route))
                    } else {
                        None
                    }
                })
            })
            .max_by_key(|(specificity, _)| *specificity)
            .map(|(_, route)| route)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn route(id: &str, hostname: &str) -> Route {
        Route {
            id: id.into(),
            hostnames: vec![hostname.into()],
            proxy_protocol: false,
            endpoints: vec![],
            errors: vec![],
        }
    }
    #[test]
    fn exact_wins_and_wildcard_does_not_match_apex() {
        let snapshot = Snapshot {
            routes: vec![
                route("wild", "*.asorda.net"),
                route("exact", "PLAY.Asorda.Net."),
            ],
        }
        .validate();
        assert_eq!(snapshot.resolve("play.asorda.net").unwrap().id, "exact");
        assert_eq!(snapshot.resolve("dev.asorda.net").unwrap().id, "wild");
        assert!(snapshot.resolve("asorda.net").is_none());
    }
    #[test]
    fn conflicts_disable_all_owners() {
        let snapshot = Snapshot {
            routes: vec![route("a", "play.asorda.net"), route("b", "PLAY.asorda.net")],
        }
        .validate();
        assert!(snapshot.resolve("play.asorda.net").is_none());
        assert!(snapshot.routes.iter().all(|r| !r.errors.is_empty()));
    }
}
