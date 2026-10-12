# Akasha Ingress prototype

Rust/Tokio Minecraft Java ingress with chart-owned `MinecraftRoute` resources, hostname routing, node-local preference, global fallback and optional **PROXY protocol v2**. No Envoy, Wasm or separate routing control plane.

The repository contains `aio-agent`, `aio-dashboard`, `aio-crd` and `aio-probe`, plus a Svelte dashboard. This is a runnable prototype, not a production release.

## Run locally

Requirements: current stable Rust, Node 22+, Linux for the tested SO_REUSEPORT behaviour.

```sh
cargo build --bins
cd web
npm ci
npm run build
cd ..

# Put a Minecraft proxy or TCP test backend on 127.0.0.1:25566.
AIO_API_TOKEN=local-agent-token cargo run --bin aio-agent -- \
  --config examples/local.yaml --instance-id demo \
  --management-listen 127.0.0.1:18080

# In another terminal; agent credentials stay server-side.
AIO_API_TOKEN=local-agent-token AIO_DASHBOARD_TOKEN=local-dashboard-token \
  cargo run --bin aio-dashboard -- --agents examples/agents.yaml
```

Open http://127.0.0.1:8080 and enter the dashboard token. Tokens are supplied through environment variables; the UI keeps its dashboard token in memory. Example strings above are for local testing only. Restrict management reachability and terminate TLS before remote use.

The local YAML is reloaded every two seconds; invalid edits retain the prior valid snapshot. It supplies explicit ready endpoints to exercise routing without Kubernetes. `proxyProtocol: false` sends plain Minecraft bytes; `true` prepends exactly one v2 header. Backend configuration must agree. Hostname matching uses exact routes first, then the longest wildcard suffix. Conflicting claims disable all owning routes.

## Kubernetes

```sh
cargo run --bin aio-crd > deploy/minecraftroutes.yaml
kubectl apply -f deploy/minecraftroutes.yaml
# Create aio-system and the aio-management Secret before starting agents.
# Configure image, node selection and firewall rules in deploy/agent.yaml.
kubectl apply -f deploy/agent.yaml
kubectl apply -f deploy/route.yaml
kubectl apply -f deploy/dashboard.yaml
```

Without `--config`, the agent watches MinecraftRoutes, Services and EndpointSlices through kube-rs. `AIO_NAMESPACE=*` watches all namespaces; a concrete namespace limits discovery. Backend Services must be headless and referenced in the route namespace. Discovery connects to ready, nonterminating endpoints directly and resolves the Service port to its EndpointSlice target port. Node name determines local preference; absent node identity is global. Watch reconnect/relist preserves cached routing during outages and removes stale resources after synchronization.

The agent registration namespace is separate from the route namespace. Management binds to a concrete private node IP with an ephemeral port; the agent publishes its URL/Pod UID in a heartbeat Lease. Both old and new instances can register during surge updates. The dashboard discovers these Leases, or uses `--agents` for a static local inventory. Registrations older than 30 seconds are omitted; discovered-but-unreachable instances appear as errors.

`deploy/agent.yaml` is an example, not an automatically deployed configuration. Its Lease Role grants access to registrations in aio-system (not per-Lease admission enforcement). Restrict this namespace to trusted AIO workloads. Kubernetes RBAC cannot restrict arbitrary new Lease names with resourceNames; stronger per-instance write enforcement would require admission policy.

## Routing and operation

- Bounded handshake parsing handles TCP fragmentation, preserves NUL-delimited extensions and leaves subsequent packets untouched.
- Ready local endpoints are selected randomly before global ones. Connection failures try distinct alternatives, up to three attempts within five seconds. No retries after forwarding starts.
- Per-instance circuits open after three consecutive TCP connect failures; cooldown starts at ten seconds and doubles up to sixty seconds. Half-open admits one trial. Kubernetes readiness remains authoritative.
- Exclusions are instance-local, expire by TTL and survive automatic circuit resets. They do not close established streams.
- SO_REUSEPORT allows overlapping listeners using the same UID. Pause and SIGTERM close the listening socket while accepted streams continue. SIGTERM drains up to thirty minutes by default. Pausing alone does not exit the process.
- Existing streams are not migrated. Agent, node or Babylon crashes disconnect their sessions.
- The dashboard supports instance status, routes, circuits, events, side-effect-free hostname resolution, pause/resume, exclusions and reset. It affects no traffic when unavailable.

All management data/API operations require a bearer token; only health endpoints are unauthenticated. The dashboard has a separate token and forwards agent credentials server-side. Mutation bodies require the current instance ID and an operator reason. This prototype has a single operator permission level.

## Prototype limits / next work

- Keycloak OIDC, separate view/operate roles, actor-attributed durable audit and production TLS integration are not implemented. Current events are bounded in-memory operation records.
- Kubernetes malformed route updates are rejected rather than preserving historical valid resource specs; local-file mode does preserve last-valid configuration. Cold starts always use current resources.
- Dashboard polling has bounded concurrency/timeouts but no persistent stale-history inventory, pagination or configurable backoff yet.
- Resource limits bound total connections and per-handshake buffers; there is no separate global byte-budget allocator. Relay buffers use Tokio's bounded copy buffers.
- No live-cluster, public-IP preservation, Docker build or real Minecraft-client validation has been performed. Verify management firewall isolation and host-network surge scheduling on the target cluster.
- Lease heartbeats use annotations for instance discovery; Lease spec is not used for leader election. Instances independently calculate routes and circuit state.
- Pending connections during listener removal may fail. Tested established-stream continuity is not a guarantee of lossless handover for every SYN/backlog race.

## Checks

```sh
cargo fmt --all -- --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cd web
npm run check
npm run build
cd ..
cargo build --locked --bins
python3 scripts/smoke.py
```

Tests cover route precedence/conflicts, fragmented handshakes, framing, EndpointSlice port mapping, circuit transitions, local-to-global fallback, PROXY v2 on/off, management authentication and same-node listener overlap with established-stream continuity.

Build both binaries and UI into the provided container with `docker build -t aio:prototype .`; override its command to `/usr/local/bin/aio-dashboard` for the dashboard deployment.
