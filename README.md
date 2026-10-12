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
AIO_API_TOKEN=local-agent-token \
  cargo run --bin aio-dashboard -- --agents examples/agents.yaml
```

Open http://127.0.0.1:8080. The dashboard has no built-in login: production authentication is provided by your existing Traefik `traefik-oidc-auth` middleware. Only the internal dashboard-to-agent bearer credential is retained; it never reaches the browser. Local defaults bind to loopback. Open `/?demo` for an explicitly labeled, read-only visual preview using illustrative data.

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

The agent registration namespace is separate from the route namespace. Management binds to a concrete private node IP with an ephemeral port; the agent publishes its URL/Pod UID in a heartbeat Lease. Both old and new instances can register during surge updates. The dashboard discovers these Leases, or uses `--agents` for a static local inventory. Registrations older than 30 seconds, or removed from discovery, remain visibly stale for up to 24 hours. Stale registrations cannot receive operator commands. Discovery outages also let cached heartbeat timestamps expire instead of displaying a falsely healthy inventory.

`deploy/agent.yaml` is an example, not an automatically deployed configuration. Its Lease Role grants access to registrations in aio-system (not per-Lease admission enforcement). Restrict this namespace to trusted AIO workloads. Kubernetes RBAC cannot restrict arbitrary new Lease names with resourceNames; stronger per-instance write enforcement would require admission policy.

## Routing and operation

- Bounded handshake parsing handles TCP fragmentation, preserves NUL-delimited extensions and leaves subsequent packets untouched.
- Ready local endpoints are selected randomly before global ones. Connection failures try distinct alternatives, up to three attempts within five seconds. When global backends exist, reserve one attempt and part of the deadline for global fallback; multiple failed local backends cannot consume the full budget. Login streams are never retried after forwarding starts.
- Per-instance circuits open after three consecutive TCP connect or status-response failures; cooldown starts at ten seconds and doubles up to sixty seconds. Half-open admits one trial. Kubernetes readiness remains authoritative.
- Exclusions are instance-local, expire by TTL and survive automatic circuit resets. They do not close established streams.
- SO_REUSEPORT allows overlapping listeners using the same UID. Pause and SIGTERM close the listening socket while accepted streams continue. SIGTERM drains up to thirty minutes by default. Pausing alone does not exit the process.
- Transient accept failures retry with bounded backoff and preserve existing streams. Failed listener resume remains paused, reports the bind error and can be retried; it does not terminate the agent.
- Discovery, management and registration tasks are supervised and restarted with backoff. Failed watch workers restart the discovery group, while cached routing and established streams remain usable. Service failures are exposed through status; discovery/management restart failures affect readiness, and registration failure is diagnostic only.
- TCP keepalive is enabled (60-second idle, 20-second interval, three probes). A five-minute bidirectional inactivity deadline frees idle sessions; use `--idle-timeout-seconds 0` to disable it. Activity in either direction refreshes the deadline.
- Startup checks the file-descriptor budget: two sockets per connection plus 128 descriptors of headroom. It raises the soft limit within the hard limit, or fails with a configuration error rather than advertising an unsupported connection limit.
- Byte metrics count successful writes as they occur, including the buffered handshake and optional PROXY header, and retain partial progress on errors.
- Existing streams are not migrated. Agent, node or Babylon crashes disconnect their sessions.
- The dashboard supports instance status, routes, circuits, events, side-effect-free hostname resolution, pause/resume, exclusions and reset. It affects no traffic when unavailable.

Agent management data/API operations require a bearer token; only agent health endpoints are unauthenticated. Dashboard web/API requests rely on Traefik authentication and are not independently authenticated by AIO. Mutation bodies require the current instance ID . This prototype has a single operator permission level.

## Prototype limits / next work

- All users admitted by the configured Traefik middleware have operator access. Separate view/operate roles and durable audit storage are not implemented; optional trusted identity headers attribute dashboard operation logs. TLS/OIDC remain ingress responsibilities.
- Invalid Kubernetes route specs retain their prior valid spec only for the same resource UID. Errors remain visible through status. Deletion or recreation clears history; cold starts use current resources. Missing backends and conflicting hostnames remain unavailable rather than overriding readiness or conflict checks.
- Dashboard stale-history inventory is transient and is lost on dashboard restart. Pagination and configurable poll backoff are still pending.
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
npx playwright install chromium
npm run test:ui
cd ..
cargo build --locked --bins
python3 scripts/smoke.py
python3 scripts/fd_exhaustion.py
```

Tests cover route precedence/conflicts, fragmented handshakes, framing, EndpointSlice port mapping, last-valid spec retention, circuit transitions, reserved global fallback, PROXY v2 on/off, management authentication, worker panic recovery, stale registration handling, failed resume, live byte accounting, idle expiry and same-node listener overlap. The Linux subprocess regression deliberately exhausts descriptors and verifies established-stream continuity and management recovery.

Build both binaries and UI into the provided container with `docker build -t aio:prototype .`; override its command to `/usr/local/bin/aio-dashboard` for the dashboard deployment.

## Traefik authentication boundary

Use `deploy/dashboard-ingress.example.yaml` as an edited example, referencing your existing `traefik-oidc-auth` Middleware. Protect the complete dashboard hostname, including `/api/*` and the OIDC callback route; do not create an unauthenticated API route. Cross-namespace Middleware references require Traefik's corresponding setting, or place the Middleware in aio-system instead. Match `AIO_PUBLIC_ORIGIN` in the Deployment to the public origin (scheme + host + optional port, no trailing slash).

Keep the dashboard Service private. The included NetworkPolicy example permits only the configured Traefik namespace/pod labels; adjust them and verify enforcement for your CNI and host-network setup. Direct backend access bypasses OIDC. No authentication is inferred merely from the presence of a header.

POSTs require JSON and the `X-AIO-Request: 1` marker. Cross-site Fetch Metadata is rejected; when `AIO_PUBLIC_ORIGIN` is configured, an exact matching Origin is also required. The frontend sends same-origin session cookies and never uses CORS. Machine clients must supply the same marker/content type and configured Origin. Expired sessions are shown with a reauthentication link rather than a second login form.

For audit attribution, configure this additional header in the existing plugin's `Headers` list, after the example header-stripping middleware removes caller-supplied identity:

```yaml
Headers:
  - Name: X-Aio-User
    Value: '{{ .claims.sub }}'
    IncludeWhen: Authorized
```

Use the plugin's authenticated-only header inclusion behaviour; protect direct access and do not bypass authentication for AIO. The dashboard records actor, instance, action, target and response status in operation logs; without the header it records `unattributed`. These logs do not grant permissions or forward agent credentials to users. See the plugin's [header configuration](https://traefik-oidc-auth.sevensolutions.cc/docs/getting-started/middleware-configuration#header-block).

The dashboard groups rollout instances by node, exposes configuration/watch errors, and displays route admission, real session counts, and usable local/global backends. Hostname debugging returns server-derived matching/candidate results without opening a backend connection; the raw response remains expandable. Routine actions need no reason; endpoint exclusions default to 15 minutes. Kill alone requires confirmation, showing known route session counts and explicit instance scope.

### Route maintenance and fallback MOTDs

`MinecraftRoute.spec.allowConnections: false` persists maintenance across agent restarts. The route page provides runtime Stop, Allow and Kill controls for a selected instance or all current instances (including draining); each target has an independent result. Stop preserves existing players. Kill blocks new logins and disconnects route sessions. Runtime controls are transient and Allow cannot override declarative maintenance.

Configure `fallbackStatus` on the route (see `deploy/route.yaml`): maintenance and unavailable MOTDs, version labels, optional base64-encoded 64×64 PNG favicon, and numeric protocol (default -1). AIO forwards fresh healthy status responses; backend loss or status errors use the unavailable response. Fallback player counts are zero. Ping latency always measures AIO. No response cache is used.

### Circuit metrics and Discord notifications

Authenticated agent `/metrics` now exposes per-route/backend gauges, including healthy endpoints that have never failed. Labels are `route` (namespace/name) and `endpoint` (discovered backend ID); scrape-target labels identify the agent/node. Removed endpoints disappear from the next scrape.

| Metric | Meaning |
| --- | --- |
| `aio_backend_circuit_state{state="closed\|open\|half_open"}` | Three one-hot state series; half-open includes waiting for a trial |
| `aio_backend_circuit_cooldown_seconds` | Remaining cooldown, zero when a trial is eligible |
| `aio_backend_circuit_failures` | Consecutive failure count; gauge, not counter |
| `aio_backend_excluded` | 1 while a manual exclusion is active |
| `aio_backend_exclusion_remaining_seconds` | Remaining manual exclusion TTL |

To alert while a circuit remains open or awaiting recovery, retain the same alert identity across cooldown/trial transitions:

```promql
sum without (state) (aio_backend_circuit_state{state=~"open|half_open"}) > 0
```

Configure optional `AIO_DISCORD_WEBHOOK_URL` on each agent with an HTTPS Discord webhook URL. `deploy/agent.yaml` reads the optional `discord-webhook-url` key from the existing `aio-management` Secret. Leave that key absent to disable delivery. The URL stays out of agent APIs and AIO logs; redirects are disabled and mentions are suppressed. No webhook credential belongs in MinecraftRoute or the browser. The worker adds `wait=true` so a successful response acknowledges message creation.

Events are circuit **opened**, circuit **recovered**, and effective route **Stop/Allow/Kill** actions. Messages identify the triggering route/backend, node and instance, with failure/cooldown or session counts. An outage produces one opening notification; repeated failures and failed half-open trials are suppressed until a real successful backend connect/status response generates recovery. Manual Reset enables a new attempt and never claims recovery by itself. A later outage can notify again. Idempotent Stop/Allow, or a repeated Kill with no remaining sessions, produces no extra message. Declarative configuration updates are not operator-action notifications.

Each agent delivers independently, including draining agents: different node perspectives are intentional. There is no centralized deduplication or cross-agent ordering. Delivery is best-effort and transient: a 128-event queue uses nonblocking enqueue, a single background worker, three-second request timeouts, at most three attempts per event, and a 60-second event lifetime. Network failures and 5xx retry with backoff; 429 respects `Retry-After`/`retry_after` and successful exhausted buckets respect `X-RateLimit-Reset-After`. Other non-success responses are not retried. Full/expired queues drop events; shutdown cancels the worker after ingress draining. Retries after an ambiguous network failure may duplicate a message. There is no durable delivery guarantee.

`aio_discord_enabled`, `aio_discord_notification_queue_depth`, and `aio_discord_notifications_total{result="delivered|failed|dropped"}` expose configuration and delivery health without exposing the URL. No Discord availability affects route admission, readiness or liveness. Tests use a local mock endpoint and do not send real Discord messages. See Discord's [rate-limit contract](https://github.com/discord/discord-api-docs/blob/main/developers/topics/rate-limits.mdx).
