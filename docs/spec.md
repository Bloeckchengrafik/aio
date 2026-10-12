# Akasha Ingress Operator — minimal specification

Status: implementation specification for everbuild-org/.github-private#429. Date: 2026-10-12.

## Purpose

AIO accepts Minecraft Java TCP connections on public Kubernetes nodes, routes by the hostname in the initial handshake, prefers a proxy on the ingress node, and falls back to proxies elsewhere in the cluster. It forwards the client's address using PROXY protocol.

Multiple deployments of the same Asorda chart independently own their ingress declarations. A shared AIO installation serves all accepted routes.

## Scope and architecture

AIO is one Rust application deployed as a DaemonSet on explicitly selected public nodes. Each instance binds directly to TCP port 25565 using host networking. Each instance watches Kubernetes resources, calculates its own routing snapshot, parses handshakes, and forwards streams. There is no separate control-plane deployment, Envoy, Wasm filter, or xDS server.

AIO does not manage Babylon replicas, transfer Minecraft sessions, persist player state, or provide zone/region tiers. Babylon failure disconnects sessions attached to that process. Failover applies to new connections and reconnects.

The public networking layer must preserve the source address and deliver traffic to AIO. DNS or other public-node failover is outside this component's scope. AIO does not make a deployment with one proxy redundant.

## Configuration ownership

The shared installation owns the CRD, DaemonSet, ServiceAccount, RBAC and centralized dashboard Deployment/Service. Each application chart release owns its MinecraftRoute and backend headless Service. Removing a release removes its route without modifying another release's resources.

```yaml
apiVersion: akasha.asorda.net/v1alpha1
kind: MinecraftRoute
metadata:
  name: asorda-production
  namespace: asorda
spec:
  hostnames:
    - asorda.net
    - play.asorda.net
  backend:
    service: proxy
    port: 25565
  proxyProtocol: true
```

The chart templates the route name, hostnames and Service name from its own release configuration. Backend references are always in the route's namespace. Cross-namespace references are not supported in v1alpha1.

| Field | Contract |
| --- | --- |
| hostnames | Nonempty list of exact DNS hostnames or leading `*.` suffix patterns |
| backend.service | Name of a headless Service in the route namespace |
| backend.port | Numeric TCP Service port, 1–65535 |
| proxyProtocol | Boolean; required. `true` prepends PROXY protocol v2; `false` forwards the Minecraft stream without a PROXY header |

PROXY protocol v1 is not supported. The flag is per route; backend configuration must agree with it.

Local-first/global-fallback is fixed behaviour and has no configuration field. Hostnames have no numeric priorities. No implicit catch-all route exists.

Process configuration selects listen address, node name, watched namespaces, and operational limits. Node name comes from `spec.nodeName`. Namespace watch scope is set by the shared installation, not by individual routes. Defaults: listen `[::]:25565`, handshake timeout 5 seconds, upstream connect timeout 2 seconds, total selection/connect deadline 5 seconds, maximum handshake frame 4096 bytes. Limits may be adjusted by the shared installation.

## Hostname matching and conflicts

Normalize configured and incoming hostnames to ASCII lowercase and remove one trailing dot. Reject empty names, invalid configured patterns and duplicate normalized entries within one route. Internationalized configuration uses ASCII punycode. A wildcard matches subdomains of its suffix, not the suffix itself. Exact matches take precedence over wildcard matches; among wildcards, the longest suffix wins.

Identical normalized claims by different routes are conflicts regardless of namespace. All routes containing such claims are excluded from new-connection routing until the conflict is resolved; do not choose by watch order. Exact and wildcard claims are allowed to overlap because their precedence is deterministic. Hostnames are routing hints, never proof of identity or an authorization boundary.

Each instance validates a resource before activating it. An invalid spec update retains that resource's previous valid configuration locally, with an error reported. A newly observed invalid route is inactive. Route deletion removes its active configuration. Conflicts are evaluated against the complete active candidate set and override last-valid retention. Cold-start behaviour never depends on an unavailable historical configuration.

## Discovery

Watch MinecraftRoutes, Services and EndpointSlices in the configured namespaces. Use the Service association label to resolve EndpointSlices. Headless DNS is not used for backend selection because AIO needs endpoint readiness and node identity.

Resolve `backend.port` against the Service's TCP port and then the corresponding EndpointSlice port. Connect directly to endpoint addresses and resolved endpoint ports, including when targetPort differs from the Service port. A missing Service, non-headless Service, ambiguous port mapping or missing endpoint port makes that route unavailable.

Only endpoints with explicit `ready: true` and without `terminating: true` are eligible. Deduplicate address/port pairs. An endpoint is local only when its nodeName equals this instance's node name; missing nodeName means nonlocal. Support reachable IPv4 and IPv6 endpoint addresses.

Build immutable snapshots and publish them atomically. Established connections retain their selected upstream. Watch interruption retains the last snapshot; reconnect and relist with backoff. A successful relist must remove resources that no longer exist. Cached readiness can be stale during disconnection, so connection failures must still be handled.

## Connection handling

1. Accept a TCP connection and record its observed source and destination addresses.
2. Buffer and parse one complete Minecraft handshake frame, enforcing size and time limits. Handle arbitrary TCP fragmentation and coalescing. Decode bounded VarInts, packet ID, protocol version, hostname, port and next-state. Accept status and login handshakes; reject malformed frames and unsupported next-state values.
3. Extract the hostname. For NUL-delimited handshake extensions, match only the first hostname segment; preserve the entire original frame. Do not rewrite handshake content. Legacy pre-handshake ping is outside v1alpha1 scope.
4. Match one accepted route. Unknown or unavailable routes close the connection without connecting to a backend.
5. Select an eligible local endpoint uniformly, excluding open circuits and operator exclusions. If no usable local endpoints exist, select among usable global endpoints uniformly.
6. Establish the upstream connection. On connection failure before application bytes are sent, try another distinct local endpoint, then global endpoints. Attempt at most three distinct endpoints within the total deadline. When global endpoints exist, reserve at least one attempt and a bounded part of the deadline for them. No retries after forwarding begins.
7. If proxyProtocol is true, send exactly one PROXY protocol v2 header containing the observed client address and ingress destination. If false, send no PROXY header. Then forward all buffered client bytes in their original order.
8. Forward bidirectionally with bounded buffering, backpressure and TCP half-close propagation until completion or failure.

AIO does not parse Minecraft play traffic. It does not accept an inbound PROXY header from public clients. Failure while writing the upstream PROXY header closes the connection rather than attempting session recovery.

## Lifecycle and failure behaviour

An AIO instance becomes ready after initial resource synchronization and successful listener binding. Zero routes or an unavailable route does not make the whole instance unready. Liveness tests process health without requiring Kubernetes API availability.

### Updates and listener draining

Enable Linux SO_REUSEPORT before binding on every instance, using the same effective UID. Normally one instance runs per node; old and new versions may overlap during updates. Complete initial discovery synchronization before binding the public listener. The replacement becomes ready only after its public and management listeners are usable.

On termination, readiness becomes false and AIO closes its listening socket, removing itself from new-connection selection. Merely stopping accept() is insufficient. Accepted sockets remain open and continue forwarding. Existing streams drain until completion or a configurable deadline. Default drain deadline: 30 minutes; pod termination grace must exceed it. The management API remains available during draining. Long-lived sessions can still be disconnected at the deadline. Forced termination and process crashes disconnect that instance's streams.

Use DaemonSet RollingUpdate with maxSurge: 1, maxUnavailable: 0 and minReadySeconds: 5. The new version must be ready before the old version is terminated. Verify scheduling on the target Kubernetes version: host-network port reservations must not prevent overlap. Do not declare fixed hostPort/public containerPort reservations that prevent same-node scheduling. Management endpoints must also permit overlap as specified below. Closing a listener can affect pending, unaccepted connections; handover is not claimed to be completely lossless.

Do not deactivate automatically solely because a Kubernetes watch is disconnected. Explicit admission deactivation is available for operation/debugging; automatic self-deactivation beyond startup and shutdown is deferred.

Endpoint removal or route changes affect new connections only. No new upstream selection is attempted for an established stream. Transient accept failures retry with bounded backoff without terminating established streams; failed resume remains paused and reports the bind failure. Background services are supervised and restarted independently of forwarding. TCP keepalive and a configurable inactivity deadline (default five minutes, zero disables it) bound dead/idle connection lifetimes. Startup verifies or raises the soft descriptor limit within the hard limit for two sockets per connection plus 128 descriptors of headroom. New connections can fail during stale discovery or before a replacement proxy becomes ready; no zero-downtime guarantee is claimed.

## Security and deployment

Run as a non-root user. Public port 25565 requires no privileged-port capability. Use `hostNetwork: true`, `dnsPolicy: ClusterFirstWithHostNet`, and an explicit node selector for public ingress nodes. AIO instances share this port using SO_REUSEPORT during updates; unrelated software must not bind it.

RBAC is read-only get/list/watch for MinecraftRoutes, Services and EndpointSlices in allowed namespaces. Node API access is unnecessary. The routing process needs no write access to application workloads. Management authentication credentials may be supplied separately.

When proxyProtocol is enabled, Babylon must accept PROXY protocol v2 and permit that framing only from trusted ingress paths. When disabled, the backend must accept a plain Minecraft stream and sees AIO as the TCP peer. The deployment's firewall/network controls establish that trust. Correct source-address preservation must be verified end to end.

Enforce configurable concurrent-connection and aggregate handshake-buffer limits in addition to per-connection limits. Reject excess admission predictably. Do not log raw handshake payloads or create metrics labels from arbitrary client-provided hostnames.

## Circuit breaking

Circuit state is local to each AIO instance and keyed by backend Pod UID (EndpointSlice targetRef when available), address and port. A replacement Pod must not inherit the failed predecessor's circuit merely because an IP is reused. Remove state when its endpoint disappears. Circuit state is transient and is not synchronized through the dashboard.

Defaults, configurable at installation level:

- Closed: endpoints are selectable. Three consecutive upstream connect failures/timeouts open the circuit; successful connects reset the counter.
- Open: exclude the endpoint for 10 seconds. Other local endpoints are preferred, then global endpoints. Fail closed when none are usable; do not bypass every open circuit in a panic mode.
- Half-open: after the cooldown, allow one connection attempt at a time. Success closes the circuit; failure reopens it with exponential cooldown up to 60 seconds. Other connections use alternate endpoints while the trial is pending.

Only upstream connection establishment feeds the automatic breaker initially. Client disconnects, idle sessions, ordinary stream closure and application-level Minecraft rejection do not. Successful TCP connection is not proof of application health; Kubernetes readiness remains a separate prerequisite. Selection of half-open probes must be concurrency-safe. No synthetic Minecraft health probes are required for the first version.

Operators may exclude an endpoint from new selections or reset its automatic circuit. A reset does not override Kubernetes readiness. Exclusions do not kill existing streams. Overrides are instance-local, use a required reason and bounded TTL (default 15 minutes, maximum 24 hours), and disappear on restart or endpoint removal. Central dashboard actions explicitly select target instances; partial success must be visible. They never mutate chart-owned routes or Kubernetes endpoint readiness.

## HTTP management API

Every AIO instance exposes a small versioned JSON API, health endpoints and Prometheus metrics. Bind a dynamically allocated management port on the node's private/internal address to permit old/new host-network instances to coexist. Publish an instance registration through a namespaced Lease containing Pod UID, node name, management address/port, version and heartbeat; refresh every 10 seconds. Treat registrations older than 30 seconds as stale. Address selection is installation-controlled, not taken from browser input. Remove registration on clean exit; stale registration is expected after a crash. RBAC adds narrowly scoped get/create/update/patch/delete access for the instance's registration Lease. No application resource writes are needed.

Kubelet readiness/liveness can use an exec probe against the process's locally published management address, avoiding a fixed host port. Registration remains present while draining. Dashboard discovery is separate from the routing path: inability to publish a registration raises a diagnostic error but does not interrupt an otherwise functional public listener.

| Endpoint | Purpose |
| --- | --- |
| GET /health/live, /health/ready | Process health and admission readiness |
| GET /metrics | Prometheus counters/gauges |
| GET /api/v1/status | Instance identity, accepting/draining/paused state, watch health, configuration generation, active connection count |
| GET /api/v1/routes | Effective route definitions, validation/conflict errors, resource generation and local/global usable counts |
| GET /api/v1/endpoints | Backend identity, locality, readiness, circuit state, cooldown, failure counters and operator exclusion |
| POST /api/v1/debug/resolve | Dry-run hostname matching and candidate tiers; does not open connections or alter circuit state |
| GET /api/v1/events | Bounded recent validation, circuit and operator events; no raw handshake payloads |
| POST /api/v1/admission | Pause/resume new admission; pause closes the listener and preserves accepted streams |
| POST /api/v1/endpoints/exclude | Add/remove a TTL-based endpoint exclusion with reason |
| POST /api/v1/endpoints/reset | Reset an automatic endpoint circuit with reason |

Mutations include the expected instance Pod UID to prevent commands intended for a previous process affecting its replacement. Resume is rejected while terminating or before synchronization, and attempts to rebind must succeed before readiness becomes true. An operator pause does not itself fail liveness or request pod replacement; readiness remains false until resumed. Keep endpoints paginated and event history bounded. Connection-level payload inspection and forced session termination are outside the initial scope.

Management access is private and authenticated. The shared installation provides a dashboard-to-agent credential; health endpoints are restricted by network reachability and may be unauthenticated. Enforce authenticated reads and mutations, request-size limits and request timeouts. Audit mutations with actor identity, target instance, endpoint, reason and result. Credentials and client IPs must not appear in normal dashboard/event output.

## Centralized dashboard

Deploy a small centralized web application, with a Svelte frontend and a thin server-side aggregation/API layer. It watches AIO instance registrations and queries agents directly; browsers never receive agent credentials or contact agents directly. The dashboard is an operational interface, not a routing control plane. Its failure has no effect on traffic, discovery, draining or automatic circuit breaking.

Use a Traefik-style overview: instance health and version, active connections, routes, local/global endpoint availability, circuit states, conflicts and watch errors. Show route details per instance because locality and cached configuration may differ. Include hostname resolution debugging, recent bounded events, and explicit pause/resume, endpoint exclusion and circuit reset controls. No route editor: GitOps/Helm remains authoritative.

Authenticate browser users through the existing Traefik traefik-oidc-auth middleware on the entire dashboard hostname. AIO implements no separate OIDC flow or browser bearer-token login. All users admitted by that middleware initially have operator access; role separation can be added at that boundary. Restrict direct dashboard access. JSON POSTs require a same-origin request marker, reject cross-site Fetch Metadata and enforce the configured public Origin. Mutations require an operator reason; optionally use trusted X-Aio-User claim headers for dashboard operation logs. Operator actions preview the selected instances; fan-out returns per-instance results, including timeouts. No implicit all-node pause or transactional cluster-wide guarantee.

Poll at a modest interval (default 5 seconds) with bounded concurrency, per-agent timeouts and backoff for unreachable agents. Retain stale/removed registrations for up to 24 hours in memory and reject mutations to them. Display last-observed timestamps and stale/unreachable/draining states; stale values must not look healthy. Discover both versions during surge updates by Pod UID, not just node name. Agent management ports are protected by host firewall/private-network controls; host-network isolation must not be assumed from ordinary pod NetworkPolicy alone.

## Observability

Record configuration generation, synchronization/watch health, accepted/unavailable/conflicting routes, active connections, local/global selections, connect failures, circuit transitions, handshake rejection categories and bytes forwarded. Use bounded route identifiers and error categories as labels, never arbitrary client hostnames. Report the dynamically registered metrics targets through the deployment's monitoring discovery integration.

Log route validation/conflict errors with namespace/name. Defer MinecraftRoute status writes: independent instances must not compete to write a single aggregate status. The dashboard presents their distinct effective states instead.

## Acceptance criteria

- Two releases of the same chart can independently add, update and remove routes.
- Exact and wildcard matching is deterministic; duplicate claims fail closed across namespaces.
- Local proxies receive new connections while ready; absence or connection failure triggers bounded global fallback.
- Service port/targetPort differences, multiple EndpointSlices and IPv4/IPv6 discovery work.
- With proxyProtocol enabled, PROXY v2 conveys the real client address and precedes the unchanged original handshake; with it disabled, the stream starts directly with the unchanged handshake.
- Fragmented, coalesced, oversized, malformed and stalled handshakes are handled within defined limits.
- Kubernetes API/watch outage preserves usable cached routing; relist correctly removes stale routes/endpoints.
- Backend deletion and route changes do not deliberately interrupt established streams.
- Surge updates overlap same-node instances; the new version synchronizes before accepting, the old listener closes while established streams continue, and new-connection behaviour during handover is tested.
- Management ports and instance registrations coexist across versions and are cleaned up or expire correctly.
- Graceful shutdown stops admission and drains within the configured deadline.
- Circuit thresholds, bounded half-open trials, cooldown, local/global fallback and endpoint identity changes behave deterministically under concurrent connections.
- Debug resolution is side-effect free; exclusions and resets respect readiness, TTL and expected instance identity.
- The configured Traefik middleware protects dashboard web/API requests; caller-supplied identity headers are stripped before claim injection, and same-origin JSON POST defenses are enforced. Actions expose errors and generate operation logs.
- Dashboard/agent-management outages do not interrupt routing; unreachable or stale instances are clearly shown.
- Babylon/AIO crashes have the documented disconnect behaviour; new connections can use surviving instances.

Implementation starts with the CRD and discovery snapshot, then handshake/PROXY forwarding, local/global selection, surge/drain lifecycle, circuit breaking and the management API. Add the centralized dashboard after these contracts work. No separate control plane or additional locality hierarchy is required for this scope.
