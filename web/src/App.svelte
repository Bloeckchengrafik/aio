<script lang="ts">
  import { onMount, tick } from "svelte";
  import Sigil from "./Sigil.svelte";
  import { demoFleet } from "./demo";
  import {
    admission,
    bytes,
    duration,
    nodeName,
    problems,
    usable,
  } from "./view";
  import type { Instance, Endpoint, Resolution, Operation } from "./types";

  let instances = $state<Instance[]>([]);
  let page = $state<"fleet" | "routes" | "activity">("fleet");
  let selected = $state("");
  let routeId = $state("");
  let loading = $state(true);
  let refreshing = $state(false);
  let error = $state("");
  let authExpired = $state(false);
  let notice = $state("");
  let observedAt = $state(0);
  let query = $state("");
  let hostname = $state("asorda.net");
  let resolution = $state<Resolution | null>(null);
  let debugError = $state("");
  let debugging = $state(false);
  let operation = $state<Operation | null>(null);
  let reason = $state("");
  let ttl = $state("900");
  let operating = $state(false);
  let operationError = $state("");
  let dialog: HTMLDialogElement;
  let demo = $state(false);
  let rates = $state<Record<string, number>>({});
  let controller: AbortController | undefined;
  let disposed = false;

  const current = $derived(instances.find((i) => i.instanceId === selected));
  const groups = $derived(
    [...new Set(instances.map(nodeName))]
      .sort()
      .map((name) => ({
        name,
        instances: instances.filter((i) => nodeName(i) === name),
      })),
  );
  const active = $derived(
    instances.reduce((n, i) => n + (i.data?.status.activeConnections ?? 0), 0),
  );
  const accepting = $derived(
    instances.filter((i) => admission(i) === "Accepting").length,
  );
  const issueItems = $derived(
    instances.flatMap((instance) =>
      problems(instance).map((message) => ({ instance, message })),
    ),
  );
  const routeList = $derived(
    (current?.data?.routes.routes ?? []).filter((r) =>
      `${r.id} ${r.hostnames.join(" ")}`
        .toLowerCase()
        .includes(query.toLowerCase()),
    ),
  );
  const route = $derived(
    current?.data?.routes.routes.find((r) => r.id === routeId),
  );
  const endpoints = $derived(
    (current?.data?.endpoints ?? []).filter(
      (e) => !routeId || e.routeId === routeId,
    ),
  );
  const local = $derived(endpoints.filter((e) => e.local));
  const global = $derived(endpoints.filter((e) => !e.local));
  const counters = $derived(current?.data?.status.counters);
  const selections = $derived(
    (counters?.localSelections ?? 0) + (counters?.globalSelections ?? 0),
  );
  const localPercent = $derived(
    selections
      ? Math.round(((counters?.localSelections ?? 0) / selections) * 100)
      : null,
  );
  const eventList = $derived(
    instances
      .flatMap((i) =>
        (i.data?.events ?? []).map((event) => ({ ...event, instance: i })),
      )
      .sort((a, b) => b.timestamp - a.timestamp)
      .slice(0, 60),
  );

  function selectInstance(instance: Instance) {
    selected = instance.instanceId;
    routeId = instance.data?.routes.routes[0]?.id ?? "";
    resolution = null;
    debugError = "";
  }
  async function refresh() {
    if (demo || refreshing || disposed) return;
    refreshing = true;
    controller = new AbortController();
    try {
      const response = await fetch("/api/instances", {
        headers: { Accept: "application/json" },
        credentials: "same-origin",
        signal: controller.signal,
      });
      if (response.status === 401 || response.status === 403) {
        authExpired = true;
        throw new Error("Your ingress session needs authentication.");
      }
      if (
        !response.ok ||
        !response.headers.get("content-type")?.includes("application/json")
      )
        throw new Error(
          "The dashboard did not return fleet data. Reopen this page to authenticate through Traefik.",
        );
      const result: { instances: Instance[]; observedAt: number } =
        await response.json();
      if (disposed) return;
      const nextRates: Record<string, number> = {};
      for (const next of result.instances) {
        const old = instances.find((i) => i.instanceId === next.instanceId);
        const elapsed = next.observedAt - (old?.observedAt ?? next.observedAt);
        const delta =
          (next.data?.status.counters?.bytes ?? 0) -
          (old?.data?.status.counters?.bytes ?? 0);
        if (
          old?.reachable &&
          next.reachable &&
          old.data?.status.counters &&
          next.data?.status.counters &&
          elapsed > 0 &&
          delta >= 0
        )
          nextRates[next.instanceId] = delta / elapsed;
      }
      rates = nextRates;
      instances = result.instances.sort(
        (a, b) =>
          nodeName(a).localeCompare(nodeName(b)) ||
          Number(Boolean(a.data?.status.terminating)) -
            Number(Boolean(b.data?.status.terminating)),
      );
      observedAt = result.observedAt;
      error = "";
      authExpired = false;
      if (!instances.some((i) => i.instanceId === selected)) {
        const first =
          instances.find((i) => i.reachable && !i.data?.status.terminating) ??
          instances[0];
        if (first) selectInstance(first);
      }
      if (
        current?.data &&
        !current.data.routes.routes.some((r) => r.id === routeId)
      )
        routeId = current.data.routes.routes[0]?.id ?? "";
    } catch (e) {
      if (!disposed && !(e instanceof DOMException && e.name === "AbortError"))
        error = String(e);
    } finally {
      refreshing = false;
      loading = false;
    }
  }
  async function apiAction(
    instance: string,
    name: string,
    body: Record<string, unknown>,
  ) {
    const response = await fetch(
      `/api/instances/${encodeURIComponent(instance)}/${name}`,
      {
        method: "POST",
        credentials: "same-origin",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          "X-AIO-Request": "1",
        },
        body: JSON.stringify(body),
      },
    );
    if (response.status === 401 || response.status === 403) authExpired = true;
    const text = await response.text();
    if (!response.ok)
      throw new Error(text || `Request failed: ${response.status}`);
    if (!response.headers.get("content-type")?.includes("application/json"))
      throw new Error("Authentication session expired. Reopen the dashboard.");
    return JSON.parse(text);
  }
  async function resolveHostname() {
    if (!current || debugging) return;
    debugging = true;
    debugError = "";
    resolution = null;
    try {
      if (demo) {
        const normalized = hostname.toLowerCase().replace(/\.$/, "");
        const matched =
          current.data?.routes.routes.find((r) =>
            r.hostnames.includes(normalized),
          ) ??
          current.data?.routes.routes.find((r) =>
            r.hostnames.some(
              (h) => h.startsWith("*.") && normalized.endsWith(h.slice(1)),
            ),
          );
        resolution = {
          hostname: normalized,
          routeId: matched?.id ?? null,
          proxyProtocol: matched?.proxyProtocol,
          candidates: (
            current.data?.endpoints.filter((e) => e.routeId === matched?.id) ??
            []
          ).map((e) => ({
            id: e.endpoint.id,
            local: e.local,
            usable: usable(e),
          })),
        };
      } else
        resolution = await apiAction(current.instanceId, "resolve", {
          hostname,
        });
      if (resolution?.routeId) routeId = resolution.routeId;
    } catch (e) {
      debugError = String(e);
    } finally {
      debugging = false;
    }
  }
  async function requestOperation(
    name: Operation["name"],
    body: Record<string, unknown>,
    title: string,
    endpoint?: string,
  ) {
    if (!current || demo || !current.reachable || current.stale) return;
    operation = { instance: current, name, body, title, endpoint };
    reason = "";
    ttl = "900";
    operationError = "";
    await tick();
    dialog.showModal();
  }
  async function executeOperation() {
    if (!operation || !reason.trim() || operating) return;
    operating = true;
    operationError = "";
    try {
      const body = {
        ...operation.body,
        reason: reason.trim(),
        ...(operation.name === "exclude" && operation.body.ttlSeconds !== 0
          ? { ttlSeconds: Number(ttl) }
          : {}),
      };
      await apiAction(operation.instance.instanceId, operation.name, body);
      notice = `${operation.title} completed on ${nodeName(operation.instance)}.`;
      dialog.close();
      operation = null;
      await refresh();
    } catch (e) {
      operationError = String(e);
    } finally {
      operating = false;
    }
  }
  function endpointLabel(e: Endpoint): string {
    return e.circuit.exclusion
      ? "Excluded"
      : e.circuit.state === "halfOpen"
        ? "Half-open"
        : e.circuit.state === "open"
          ? "Circuit open"
          : "Ready";
  }
  function dotClass(i: Instance): string {
    const state = admission(i);
    return state === "Accepting"
      ? "healthy"
      : state === "Draining"
        ? "draining"
        : state === "Paused"
          ? "paused"
          : "danger";
  }

  onMount(() => {
    demo = new URLSearchParams(window.location.search).has("demo");
    if (demo) {
      instances = demoFleet();
      selectInstance(instances[0]);
      loading = false;
      observedAt = Math.floor(Date.now() / 1000);
    } else void refresh();
    const interval = setInterval(refresh, 5000);
    return () => {
      disposed = true;
      clearInterval(interval);
      controller?.abort();
    };
  });
</script>

<div class="app-shell">
  <aside class="sidebar">
    <a class="brand" href="/" aria-label="Akasha ingress home"
      ><Sigil size={43} />
      <div>AKASHA<span>INGRESS OBSERVATORY</span></div></a
    >
    <div class="sidebar-divider"><span>✧</span></div>
    <span class="eyebrow sidebar-label">EXPLORE</span>
    <nav aria-label="Main navigation">
      <button class:active={page === "fleet"} onclick={() => (page = "fleet")}
        ><span class="nav-icon">◈</span>Fleet overview<span class="nav-count"
          >{groups.length}</span
        ></button
      >
      <button class:active={page === "routes"} onclick={() => (page = "routes")}
        ><span class="nav-icon">⌘</span>Routes & backends</button
      >
      <button
        class:active={page === "activity"}
        onclick={() => (page = "activity")}
        ><span class="nav-icon">≋</span>Activity log</button
      >
    </nav>
    <div class="sidebar-label eyebrow nodes-heading">PUBLIC NODES</div>
    <div class="node-navigation">
      {#each groups as group}<button
          class:chosen={current && nodeName(current) === group.name}
          onclick={() =>
            selectInstance(
              group.instances.find((i) => i.data?.status.accepting) ??
                group.instances[0],
            )}
          ><span
            class="status-dot {group.instances.some(
              (i) => admission(i) === 'Accepting',
            )
              ? 'healthy'
              : 'danger'}"
          ></span><span>{group.name}</span><small
            >{group.instances.length > 1
              ? `${group.instances.length} instances`
              : "1 instance"}</small
          ></button
        >{/each}
    </div>
    <div class="sidebar-bottom">
      <Sigil size={75} />
      <p>Every connection,<br />a path through the stars.</p>
      <span class="version">AKASHA INGRESS · v0.1</span>
    </div>
  </aside>

  <main>
    <header class="topbar">
      <div class="breadcrumb">
        Akasha <span>/</span>
        {page === "fleet"
          ? "Fleet overview"
          : page === "routes"
            ? "Routes & backends"
            : "Activity log"}
      </div>
      <div class="topbar-right">
        {#if demo}<span class="badge amber">Read-only demo</span><a
            class="text-link"
            href="/">Live fleet ↗</a
          >{:else}<span class="live-indicator"
            ><span class="status-dot {error ? 'danger' : 'healthy'}"
            ></span>{loading
              ? "Connecting"
              : error
                ? "Updates interrupted"
                : "Live · every 5s"}</span
          ><button
            class="icon-button"
            aria-label="Refresh fleet"
            title="Refresh fleet"
            disabled={refreshing}
            onclick={refresh}>↻</button
          >{/if}
      </div>
    </header>
    {#if demo}<div class="demo-banner">
        Preview constellation · illustrative data, operator actions disabled.
      </div>{/if}
    {#if error}<div class="alert">
        <strong>Fleet updates interrupted.</strong>
        {error} Displayed values may be stale. {#if authExpired}<a href="/"
            >Reauthenticate through Traefik ↗</a
          >{/if}
      </div>{/if}
    {#if notice}<div class="notice" role="status">
        {notice}<button
          class="icon-button"
          aria-label="Dismiss operation result"
          onclick={() => (notice = "")}>×</button
        >
      </div>{/if}

    <section class="page-heading">
      <div>
        <div class="eyebrow gold">THE AKASHA NETWORK</div>
        <h1>
          {page === "fleet"
            ? "A constellation of connections."
            : page === "routes"
              ? "Find the path."
              : "Signals from the fleet."}
        </h1>
        <p>
          {page === "fleet"
            ? "Local by preference. Global when it matters."
            : page === "routes"
              ? "Inspect a route, follow its backends, and resolve a hostname."
              : "Configuration changes, circuit transitions, and operator actions."}
        </p>
      </div>
      <div class="heading-sigil"><Sigil size={98} /></div>
    </section>

    {#if loading}<div class="empty-state">
        <Sigil size={56} />
        <h2>Reading the constellation…</h2>
        <p>Discovering ingress instances and their routing snapshots.</p>
      </div>
    {:else if !instances.length}<div class="empty-state">
        <Sigil size={64} />
        <h2>No ingress instances discovered</h2>
        <p>
          Agents appear here after registering with the dashboard. Check the
          registration namespace and agent connectivity.
        </p>
        <button class="secondary" onclick={refresh}>Refresh fleet</button>
      </div>
    {:else}
      {#if page === "fleet"}
        <div class="fleet-stats">
          <div>
            <span class="eyebrow">PUBLIC NODES</span><strong
              >{groups.length.toString().padStart(2, "0")}<small
                >in the constellation</small
              ></strong
            >
          </div>
          <div>
            <span class="eyebrow">ACTIVE STREAMS</span><strong
              >{active.toLocaleString()}<small
                >including draining instances</small
              ></strong
            >
          </div>
          <div>
            <span class="eyebrow">ACCEPTING INSTANCES</span><strong
              >{accepting}<small>ready for new connections</small></strong
            >
          </div>
          <div>
            <span class="eyebrow">NEEDS ATTENTION</span><strong
              class:warning={issueItems.length > 0}
              >{issueItems.length.toString().padStart(2, "0")}<small
                >{issueItems.length
                  ? "inspect the signals below"
                  : "the fleet is quiet"}</small
              ></strong
            >
          </div>
        </div>
        {#if issueItems.length}<div class="attention">
            <span class="attention-icon">!</span>
            <div>
              <strong>{issueItems.length} operational signals</strong>
              <p>
                {issueItems
                  .slice(0, 2)
                  .map((i) => `${nodeName(i.instance)}: ${i.message}`)
                  .join(" · ")}
              </p>
            </div>
            <button
              class="text-link"
              onclick={() => {
                selectInstance(issueItems[0].instance);
                page = "routes";
              }}>Inspect →</button
            >
          </div>{/if}
        <div class="section-heading">
          <h2>Fleet constellation <span class="heading-star">✧</span></h2>
          <span class="muted">Select a node to inspect its paths</span>
        </div>
        <div class="node-grid">
          {#each groups as group, index}
            <article
              class="node-card"
              class:selected={current && nodeName(current) === group.name}
            >
              <div class="node-card-top">
                <span class="node-index"
                  >{String(index + 1).padStart(2, "0")} / PUBLIC INGRESS</span
                ><span class="small-star">✦</span>
              </div>
              <h3>
                <button
                  onclick={() =>
                    selectInstance(
                      group.instances.find((i) => i.data?.status.accepting) ??
                        group.instances[0],
                    )}>{group.name}<span>↗</span></button
                >
              </h3>
              <div class="constellation" aria-hidden="true">
                <svg viewBox="0 0 300 68"
                  ><path
                    d="M20 38 103 20 174 43 280 16M103 20 220 59M174 43 220 59"
                  /><circle cx="20" cy="38" r="3" /><circle
                    cx="103"
                    cy="20"
                    r="5"
                  /><circle cx="174" cy="43" r="3" /><circle
                    cx="220"
                    cy="59"
                    r="2"
                  /><circle cx="280" cy="16" r="3" /></svg
                >
              </div>
              {#each group.instances as instance}<button
                  class="instance-row"
                  class:instance-selected={selected === instance.instanceId}
                  onclick={() => selectInstance(instance)}
                  ><span class="status-dot {dotClass(instance)}"></span>
                  <div>
                    <strong>{admission(instance)}</strong><small
                      >{instance.data?.status.version
                        ? `v${instance.data.status.version}`
                        : instance.instanceId.slice(0, 12)} · {instance.instanceId.slice(
                        -8,
                      )}</small
                    >
                  </div>
                  <div class="instance-streams">
                    {instance.data?.status.activeConnections?.toLocaleString() ??
                      "—"}<small>streams</small>
                  </div></button
                >{/each}
              <div class="node-footer">
                <span
                  >{group.instances.filter((i) => i.reachable).length}/{group
                    .instances.length} reachable</span
                ><span
                  >{group.instances.some((i) => i.data?.status.terminating)
                    ? "Update in progress"
                    : "Node-local routing"}</span
                >
              </div>
            </article>
          {/each}
        </div>
      {/if}

      {#if page !== "activity" && current}
        <div class="section-heading inspector-heading">
          <h2>
            {page === "fleet" ? "Selected instance" : "Route inspector"}
            <span class="heading-star">✧</span>
          </h2>
          <div class="instance-selector">
            <label for="instance-select">Observe</label><select
              id="instance-select"
              value={selected}
              onchange={(e) => {
                const next = instances.find(
                  (i) => i.instanceId === e.currentTarget.value,
                );
                if (next) selectInstance(next);
              }}
              >{#each instances as instance}<option value={instance.instanceId}
                  >{nodeName(instance)} · {admission(instance)} · {instance.instanceId.slice(
                    -8,
                  )}</option
                >{/each}</select
            >
          </div>
        </div>
        {#if !current.reachable || !current.data}<div class="alert">
            <strong>{admission(current)} · {nodeName(current)}</strong>
            <p>{current.error ?? "No current response from this instance."}</p>
            {#if current.lastHeartbeat}<small
                >Last heartbeat {new Date(
                  current.lastHeartbeat * 1000,
                ).toLocaleString()}</small
              >{/if}
          </div>
        {:else}
          <div class="instance-summary">
            <div>
              <span class="status-dot {dotClass(current)}"></span><strong
                >{nodeName(current)}</strong
              ><span
                class="badge {current.data.status.terminating
                  ? 'blue'
                  : current.data.status.accepting
                    ? 'teal'
                    : 'amber'}">{admission(current)}</span
              ><code>{current.instanceId}</code>
            </div>
            <div>
              <span class="muted"
                >v{current.data.status.version} · {duration(
                  current.data.status.uptimeSeconds ?? 0,
                )} uptime</span
              ><button
                class="secondary small"
                disabled={demo || current.data.status.terminating}
                onclick={() =>
                  requestOperation(
                    "admission",
                    { accepting: !current!.data!.status.accepting },
                    current!.data!.status.accepting
                      ? "Pause new connections"
                      : "Resume admission",
                  )}
                >{current.data.status.accepting
                  ? "Pause admission"
                  : "Resume admission"}</button
              >
            </div>
          </div>
          {#if problems(current).length}<details class="diagnostics">
              <summary
                >{problems(current).length} signals on this instance</summary
              >{#each problems(current) as problem}<p>{problem}</p>{/each}
            </details>{/if}
          <div class="inspector-layout">
            <section class="route-list panel">
              <div class="panel-title">
                <h3>Routes</h3>
                <span class="badge neutral"
                  >{current.data.routes.routes.length}</span
                >
              </div>
              <input
                class="search"
                placeholder="Find a hostname or route…"
                aria-label="Filter routes"
                bind:value={query}
              />
              {#each routeList as candidate}<button
                  class="route-item"
                  class:route-selected={routeId === candidate.id}
                  onclick={() => {
                    routeId = candidate.id;
                    resolution = null;
                  }}
                  ><span class="route-symbol"
                    >{candidate.errors.length ? "!" : "✧"}</span
                  >
                  <div>
                    <strong>{candidate.hostnames[0]}</strong><small
                      >{candidate.id}</small
                    >
                  </div>
                  <span class="route-chevron">›</span></button
                >{/each}
              {#if !routeList.length}<p class="muted list-empty">
                  No routes match this filter.
                </p>{/if}
              <div class="route-list-note">
                <span>◇</span>Chart-owned routes<br />Generation {current.data
                  .status.generation}
              </div>
            </section>
            <section class="route-detail panel">
              {#if route}<div class="route-detail-header">
                  <div>
                    <span class="eyebrow">SELECTED ROUTE</span>
                    <h3>{route.hostnames[0]}</h3>
                    <code>{route.id}</code>
                  </div>
                  <span
                    class="badge {route.proxyProtocol
                      ? 'gold-badge'
                      : 'neutral'}"
                    >{route.proxyProtocol ? "PROXY v2" : "Plain TCP"}</span
                  >
                </div>
                <div class="hostname-tags">
                  {#each route.hostnames as host}<code>{host}</code>{/each}
                </div>
                {#if route.errors.length}<div class="alert">
                    {route.errors.join(" · ")}
                  </div>{/if}
                <div class="routing-map">
                  <div class="entry-node">
                    <span class="orbit-ring"><Sigil size={34} /></span><strong
                      >{nodeName(current)}</strong
                    ><small>Traffic enters here</small>
                  </div>
                  <div class="route-branches" aria-hidden="true">
                    <span class="solid-branch"></span><span
                      class="dashed-branch"
                    ></span>
                  </div>
                  <div class="routing-tiers">
                    <div>
                      <span class="tier-dot teal-dot"></span><strong
                        >Local first</strong
                      ><span
                        >{local.filter(usable).length} usable / {local.length} ready</span
                      >
                    </div>
                    <div>
                      <span class="tier-dot gold-dot"></span><strong
                        >Global fallback</strong
                      ><span
                        >{global.filter(usable).length} usable / {global.length}
                        ready</span
                      >
                    </div>
                  </div>
                </div>
                <div class="traffic-distribution">
                  <div>
                    <span>Instance locality</span><strong
                      >{localPercent === null
                        ? counters
                          ? "No selections yet"
                          : "Counters unavailable"
                        : `${localPercent}% local`}</strong
                    >
                  </div>
                  <div class="distribution-track">
                    <span style:width={`${localPercent ?? 0}%`}></span>
                  </div>
                  <div class="distribution-labels">
                    <span
                      ><i class="tier-dot teal-dot"
                      ></i>{counters?.localSelections?.toLocaleString() ?? "—"} local</span
                    ><span
                      >{counters?.globalSelections?.toLocaleString() ?? "—"} global<i
                        class="tier-dot gold-dot"
                      ></i></span
                    >
                  </div>
                  <small
                    >Lifetime selections across this instance · {rates[
                      selected
                    ] === undefined
                      ? demo
                        ? "No live throughput in demo"
                        : "measuring throughput…"
                      : `${bytes(rates[selected])}/s forwarded`}</small
                  >
                </div>
                {#each [{ name: "Local backends", values: local }, { name: "Global backends", values: global }] as tier}<div
                    class="backend-tier"
                  >
                    <div class="tier-heading">
                      <span class="eyebrow">{tier.name}</span><span
                        >{tier.values.length}</span
                      >
                    </div>
                    {#each tier.values as endpoint}<div class="backend-row">
                        <span
                          class="status-dot {endpoint.circuit.exclusion
                            ? 'paused'
                            : endpoint.circuit.state === 'open'
                              ? 'danger'
                              : endpoint.circuit.state === 'halfOpen'
                                ? 'paused'
                                : 'healthy'}"
                        ></span>
                        <div class="backend-address">
                          <code>{endpoint.endpoint.address}</code><small
                            >{endpoint.endpoint.nodeName ??
                              "Node identity unavailable"}</small
                          >
                        </div>
                        <span class="backend-state"
                          >{endpointLabel(
                            endpoint,
                          )}{#if endpoint.circuit.cooldownSeconds}<small
                              >{endpoint.circuit.cooldownSeconds}s cooldown</small
                            >{/if}</span
                        >
                        <details class="backend-menu">
                          <summary
                            aria-label={`Actions for ${endpoint.endpoint.address}`}
                            >•••</summary
                          >
                          <div>
                            <button
                              disabled={demo}
                              onclick={() =>
                                requestOperation(
                                  "reset",
                                  { endpointId: endpoint.endpoint.id },
                                  "Reset circuit",
                                  endpoint.endpoint.address,
                                )}>Reset circuit</button
                            ><button
                              disabled={demo}
                              onclick={() =>
                                requestOperation(
                                  "exclude",
                                  {
                                    endpointId: endpoint.endpoint.id,
                                    ttlSeconds: endpoint.circuit.exclusion
                                      ? 0
                                      : 900,
                                  },
                                  endpoint.circuit.exclusion
                                    ? "Include backend"
                                    : "Exclude backend",
                                  endpoint.endpoint.address,
                                )}
                              >{endpoint.circuit.exclusion
                                ? "Include backend"
                                : "Exclude temporarily"}</button
                            >
                          </div>
                        </details>
                      </div>
                      {#if endpoint.circuit.exclusion}<p
                          class="exclusion-reason"
                        >
                          Excluded: {endpoint.circuit.exclusion.reason}
                        </p>{/if}{/each}{#if !tier.values.length}<p
                        class="tier-empty"
                      >
                        {tier.name === "Local backends"
                          ? "No ready local endpoints. Connections use global fallback."
                          : "No ready global endpoints for this route."}
                      </p>{/if}
                  </div>{/each}
              {:else}<div class="empty-state compact">
                  <Sigil />
                  <h3>Select a route</h3>
                  <p>Its local and global paths will appear here.</p>
                </div>{/if}
            </section>
          </div>
          <section class="debugger panel">
            <div class="debugger-heading">
              <div>
                <span class="eyebrow gold">TRACE A PATH</span>
                <h3>Hostname debugger</h3>
              </div>
              <span class="badge neutral">No connection opened</span>
            </div>
            <form
              class="debug-form"
              onsubmit={(e) => {
                e.preventDefault();
                void resolveHostname();
              }}
            >
              <label class="sr-only" for="hostname">Hostname to resolve</label
              ><input
                id="hostname"
                bind:value={hostname}
                required
                placeholder="play.asorda.net"
              /><button class="primary" disabled={debugging}
                >{debugging ? "Resolving…" : "Resolve hostname"}<span>→</span
                ></button
              >
            </form>
            {#if debugError}<div class="alert" role="alert">
                {debugError}
              </div>{/if}
            {#if resolution}<div class="resolution">
                <span
                  class="status-dot {resolution.routeId &&
                  resolution.candidates.some((c) => c.usable)
                    ? 'healthy'
                    : 'danger'}"
                ></span>
                <div>
                  <strong>{resolution.hostname}</strong>
                  <p>
                    {resolution.routeId
                      ? `${current.data.routes.routes.find((r) => r.id === resolution!.routeId)?.hostnames.includes(resolution.hostname) ? "Exact" : "Wildcard"} match → ${resolution.routeId} → ${resolution.candidates.some((c) => c.local && c.usable) ? "local backend available" : resolution.candidates.some((c) => c.usable) ? "global fallback available" : "no usable backend"}`
                      : "No accepted route matches this hostname."}
                  </p>
                </div>
              </div>
              <details class="raw-result">
                <summary>Inspect raw response</summary>
                <pre>{JSON.stringify(resolution, null, 2)}</pre>
              </details>{/if}
          </section>
        {/if}
      {/if}
      {#if page === "activity"}<section class="panel activity-panel">
          <div class="panel-title">
            <h3>Recent signals</h3>
            <span class="muted">Newest first · bounded agent history</span>
          </div>
          {#each eventList as event}<div class="activity-row">
              <time
                >{new Date(event.timestamp * 1000).toLocaleTimeString()}</time
              ><span class="event-mark">✧</span>
              <div>
                <strong>{nodeName(event.instance)}</strong>
                <p>{event.message}</p>
                <code>{event.instance.instanceId.slice(-12)}</code>
              </div>
            </div>{/each}{#if !eventList.length}<div
              class="empty-state compact"
            >
              <p>No events reported by reachable agents.</p>
            </div>{/if}
        </section>{/if}
    {/if}
    <footer>
      <span>✧ AKASHA · Small paths. Shared horizons.</span><span
        >{observedAt
          ? `Last observed ${new Date(observedAt * 1000).toLocaleTimeString()}`
          : "Waiting for discovery"}</span
      >
    </footer>
  </main>
</div>

<dialog
  bind:this={dialog}
  onclose={() => (operation = null)}
  class="operation-dialog"
>
  {#if operation}<form
      onsubmit={(e) => {
        e.preventDefault();
        void executeOperation();
      }}
    >
      <div class="dialog-heading">
        <span class="eyebrow gold">OPERATOR ACTION</span><button
          type="button"
          class="icon-button"
          aria-label="Close operation dialog"
          disabled={operating}
          onclick={() => dialog.close()}>×</button
        >
      </div>
      <h2>{operation.title}</h2>
      <p>
        This affects new connections on the selected instance. Established
        streams stay open.
      </p>
      <div class="operation-target">
        <strong>{nodeName(operation.instance)}</strong><code
          >{operation.instance.instanceId}</code
        >{#if operation.endpoint}<code>{operation.endpoint}</code>{/if}
      </div>
      {#if operation.name === "exclude" && operation.body.ttlSeconds !== 0}<label
          for="ttl">Exclude for</label
        ><select id="ttl" bind:value={ttl}
          ><option value="300">5 minutes</option><option value="900"
            >15 minutes</option
          ><option value="3600">1 hour</option></select
        >{/if}<label for="reason">Reason</label><textarea
        id="reason"
        bind:value={reason}
        maxlength="256"
        required
        placeholder="What are you investigating or maintaining?"
        rows="3"
      ></textarea>{#if operationError}<div class="alert" role="alert">
          {operationError}
        </div>{/if}
      <div class="dialog-actions">
        <button
          type="button"
          class="secondary"
          disabled={operating}
          onclick={() => dialog.close()}>Cancel</button
        ><button class="primary" disabled={operating || !reason.trim()}
          >{operating ? "Applying…" : operation.title}</button
        >
      </div>
    </form>{/if}
</dialog>
