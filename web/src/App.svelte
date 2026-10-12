<script lang="ts">
  import { onMount } from 'svelte';
  type Endpoint = { endpoint: { id: string; address: string }; local: boolean; circuit: { state: string; failures: number; exclusion: { reason: string } | null } };
  type Route = { id: string; hostnames: string[]; proxyProtocol: boolean; errors: string[]; endpoints: unknown[] };
  type Instance = { instanceId: string; reachable: boolean; error?: string; observedAt: number; data?: { status: { nodeName: string; accepting: boolean; terminating: boolean; activeConnections: number; generation: number; watchError: string | null }; routes: { routes: Route[] }; endpoints: Endpoint[]; events: { timestamp: number; message: string }[] } };
  let token = $state('');
  let instances = $state<Instance[]>([]);
  let selected = $state('');
  let error = $state('');
  let hostname = $state('asorda.net');
  let reason = $state('');
  let output = $state('');
  let busy = $state(false);
  let refreshedAt = $state(0);
  const current = $derived(instances.find(i => i.instanceId === selected));
  async function refresh() {
    if (!token || busy) return;
    busy = true;
    try {
      const response = await fetch('/api/instances', { headers: { Authorization: `Bearer ${token}` } });
      if (!response.ok) throw new Error(`Dashboard returned ${response.status}`);
      const data = await response.json(); instances = data.instances; refreshedAt = Date.now(); error = '';
      if (!selected && instances.length) selected = instances[0].instanceId;
    } catch (e) { error = String(e); } finally { busy = false; }
  }
  async function action(name: string, body: Record<string, unknown>) {
    if (!selected) return;
    if (name !== 'resolve') {
      if (!reason.trim()) { output = 'Enter a reason first.'; return; }
      if (!confirm(`${name} on ${selected}? Existing streams remain open.`)) return;
      body.reason = reason;
    }
    try {
      const response = await fetch(`/api/instances/${encodeURIComponent(selected)}/${name}`, { method: 'POST', headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      const text = await response.text();
      if (!response.ok) throw new Error(text);
      output = JSON.stringify(JSON.parse(text), null, 2); await refresh();
    } catch (e) { output = String(e); }
  }
  onMount(() => { const interval = setInterval(refresh, 5000); return () => clearInterval(interval); });
</script>

<div class="shell">
  <aside><div class="brand">✦ AKASHA<span>INGRESS / OPERATIONS</span></div><div class="nav-label">INSTANCES</div>
    {#each instances as instance}<button class:chosen={selected === instance.instanceId} onclick={() => selected = instance.instanceId}><i class:healthy={instance.reachable && instance.data?.status.accepting}></i>{instance.data?.status.nodeName ?? instance.instanceId}<small>{instance.instanceId}</small></button>{/each}
    <div class="aside-note">Node-local routing<br>Global fallback<br>PROXY protocol v2</div>
  </aside>
  <main>
    <header><div><p>NETWORK CONTROL</p><h1>Ingress overview</h1></div><span class="prototype">PROTOTYPE</span></header>
    <section class="access"><input type="password" bind:value={token} placeholder="Dashboard access token" aria-label="Dashboard access token"><button onclick={refresh} disabled={busy}>{busy ? 'Refreshing…' : 'Connect / refresh'}</button><small>{refreshedAt ? `Observed ${new Date(refreshedAt).toLocaleTimeString()}` : 'Connect to inspect the fleet'}</small></section>
    {#if error}<div class="error">{error} — displayed data may be stale.</div>{/if}
    {#if current?.reachable && current.data}
      {@const data = current.data}
      <div class="stats"><article><span>ADMISSION</span><strong>{data.status.terminating ? 'Draining' : data.status.accepting ? 'Accepting' : 'Paused'}</strong></article><article><span>ACTIVE STREAMS</span><strong>{data.status.activeConnections}</strong></article><article><span>ROUTES</span><strong>{data.routes.routes.length}</strong></article><article><span>CONFIG GENERATION</span><strong>{data.status.generation}</strong></article></div>
      {#if data.status.watchError}<div class="error">Discovery: {data.status.watchError}</div>{/if}
      <section><h2>Routes</h2><table><thead><tr><th>Route</th><th>Hostnames</th><th>Framing</th><th>Status</th></tr></thead><tbody>{#each data.routes.routes as route}<tr><td>{route.id}</td><td>{route.hostnames.join(', ')}</td><td>{route.proxyProtocol ? 'PROXY v2' : 'Plain TCP'}</td><td class:bad={route.errors.length > 0}>{route.errors.join('; ') || `${route.endpoints.length} endpoints`}</td></tr>{/each}</tbody></table></section>
      <section><h2>Backends & circuits</h2><p class="hint">Controls affect the selected instance only.</p><input bind:value={reason} placeholder="Operator reason" aria-label="Operator reason"><table><thead><tr><th>Backend</th><th>Tier</th><th>Circuit</th><th>Actions</th></tr></thead><tbody>{#each data.endpoints as endpoint}<tr><td>{endpoint.endpoint.address}<small>{endpoint.endpoint.id}</small></td><td>{endpoint.local ? 'Local' : 'Global'}</td><td>{endpoint.circuit.exclusion ? `Excluded: ${endpoint.circuit.exclusion.reason}` : endpoint.circuit.state}<small>{endpoint.circuit.failures} failures</small></td><td><button onclick={() => action('reset', { endpointId: endpoint.endpoint.id })}>Reset</button><button onclick={() => action('exclude', { endpointId: endpoint.endpoint.id, ttlSeconds: endpoint.circuit.exclusion ? 0 : 900 })}>{endpoint.circuit.exclusion ? 'Include' : 'Exclude 15m'}</button></td></tr>{/each}</tbody></table><button onclick={() => action('admission', { accepting: !data.status.accepting })} disabled={data.status.terminating}>{data.status.accepting ? 'Pause new connections' : 'Resume admission'}</button></section>
      <section><h2>Hostname debugger</h2><div class="access"><input bind:value={hostname} aria-label="Hostname"><button onclick={() => action('resolve', { hostname })}>Resolve without connecting</button></div>{#if output}<pre>{output}</pre>{/if}</section>
      <section><h2>Recent events</h2>{#each [...data.events].reverse() as event}<div class="event"><time>{new Date(event.timestamp * 1000).toLocaleTimeString()}</time><span>{event.message}</span></div>{/each}</section>
    {:else if current}<div class="error">Instance unreachable: {current.error ?? 'No current response'}</div>
    {:else}<section class="empty"><h2>Inspect your ingress fleet</h2><p>Connect with a dashboard token to see routes, local and global backends, and circuit state.</p></section>{/if}
  </main>
</div>

<style>
  :global(body){margin:0;background:#0d121a;color:#dce5f0;font-family:system-ui,sans-serif;font-size:14px} :global(*){box-sizing:border-box} .shell{display:flex;min-height:100vh}aside{width:250px;background:#111925;border-right:1px solid #263244;padding:30px 20px;flex-shrink:0}.brand{font-size:22px;letter-spacing:3px;color:#80ddd0;font-weight:700}.brand span{display:block;font-size:9px;letter-spacing:2px;color:#8392a8;margin:10px 0 42px}.nav-label{font-size:10px;color:#8392a8;letter-spacing:2px;margin-bottom:12px}aside button{text-align:left;width:100%;margin:4px 0;background:transparent;border-color:transparent}aside button.chosen{background:#20323b;border-color:#3a635f}i{display:inline-block;width:7px;height:7px;border-radius:50%;background:#b96b67;margin-right:9px}i.healthy{background:#7ed6bb}.aside-note{margin-top:50px;line-height:2;color:#6f839b;font-size:12px}main{padding:35px 40px;max-width:1350px;width:100%}header{display:flex;justify-content:space-between;align-items:center;margin-bottom:30px}header p{color:#7bcbbd;font-size:10px;letter-spacing:2px}h1{font-size:30px;margin:5px 0}h2{font-size:17px;margin-top:0}.prototype{border:1px solid #4e5b72;padding:7px 10px;border-radius:5px;font-size:10px;letter-spacing:1px}section{background:#131c29;border:1px solid #263244;border-radius:9px;padding:22px;margin:20px 0}.access{display:flex;gap:12px;align-items:center;flex-wrap:wrap}.access input{max-width:350px}.stats{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin:24px 0}article{background:#16232c;border:1px solid #30464c;padding:20px;border-radius:8px}article span{font-size:10px;color:#89a3ab;letter-spacing:1px}article strong{display:block;margin-top:12px;font-size:26px}input{background:#0c1420;border:1px solid #36465d;border-radius:5px;color:#e6eef7;padding:10px;width:100%;max-width:450px}button{background:#223d42;border:1px solid #426268;color:#dceeea;border-radius:5px;padding:9px 12px;cursor:pointer}button:disabled{opacity:.5;cursor:default}td button{margin-right:5px;padding:5px 8px}table{width:100%;border-collapse:collapse;margin:17px 0;text-align:left}th{font-size:10px;text-transform:uppercase;color:#8da0b8;letter-spacing:1px}th,td{padding:13px 10px;border-bottom:1px solid #263244}small{display:block;color:#8392a8;font-size:11px;margin-top:5px;overflow-wrap:anywhere}.hint{color:#92a3b8;font-size:12px}.error,.bad{color:#ffada3}.error{padding:14px;border:1px solid #77443f;border-radius:7px;background:#291d23}.event{display:flex;gap:20px;padding:9px 0;font-size:12px;border-bottom:1px solid #263244}.event time{color:#7d93ad;flex-shrink:0}pre{white-space:pre-wrap;overflow-wrap:anywhere;background:#0c1420;padding:18px;border-radius:6px;font-size:12px}.empty{padding:55px;color:#93a5ba}@media(max-width:900px){aside{width:180px}main{padding:24px 18px}.stats{grid-template-columns:repeat(2,1fr)}section{overflow:auto}}@media(max-width:600px){.shell{display:block}aside{width:100%;padding:15px}.brand span,.aside-note{display:none}aside button{width:auto}main{padding:20px 12px}h1{font-size:24px}}
</style>
