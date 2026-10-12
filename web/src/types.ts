export type Circuit = {
  state: "closed" | "open" | "halfOpen";
  failures: number;
  cooldownSeconds: number;
  exclusion: { reason: string; remainingSeconds?: number } | null;
};
export type Backend = { id: string; address: string; nodeName?: string | null };
export type Endpoint = {
  routeId: string;
  endpoint: Backend;
  local: boolean;
  circuit: Circuit;
};
export type Route = {
  id: string;
  hostnames: string[];
  admission?: "normal" | "maintenance" | "unavailable";
  allowConnections?: boolean;
  activeConnections?: number;
  usableLocal?: number;
  usableGlobal?: number;
  proxyProtocol: boolean;
  endpoints: Backend[];
  errors: string[];
  backendErrors?: string[];
};
export type Counters = {
  connections: number;
  localSelections: number;
  globalSelections: number;
  errors: number;
  bytes: number;
};
export type Status = {
  instanceId: string;
  nodeName: string;
  version: string;
  synced: boolean;
  accepting: boolean;
  terminating: boolean;
  generation: number;
  activeConnections: number;
  watchError: string | null;
  listenerError?: string | null;
  serviceErrors?: Record<string, string>;
  configErrors?: Record<string, string>;
  uptimeSeconds?: number;
  counters?: Counters;
};
export type Instance = {
  instanceId: string;
  nodeName?: string | null;
  reachable: boolean;
  stale?: boolean;
  lastHeartbeat?: number | null;
  observedAt: number;
  error?: string;
  data?: {
    status: Status;
    routes: { generation: number; routes: Route[] };
    endpoints: Endpoint[];
    events: { timestamp: number; message: string }[];
  };
};
export type Resolution = {
  hostname: string;
  routeId: string | null;
  proxyProtocol?: boolean;
  candidates: { id: string; local: boolean; usable: boolean }[];
};
export type Operation = {
  instance: Instance;
  name: "admission" | "exclude" | "reset" | "route";
  title: string;
  body: Record<string, unknown>;
  endpoint?: string;
};
