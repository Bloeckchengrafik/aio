import type { Instance, Route, Endpoint } from "./types";
// Explicitly marked, read-only preview data. Never merged with a live fleet.
export function demoFleet(): Instance[] {
  const endpoints: Endpoint[] = [
    {
      routeId: "asorda/production",
      local: true,
      endpoint: {
        id: "babylon-a/10.42.0.18:25565",
        address: "10.42.0.18:25565",
        nodeName: "nbg-01",
      },
      circuit: {
        state: "closed",
        failures: 0,
        cooldownSeconds: 0,
        exclusion: null,
      },
    },
    {
      routeId: "asorda/production",
      local: false,
      endpoint: {
        id: "babylon-b/10.42.1.24:25565",
        address: "10.42.1.24:25565",
        nodeName: "fsn-02",
      },
      circuit: {
        state: "closed",
        failures: 0,
        cooldownSeconds: 0,
        exclusion: null,
      },
    },
    {
      routeId: "asorda/construct",
      local: false,
      endpoint: {
        id: "construct/10.42.2.11:25565",
        address: "10.42.2.11:25565",
        nodeName: "hel-03",
      },
      circuit: {
        state: "closed",
        failures: 0,
        cooldownSeconds: 0,
        exclusion: null,
      },
    },
  ];
  const routes: Route[] = [
    {
      id: "asorda/production",
      hostnames: ["asorda.net", "play.asorda.net"],
      proxyProtocol: true,
      endpoints: endpoints.slice(0, 2).map((e) => e.endpoint),
      errors: [],
    },
    {
      id: "asorda/construct",
      hostnames: ["construct.asorda.net"],
      proxyProtocol: true,
      endpoints: [endpoints[2].endpoint],
      errors: [],
    },
    {
      id: "asorda-preview/preview",
      hostnames: ["*.preview.asorda.net"],
      proxyProtocol: false,
      endpoints: [],
      errors: [],
    },
  ];
  const now = Math.floor(Date.now() / 1000);
  return ["nbg-01", "fsn-02", "hel-03"].flatMap((node, index) => {
    const instance: Instance = {
      instanceId: `${node}-current`,
      nodeName: node,
      reachable: true,
      observedAt: now,
      data: {
        status: {
          instanceId: `${node}-current`,
          nodeName: node,
          version: "0.1.0",
          synced: true,
          accepting: true,
          terminating: false,
          generation: 42,
          activeConnections: [128, 76, 32][index],
          watchError: null,
          uptimeSeconds: 14820,
          counters: {
            connections: 1400 + index * 600,
            localSelections: 1300 + index * 520,
            globalSelections: 100 + index * 80,
            bytes: 41200000 + index * 4000000,
            errors: 0,
          },
        },
        routes: { generation: 42, routes },
        endpoints: endpoints.map((e) => ({
          ...e,
          local: e.endpoint.nodeName === node,
        })),
        events: [
          { timestamp: now - 32, message: "listener accepting" },
          {
            timestamp: now - 91,
            message: "configuration synchronized; generation 42",
          },
        ],
      },
    };
    if (index === 0) {
      const old = structuredClone(instance);
      old.instanceId = "nbg-01-draining";
      old.data!.status.instanceId = old.instanceId;
      old.data!.status.terminating = true;
      old.data!.status.accepting = false;
      old.data!.status.activeConnections = 9;
      old.data!.status.version = "0.0.9";
      old.data!.status.counters = undefined;
      return [instance, old];
    }
    return [instance];
  });
}
