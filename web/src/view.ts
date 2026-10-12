import type { Instance, Endpoint } from "./types";
export function admission(instance: Instance): string {
  if (instance.stale) return "Stale";
  if (!instance.reachable || !instance.data) return "Unreachable";
  const status = instance.data.status;
  if (status.terminating) return "Draining";
  if (status.listenerError) return "Degraded";
  return status.accepting ? "Accepting" : "Paused";
}
export function usable(endpoint: Endpoint): boolean {
  return endpoint.circuit.state !== "open" && !endpoint.circuit.exclusion;
}
export function problems(instance: Instance): string[] {
  if (!instance.reachable) return [instance.error ?? "No current response"];
  const status = instance.data?.status;
  return [
    ...(status?.watchError ? [status.watchError] : []),
    ...(status?.listenerError ? [status.listenerError] : []),
    ...Object.values(status?.serviceErrors ?? {}),
    ...Object.values(status?.configErrors ?? {}),
    ...(instance.data?.routes.routes.flatMap((r) => [
      ...r.errors,
      ...(r.backendErrors ?? []),
    ]) ?? []),
    ...(instance.data?.endpoints
      .filter((e) => e.circuit.state === "open")
      .map((e) => `Circuit open: ${e.endpoint.address}`) ?? []),
  ];
}
export function nodeName(instance: Instance): string {
  return (
    instance.data?.status.nodeName ?? instance.nodeName ?? instance.instanceId
  );
}
export function bytes(value: number): string {
  if (value >= 1024 ** 2) return `${(value / 1024 ** 2).toFixed(1)} MiB`;
  if (value >= 1024) return `${(value / 1024).toFixed(1)} KiB`;
  return `${Math.round(value)} B`;
}
export function duration(seconds: number): string {
  if (seconds >= 86400)
    return `${Math.floor(seconds / 86400)}d ${Math.floor((seconds % 86400) / 3600)}h`;
  if (seconds >= 3600)
    return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  return `${Math.floor(seconds / 60)}m`;
}
