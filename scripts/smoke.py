#!/usr/bin/env python3
"""Exercise the built agent + dashboard locally; no cluster or extra Python packages."""
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def request(url, token=None, body=None):
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    data = None if body is None else json.dumps(body).encode()
    if data:
        headers["Content-Type"] = "application/json"
    with urllib.request.urlopen(urllib.request.Request(url, data=data, headers=headers), timeout=5) as response:
        return response.read()


def wait(url):
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            request(url)
            return
        except (OSError, urllib.error.URLError):
            time.sleep(0.05)
    raise AssertionError(f"startup timed out: {url}")


def main():
    ingress, management, dashboard = free_port(), free_port(), free_port()
    agent_url = f"http://127.0.0.1:{management}"
    dashboard_url = f"http://127.0.0.1:{dashboard}"
    processes = []
    with tempfile.TemporaryDirectory(prefix="aio-smoke-") as directory:
        agents = Path(directory) / "agents.yaml"
        agents.write_text(f"- instanceId: smoke\n  url: {agent_url}\n")
        env = os.environ | {"AIO_API_TOKEN": "smoke-agent", "AIO_DASHBOARD_TOKEN": "smoke-dashboard"}
        try:
            processes.append(subprocess.Popen([str(ROOT / "target/debug/aio-agent"), "--config", str(ROOT / "examples/local.yaml"), "--instance-id", "smoke", "--listen", f"127.0.0.1:{ingress}", "--management-listen", f"127.0.0.1:{management}", "--drain-timeout-seconds", "1"], env=env, cwd=ROOT, stdout=subprocess.DEVNULL))
            wait(f"{agent_url}/health/ready")
            processes.append(subprocess.Popen([str(ROOT / "target/debug/aio-dashboard"), "--agents", str(agents), "--listen", f"127.0.0.1:{dashboard}"], env=env, cwd=ROOT, stdout=subprocess.DEVNULL))
            wait(dashboard_url)
            assert b"Akasha Ingress" in request(dashboard_url)
            try:
                request(f"{dashboard_url}/api/instances")
                raise AssertionError("dashboard API accepted anonymous access")
            except urllib.error.HTTPError as error:
                assert error.code == 401
            fleet = json.loads(request(f"{dashboard_url}/api/instances", "smoke-dashboard"))
            assert fleet["instances"][0]["reachable"]
            assert fleet["instances"][0]["data"]["status"]["accepting"]
            url = f"{dashboard_url}/api/instances/smoke"
            result = json.loads(request(f"{url}/resolve", "smoke-dashboard", {"hostname": "PLAY.Asorda.Net."}))
            assert result["routeId"] == "demo/main" and not result["proxyProtocol"]
            result = json.loads(request(f"{url}/admission", "smoke-dashboard", {"accepting": False, "reason": "smoke pause"}))
            assert not result["accepting"]
            result = json.loads(request(f"{url}/admission", "smoke-dashboard", {"accepting": True, "reason": "smoke resume"}))
            assert result["accepting"]
            print("Agent + dashboard smoke passed: static UI, authenticated aggregation, hostname debug, pause/resume.")
        finally:
            for process in reversed(processes):
                process.terminate()
            for process in reversed(processes):
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()
