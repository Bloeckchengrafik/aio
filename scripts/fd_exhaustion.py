#!/usr/bin/env python3
"""Linux regression: exhausted accept descriptors must not kill established streams."""
import json
import os
from pathlib import Path
import resource
import socket
import subprocess
import tempfile
import threading
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def recv_exact(sock, size):
    result = b""
    while len(result) < size:
        data = sock.recv(size - len(result))
        assert data, "established stream closed"
        result += data
    return result


def main():
    ingress, management = port(), port()
    backend = socket.socket()
    backend.bind(("127.0.0.1", 0))
    backend.listen()
    backend_port = backend.getsockname()[1]
    def echo():
        stream, _ = backend.accept()
        with stream:
            while data := stream.recv(4096):
                stream.sendall(data)
    thread = threading.Thread(target=echo, daemon=True)
    thread.start()
    holders = []
    process = None
    with tempfile.TemporaryDirectory(prefix="aio-fd-regression-") as directory:
        config = Path(directory) / "routes.yaml"
        config.write_text(f"routes:\n  - id: test/main\n    hostnames: [asorda.net]\n    proxyProtocol: false\n    endpoints:\n      - id: backend\n        address: 127.0.0.1:{backend_port}\n        nodeName: local\n")
        log = Path(directory) / "agent.log"
        with log.open("w") as output:
            try:
                process = subprocess.Popen([str(ROOT / "target/debug/aio-agent"), "--config", str(config), "--listen", f"127.0.0.1:{ingress}", "--management-listen", f"127.0.0.1:{management}", "--max-connections", "1", "--drain-timeout-seconds", "1"], env=os.environ | {"AIO_API_TOKEN": "fd-test", "TOKIO_WORKER_THREADS": "2", "RUST_LOG": "aio=info,axum=warn"}, stdout=output, stderr=output, preexec_fn=lambda: resource.setrlimit(resource.RLIMIT_NOFILE, (160, 160)))
                deadline = time.monotonic() + 10
                while True:
                    try:
                        urllib.request.urlopen(f"http://127.0.0.1:{management}/health/ready", timeout=.2).close()
                        break
                    except OSError:
                        assert process.poll() is None, log.read_text()
                        assert time.monotonic() < deadline, log.read_text()
                        time.sleep(.05)
                with socket.create_connection(("127.0.0.1", ingress), timeout=2) as client:
                    host = b"asorda.net"
                    frame = bytes([0, 47, len(host)]) + host + bytes([0x63, 0xdd, 2])
                    packet = bytes([len(frame)]) + frame
                    client.sendall(packet)
                    assert recv_exact(client, len(packet)) == packet
                    # Hold incomplete HTTP requests to occupy the agent's remaining descriptors.
                    for _ in range(190):
                        try:
                            holders.append(socket.create_connection(("127.0.0.1", management), timeout=.03))
                        except OSError:
                            break
                    # Wait for management accept to prove exhaustion, avoiding an admission race.
                    deadline = time.monotonic() + 5
                    while "Too many open files" not in log.read_text():
                        assert process.poll() is None, log.read_text()
                        assert time.monotonic() < deadline, "did not exhaust descriptors"
                        time.sleep(.05)
                    # Trigger public accept while the agent has no descriptors available.
                    holders.append(socket.create_connection(("127.0.0.1", ingress), timeout=1))
                    deadline = time.monotonic() + 5
                    while "accept failed; retrying" not in log.read_text():
                        assert process.poll() is None, log.read_text()
                        assert time.monotonic() < deadline, "did not exercise accept failure: " + log.read_text()
                        time.sleep(.05)
                    client.sendall(b"alive-under-emfile")
                    assert recv_exact(client, 18) == b"alive-under-emfile"
                    assert process.poll() is None
                    for holder in holders:
                        holder.close()
                    holders.clear()
                    deadline = time.monotonic() + 10
                    while True:
                        try:
                            request = urllib.request.Request(f"http://127.0.0.1:{management}/api/v1/status", headers={"Authorization": "Bearer fd-test"})
                            with urllib.request.urlopen(request, timeout=.3) as response:
                                status = json.load(response)
                            assert status["activeConnections"] == 1
                            break
                        except OSError:
                            assert time.monotonic() < deadline, log.read_text()
                            time.sleep(.05)
                    client.sendall(b"recovered")
                    assert recv_exact(client, 9) == b"recovered"
                print("FD exhaustion regression passed: accept error exercised, stream survived, management recovered.")
            finally:
                for holder in holders:
                    holder.close()
                backend.close()
                if process:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()


if __name__ == "__main__":
    main()
