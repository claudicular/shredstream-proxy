"""Optional localhost smoke test against a built proxy.

BENCH_PROXY_BIN=/absolute/path/to/jito-shredstream-proxy python3 -m unittest discover -s scripts -p test_bench_proxy.py -v
Uses an isolated cwd/environment, ephemeral loopback ports, and no RPC/Influx/Jito.
"""

import json
import os
from pathlib import Path
import socket
import struct
import subprocess
import sys
import tempfile
import time
import unittest

import bench


@unittest.skipUnless(os.environ.get("BENCH_PROXY_BIN"), "set BENCH_PROXY_BIN to run the localhost proxy smoke test")
class ProxySmoke(unittest.TestCase):
    def test_forwarding_survives_idle_start_invalid_control_stop_and_next_session(self):
        binary = str(Path(os.environ["BENCH_PROXY_BIN"]).resolve())
        helper = str(Path(bench.__file__).resolve())
        with tempfile.TemporaryDirectory(prefix="ss-bench-proxy-") as directory:
            control = Path(directory) / "control.json"
            # Exercise IPv4 RPC destinations on Linux. The existing IPv6 send
            # socket's macOS fallback needs an IPv6 destination.
            ipv4 = sys.platform == "linux"
            with socket.socket(socket.AF_INET if ipv4 else socket.AF_INET6, socket.SOCK_DGRAM) as destination, \
                    socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sender, \
                    socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as port_probe, \
                    (Path(directory) / "proxy.log").open("w+") as log:
                destination.bind(("127.0.0.1" if ipv4 else "::1", 0))
                destination.settimeout(3)
                sender.bind(("127.0.0.1", 0))
                port_probe.bind(("127.0.0.1", 0))
                port = port_probe.getsockname()[1]
                port_probe.close()
                dest = f"127.0.0.1:{destination.getsockname()[1]}" if ipv4 else f"[::1]:{destination.getsockname()[1]}"
                process = subprocess.Popen([
                    binary, "forward-only", "--src-bind-addr", "127.0.0.1",
                    "--src-bind-port", str(port), "--num-threads", "1",
                    "--dest-ip-ports", dest,
                    "--multicast-device", "ss-bench-test-no-device",
                    "--enable-benchmark", "--benchmark-kernel-timestamps",
                    "--benchmark-control-path", str(control),
                    "--benchmark-window-slots", "4", "--benchmark-flush-secs", "1",
                ], cwd=directory, env={"PATH": os.environ.get("PATH", ""), "RUST_LOG": "error"},
                    stdout=log, stderr=subprocess.STDOUT)
                try:
                    deadline = time.monotonic() + 15
                    while not bench.status_path(control).exists():
                        if process.poll() is not None or time.monotonic() >= deadline:
                            log.seek(0)
                            self.fail("proxy did not start: " + log.read())
                        time.sleep(0.05)
                    # The benchmark thread can publish status just before the
                    # existing forwarder creates its listening socket.
                    time.sleep(0.5)
                    serial = 0

                    def forward_batch():
                        nonlocal serial
                        expected = set()
                        for _ in range(16):
                            serial += 1
                            payload = bytes(64) + bytes([0xA5]) + struct.pack("<QIHI", 100 + serial, serial, 0, 0)
                            expected.add(payload)
                            sender.sendto(payload, ("127.0.0.1", port))
                        received = set()
                        try:
                            for _ in expected:
                                received.add(destination.recvfrom(2048)[0])
                        except socket.timeout:
                            log.flush()
                            log.seek(0)
                            self.fail(f"forwarded {len(received)}/{len(expected)} packets; proxy log: {log.read()}")
                        self.assertEqual(received, expected)
                        self.assertIsNone(process.poll())

                    def command(*args):
                        result = subprocess.run([sys.executable, helper, "--control", str(control), *args],
                            cwd=directory, capture_output=True, text=True, timeout=20)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        return json.loads(result.stdout)

                    forward_batch()  # idle
                    first = command("start", "--baseline", "127.0.0.1", "--candidate", "203.0.113.10")
                    self.assertEqual(first["state"], "waiting_for_sources")
                    forward_batch()
                    control.write_text("{invalid")
                    time.sleep(1.5)
                    self.assertTrue(command("status")["control_error"])
                    forward_batch()  # rejected command did not interrupt forwarding
                    stopped = command("stop", "--wait", "10")
                    self.assertEqual(stopped["state"], "complete")
                    forward_batch()  # idle again
                    second = command("start", "--baseline", "127.0.0.1", "--candidate", "203.0.113.10")
                    self.assertNotEqual(first["session_id"], second["session_id"])
                    forward_batch()
                    command("stop", "--wait", "10")
                    self.assertIsNone(process.poll())
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:
                            process.wait(timeout=10)
                        except subprocess.TimeoutExpired:
                            process.kill()
                            process.wait(timeout=5)


if __name__ == "__main__":
    unittest.main()
