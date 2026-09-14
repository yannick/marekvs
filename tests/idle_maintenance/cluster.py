#!/usr/bin/env python3
"""Disposable Docker acceptance workload. Uses only its uniquely named resources."""
import argparse
import ipaddress
import json
import socket
import statistics
import subprocess
import time
import urllib.request
import uuid
from pathlib import Path


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


class Client:
    def __init__(self, port):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=15)
        self.file = self.sock.makefile("rb")

    def close(self):
        self.file.close()
        self.sock.close()

    def command(self, *args):
        args = [str(a).encode() if not isinstance(a, bytes) else a for a in args]
        self.sock.sendall(b"*%d\r\n" % len(args) + b"".join(
            b"$%d\r\n" % len(a) + a + b"\r\n" for a in args))
        return self.read()

    def read(self):
        line = self.file.readline()
        if not line:
            raise EOFError("RESP connection closed")
        kind, value = line[:1], line[1:-2]
        if kind == b"-":
            raise RuntimeError(value.decode())
        if kind == b"+":
            return value.decode()
        if kind == b":":
            return int(value)
        if kind == b"$":
            n = int(value)
            if n == -1:
                return None
            data = self.file.read(n + 2)
            assert len(data) == n + 2 and data[-2:] == b"\r\n"
            return data[:-2]
        if kind == b"*":
            return [self.read() for _ in range(int(value))]
        raise AssertionError(line)


def eventually(check, timeout=90):
    end = time.monotonic() + timeout
    error = None
    while time.monotonic() < end:
        try:
            if check():
                return
        except (OSError, RuntimeError, EOFError) as exc:
            error = exc
        time.sleep(0.5)
    raise AssertionError(f"condition timed out: {error}")


def metrics(port):
    with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=15) as r:
        body = r.read().decode()
    out = {}
    for line in body.splitlines():
        if line and not line.startswith("#"):
            name, value = line.split()
            out[name] = float(value)
    return out


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--image", required=True)
    ap.add_argument("--shards", type=int, default=10)
    ap.add_argument("--keys", type=int, default=13765)
    ap.add_argument("--warm", type=int, default=180)
    ap.add_argument("--sample", type=int, default=60)
    ap.add_argument("--output", type=Path, required=True)
    ap.add_argument("--expect-fixed", action="store_true", help="assert idle maintenance stops after discovery")
    args = ap.parse_args()
    assert 1 <= args.shards <= 4096 and args.sample >= 60 and args.warm >= 180
    prefix = "mkv-idle-" + uuid.uuid4().hex[:10]
    names, clients, ports, metric_ports, volumes = [], [], [], [], []
    report = {"image": args.image, "shards": args.shards, "keys": args.keys,
              "warm_seconds": args.warm, "sample_seconds": args.sample, "prefix": prefix}
    network = False
    edge_network = False
    cleanup_errors = []
    try:
        docker("network", "create", prefix)
        network = True
        docker("network", "create", prefix + "-edge")
        edge_network = True
        subnet = json.loads(docker("network", "inspect", prefix))[0]["IPAM"]["Config"][0]["Subnet"]
        net = ipaddress.ip_network(subnet)
        ips = [str(net.network_address + 10 + i) for i in range(3)]
        seeds = ",".join(f"{ip}:7946" for ip in ips)
        for i in range(3):
            name, volume = f"{prefix}-{i}", f"{prefix}-data-{i}"
            volumes.append(volume)
            docker("volume", "create", volume)
            name = docker("create", "--name", name, "--network", prefix + "-edge", "-p", "127.0.0.1::6379", "-p", "127.0.0.1::9121",
                          "-v", f"{volume}:/data", "-e", f"MAREKVS_NODE_ID={i}",
                          "-e", f"MAREKVS_ADVERTISE_IP={ips[i]}", "-e", f"MAREKVS_SEEDS={seeds}",
                          "-e", "MAREKVS_REPLICAS_N=2", "-e", f"MAREKVS_SHARDS={args.shards}",
                          "-e", "MAREKVS_DATA_DIR=/data", "-e", "MAREKVS_COLD_PURGE_SECS=1",
                          "-e", "MAREKVS_COLD_PURGE_CLEAN_ROUNDS=1", "-e", "RUST_LOG=warn", args.image)
            names.append(name)
            docker("network", "connect", "--ip", ips[i], prefix, name)
            docker("start", name)
            info = json.loads(docker("inspect", name))[0]
            ports.append(int(info["NetworkSettings"]["Ports"]["6379/tcp"][0]["HostPort"]))
            metric_ports.append(int(info["NetworkSettings"]["Ports"]["9121/tcp"][0]["HostPort"]))
            def ready():
                c = Client(ports[i])
                try:
                    return c.command("PING") == "PONG"
                finally:
                    c.close()
            eventually(ready)
            if i == 0:
                c = Client(ports[0])
                try:
                    # Populate the sole owner, then join peers. Ownership loss
                    # creates cold copies and exercises real cleanup rounds.
                    for start in range(0, args.keys, 128):
                        pairs = [v for k in range(start, min(start + 128, args.keys))
                                 for v in (f"idle-key-{k}", "value")]
                        assert c.command("MSET", *pairs) == "OK"
                finally:
                    c.close()
        eventually(lambda: all(metrics(p).get("marekvs_cluster_members") == 3 for p in metric_ports))
        print(f"{prefix}: three nodes ready; warming {args.warm}s", flush=True)
        time.sleep(args.warm)
        if args.expect_fixed:
            def stable_discovery():
                first = [metrics(p) for p in metric_ports]
                if any(m.get("marekvs_expiry_partitions_completed_total", 0) < 4096 for m in first):
                    return False
                time.sleep(5)
                second = [metrics(p) for p in metric_ports]
                return all(a["marekvs_expiry_iterators_total"] == b["marekvs_expiry_iterators_total"]
                           and a["marekvs_cluster_owned_partitions"] == b["marekvs_cluster_owned_partitions"]
                           and b["marekvs_cluster_members"] == 3
                           and b["marekvs_join_gate_pending_pids"] == 0
                           for a, b in zip(first, second))
            eventually(stable_discovery, 120)
        before = [metrics(p) for p in metric_ports]
        report["metrics_before"] = before
        report["image_digest"] = json.loads(docker("inspect", names[0]))[0]["Image"]
        samples = []
        end = time.monotonic() + args.sample
        while time.monotonic() < end:
            rows = [json.loads(line) for line in docker("stats", "--no-stream", "--format", "{{json .}}", *names).splitlines()]
            samples.append([float(next(r["CPUPerc"] for r in rows if n.startswith(r["ID"])).rstrip("%")) for n in names])
            time.sleep(min(2, max(0, end - time.monotonic())))
        after = [metrics(p) for p in metric_ports]
        report["metrics_after"] = after
        if args.expect_fixed:
            for a, b in zip(before, after):
                assert b["marekvs_expiry_iterators_total"] == a["marekvs_expiry_iterators_total"], "expiry work continued while idle"
                assert b["marekvs_db_range_deletes"] == a["marekvs_db_range_deletes"], "empty cleanup added range tombstones"
                assert b["marekvs_cluster_members"] == a["marekvs_cluster_members"] == 3
                assert b["marekvs_cluster_owned_partitions"] == a["marekvs_cluster_owned_partitions"]
                assert b["marekvs_connected_clients"] == a["marekvs_connected_clients"] == 0
            report["structural_checks"] = "passed: completed discovery, zero idle expiry/range work, stable ownership"
        report["cpu_samples_percent"] = samples
        report["cpu_mean_percent"] = [statistics.mean(v) for v in zip(*samples)]
        report["cpu_peak_percent"] = [max(v) for v in zip(*samples)]
        print(f"{prefix}: idle CPU means {report['cpu_mean_percent']}", flush=True)
        clients = [Client(p) for p in ports]
        latency_runs = []
        for _ in range(3):
            latencies = []
            start = time.perf_counter()
            for i in range(1000):
                t = time.perf_counter()
                assert clients[0].command("SET", f"load-{i % 100}", "value") == "OK"
                latencies.append((time.perf_counter() - t) * 1000)
                t = time.perf_counter()
                assert clients[0].command("GET", f"load-{i % 100}") == b"value"
                latencies.append((time.perf_counter() - t) * 1000)
            elapsed = time.perf_counter() - start
            latencies.sort()
            latency_runs.append({"ops_per_second": 2000 / elapsed, "p50_ms": latencies[999],
                                 "p95_ms": latencies[1899], "p99_ms": latencies[1979]})
        report["client_runs"] = latency_runs
        assert clients[0].command("SET", "ttl-key", "value", "PX", 3000) == "OK"
        eventually(lambda: all(c.command("GET", "ttl-key") == b"value" for c in clients), 2)
        eventually(lambda: all(c.command("GET", "ttl-key") is None for c in clients), 15)
        assert clients[0].command("HSET", "ttl-hash", "f", "value", "persistent", "value") == 2
        assert clients[0].command("HPEXPIRE", "ttl-hash", 3000, "FIELDS", 1, "f") == [1]
        eventually(lambda: all(c.command("HGET", "ttl-hash", "f") is None for c in clients), 15)
        eventually(lambda: all(c.command("HGET", "ttl-hash", "persistent") == b"value" for c in clients))
        # Choose a home key for node 2, so its repair must be autonomous.
        slots = clients[0].command("CLUSTER", "SLOTS")
        node2_id = clients[2].command("CLUSTER", "MYID")
        repair_key = None
        for candidate in range(100):
            key = f"repair-after-restart-{candidate}"
            slot = clients[0].command("CLUSTER", "KEYSLOT", key)
            if any(row[0] <= slot <= row[1] and any(owner[2] == node2_id for owner in row[2:]) for row in slots):
                repair_key = key
                break
        assert repair_key is not None
        # Stop/restart only our own node with its own persistent test volume.
        clients[2].close()
        docker("stop", names[2])
        assert clients[0].command("SET", repair_key, "value") == "OK"
        docker("start", names[2])
        ports[2] = int(json.loads(docker("inspect", names[2]))[0]["NetworkSettings"]["Ports"]["6379/tcp"][0]["HostPort"])
        eventually(lambda: socket.create_connection(("127.0.0.1", ports[2]), timeout=1).close() is None)
        metric_ports[2] = int(json.loads(docker("inspect", names[2]))[0]["NetworkSettings"]["Ports"]["9121/tcp"][0]["HostPort"])
        eventually(lambda: metrics(metric_ports[2]).get("marekvs_join_gate_pending_pids") == 0)
        time.sleep(20)  # allow the normal AE bound before a local-only probe
        # Disconnect only mesh; the separate edge network keeps the published
        # client port reachable. GET cannot fetch a missing record from peers.
        docker("network", "disconnect", prefix, names[2])
        try:
            time.sleep(4)
            clients[2] = Client(ports[2])
            assert clients[2].command("GET", repair_key) == b"value", "autonomous repair missing local home record"
            report["local_repair_check"] = "passed with mesh disconnected before first GET"
        finally:
            docker("network", "connect", "--ip", ips[2], prefix, names[2])
        assert clients[2].command("GET", "ttl-key") is None
        assert clients[2].command("HGET", "ttl-hash", "f") is None
        report["seed_reads"] = {}
        for i in (0, args.keys // 2, args.keys - 1):
            def seed_converged():
                values = []
                for port in ports:
                    c = Client(port)
                    try:
                        value = c.command("GET", f"idle-key-{i}")
                        values.append(None if value is None else value.decode())
                    finally:
                        c.close()
                report["seed_reads"][str(i)] = values
                return values == ["value"] * 3
            eventually(seed_converged)

        report["behavior_checks"] = "passed: key/member TTL, persistent member, disconnect/restart repair, seed data retained"
    except BaseException as exc:
        report["error"] = repr(exc)
        raise
    finally:
        try:
            for c in clients:
                try:
                    c.close()
                except OSError as exc:
                    cleanup_errors.append(str(exc))
            report["logs"] = {}
            for n in names:
                try:
                    report["logs"][n] = docker("logs", "--tail", "50", n)
                except (OSError, subprocess.CalledProcessError) as exc:
                    report["logs"][n] = str(exc)
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")
        finally:
            commands = [("rm", "-f", n) for n in names] + [("volume", "rm", v) for v in volumes]
            if network:
                commands.append(("network", "rm", prefix))
            if edge_network:
                commands.append(("network", "rm", prefix + "-edge"))
            for command in commands:
                try:
                    docker(*command)
                except (OSError, subprocess.CalledProcessError) as exc:
                    cleanup_errors.append(str(exc))
            if cleanup_errors:
                report["cleanup_errors"] = cleanup_errors
                args.output.write_text(json.dumps(report, indent=2) + "\n")
                raise RuntimeError("test resource cleanup incomplete: " + "; ".join(cleanup_errors))



if __name__ == "__main__":
    main()
