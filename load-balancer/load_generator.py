"""
Performance Load Generator & Experimentation Tool
for Secure Group-Chat Application & Dynamic Load Balancer.

Supports:
- Target API Routes: POST /message and GET /feed (as specified by instructor)
- WebSocket Chat stress testing (/ws)
- Variable number of concurrent users / clients (--clients)
- Random / variable message lengths (--min-len, --max-len)
- Random / variable time intervals between messages (--interval, --jitter, or Poisson distribution)
- Automatic latency percentile calculations (Avg, Min, Max, P50, P90, P95, P99)
- JSON export for reporting and benchmarking plots
"""

import argparse
import asyncio
import json
import math
import os
import random
import statistics
import string
import sys
import time
from typing import Dict, List, Any, Optional
import urllib.request
import urllib.parse
import aiohttp
import websockets

# Ensure clean UTF-8 stdout across Windows/Linux
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass



def random_message(min_len: int = 10, max_len: int = 150) -> str:
    """Generate a random sentence or string between min_len and max_len."""
    length = random.randint(min_len, max_len)
    words = [
        "quantum", "distributed", "consensus", "cluster", "loadbalancer", "latency",
        "throughput", "resilience", "failover", "algorithm", "threshold", "performance",
        "crypto", "signature", "encryption", "payload", "pipeline", "database", "stream",
        "websocket", "goroutine", "dynamic", "network", "benchmark", "analysis"
    ]
    cur = []
    cur_len = 0
    while cur_len < length:
        w = random.choice(words)
        cur.append(w)
        cur_len += len(w) + 1
    text = " ".join(cur)
    return text[:length]


def random_delay(base_interval: float, jitter: float = 0.5, dist: str = "uniform") -> float:
    """Generate a random interval between requests."""
    if base_interval <= 0:
        return 0.0
    if dist == "exponential":
        # Poisson process arrival times (exponential inter-arrival)
        return random.expovariate(1.0 / base_interval)
    else:
        # Uniform jitter: base * (1 +/- jitter)
        low = max(0.0, base_interval * (1.0 - jitter))
        high = base_interval * (1.0 + jitter)
        return random.uniform(low, high)


# ===========================================================================
# HTTP Simulated Client (Testing POST /message and GET /feed)
# ===========================================================================

class HTTPSimulatedClient:
    def __init__(
        self,
        client_id: int,
        base_url: str,
        num_messages: int,
        feed_freq: int,
        min_len: int,
        max_len: int,
        interval: float,
        jitter: float,
    ):
        self.client_id = client_id
        self.base_url = base_url.rstrip("/")
        self.num_messages = num_messages
        self.feed_freq = feed_freq  # fetch feed every N messages
        self.min_len = min_len
        self.max_len = max_len
        self.interval = interval
        self.jitter = jitter

        self.username = f"user_{client_id}_{''.join(random.choices(string.ascii_lowercase, k=4))}"
        self.post_latencies: List[float] = []
        self.feed_latencies: List[float] = []
        self.messages_sent = 0
        self.feeds_fetched = 0
        self.errors: List[str] = []
        self.start_time: float = 0
        self.end_time: float = 0

    async def run(self, session: aiohttp.ClientSession) -> None:
        self.start_time = time.time()
        msg_url = f"{self.base_url}/message"
        feed_url = f"{self.base_url}/feed"

        try:
            for i in range(self.num_messages):
                # 1. Random message content & variable length
                text = random_message(self.min_len, self.max_len)
                msg_id = f"msg_{self.username}_{i}_{int(time.time()*1000)}"

                payload = {
                    "client-name": self.username,
                    "msg": text,
                    "id": msg_id,
                    "timestamp": int(time.time() * 1000),
                }

                # Send POST /message and record latency
                t0 = time.time()
                try:
                    async with session.post(msg_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                        lat = (time.time() - t0) * 1000.0
                        if resp.status < 400:
                            self.post_latencies.append(lat)
                            self.messages_sent += 1
                        else:
                            self.errors.append(f"POST HTTP {resp.status}")
                except Exception as exc:
                    self.errors.append(f"POST err: {exc}")

                # 2. Optionally fetch GET /feed
                if self.feed_freq > 0 and (i + 1) % self.feed_freq == 0:
                    t_feed = time.time()
                    try:
                        async with session.get(feed_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                            f_lat = (time.time() - t_feed) * 1000.0
                            if resp.status < 400:
                                self.feed_latencies.append(f_lat)
                                self.feeds_fetched += 1
                            else:
                                self.errors.append(f"FEED HTTP {resp.status}")
                    except Exception as exc:
                        self.errors.append(f"FEED err: {exc}")

                # 3. Variable random inter-arrival time
                sleep_sec = random_delay(self.interval, self.jitter)
                if sleep_sec > 0:
                    await asyncio.sleep(sleep_sec)

        except Exception as exc:
            self.errors.append(f"Client exception: {exc}")
        finally:
            self.end_time = time.time()

    def summary(self) -> Dict[str, Any]:
        duration = self.end_time - self.start_time if self.end_time else 0
        return {
            "client_id": self.client_id,
            "username": self.username,
            "messages_sent": self.messages_sent,
            "feeds_fetched": self.feeds_fetched,
            "avg_post_latency_ms": round(statistics.mean(self.post_latencies), 2) if self.post_latencies else None,
            "avg_feed_latency_ms": round(statistics.mean(self.feed_latencies), 2) if self.feed_latencies else None,
            "duration_s": round(duration, 2),
            "errors": self.errors,
        }


# ===========================================================================
# WebSocket Simulated Client (/ws)
# ===========================================================================

class WSSimulatedClient:
    def __init__(
        self,
        client_id: int,
        ws_url: str,
        num_messages: int,
        min_len: int,
        max_len: int,
        interval: float,
        jitter: float,
    ):
        self.client_id = client_id
        self.ws_url = ws_url
        self.num_messages = num_messages
        self.min_len = min_len
        self.max_len = max_len
        self.interval = interval
        self.jitter = jitter

        self.username = f"ws_bot_{client_id}_{''.join(random.choices(string.ascii_lowercase, k=4))}"
        self.connected = False
        self.join_latency_ms: Optional[float] = None
        self.msg_latencies: List[float] = []
        self.messages_sent = 0
        self.messages_received = 0
        self.errors: List[str] = []
        self.start_time: float = 0
        self.end_time: float = 0

    async def run(self) -> None:
        self.start_time = time.time()
        try:
            async with websockets.connect(
                self.ws_url,
                open_timeout=15,
                close_timeout=5,
                max_size=4 * 1024 * 1024,
            ) as ws:
                self.connected = True
                t0 = time.time()
                await ws.send(json.dumps({
                    "type": "join",
                    "username": self.username,
                    "room": "general",
                }))

                # Wait for welcome
                while True:
                    raw = await asyncio.wait_for(ws.recv(), timeout=10)
                    data = json.loads(raw)
                    if data.get("type") == "welcome":
                        self.join_latency_ms = (time.time() - t0) * 1000.0
                        break

                pending_sends: Dict[str, float] = {}

                async def sender():
                    for i in range(self.num_messages):
                        text = random_message(self.min_len, self.max_len)
                        msg_id = f"{self.username}_{i}"
                        pending_sends[msg_id] = time.time()
                        await ws.send(json.dumps({"type": "message", "text": text}))
                        self.messages_sent += 1
                        delay = random_delay(self.interval, self.jitter)
                        if delay > 0:
                            await asyncio.sleep(delay)

                async def receiver():
                    deadline = time.time() + (self.num_messages * (self.interval + 1.0)) + 15
                    delivered_count = 0
                    try:
                        while delivered_count < self.num_messages and time.time() < deadline:
                            raw = await asyncio.wait_for(ws.recv(), timeout=10)
                            data = json.loads(raw)
                            self.messages_received += 1
                            if data.get("type") == "delivered":
                                delivered_count += 1
                                if pending_sends:
                                    k = min(pending_sends, key=pending_sends.get)
                                    send_t = pending_sends.pop(k)
                                    self.msg_latencies.append((time.time() - send_t) * 1000.0)
                    except Exception:
                        pass

                await asyncio.gather(sender(), receiver())
                await ws.close()

        except Exception as exc:
            self.errors.append(str(exc))
        finally:
            self.end_time = time.time()

    def summary(self) -> Dict[str, Any]:
        duration = self.end_time - self.start_time if self.end_time else 0
        return {
            "client_id": self.client_id,
            "username": self.username,
            "connected": self.connected,
            "join_latency_ms": round(self.join_latency_ms, 2) if self.join_latency_ms else None,
            "messages_sent": self.messages_sent,
            "messages_received": self.messages_received,
            "avg_latency_ms": round(statistics.mean(self.msg_latencies), 2) if self.msg_latencies else None,
            "duration_s": round(duration, 2),
            "errors": self.errors,
        }


# ===========================================================================
# Metric Aggregation
# ===========================================================================

def calc_percentiles(vals: List[float]) -> Dict[str, Any]:
    if not vals:
        return {"count": 0, "avg": 0, "min": 0, "max": 0, "p50": 0, "p90": 0, "p95": 0, "p99": 0}
    s = sorted(vals)
    n = len(s)

    def p(pct: float) -> float:
        idx = max(0, min(n - 1, int(math.ceil(pct * n)) - 1))
        return round(s[idx], 2)

    return {
        "count": n,
        "avg": round(statistics.mean(s), 2),
        "min": round(s[0], 2),
        "max": round(s[-1], 2),
        "p50": p(0.50),
        "p90": p(0.90),
        "p95": p(0.95),
        "p99": p(0.99),
    }


# ===========================================================================
# Main Load Test Orchestrator
# ===========================================================================

async def run_http_load_test(
    url: str,
    num_clients: int,
    num_messages: int,
    feed_freq: int,
    min_len: int,
    max_len: int,
    interval: float,
    jitter: float,
    ramp_delay: float,
) -> Dict[str, Any]:
    print(f"\n{'='*70}")
    print("  🚀 HTTP API LOAD GENERATOR (/message & /feed)")
    print(f"{'='*70}")
    print(f"  Target URL           : {url}")
    print(f"  Concurrent Clients   : {num_clients}")
    print(f"  Messages / Client    : {num_messages}")
    print(f"  Total Requests       : ~{num_clients * num_messages}")
    print(f"  Message Length       : {min_len} to {max_len} chars (randomized)")
    print(f"  Time Interval        : {interval}s ± {int(jitter*100)}% jitter")
    print(f"  Feed Polling Freq    : every {feed_freq} msgs" if feed_freq > 0 else "  Feed Polling Freq    : disabled")
    print(f"{'='*70}\n")

    clients = [
        HTTPSimulatedClient(i, url, num_messages, feed_freq, min_len, max_len, interval, jitter)
        for i in range(num_clients)
    ]

    t0 = time.time()
    connector = aiohttp.TCPConnector(limit=num_clients * 2, keepalive_timeout=60)
    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = []
        for i, client in enumerate(clients):
            async def launch(c: HTTPSimulatedClient, delay: float):
                if delay > 0:
                    await asyncio.sleep(delay)
                await c.run(session)

            tasks.append(asyncio.create_task(launch(client, i * ramp_delay)))

        # Progress monitor
        async def monitor():
            while not all(t.done() for t in tasks):
                done = sum(1 for c in clients if c.end_time > 0)
                sent = sum(c.messages_sent for c in clients)
                errs = sum(len(c.errors) for c in clients)
                print(f"\r  [Running] Clients done: {done}/{num_clients} | Messages sent: {sent} | Errors: {errs}", end="", flush=True)
                await asyncio.sleep(0.4)
            print()

        mon = asyncio.create_task(monitor())
        await asyncio.gather(*tasks)
        mon.cancel()

    duration = time.time() - t0

    all_posts = []
    all_feeds = []
    total_sent = 0
    total_feeds = 0
    total_errs = 0

    for c in clients:
        all_posts.extend(c.post_latencies)
        all_feeds.extend(c.feed_latencies)
        total_sent += c.messages_sent
        total_feeds += c.feeds_fetched
        total_errs += len(c.errors)

    post_stats = calc_percentiles(all_posts)
    feed_stats = calc_percentiles(all_feeds)
    rps = round((total_sent + total_feeds) / duration, 2) if duration > 0 else 0

    results = {
        "config": {
            "mode": "http",
            "url": url,
            "num_clients": num_clients,
            "num_messages_per_client": num_messages,
            "min_length": min_len,
            "max_length": max_len,
            "interval_s": interval,
            "jitter": jitter,
        },
        "summary": {
            "duration_s": round(duration, 2),
            "total_messages_sent": total_sent,
            "total_feeds_fetched": total_feeds,
            "total_requests": total_sent + total_feeds,
            "throughput_rps": rps,
            "total_errors": total_errs,
        },
        "post_message_latency_ms": post_stats,
        "get_feed_latency_ms": feed_stats,
    }

    print(f"\n{'='*70}")
    print("  📊 EVALUATION RESULTS SUMMARY")
    print(f"{'='*70}")
    print(f"  Duration           : {results['summary']['duration_s']} s")
    print(f"  Total Requests     : {results['summary']['total_requests']} (Sent: {total_sent}, Feeds: {total_feeds})")
    print(f"  Throughput         : {rps} req/sec")
    print(f"  Total Errors       : {total_errs}")
    print(f"{'─'*70}")
    print(f"  {'Metric':<18} {'POST /message':>20} {'GET /feed':>20}")
    print(f"  {'─'*18} {'─'*20} {'─'*20}")
    for k in ["avg", "p50", "p90", "p95", "p99", "min", "max"]:
        print(f"  {k.upper():<18} {post_stats[k]:>17.2f} ms {feed_stats[k]:>17.2f} ms")
    print(f"  {'Samples':<18} {post_stats['count']:>20} {feed_stats['count']:>20}")
    print(f"{'='*70}\n")

    # Save results
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_file = f"results_http_{ts}.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"  📁 Output saved to: {out_file}\n")
    return results


async def run_ws_load_test(
    url: str,
    num_clients: int,
    num_messages: int,
    min_len: int,
    max_len: int,
    interval: float,
    jitter: float,
    ramp_delay: float,
) -> Dict[str, Any]:
    print(f"\n{'='*70}")
    print("  🚀 WEBSOCKET LOAD GENERATOR (/ws)")
    print(f"{'='*70}")
    print(f"  Target URL           : {url}")
    print(f"  Concurrent Clients   : {num_clients}")
    print(f"  Messages / Client    : {num_messages}")
    print(f"  Message Length       : {min_len} to {max_len} chars")
    print(f"  Interval             : {interval}s ± {int(jitter*100)}% jitter")
    print(f"{'='*70}\n")

    clients = [
        WSSimulatedClient(i, url, num_messages, min_len, max_len, interval, jitter)
        for i in range(num_clients)
    ]

    t0 = time.time()
    tasks = []
    for i, c in enumerate(clients):
        async def launch(client: WSSimulatedClient, delay: float):
            if delay > 0:
                await asyncio.sleep(delay)
            await client.run()

        tasks.append(asyncio.create_task(launch(c, i * ramp_delay)))

    await asyncio.gather(*tasks)
    duration = time.time() - t0

    all_lat = []
    all_joins = []
    total_sent = 0
    total_errs = 0

    for c in clients:
        all_lat.extend(c.msg_latencies)
        if c.join_latency_ms is not None:
            all_joins.append(c.join_latency_ms)
        total_sent += c.messages_sent
        total_errs += len(c.errors)

    join_stats = calc_percentiles(all_joins)
    msg_stats = calc_percentiles(all_lat)
    rps = round(total_sent / duration, 2) if duration > 0 else 0

    results = {
        "config": {
            "mode": "ws",
            "url": url,
            "num_clients": num_clients,
            "num_messages_per_client": num_messages,
        },
        "summary": {
            "duration_s": round(duration, 2),
            "total_messages_sent": total_sent,
            "throughput_msgs_per_sec": rps,
            "total_errors": total_errs,
        },
        "join_latency_ms": join_stats,
        "message_relay_latency_ms": msg_stats,
    }

    print(f"\n{'='*70}")
    print("  📊 WEBSOCKET EVALUATION SUMMARY")
    print(f"{'='*70}")
    print(f"  Duration           : {results['summary']['duration_s']} s")
    print(f"  Throughput         : {rps} msg/sec")
    print(f"  Total Errors       : {total_errs}")
    print(f"{'─'*70}")
    print(f"  {'Metric':<18} {'WS Handshake':>20} {'Message Relay':>20}")
    print(f"  {'─'*18} {'─'*20} {'─'*20}")
    for k in ["avg", "p50", "p90", "p95", "p99", "min", "max"]:
        print(f"  {k.upper():<18} {join_stats[k]:>17.2f} ms {msg_stats[k]:>17.2f} ms")
    print(f"{'='*70}\n")

    ts = time.strftime("%Y%m%d_%H%M%S")
    out_file = f"results_ws_{ts}.json"
    with open(out_file, "w") as f:
        json.dump(results, f, indent=2)
    print(f"  📁 Output saved to: {out_file}\n")
    return results


def main():
    parser = argparse.ArgumentParser(description="Performance Load Generator for Group Chat")
    parser.add_argument("--mode", choices=["http", "ws"], default="http", help="Protocol mode (default: http)")
    parser.add_argument("--url", default="http://localhost:4000", help="Load Balancer URL (default: http://localhost:4000)")
    parser.add_argument("--clients", type=int, default=30, help="Number of concurrent clients (default: 30)")
    parser.add_argument("--messages", type=int, default=15, help="Number of messages per client (default: 15)")
    parser.add_argument("--feed-freq", type=int, default=3, help="Fetch GET /feed every N messages (default: 3)")
    parser.add_argument("--min-len", type=int, default=10, help="Min message length (default: 10)")
    parser.add_argument("--max-len", type=int, default=120, help="Max message length (default: 120)")
    parser.add_argument("--interval", type=float, default=0.1, help="Base time interval between messages in seconds (default: 0.1)")
    parser.add_argument("--jitter", type=float, default=0.5, help="Random interval jitter fraction 0..1 (default: 0.5)")
    parser.add_argument("--ramp-delay", type=float, default=0.02, help="Ramp up stagger delay between clients (default: 0.02)")
    args = parser.parse_args()

    if args.mode == "http":
        asyncio.run(run_http_load_test(
            url=args.url,
            num_clients=args.clients,
            num_messages=args.messages,
            feed_freq=args.feed_freq,
            min_len=args.min_len,
            max_len=args.max_len,
            interval=args.interval,
            jitter=args.jitter,
            ramp_delay=args.ramp_delay,
        ))
    else:
        ws_url = args.url
        if ws_url.startswith("http://"):
            ws_url = ws_url.replace("http://", "ws://") + "/ws"
        elif ws_url.startswith("https://"):
            ws_url = ws_url.replace("https://", "wss://") + "/ws"
        asyncio.run(run_ws_load_test(
            url=ws_url,
            num_clients=args.clients,
            num_messages=args.messages,
            min_len=args.min_len,
            max_len=args.max_len,
            interval=args.interval,
            jitter=args.jitter,
            ramp_delay=args.ramp_delay,
        ))


if __name__ == "__main__":
    main()
