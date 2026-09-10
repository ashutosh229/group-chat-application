"""
Benchmark & Plotting Tool for Group Chat Dynamic Load Balancer Report.
Generates evaluation graphs (Response Time, Throughput, Backend Load Distribution)
and saves high-resolution figures for report inclusion.
"""

import argparse
import asyncio
import json
import os
import sys
import time
from typing import List, Dict, Any

if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import numpy as np

# Try importing the load generator runner
sys.path.insert(0, os.path.dirname(__file__))
try:
    from load_generator import run_http_load_test
except ImportError:
    pass


async def run_client_sweep(url: str, client_counts: List[int], msgs_per_client: int = 15) -> Dict[str, Any]:
    """Run load generator across varying client counts and record metrics."""
    sweep_results = []
    for c in client_counts:
        print(f"\n>>> Running evaluation with {c} concurrent clients...")
        res = await run_http_load_test(
            url=url,
            num_clients=c,
            num_messages=msgs_per_client,
            feed_freq=3,
            min_len=20,
            max_len=100,
            interval=0.08,
            jitter=0.4,
            ramp_delay=0.02,
        )
        sweep_results.append({
            "clients": c,
            "throughput": res["summary"]["throughput_rps"],
            "post_avg_ms": res["post_message_latency_ms"]["avg"],
            "post_p50_ms": res["post_message_latency_ms"]["p50"],
            "post_p95_ms": res["post_message_latency_ms"]["p95"],
            "post_p99_ms": res["post_message_latency_ms"]["p99"],
            "feed_avg_ms": res["get_feed_latency_ms"]["avg"],
            "feed_p95_ms": res["get_feed_latency_ms"]["p95"],
            "errors": res["summary"]["total_errors"],
        })
        await asyncio.sleep(1.0)
    return {"sweep": sweep_results}


def generate_plots(data: Dict[str, Any], output_dir: str = "."):
    """Generate professional evaluation plots for the assignment report."""
    os.makedirs(output_dir, exist_ok=True)
    sweep = data.get("sweep", [])
    if not sweep:
        print("No sweep data available to plot.")
        return

    clients = [x["clients"] for x in sweep]
    post_avg = [x["post_avg_ms"] for x in sweep]
    post_p95 = [x["post_p95_ms"] for x in sweep]
    post_p99 = [x["post_p99_ms"] for x in sweep]
    feed_avg = [x["feed_avg_ms"] for x in sweep]
    throughput = [x["throughput"] for x in sweep]

    # Style configuration
    plt.style.use('seaborn-v0_8-whitegrid' if 'seaborn-v0_8-whitegrid' in plt.style.available else 'default')
    colors = ['#0284c7', '#8b5cf6', '#ec4899', '#10b981', '#f59e0b']

    # 1. Response Time vs Concurrency Plot
    plt.figure(figsize=(9, 5.5), dpi=300)
    plt.plot(clients, post_avg, marker='o', linewidth=2.2, color='#0284c7', label='POST /message (Avg Latency)')
    plt.plot(clients, post_p95, marker='s', linewidth=2.0, linestyle='--', color='#8b5cf6', label='POST /message (P95 Latency)')
    plt.plot(clients, post_p99, marker='^', linewidth=1.8, linestyle=':', color='#ec4899', label='POST /message (P99 Latency)')
    plt.plot(clients, feed_avg, marker='D', linewidth=2.0, color='#10b981', label='GET /feed (Avg Latency)')

    plt.title('Response Time vs. Concurrent Clients (Dynamic Load Balancer)', fontsize=13, fontweight='bold', pad=12)
    plt.xlabel('Number of Concurrent Simulated Clients', fontsize=11, fontweight='600')
    plt.ylabel('Response Time (ms)', fontsize=11, fontweight='600')
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend(frameon=True, facecolor='#ffffff', edgecolor='#cbd5e1', fontsize=10)
    plt.tight_layout()
    latency_plot_path = os.path.join(output_dir, 'response_time_vs_clients.png')
    plt.savefig(latency_plot_path)
    plt.close()
    print(f"  📊 Generated: {latency_plot_path}")

    # 2. Throughput (RPS) vs Concurrency Plot
    plt.figure(figsize=(9, 5.5), dpi=300)
    plt.plot(clients, throughput, marker='o', linewidth=2.5, color='#0ea5e9', label='Throughput (Req/sec)')
    plt.fill_between(clients, throughput, alpha=0.15, color='#0ea5e9')

    plt.title('System Throughput vs. Concurrent Clients', fontsize=13, fontweight='bold', pad=12)
    plt.xlabel('Number of Concurrent Simulated Clients', fontsize=11, fontweight='600')
    plt.ylabel('Throughput (Requests / Second)', fontsize=11, fontweight='600')
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.legend(frameon=True, facecolor='#ffffff', edgecolor='#cbd5e1', fontsize=10)
    plt.tight_layout()
    throughput_plot_path = os.path.join(output_dir, 'throughput_vs_clients.png')
    plt.savefig(throughput_plot_path)
    plt.close()
    print(f"  📊 Generated: {throughput_plot_path}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark and Plot Generator for Dynamic Load Balancer")
    parser.add_argument("--url", default="http://localhost:4000", help="Load Balancer URL")
    parser.add_argument("--clients", default="10,25,50,75,100", help="Comma-separated list of client loads to sweep")
    parser.add_argument("--messages", type=int, default=15, help="Messages per client")
    parser.add_argument("--out-dir", default=".", help="Output directory for plots and JSON")
    args = parser.parse_args()

    client_counts = [int(c.strip()) for c in args.clients.split(",") if c.strip()]
    data = asyncio.run(run_client_sweep(args.url, client_counts, args.messages))
    
    # Save sweep data
    sweep_file = os.path.join(args.out_dir, "benchmark_sweep_results.json")
    with open(sweep_file, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\n  📁 Sweep data saved to: {sweep_file}")

    # Generate plots
    generate_plots(data, args.out_dir)


if __name__ == "__main__":
    main()
