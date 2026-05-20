import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
from datetime import datetime, timezone

import nats


def percentile(values, q):
    if not values:
        return float("nan")
    values = sorted(values)
    idx = min(len(values) - 1, max(0, round((len(values) - 1) * q)))
    return values[idx]


async def wait_for(predicate, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0)
    raise TimeoutError("timed out waiting for benchmark condition")


async def connect(url, **kwargs):
    return await nats.connect(url, connect_timeout=2, **kwargs)


async def bench_publish_batch(url, subject, payload, messages, timeout):
    nc = await connect(url, pending_size=max(2 * 1024 * 1024, messages * (len(payload) + 64)))
    try:
        warmup = min(messages, 1000)
        for _ in range(warmup):
            await nc.publish(subject, payload)
        await nc.flush(timeout)

        started = time.perf_counter()
        for _ in range(messages):
            await nc.publish(subject, payload)
        await nc.flush(timeout)
        seconds = time.perf_counter() - started
        return {
            "messages": messages,
            "seconds": seconds,
            "messages_per_second": messages / seconds,
            "payload_mib_per_second": messages * len(payload) / seconds / 1024**2,
        }
    finally:
        await nc.close()


async def bench_publish_flush_each(url, subject, payload, messages, timeout):
    nc = await connect(url)
    try:
        warmup = min(messages, 100)
        for _ in range(warmup):
            await nc.publish(subject, payload)
            await nc.flush(timeout)

        started = time.perf_counter()
        for _ in range(messages):
            await nc.publish(subject, payload)
            await nc.flush(timeout)
        seconds = time.perf_counter() - started
        return {
            "messages": messages,
            "seconds": seconds,
            "messages_per_second": messages / seconds,
            "payload_mib_per_second": messages * len(payload) / seconds / 1024**2,
        }
    finally:
        await nc.close()


async def bench_callback_dispatch(url, subject, payload, messages, timeout):
    sub_client = await connect(url)
    pub_client = await connect(url, pending_size=max(2 * 1024 * 1024, messages * (len(payload) + 64)))
    received = 0

    async def cb(_msg):
        nonlocal received
        received += 1

    try:
        await sub_client.subscribe(subject, cb=cb)
        await sub_client.flush(timeout)

        warmup = min(messages, 1000)
        for _ in range(warmup):
            await pub_client.publish(subject, payload)
        await pub_client.flush(timeout)
        await wait_for(lambda: received >= warmup, timeout)
        received = 0

        started = time.perf_counter()
        for _ in range(messages):
            await pub_client.publish(subject, payload)
        await pub_client.flush(timeout)
        await wait_for(lambda: received >= messages, timeout)
        seconds = time.perf_counter() - started
        return {
            "messages": messages,
            "received": received,
            "seconds": seconds,
            "messages_per_second": messages / seconds,
        }
    finally:
        await pub_client.close()
        await sub_client.close()


async def bench_request_reply(url, subject, payload, requests, timeout):
    service = await connect(url)
    client = await connect(url)

    async def cb(msg):
        await service.publish(msg.reply, payload)

    try:
        await service.subscribe(subject, cb=cb)
        await service.flush(timeout)

        warmup = min(requests, 200)
        for _ in range(warmup):
            await client.request(subject, payload, timeout=timeout)

        latencies = []
        started = time.perf_counter()
        for _ in range(requests):
            request_started = time.perf_counter()
            await client.request(subject, payload, timeout=timeout)
            latencies.append((time.perf_counter() - request_started) * 1000)
        seconds = time.perf_counter() - started
        return {
            "requests": requests,
            "seconds": seconds,
            "requests_per_second": requests / seconds,
            "latency_ms_mean": statistics.mean(latencies),
            "latency_ms_p50": percentile(latencies, 0.50),
            "latency_ms_p95": percentile(latencies, 0.95),
            "latency_ms_p99": percentile(latencies, 0.99),
            "latency_ms_max": max(latencies),
        }
    finally:
        await client.close()
        await service.close()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", required=True)
    parser.add_argument("--messages", type=int, default=200000)
    parser.add_argument("--requests", type=int, default=20000)
    parser.add_argument("--payload-bytes", type=int, default=64)
    parser.add_argument("--timeout", type=float, default=90.0)
    parser.add_argument("--json", required=True)
    args = parser.parse_args()

    payload = b"x" * args.payload_bytes
    prefix = f"python.perf.{time.time_ns()}"
    benchmarks = {
        "publish_batch": await bench_publish_batch(args.url, f"{prefix}.publish.batch", payload, args.messages, args.timeout),
        "publish_flush_each": await bench_publish_flush_each(args.url, f"{prefix}.publish.flush", payload, args.messages, args.timeout),
        "callback_dispatch": await bench_callback_dispatch(args.url, f"{prefix}.callback", payload, args.messages, args.timeout),
        "request_reply": await bench_request_reply(args.url, f"{prefix}.request", payload, args.requests, args.timeout),
    }
    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "environment": {
            "client": "nats.py",
            "python_version": sys.version.split()[0],
            "platform": platform.platform(),
            "url": args.url,
            "messages": str(args.messages),
            "requests": str(args.requests),
            "payload_bytes": str(args.payload_bytes),
            "flush_semantics": "server_round_trip",
        },
        "benchmarks": benchmarks,
    }
    with open(args.json, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)
        f.write("\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
