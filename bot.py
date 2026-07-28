import asyncio
import aiohttp
from aiohttp import web
import time
import requests
import os

# --- YOUR CREDENTIALS ---
WORKER_URL = "https://cssdeals.gamerraghav64.workers.dev"
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1531668743896301689/Gh2_KSjIcIpfCzzPp2sP_mnwWL9JAh1rSBBEJKJ-KIaN0ovxzwtoP6xkpq1zrS3c_QsA"

inventory_state = {}
last_total = None
last_top_ids = []
first_run = True
last_full_scan_time = 0
MIN_EXPECTED_ITEMS = 9000  

# 10 Parallel Ranges (~10 pages per worker)
PAGE_RANGES = [
    (1, 10), (11, 20), (21, 30), (31, 40), (41, 50),
    (51, 60), (61, 70), (71, 80), (81, 90), (91, 97)
]

def send_discord_alert(title, price, product_url, image_url, alert_type):
    data = {
        "embeds": [{
            "title": f"🚨 {alert_type} 🚨",
            "description": title,
            "url": product_url,
            "color": 16711680 if alert_type == "NEW DROP" else 65280,
            "fields": [{"name": "Price", "value": f"¥{price}", "inline": True}],
            "thumbnail": {"url": image_url}
        }]
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=data)
    except Exception:
        pass

def process_products_list(products):
    global first_run
    new_items_found = False
    
    for product in products:
        prod_id = product["id"]
        title = product["title"]
        link = product.get("sourceLink", "https://cssdeals.com")
        
        if product.get("skus") and len(product["skus"]) > 0:
            price = product["skus"][0]["price"]
            image_url = product["skus"][0]["image"]
            current_stock = int(product["skus"][0]["quantity"])
        else:
            continue

        if not first_run:
            if prod_id not in inventory_state:
                print(f"🔥 INSTANT NEW DROP: {title}")
                send_discord_alert(title, price, link, image_url, "NEW DROP")
                new_items_found = True
            elif inventory_state[prod_id] == 0 and current_stock > 0:
                print(f"🔄 RESTOCK/RETURN: {title}")
                send_discord_alert(title, price, link, image_url, "RESTOCK")
                new_items_found = True

        inventory_state[prod_id] = current_stock

    return new_items_found

async def fetch_worker_range(session, start, end):
    try:
        async with session.get(f"{WORKER_URL}?mode=full&start={start}&end={end}", timeout=15) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("records", [])
    except Exception as e:
        print(f"Range {start}-{end} error: {e}")
    return []

async def run_full_scan(session):
    global first_run, last_full_scan_time
    start_time = time.time()
    try:
        # Fire 10 requests to Cloudflare Workers simultaneously
        tasks = [fetch_worker_range(session, start, end) for start, end in PAGE_RANGES]
        results = await asyncio.gather(*tasks)
        
        products = [item for chunk in results for item in chunk]

        if len(products) < MIN_EXPECTED_ITEMS and not first_run:
            print(f"⚠️ Warning: Network drop detected (only {len(products)} items). Skipping.")
            return

        process_products_list(products)

        elapsed = time.time() - start_time
        last_full_scan_time = time.time()
        
        if first_run:
            print(f"✅ Baseline initialized! Tracking {len(inventory_state)} items in {elapsed:.2f}s.")
            first_run = False
        else:
            print(f"⚡ Full 97-page scan completed in {elapsed:.2f} seconds.")
            
    except Exception as e:
        print(f"Full scan error: {e}")

# --- WEB SERVER (KEEPS THE CLOUD AWAKE) ---
async def keep_alive(request):
    return web.Response(text="Bot is awake and scanning!")

async def start_server():
    app = web.Application()
    app.router.add_get('/', keep_alive)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"🌐 Cloud Keep-Alive Server running on port {port}")

# --- BOT LOOP ---
async def bot_loop():
    global last_total, last_top_ids
    print("Starting Ultra-Fast 10-Chunk Sentinel Bot...")
    async with aiohttp.ClientSession() as session:
        print("Running initial full catalog sync...")
        await run_full_scan(session)
        while True:
            loop_start = time.time()
            try:
                async with session.get(f"{WORKER_URL}?mode=sentinel", timeout=5) as resp:
                    if resp.status == 200:
                        sentinel_data = await resp.json()
                        
                        if sentinel_data.get("worker_error"):
                            print(f"⚠️ Worker Error: {sentinel_data.get('message')}")
                            await asyncio.sleep(5)
                            continue

                        current_total = sentinel_data.get("total")
                        page_1_records = sentinel_data.get("records", [])
                        current_top_ids = [p["id"] for p in page_1_records]

                        # Process Page 1 records immediately
                        if not first_run:
                            process_products_list(page_1_records)

                        trigger_scan = (last_total is not None) and (current_total != last_total or current_top_ids != last_top_ids)
                        heartbeat_due = (time.time() - last_full_scan_time) > 60

                        if trigger_scan or heartbeat_due:
                            reason = "Change Detected!" if trigger_scan else "60s Deep Sweep"
                            print(f"⚡ Running Full Scan ({reason})")
                            asyncio.create_task(run_full_scan(session))

                        last_total = current_total
                        last_top_ids = current_top_ids
            except Exception:
                pass

            elapsed = time.time() - loop_start
            await asyncio.sleep(max(0.1, 1.0 - elapsed))

async def main():
    await asyncio.gather(
        start_server(),
        bot_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())
