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

async def fetch_worker_chunk(session, chunk_id):
    try:
        async with session.get(f"{WORKER_URL}?mode=full&chunk={chunk_id}", timeout=10) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("records", [])
    except Exception:
        pass
    return []

async def run_full_scan(session):
    global first_run, last_full_scan_time
    try:
        chunk1, chunk2 = await asyncio.gather(
            fetch_worker_chunk(session, 1),
            fetch_worker_chunk(session, 2)
        )
        products = chunk1 + chunk2

        if len(products) < MIN_EXPECTED_ITEMS and not first_run:
            print(f"⚠️ Warning: Network drop detected (only {len(products)} items). Skipping.")
            return

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
                    print(f"🔥 NEW DROP: {title}")
                    send_discord_alert(title, price, link, image_url, "NEW DROP")
                elif inventory_state[prod_id] == 0 and current_stock > 0:
                    print(f"🔄 RESTOCK/RETURN: {title}")
                    send_discord_alert(title, price, link, image_url, "RESTOCK")

            inventory_state[prod_id] = current_stock

        last_full_scan_time = time.time()
        if first_run:
            print(f"✅ Baseline initialized! Tracking {len(inventory_state)} items.")
            first_run = False
            
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
    print("Starting Optimized Sentinel + Heartbeat Bot...")
    async with aiohttp.ClientSession() as session:
        print("Running initial full catalog sync...")
        await run_full_scan(session)
        while True:
            start_time = time.time()
            try:
                async with session.get(f"{WORKER_URL}?mode=sentinel", timeout=3) as resp:
                    if resp.status == 200:
                        sentinel_data = await resp.json()
                        current_total = sentinel_data.get("total")
                        current_top_ids = sentinel_data.get("top_ids", [])
                        
                        trigger_scan = (last_total is not None) and (current_total != last_total or current_top_ids != last_top_ids)
                        heartbeat_due = (time.time() - last_full_scan_time) > 60

                        if trigger_scan or heartbeat_due:
                            reason = "Change Detected!" if trigger_scan else "60s Deep Sweep"
                            print(f"⚡ Triggering Full Scan ({reason})")
                            await run_full_scan(session)

                        last_total = current_total
                        last_top_ids = current_top_ids
            except Exception:
                pass

            elapsed = time.time() - start_time
            await asyncio.sleep(max(0.05, 0.3 - elapsed))

async def main():
    # Runs the web server and the bot loop simultaneously
    await asyncio.gather(
        start_server(),
        bot_loop()
    )

if __name__ == "__main__":
    asyncio.run(main())