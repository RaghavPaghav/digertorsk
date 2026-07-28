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
last_drop_time = 0  
is_scanning = False  
MIN_EXPECTED_ITEMS = 9000  

PAGE_RANGES = [
    (1, 10), (11, 20), (21, 30), (31, 40), (41, 50),
    (51, 60), (61, 70), (71, 80), (81, 90), (91, 97)
]

def send_discord_alert(title, price, css_link, orig_link, image_url, alert_type):
    fields = [{"name": "Price", "value": f"¥{price}", "inline": True}]
    if orig_link and orig_link.startswith("http"):
        fields.append({"name": "Original Link", "value": f"[View Original]({orig_link})", "inline": True})
        
    data = {
        "embeds": [{
            "title": f"🚨 {alert_type} 🚨",
            "description": title,
            "url": css_link, 
            "color": 16711680 if alert_type == "NEW DROP" else 65280,
            "fields": fields,
            "thumbnail": {"url": image_url}
        }]
    }
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=data)
    except Exception:
        pass

def process_products_list(products, source="background"):
    global first_run
    new_items_found = False
    alerts_sent_this_batch = 0
    MAX_ALERTS = 5 # SPAM SHIELD: Never send more than 5 alerts at once
    
    for product in products:
        prod_id = str(product["id"])
        title = product.get("title", "No Title")
        css_link = f"https://cssdeals.com/product-detail.html?itemid={prod_id}"
        orig_link = product.get("sourceLink", "")
        
        if product.get("skus") and len(product["skus"]) > 0:
            price = product["skus"][0].get("price", "N/A")
            image_url = product["skus"][0].get("image", "")
            current_stock = sum(int(sku.get("quantity", 0)) for sku in product["skus"])
        else:
            continue

        if not first_run:
            if prod_id not in inventory_state:
                # ONLY allow 'NEW DROP' alerts if they were found on Page 1 (Sentinel)
                # If found in background, it just means a previous scan missed it. Add quietly.
                if source == "sentinel":
                    if alerts_sent_this_batch < MAX_ALERTS:
                        print(f"🔥 INSTANT NEW DROP: {title}")
                        send_discord_alert(title, price, css_link, orig_link, image_url, "NEW DROP")
                        alerts_sent_this_batch += 1
                    new_items_found = True
            elif inventory_state.get(prod_id, 0) == 0 and current_stock > 0:
                # Restocks can happen anywhere, so we allow background to alert this
                if alerts_sent_this_batch < MAX_ALERTS:
                    print(f"🔄 RESTOCK/RETURN: {title}")
                    send_discord_alert(title, price, css_link, orig_link, image_url, "RESTOCK")
                    alerts_sent_this_batch += 1
                new_items_found = True

        # Always update the database so we have accurate stock
        inventory_state[prod_id] = current_stock

    if alerts_sent_this_batch >= MAX_ALERTS:
        print("⚠️ Spam Shield Activated: Suppressed further alerts for this batch.")

    return new_items_found

async def fetch_worker_range(session, start, end):
    try:
        async with session.get(f"{WORKER_URL}?mode=full&start={start}&end={end}", timeout=15) as resp:
            if resp.status == 200:
                data = await resp.json()
                return data.get("records", [])
    except Exception:
        pass
    return []

async def run_full_scan(session):
    global first_run, last_full_scan_time, is_scanning, last_drop_time
    start_time = time.time()
    try:
        tasks = [fetch_worker_range(session, start, end) for start, end in PAGE_RANGES]
        results = await asyncio.gather(*tasks)
        
        products = [item for chunk in results for item in chunk]

        if len(products) < MIN_EXPECTED_ITEMS:
            if first_run:
                print(f"❌ Baseline setup failed (Only got {len(products)} items). Retrying...")
            else:
                print(f"⚠️ Warning: Network drop detected (only {len(products)} items). Skipping update.")
            return

        # source="background" means it is forbidden from triggering "NEW DROP" alerts
        found_new = process_products_list(products, source="background")
        
        if found_new:
            last_drop_time = time.time()

        elapsed = time.time() - start_time
        last_full_scan_time = time.time()
        
        if first_run:
            print(f"✅ Baseline initialized! Tracking {len(inventory_state)} items in {elapsed:.2f}s.")
            first_run = False
            
    except Exception as e:
        print(f"Full scan error: {e}")
    finally:
        is_scanning = False

async def background_scan(session):
    global is_scanning
    is_scanning = True
    await run_full_scan(session)

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
    global last_total, last_top_ids, is_scanning, last_full_scan_time, last_drop_time
    print("Starting Apex Bot with Spam Shields & Adrenaline Mode...")
    async with aiohttp.ClientSession() as session:
        print("Running initial full catalog sync...")
        await run_full_scan(session)
        
        while first_run:
            await asyncio.sleep(2)
            await run_full_scan(session)

        while True:
            loop_start = time.time()
            try:
                async with session.get(f"{WORKER_URL}?mode=sentinel", timeout=5) as resp:
                    if resp.status == 200:
                        sentinel_data = await resp.json()
                        
                        if sentinel_data.get("worker_error"):
                            await asyncio.sleep(5)
                            continue

                        current_total = sentinel_data.get("total")
                        page_1_records = sentinel_data.get("records", [])
                        current_top_ids = [p["id"] for p in page_1_records]

                        if not first_run:
                            # source="sentinel" means this IS allowed to trigger "NEW DROP" alerts
                            found_new = process_products_list(page_1_records, source="sentinel")
                            if found_new:
                                last_drop_time = time.time() 

                        trigger_scan = (last_total is not None) and (current_total != last_total or current_top_ids != last_top_ids)
                        
                        is_adrenaline = (time.time() - last_drop_time) < 300 
                        
                        cooldown_limit = 2 if is_adrenaline else 15
                        heartbeat_limit = 15 if is_adrenaline else 60

                        time_since_last_scan = time.time() - last_full_scan_time
                        cooldown_cleared = time_since_last_scan > cooldown_limit
                        heartbeat_due = time_since_last_scan > heartbeat_limit

                        if not is_scanning:
                            if trigger_scan and cooldown_cleared:
                                prefix = "🔥 ADRENALINE:" if is_adrenaline else "⚡"
                                print(f"{prefix} Change Detected! Background scan running...")
                                asyncio.create_task(background_scan(session))
                            elif heartbeat_due:
                                prefix = "🔥 ADRENALINE SWEEP:" if is_adrenaline else "⚡ Normal Sweep:"
                                print(f"{prefix} Checking for hidden restocks...")
                                asyncio.create_task(background_scan(session))

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
