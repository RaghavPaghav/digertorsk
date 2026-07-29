import asyncio
import aiohttp
from aiohttp import web
import time
import math
import os

# CONFIGURATION
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1531974113067929681/0lJqevpqVFh7Y8XZOSM2CVW_f9lnv-kJfcY48FG8BRytfTqm-Ea56IMyy2d2sKs9fk4s"
BASE_URL = "https://cssdeals.com/api/product?fields=1&page={}&pageSize=100&sortBy=stock&order=desc"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://cssdeals.com/"
}

TOP_PAGES_FOR_DROPS = 20   # Pages 1-20 checked every 1.0s for new drops
SAFETY_SWEEP_INTERVAL = 60 # Force a full catalog sweep every 60s as a fail-safe

async def send_discord_alert(session, alert_type, title, item_id, qty, price=None, source_link=None, image_url=None):
    """Sends a non-blocking Discord embed notification."""
    if not DISCORD_WEBHOOK_URL:
        return

    is_drop = "DROP" in alert_type
    color = 3066993 if is_drop else 15158332  # Green for Drop, Orange for Restock
    header_title = "🌟 NEW PRODUCT DROP" if is_drop else "🚨 WAREHOUSE RESTOCK"

    embed = {
        "title": f"{header_title}: {title[:200]}",
        "url": source_link if source_link else "https://cssdeals.com/",
        "color": color,
        "fields": [
            {"name": "Product ID", "value": f"`{item_id}`", "inline": True},
            {"name": "Quantity", "value": f"**{qty}**", "inline": True},
        ],
        "footer": {"text": f"CSSDeals Radar • {alert_type}"},
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

    if price:
        embed["fields"].append({"name": "Price", "value": f"¥{price}", "inline": True})

    if image_url:
        if image_url.startswith("//"):
            image_url = "https:" + image_url
        embed["thumbnail"] = {"url": image_url}

    payload = {
        "username": "CSSDeals Monitor",
        "avatar_url": "https://cssdeals.com/favicon.ico",
        "embeds": [embed]
    }

    try:
        async with session.post(
            DISCORD_WEBHOOK_URL, 
            json=payload, 
            timeout=aiohttp.ClientTimeout(total=2.0)
        ) as res:
            if res.status not in (200, 204):
                print(f"[-] Discord Webhook failed with status: {res.status}")
    except Exception as e:
        print(f"[-] Failed to send Discord alert: {e}")

class AllPagesRadar:
    def __init__(self):
        self.known_inventory = {}
        self.current_total = 0
        self.total_pages = 1
        self.state_lock = asyncio.Lock()
        self.is_sweeping = False

    async def fetch_page(self, session, page_num):
        url = BASE_URL.format(page_num)
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=1.5)) as response:
                if response.status == 200:
                    return await response.json()
        except Exception:
            pass
        return None

    async def update_inventory_state(self, session, new_items, source_label="FAST-LOOP", is_initialization=False):
        async with self.state_lock:
            found_changes = False
            for item_id, current_data in new_items.items():
                old_data = self.known_inventory.get(item_id)

                # SILENT INITIALIZATION: Prevents Discord spam on startup
                if is_initialization:
                    self.known_inventory[item_id] = current_data
                    continue

                # Case 1: Stock increased -> WAREHOUSE RESTOCK / RETURN
                if old_data and current_data["qty"] > old_data["qty"]:
                    print(f"\n[{source_label}] 🚨 RESTOCK ALERT: '{current_data['title']}' (Qty: {old_data['qty']} -> {current_data['qty']})")
                    
                    asyncio.create_task(
                        send_discord_alert(
                            session,
                            alert_type=f"{source_label} RESTOCK",
                            title=current_data["title"],
                            item_id=item_id,
                            qty=current_data["qty"],
                            price=current_data.get("price"),
                            source_link=current_data.get("sourceLink"),
                            image_url=current_data.get("image")
                        )
                    )
                    self.known_inventory[item_id] = current_data
                    found_changes = True

                # Case 2: Brand new ID -> NEW PRODUCT DROP
                elif not old_data and current_data["qty"] > 0:
                    print(f"\n[{source_label}] 🌟 NEW DROP DETECTED: '{current_data['title']}' (Qty: {current_data['qty']})")
                    
                    asyncio.create_task(
                        send_discord_alert(
                            session,
                            alert_type=f"{source_label} DROP",
                            title=current_data["title"],
                            item_id=item_id,
                            qty=current_data["qty"],
                            price=current_data.get("price"),
                            source_link=current_data.get("sourceLink"),
                            image_url=current_data.get("image")
                        )
                    )
                    self.known_inventory[item_id] = current_data
                    found_changes = True

                elif not old_data:
                    self.known_inventory[item_id] = current_data

            return found_changes

    async def background_all_pages_sweep(self, session):
        if self.is_sweeping:
            return
        
        self.is_sweeping = True
        start_time = time.time()
        print(f"\n[ALL-PAGES SWEEP] Scanning Pages {TOP_PAGES_FOR_DROPS + 1} to {self.total_pages}...")

        try:
            tasks = [
                self.fetch_page(session, page) 
                for page in range(TOP_PAGES_FOR_DROPS + 1, self.total_pages + 1)
            ]
            
            deep_items = {}
            results = await asyncio.gather(*tasks)

            for data in results:
                if not data or "data" not in data or not data["data"].get("records"):
                    continue
                for item in data["data"]["records"]:
                    item_id = item["id"]
                    sku_info = item["skus"][0] if item.get("skus") else {}
                    
                    deep_items[item_id] = {
                        "title": item.get("title", "Unknown"),
                        "qty": int(sku_info.get("quantity", 0)),
                        "price": sku_info.get("price"),
                        "image": sku_info.get("image") or item.get("thumbnail"),
                        "sourceLink": item.get("sourceLink")
                    }

            await self.update_inventory_state(session, deep_items, source_label="DEEP-SWEEP")
            elapsed = time.time() - start_time
            print(f"[ALL-PAGES SWEEP] Finished in {elapsed:.3f}s | Total Tracked: {len(self.known_inventory)}\n")
        finally:
            self.is_sweeping = False

    async def scan_top_pages_for_drops(self, session):
        tasks = [self.fetch_page(session, page) for page in range(1, TOP_PAGES_FOR_DROPS + 1)]
        results = await asyncio.gather(*tasks)

        top_items = {}
        page_1_data = None

        for idx, data in enumerate(results):
            if not data or "data" not in data:
                continue
            if idx == 0:
                page_1_data = data
            
            for item in data.get("data", {}).get("records", []):
                item_id = item["id"]
                sku_info = item["skus"][0] if item.get("skus") else {}
                
                top_items[item_id] = {
                    "title": item.get("title", "Unknown"),
                    "qty": int(sku_info.get("quantity", 0)),
                    "price": sku_info.get("price"),
                    "image": sku_info.get("image") or item.get("thumbnail"),
                    "sourceLink": item.get("sourceLink")
                }

        found_in_top = await self.update_inventory_state(session, top_items, source_label="TOP-10 DROPS")
        return page_1_data, found_in_top

    async def run(self):
        connector = aiohttp.TCPConnector(
            limit=110,
            limit_per_host=110,
            keepalive_timeout=60,
            ttl_dns_cache=300
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            print("--- [1/2] Initializing Baseline State (Silent Startup) ---")
            page_1 = await self.fetch_page(session, 1)
            if not page_1 or "data" not in page_1:
                print("[-] Could not connect to API.")
                return

            self.current_total = int(page_1["data"]["total"])
            self.total_pages = math.ceil(self.current_total / 100)
            
            # Silent baseline initialization across all pages
            start_tasks = [self.fetch_page(session, p) for p in range(1, self.total_pages + 1)]
            initial_items = {}
            for data in await asyncio.gather(*start_tasks):
                if data and "data" in data:
                    for item in data.get("data", {}).get("records", []):
                        item_id = item["id"]
                        sku_info = item["skus"][0] if item.get("skus") else {}
                        initial_items[item_id] = {
                            "title": item.get("title", "Unknown"),
                            "qty": int(sku_info.get("quantity", 0)),
                            "price": sku_info.get("price"),
                            "image": sku_info.get("image") or item.get("thumbnail"),
                            "sourceLink": item.get("sourceLink")
                        }

            await self.update_inventory_state(session, initial_items, is_initialization=True)
            print(f"[*] Baseline established silently: {self.current_total} items tracked.")
            print(f"--- [2/2] Radar Armed. Monitoring Discord Webhook ---\n")

            last_safety_sweep = time.time()

            while True:
                time_start = time.time()

                page_1_data, found_in_top = await self.scan_top_pages_for_drops(session)

                if page_1_data and "data" in page_1_data:
                    new_total = int(page_1_data["data"]["total"])
                    time_since_last_sweep = time.time() - last_safety_sweep

                    if (new_total > self.current_total and not found_in_top) or (time_since_last_sweep > SAFETY_SWEEP_INTERVAL):
                        
                        if time_since_last_sweep > SAFETY_SWEEP_INTERVAL:
                            print("[SAFETY-NET] Running 60s scheduled full sweep...")
                            last_safety_sweep = time.time()
                        else:
                            print(f"[TRIPWIRE] Total jumped by +{new_total - self.current_total}!")

                        self.current_total = new_total
                        self.total_pages = math.ceil(self.current_total / 100)
                        
                        asyncio.create_task(self.background_all_pages_sweep(session))
                    
                    elif new_total < self.current_total:
                        self.current_total = new_total

                elapsed = time.time() - time_start
                await asyncio.sleep(max(0, 1.0 - elapsed))

# --- DUMMY WEB SERVER (KEEPS RENDER ALIVE) ---
async def health_check(request):
    """Returns HTTP 200 so UptimeRobot can keep the container awake."""
    return web.Response(text="CSSDeals Radar is active and monitoring.")

async def main_with_server():
    radar = AllPagesRadar()
    
    # Run the radar loop as an independent background task
    asyncio.create_task(radar.run())
    
    # Start the dummy HTTP server on the port Render assigns
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/health", health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    print(f"[+] Web server active on port {port}.")
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_with_server())