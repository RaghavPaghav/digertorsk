import asyncio
import aiohttp
from aiohttp import web
import time
import math
import os
import sys
import json

# Force immediate console output on Render
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(line_buffering=True)

# CONFIGURATION
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1531974113067929681/0lJqevpqVFh7Y8XZOSM2CVW_f9lnv-kJfcY48FG8BRytfTqm-Ea56IMyy2d2sKs9fk4s"
BASE_URL = "https://cssdeals.com/api/product?fields=1&page={}&pageSize=100"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Referer": "https://cssdeals.com/"
}

TOP_PAGES_FOR_DROPS = 10   # Pages 1-10 checked every 1.0s for new drops
SAFETY_SWEEP_INTERVAL = 60 # Force a full catalog sweep every 60s as a fail-safe

async def send_discord_alert(session, alert_type, title, item_id, qty, price=0.0, source_link=None, image_url=None):
    """Sends a formatted Discord embed with CSSDeals product link and special ¥0 highlighting."""
    if not DISCORD_WEBHOOK_URL:
        return

    is_drop = "DROP" in alert_type
    is_free_special = (float(price or 0.0) == 0.0)

    # Choose Header Title & Color
    if is_free_special:
        color = 16766720  # Bright Gold / Yellow for ¥0 Special Drops
        header_title = "🔥 SPECIAL / ¥0 FREE DROP" if is_drop else "🔥 SPECIAL / ¥0 RESTOCK"
    elif is_drop:
        color = 3066993   # Green for standard drop
        header_title = "🌟 NEW PRODUCT DROP"
    else:
        color = 15158332  # Orange for restock
        header_title = "🚨 WAREHOUSE RESTOCK"

    # Always link main header to CSSDeals item page
    cssdeals_url = f"https://cssdeals.com/item/{item_id}"
    price_display = "🔥 **¥0.00 (FREE/NEW)**" if is_free_special else f"**¥{price}**"

    embed = {
        "title": f"{header_title}: {title[:180]}",
        "url": cssdeals_url,
        "color": color,
        "fields": [
            {"name": "Product ID", "value": f"`{item_id}`", "inline": True},
            {"name": "Quantity", "value": f"**{qty}**", "inline": True},
            {"name": "Price", "value": price_display, "inline": True},
            {"name": "CSSDeals Link", "value": f"[Open in CSSDeals]({cssdeals_url})", "inline": True}
        ],
        "footer": {"text": f"CSSDeals Radar • {alert_type}"},
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    }

    # Add Original Source/Taobao Link if available
    if source_link and source_link.startswith("http"):
        embed["fields"].append({"name": "Original Link", "value": f"[View Source Page]({source_link})", "inline": True})

    if image_url:
        if image_url.startswith("//"):
            image_url = "https:" + image_url
        elif not image_url.startswith("http"):
            image_url = None
        if image_url:
            embed["thumbnail"] = {"url": image_url}

    payload = {
        "username": "CSSDeals Radar Engine",
        "avatar_url": "https://cssdeals.com/favicon.ico",
        "embeds": [embed]
    }

    try:
        async with session.post(
            DISCORD_WEBHOOK_URL, 
            json=payload, 
            timeout=aiohttp.ClientTimeout(total=3.0)
        ) as res:
            if res.status not in (200, 204):
                err_text = await res.text()
                print(f"[-] Discord Webhook failed ({res.status}): {err_text}")
    except Exception as e:
        print(f"[-] Failed to send Discord alert: {e}")

class AllPagesRadar:
    def __init__(self):
        self.known_inventory = {}
        self.current_total = 0
        self.total_pages = 1
        self.state_lock = asyncio.Lock()
        self.is_sweeping = False
        
        # Dashboard & Status Metrics
        self.start_time = time.time()
        self.last_scan_time = None
        self.last_sweep_time = None
        self.last_tripwire_time = None
        self.scan_counter = 0
        self.status_message = "Initializing..."
        self.event_log = []

    def add_event(self, event_type, title, details, item_id=None):
        event = {
            "timestamp": time.strftime("%H:%M:%S", time.localtime()),
            "type": event_type,
            "title": title[:80],
            "details": details,
            "item_id": item_id
        }
        self.event_log.insert(0, event)
        self.event_log = self.event_log[:50]

    async def fetch_page(self, session, page_num, timeout_secs=2.5):
        url = BASE_URL.format(page_num)
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=timeout_secs)) as response:
                if response.status == 200:
                    return await response.json()
                elif page_num == 1:
                    print(f"[-] API Error (Page {page_num}): HTTP {response.status}")
        except asyncio.TimeoutError:
            if page_num == 1:
                print(f"[-] API Timeout (Page {page_num}) after {timeout_secs}s")
        except Exception as e:
            if page_num == 1:
                print(f"[-] API Connection Exception: {e}")
        return None

    def extract_item_data(self, item):
        """Extracts cleaned item ID, price, image, and quantity from any item record."""
        item_id = str(item["id"])  # Cast to str to prevent string/int dictionary key mismatch
        sku_info = item["skus"][0] if item.get("skus") else {}

        # Multi-field fallback to ensure Price is captured
        raw_price = item.get("price") or sku_info.get("price") or item.get("originalPrice") or 0.0
        try:
            price_val = float(raw_price)
        except (ValueError, TypeError):
            price_val = 0.0

        image_url = item.get("thumbnail") or sku_info.get("image") or item.get("image")

        return item_id, {
            "title": item.get("title", "Unknown Product"),
            "qty": int(sku_info.get("quantity", 0)),
            "price": price_val,
            "image": image_url,
            "sourceLink": item.get("sourceLink")
        }

    async def update_inventory_state(self, session, new_items, source_label="FAST-LOOP", is_initialization=False, is_full_sweep=False):
        async with self.state_lock:
            found_changes = False
            for item_id, current_data in new_items.items():
                old_data = self.known_inventory.get(item_id)
                new_qty = current_data["qty"]
                old_qty = old_data["qty"] if old_data else 0

                if is_initialization:
                    self.known_inventory[item_id] = current_data
                    continue

                # Case 1: Restock / Return
                if old_data and new_qty > old_qty:
                    msg = f"Qty: {old_qty} -> {new_qty} | ¥{current_data['price']}"
                    print(f"\n[{source_label}] 🚨 RESTOCK ALERT: '{current_data['title']}' ({msg})")
                    self.add_event("RESTOCK", current_data["title"], msg, item_id)
                    asyncio.create_task(
                        send_discord_alert(
                            session,
                            alert_type=f"{source_label} RESTOCK",
                            title=current_data["title"],
                            item_id=item_id,
                            qty=new_qty,
                            price=current_data["price"],
                            source_link=current_data.get("sourceLink"),
                            image_url=current_data.get("image")
                        )
                    )
                    found_changes = True

                # Case 2: New Product Drop
                elif not old_data and new_qty > 0:
                    msg = f"New Drop | Qty: {new_qty} | ¥{current_data['price']}"
                    print(f"\n[{source_label}] 🌟 NEW DROP DETECTED: '{current_data['title']}' ({msg})")
                    self.add_event("DROP", current_data["title"], msg, item_id)
                    asyncio.create_task(
                        send_discord_alert(
                            session,
                            alert_type=f"{source_label} DROP",
                            title=current_data["title"],
                            item_id=item_id,
                            qty=new_qty,
                            price=current_data["price"],
                            source_link=current_data.get("sourceLink"),
                            image_url=current_data.get("image")
                        )
                    )
                    found_changes = True

                # ALWAYS overwrite known_inventory so purchases (e.g. 1 -> 0) are remembered!
                self.known_inventory[item_id] = current_data

            # During a full sweep, zero out any item that vanished from the API catalog
            if is_full_sweep and not is_initialization:
                for known_id in list(self.known_inventory.keys()):
                    if known_id not in new_items:
                        self.known_inventory[known_id]["qty"] = 0

            return found_changes

    async def background_all_pages_sweep(self, session):
        if self.is_sweeping:
            return
        
        self.is_sweeping = True
        self.status_message = f"Deep Sweeping all {self.total_pages} pages..."
        start_time = time.time()
        print(f"\n[ALL-PAGES SWEEP] Scanning all {self.total_pages} pages to reconcile inventory...")

        try:
            tasks = [self.fetch_page(session, page) for page in range(1, self.total_pages + 1)]
            deep_items = {}
            results = await asyncio.gather(*tasks)

            for data in results:
                if not data or "data" not in data or not data["data"].get("records"):
                    continue
                for item in data["data"]["records"]:
                    item_id, cleaned_data = self.extract_item_data(item)
                    deep_items[item_id] = cleaned_data

            await self.update_inventory_state(session, deep_items, source_label="FULL-SWEEP", is_full_sweep=True)
            elapsed = time.time() - start_time
            self.last_sweep_time = time.time()
            self.status_message = "Monitoring Top 10 Pages (1.0s Heartbeat)"
            print(f"[ALL-PAGES SWEEP] Finished in {elapsed:.3f}s | Active items tracked: {len(deep_items)}\n")
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
                item_id, cleaned_data = self.extract_item_data(item)
                top_items[item_id] = cleaned_data

        found_in_top = await self.update_inventory_state(session, top_items, source_label="TOP-10 DROPS")
        self.last_scan_time = time.time()
        self.scan_counter += 1
        return page_1_data, found_in_top

    async def run(self):
        connector = aiohttp.TCPConnector(
            limit=200,
            limit_per_host=200,
            keepalive_timeout=60,
            ttl_dns_cache=300
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            page_1 = None
            retry_count = 0
            while not page_1 or "data" not in page_1:
                retry_count += 1
                self.status_message = f"Connecting to API (Attempt {retry_count})..."
                print(f"[*] Attempting API connection (Attempt {retry_count})...")
                
                page_1 = await self.fetch_page(session, 1, timeout_secs=4.0)
                if not page_1 or "data" not in page_1:
                    print("[-] Failed to reach API. Retrying in 3 seconds...")
                    await asyncio.sleep(3.0)

            self.current_total = int(page_1["data"]["total"])
            self.total_pages = math.ceil(self.current_total / 100)
            
            self.status_message = f"Scanning {self.total_pages} pages for baseline..."
            print(f"[*] Connection successful! Fetching {self.total_pages} pages for baseline...")
            
            start_tasks = [self.fetch_page(session, p, timeout_secs=3.5) for p in range(1, self.total_pages + 1)]
            initial_items = {}
            for data in await asyncio.gather(*start_tasks):
                if data and "data" in data:
                    for item in data.get("data", {}).get("records", []):
                        item_id, cleaned_data = self.extract_item_data(item)
                        initial_items[item_id] = cleaned_data

            await self.update_inventory_state(session, initial_items, is_initialization=True, is_full_sweep=True)
            self.last_sweep_time = time.time()
            self.status_message = "Monitoring Top 10 Pages (1.0s Heartbeat)"
            self.add_event("SYSTEM", "Baseline Established", f"Tracked {len(initial_items)} items across {self.total_pages} pages.")
            print(f"[*] Baseline established silently: {self.current_total} items tracked.")
            print(f"--- [2/2] Radar Armed. Monitoring Discord Webhook ---\n")

            last_safety_sweep = time.time()

            while True:
                time_start = time.time()
                page_1_data, found_in_top = await self.scan_top_pages_for_drops(session)

                if page_1_data and "data" in page_1_data:
                    new_total = int(page_1_data["data"]["total"])
                    time_since_last_sweep = time.time() - last_safety_sweep

                    if (new_total != self.current_total) or (time_since_last_sweep > SAFETY_SWEEP_INTERVAL):
                        if time_since_last_sweep > SAFETY_SWEEP_INTERVAL:
                            print("[SAFETY-NET] Running 60s scheduled full sweep...")
                            last_safety_sweep = time.time()
                        else:
                            diff = new_total - self.current_total
                            msg = f"Count changed by {diff:+d} ({self.current_total} -> {new_total})"
                            print(f"[TRIPWIRE] {msg}")
                            self.add_event("TRIPWIRE", "Catalog Size Change Detected", msg)
                            self.last_tripwire_time = time.time()

                        self.current_total = new_total
                        self.total_pages = math.ceil(self.current_total / 100)
                        asyncio.create_task(self.background_all_pages_sweep(session))

                elapsed = time.time() - time_start
                await asyncio.sleep(max(0, 1.0 - elapsed))

# --- WEB DASHBOARD & API HANDLERS ---

async def api_status_handler(request):
    radar = request.app["radar"]
    now = time.time()
    
    def format_relative(ts):
        if not ts: return "Never"
        diff = int(now - ts)
        if diff < 60: return f"{diff}s ago"
        return f"{diff//60}m {diff%60}s ago"

    uptime_secs = int(now - radar.start_time)
    hours, remainder = divmod(uptime_secs, 3600)
    minutes, seconds = divmod(remainder, 60)
    uptime_str = f"{hours}h {minutes}m {seconds}s" if hours else f"{minutes}m {seconds}s"

    data = {
        "status": radar.status_message,
        "uptime": uptime_str,
        "api_total": radar.current_total,
        "tracked_items": len(radar.known_inventory),
        "total_pages": radar.total_pages,
        "scan_counter": radar.scan_counter,
        "last_scan": format_relative(radar.last_scan_time),
        "last_sweep": format_relative(radar.last_sweep_time),
        "last_tripwire": format_relative(radar.last_tripwire_time),
        "events": radar.event_log
    }
    return web.json_response(data)

async def health_check_handler(request):
    return web.Response(text="OK")

async def dashboard_handler(request):
    html = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CSSDeals Sub-Second Radar</title>
    <style>
        :root {
            --bg: #0f172a;
            --card: #1e293b;
            --border: #334155;
            --text: #f8fafc;
            --muted: #94a3b8;
            --accent: #38bdf8;
            --green: #4ade80;
            --orange: #fb923c;
            --purple: #c084fc;
            --gold: #facc15;
        }
        * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
        body { background-color: var(--bg); color: var(--text); padding: 2rem; min-height: 100vh; }
        .container { max-width: 1100px; margin: 0 auto; }
        header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 2rem; border-bottom: 1px solid var(--border); padding-bottom: 1rem; }
        h1 { font-size: 1.5rem; font-weight: 700; color: var(--text); display: flex; align-items: center; gap: 0.5rem; }
        
        /* 1-Second Animated Heartbeat Beacon */
        .heartbeat-box { display: flex; align-items: center; gap: 0.75rem; background: #064e3b; border: 1px solid #059669; padding: 0.5rem 1rem; border-radius: 9999px; }
        .beacon { width: 12px; height: 12px; background-color: var(--green); border-radius: 50%; box-shadow: 0 0 0 0 rgba(74, 222, 128, 0.7); animation: pulse-animation 1s infinite; }
        @keyframes pulse-animation {
            0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(74, 222, 128, 0.8); }
            70% { transform: scale(1); box-shadow: 0 0 0 10px rgba(74, 222, 128, 0); }
            100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(74, 222, 128, 0); }
        }
        .heartbeat-text { font-size: 0.8rem; font-weight: 700; color: var(--green); text-transform: uppercase; letter-spacing: 0.05em; }

        .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 1rem; margin-bottom: 2rem; }
        .card { background: var(--card); border: 1px solid var(--border); border-radius: 0.75rem; padding: 1.25rem; }
        .card-label { font-size: 0.8rem; color: var(--muted); text-transform: uppercase; letter-spacing: 0.05em; margin-bottom: 0.5rem; }
        .card-value { font-size: 1.75rem; font-weight: 700; color: var(--text); }
        .card-sub { font-size: 0.75rem; color: var(--muted); margin-top: 0.25rem; }
        .section-title { font-size: 1.1rem; font-weight: 600; margin-bottom: 1rem; color: var(--text); }
        .table-container { background: var(--card); border: 1px solid var(--border); border-radius: 0.75rem; overflow: hidden; }
        table { width: 100%; border-collapse: collapse; text-align: left; font-size: 0.9rem; }
        th { background: #0f172a; padding: 0.75rem 1rem; color: var(--muted); font-weight: 600; font-size: 0.75rem; text-transform: uppercase; border-bottom: 1px solid var(--border); }
        td { padding: 0.75rem 1rem; border-bottom: 1px solid var(--border); }
        tr:last-child td { border-bottom: none; }
        .tag { display: inline-block; padding: 0.2rem 0.5rem; border-radius: 0.35rem; font-size: 0.75rem; font-weight: 700; }
        .tag-DROP { background: rgba(74, 222, 128, 0.15); color: var(--green); border: 1px solid rgba(74, 222, 128, 0.3); }
        .tag-RESTOCK { background: rgba(251, 146, 60, 0.15); color: var(--orange); border: 1px solid rgba(251, 146, 60, 0.3); }
        .tag-TRIPWIRE { background: rgba(192, 132, 252, 0.15); color: var(--purple); border: 1px solid rgba(192, 132, 252, 0.3); }
        .tag-SYSTEM { background: rgba(56, 189, 248, 0.15); color: var(--accent); border: 1px solid rgba(56, 189, 248, 0.3); }
        .empty-log { padding: 2rem; text-align: center; color: var(--muted); }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <h1>⚡ CSSDeals Sub-Second Radar</h1>
                <div style="font-size: 0.85rem; color: var(--muted); margin-top: 0.25rem;" id="status-text">Connecting to radar engine...</div>
            </div>
            <div class="heartbeat-box">
                <div class="beacon"></div>
                <div class="heartbeat-text" id="scan-counter-text">1.0s Heartbeat • Scan #0</div>
            </div>
        </header>

        <div class="grid">
            <div class="card">
                <div class="card-label">Catalog Total (API)</div>
                <div class="card-value" id="val-total">--</div>
                <div class="card-sub" id="sub-pages">-- pages total</div>
            </div>
            <div class="card">
                <div class="card-label">Tracked In Memory</div>
                <div class="card-value" id="val-tracked">--</div>
                <div class="card-sub">Active in-stock items</div>
            </div>
            <div class="card">
                <div class="card-label">Last Top-10 Scan</div>
                <div class="card-value" id="val-scan" style="font-size: 1.4rem;">--</div>
                <div class="card-sub" style="color: var(--green);">● 1.0s Active Loop</div>
            </div>
            <div class="card">
                <div class="card-label">Last Full Reconcile</div>
                <div class="card-value" id="val-sweep" style="font-size: 1.4rem;">--</div>
                <div class="card-sub" id="sub-tripwire">Tripwire: Never</div>
            </div>
        </div>

        <div class="section-title">Recent Radar Events & Alerts</div>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th style="width: 100px;">Time</th>
                        <th style="width: 110px;">Type</th>
                        <th>Event / Product Title</th>
                        <th>Details</th>
                    </tr>
                </thead>
                <tbody id="event-tbody">
                    <tr><td colspan="4" class="empty-log">Loading event stream...</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        async function updateDashboard() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                
                document.getElementById('status-text').textContent = 'Status: ' + data.status + ' • Uptime: ' + data.uptime;
                document.getElementById('scan-counter-text').textContent = '1.0s Heartbeat • Scan #' + data.scan_counter.toLocaleString();
                document.getElementById('val-total').textContent = data.api_total.toLocaleString();
                document.getElementById('sub-pages').textContent = data.total_pages + ' pages total';
                document.getElementById('val-tracked').textContent = data.tracked_items.toLocaleString();
                document.getElementById('val-scan').textContent = data.last_scan;
                document.getElementById('val-sweep').textContent = data.last_sweep;
                document.getElementById('sub-tripwire').textContent = 'Tripwire: ' + data.last_tripwire;

                const tbody = document.getElementById('event-tbody');
                if (data.events && data.events.length > 0) {
                    tbody.innerHTML = data.events.map(ev => `
                        <tr>
                            <td style="color: var(--muted); font-family: monospace;">${ev.timestamp}</td>
                            <td><span class="tag tag-${ev.type}">${ev.type}</span></td>
                            <td style="font-weight: 500;">${ev.title}</td>
                            <td style="color: var(--muted);">${ev.details}</td>
                        </tr>
                    `).join('');
                } else {
                    tbody.innerHTML = '<tr><td colspan="4" class="empty-log">No alerts detected yet since startup.</td></tr>';
                }
            } catch (err) {
                document.getElementById('status-text').textContent = 'Error fetching live radar metrics...';
            }
        }
        setInterval(updateDashboard, 1000);
        updateDashboard();
    </script>
</body>
</html>
"""
    return web.Response(text=html, content_type="text/html")

async def main_with_server():
    radar = AllPagesRadar()
    asyncio.create_task(radar.run())
    
    app = web.Application()
    app["radar"] = radar
    
    app.router.add_get("/", dashboard_handler)
    app.router.add_get("/health", health_check_handler)
    app.router.add_get("/api/status", api_status_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    
    print(f"[+] Web UI Dashboard & API active on port {port}.")
    await asyncio.Event().wait()

if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_with_server())
