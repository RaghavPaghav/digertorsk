async def fetch_page(self, session, page_num, timeout_secs=2.5):
        url = BASE_URL.format(page_num)
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=timeout_secs)) as response:
                if response.status == 200:
                    return await response.json()
                elif page_num == 1:
                    # Print exact HTTP error on page 1 so we aren't guessing
                    print(f"[-] API Error (Page {page_num}): HTTP {response.status}")
        except asyncio.TimeoutError:
            if page_num == 1:
                print(f"[-] API Timeout (Page {page_num}) after {timeout_secs}s")
        except Exception as e:
            if page_num == 1:
                print(f"[-] API Connection Exception: {e}")
        return None

    async def run(self):
        connector = aiohttp.TCPConnector(
            limit=110,
            limit_per_host=110,
            keepalive_timeout=60,
            ttl_dns_cache=300
        )

        async with aiohttp.ClientSession(connector=connector) as session:
            # --- RESILIENT STARTUP LOOP ---
            # Keeps retrying until CSSDeals responds, never giving up permanently
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
                        item_id = item["id"]
                        sku_info = item["skus"][0] if item.get("skus") else {}
                        initial_items[item_id] = {
                            "title": item.get("title", "Unknown"),
                            "qty": int(sku_info.get("quantity", 0)),
                            "price": sku_info.get("price"),
                            "image": sku_info.get("image") or item.get("thumbnail"),
                            "sourceLink": item.get("sourceLink")
                        }

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
