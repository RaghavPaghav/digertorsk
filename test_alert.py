import requests

url = "https://discord.com/api/webhooks/1531974113067929681/0lJqevpqVFh7Y8XZOSM2CVW_f9lnv-kJfcY48FG8BRytfTqm-Ea56IMyy2d2sKs9fk4s"
embed = {"title": "🚨 WAREHOUSE RESTOCK: Test Item", "color": 15158332, "fields": [{"name": "Product ID", "value": "`207960205224636417`", "inline": True}, {"name": "Quantity", "value": "**1**", "inline": True}]}
payload = {"username": "CSSDeals Monitor", "avatar_url": "https://cssdeals.com/favicon.ico", "embeds": [embed]}
requests.post(url, json=payload)