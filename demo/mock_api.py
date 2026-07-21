"""Mock API server for framework demos.

Serves synthetic data for multiple demo pipelines. Each endpoint returns
randomized but realistic JSON so every pipeline run produces fresh data.

Endpoints:
    GET /health          — health check
    GET /cash_balance    — cash balance records (finance)
    GET /orders          — customer orders (e-commerce)
    GET /customers       — customer master data (CRM)
    GET /trades          — trade executions (capital markets)
    GET /positions       — portfolio positions (capital markets)
"""

import json
import random
import string
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

# ── Seed data pools ──

ACCOUNTS = ["FUND-A", "FUND-B", "FUND-C", "FUND-D", "FUND-E"]
CURRENCIES = ["USD", "EUR", "GBP"]
CUSTOMERS = [
    ("C-1001", "Alice Chen", "alice@example.com"),
    ("C-1002", "Bob Martinez", "bob@example.com"),
    ("C-1003", "Carol Williams", "carol@example.com"),
    ("C-1004", "David Kim", "david@example.com"),
    ("C-1005", "Eva Schmidt", "eva@example.com"),
    ("C-1006", "Frank Okafor", "frank@example.com"),
]
TIERS = ["bronze", "silver", "gold", "platinum"]
REGIONS = ["APAC", "EMEA", "AMER"]
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "NVDA", "META", "JPM"]
SIDES = ["BUY", "SELL"]
STATUSES = ["FILLED", "FILLED", "FILLED", "PARTIAL", "CANCELLED"]
PRODUCTS = [
    ("SKU-101", "Widget Pro", 29.99),
    ("SKU-202", "Gadget Plus", 49.50),
    ("SKU-303", "Sensor Unit", 12.75),
    ("SKU-404", "Cable Kit", 8.99),
    ("SKU-505", "Battery Pack", 19.95),
]


def _today():
    return date.today()


def _rand_id(prefix, length=6):
    return f"{prefix}-{''.join(random.choices(string.ascii_uppercase + string.digits, k=length))}"


# ── Data generators ──


def gen_cash_balance(n=20):
    today = _today()
    return [
        {
            "account_cd": random.choice(ACCOUNTS),
            "update_timestamp": datetime.utcnow().isoformat(),
            "amt": round(random.uniform(1000, 500000), 2),
            "ccy": random.choice(CURRENCIES),
            "projected_dt": (today + timedelta(days=random.randint(0, 30))).isoformat(),
        }
        for _ in range(n)
    ]


def gen_orders(n=15):
    today = _today()
    rows = []
    for _ in range(n):
        sku, name, unit_price = random.choice(PRODUCTS)
        qty = random.randint(1, 20)
        rows.append(
            {
                "order_id": _rand_id("ORD"),
                "customer_id": random.choice(CUSTOMERS)[0],
                "product_sku": sku,
                "product_name": name,
                "quantity": qty,
                "unit_price": unit_price,
                "amount": round(unit_price * qty, 2),
                "currency": random.choice(CURRENCIES),
                "order_date": (today - timedelta(days=random.randint(0, 7))).isoformat(),
                "status": "confirmed",
            }
        )
    return rows


def gen_customers():
    return [
        {
            "customer_id": cid,
            "name": name,
            "email": email,
            "tier": random.choice(TIERS),
            "region": random.choice(REGIONS),
        }
        for cid, name, email in CUSTOMERS
    ]


def gen_trades(n=25):
    rows = []
    for _ in range(n):
        symbol = random.choice(SYMBOLS)
        price = round(random.uniform(50, 500), 2)
        qty = random.randint(10, 5000)
        rows.append(
            {
                "trade_id": _rand_id("TRD"),
                "symbol": symbol,
                "side": random.choice(SIDES),
                "quantity": qty,
                "price": price,
                "currency": "USD",
                "trade_date": datetime.utcnow().isoformat(),
                "status": random.choice(STATUSES),
            }
        )
    return rows


def gen_positions():
    portfolios = ["PORT-GROWTH", "PORT-VALUE", "PORT-INCOME"]
    rows = []
    for port in portfolios:
        for sym in random.sample(SYMBOLS, k=random.randint(3, 5)):
            rows.append(
                {
                    "portfolio_id": port,
                    "instrument": sym,
                    "quantity": random.randint(100, 10000),
                    "market_value": round(random.uniform(5000, 500000), 2),
                    "as_of_date": _today().isoformat(),
                }
            )
    return rows


# ── Route table ──

ROUTES = {
    "/health": lambda: {"status": "healthy"},
    "/cash_balance": gen_cash_balance,
    "/orders": gen_orders,
    "/customers": gen_customers,
    "/trades": gen_trades,
    "/positions": gen_positions,
}


class MockAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        handler = ROUTES.get(self.path)
        if handler:
            self._respond(200, handler())
        else:
            self._respond(404, {"error": "not found", "available": list(ROUTES.keys())})

    def _respond(self, status, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body, default=str).encode())

    def log_message(self, fmt, *args):
        print(f"  {args[0]}")


def main():
    server = HTTPServer(("0.0.0.0", 8000), MockAPIHandler)
    print("Mock API running on http://0.0.0.0:8000")
    print("Endpoints:")
    for route in ROUTES:
        print(f"  GET {route}")
    server.serve_forever()


if __name__ == "__main__":
    main()
