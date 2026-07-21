"""Lightweight mock API server for the framework demo.

Returns synthetic cash-balance data on GET /cash_balance
and a health endpoint at GET /health.

Run standalone:
    python demo/mock_api.py
"""

import json
import random
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer

ACCOUNTS = ["FUND-A", "FUND-B", "FUND-C", "FUND-D"]
CURRENCIES = ["USD", "EUR", "GBP"]


def generate_cash_balance_data(n: int = 20) -> list[dict]:
    today = date.today()
    rows = []
    for _ in range(n):
        rows.append(
            {
                "account_cd": random.choice(ACCOUNTS),
                "update_timestamp": datetime.utcnow().isoformat(),
                "amt": round(random.uniform(1000, 500000), 2),
                "ccy": random.choice(CURRENCIES),
                "projected_dt": (today + timedelta(days=random.randint(0, 30))).isoformat(),
            }
        )
    return rows


class MockAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self._respond(200, {"status": "healthy"})
        elif self.path == "/cash_balance":
            self._respond(200, generate_cash_balance_data())
        else:
            self._respond(404, {"error": "not found"})

    def _respond(self, status: int, body):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(body).encode())

    def log_message(self, format, *args):
        pass


def main():
    server = HTTPServer(("0.0.0.0", 8000), MockAPIHandler)
    print("Mock API running on http://0.0.0.0:8000")
    print("  GET /health        — health check")
    print("  GET /cash_balance  — sample data")
    server.serve_forever()


if __name__ == "__main__":
    main()
