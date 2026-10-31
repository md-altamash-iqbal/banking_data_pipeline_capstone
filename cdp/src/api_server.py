import json
import re
import sqlite3
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATABASE_PATH = PROJECT_ROOT / "database" / "cdp.db"

HOST = "127.0.0.1"
PORT = 8000


class CustomerAPIHandler(BaseHTTPRequestHandler):

    def send_json(self, status_code, data):
        """Send a JSON HTTP response."""

        response = json.dumps(data).encode("utf-8")

        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()

        self.wfile.write(response)

    def do_GET(self):
        """Handle read-only customer summary requests."""

        pattern = r"^/customers/([^/]+)/summary$"
        match = re.match(pattern, self.path)

        if not match:
            self.send_json(
                404,
                {"error": "Customer not found"}
            )
            return

        customer_id = match.group(1)

        try:
            # Open SQLite in read-only mode.
            database_uri = (
                f"file:{DATABASE_PATH.as_posix()}?mode=ro"
            )

            connection = sqlite3.connect(
                database_uri,
                uri=True
            )

            connection.row_factory = sqlite3.Row

            try:
                customer = connection.execute(
                    """
                    SELECT
                        customer_id,
                        customer_name,
                        account_count,
                        transaction_count,
                        total_transaction_amount,
                        last_transaction_date
                    FROM customer_360
                    WHERE customer_id = ?
                    """,
                    (customer_id,)
                ).fetchone()

            finally:
                connection.close()

            if customer is None:
                self.send_json(
                    404,
                    {"error": "Customer not found"}
                )
                return

            self.send_json(
                200,
                dict(customer)
            )

        except sqlite3.Error:
            self.send_json(
                500,
                {"error": "Internal server error"}
            )

    def do_POST(self):
        self.send_json(
            405,
            {"error": "Method not allowed"}
        )

    def do_PUT(self):
        self.send_json(
            405,
            {"error": "Method not allowed"}
        )

    def do_DELETE(self):
        self.send_json(
            405,
            {"error": "Method not allowed"}
        )


def main():
    if not DATABASE_PATH.exists():
        raise FileNotFoundError(
            f"CDP database not found: {DATABASE_PATH}"
        )

    server = ThreadingHTTPServer(
        (HOST, PORT),
        CustomerAPIHandler
    )

    print("CUSTOMER 360 API SERVER STARTED")
    print("--------------------------------")
    print(f"Host: {HOST}")
    print(f"Port: {PORT}")
    print(f"Database: {DATABASE_PATH}")
    print("\nAvailable endpoint:")
    print("GET /customers/<customer_id>/summary")
    print("\nPress CTRL+C to stop the server.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping API server...")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()