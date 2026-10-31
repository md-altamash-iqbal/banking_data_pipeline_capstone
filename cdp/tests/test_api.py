import json
import sqlite3
import threading
import urllib.error
import urllib.request

import pytest
from http.server import ThreadingHTTPServer

from cdp.src import api_server


@pytest.fixture
def running_api(tmp_path, monkeypatch):
    """Start the API with an isolated temporary database."""

    database_path = tmp_path / "api_test.db"

    connection = sqlite3.connect(database_path)

    connection.execute("""
        CREATE TABLE customer_360 (
            customer_id TEXT PRIMARY KEY,
            customer_name TEXT NOT NULL,
            account_count INTEGER NOT NULL,
            transaction_count INTEGER NOT NULL,
            total_transaction_amount REAL NOT NULL,
            last_transaction_date TEXT
        )
    """)

    connection.executemany(
        """
        INSERT INTO customer_360 VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            ("C001", "Alex Morgan", 2, 2, 150.0, "2026-09-02"),
            ("C002", "Jordan Lee", 0, 0, 0.0, None),
        ]
    )

    connection.commit()
    connection.close()

    monkeypatch.setattr(
        api_server,
        "DATABASE_PATH",
        database_path
    )

    server = ThreadingHTTPServer(
        ("127.0.0.1", 0),
        api_server.CustomerAPIHandler
    )

    thread = threading.Thread(
        target=server.serve_forever,
        daemon=True
    )

    thread.start()

    host, port = server.server_address
    base_url = f"http://{host}:{port}"

    yield base_url

    server.shutdown()
    server.server_close()
    thread.join()


def test_existing_customer_returns_200(running_api):
    url = f"{running_api}/customers/C001/summary"

    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode("utf-8"))

        assert response.status == 200
        assert data["customer_id"] == "C001"
        assert data["account_count"] == 2
        assert data["transaction_count"] == 2
        assert data["total_transaction_amount"] == 150.0


def test_customer_without_transactions_returns_200(running_api):
    url = f"{running_api}/customers/C002/summary"

    with urllib.request.urlopen(url) as response:
        data = json.loads(response.read().decode("utf-8"))

        assert response.status == 200
        assert data["transaction_count"] == 0
        assert data["total_transaction_amount"] == 0.0
        assert data["last_transaction_date"] is None


def test_unknown_customer_returns_404(running_api):
    url = f"{running_api}/customers/C999/summary"

    with pytest.raises(urllib.error.HTTPError) as error:
        urllib.request.urlopen(url)

    assert error.value.code == 404

    response_body = json.loads(
        error.value.read().decode("utf-8")
    )

    assert response_body == {
        "error": "Customer not found"
    }


def test_post_request_returns_405(running_api):
    url = f"{running_api}/customers/C001/summary"

    request = urllib.request.Request(
        url,
        method="POST"
    )

    with pytest.raises(urllib.error.HTTPError) as error:
        urllib.request.urlopen(request)

    assert error.value.code == 405