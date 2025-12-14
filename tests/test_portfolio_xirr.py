import sqlite3
import tempfile
from datetime import date
from pathlib import Path
import unittest

from src.etl.portfolio_xirr import calculate_portfolio_xirr
from src.repository.create_db import create_security_t, create_transaction_t


class PortfolioXirrTest(unittest.TestCase):
    def test_includes_open_positions_in_cashflows(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "portfolio.db"
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            create_security_t(cursor)
            create_transaction_t(cursor)

            cursor.execute(
                "INSERT INTO security_t (security_name, asset_type) VALUES (?, ?)",
                ("Test Stock", "stock"),
            )
            security_id = cursor.lastrowid

            cursor.execute(
                """
                INSERT INTO transaction_t
                    (security_id, broker_id, transaction_date, transaction_type,
                     shares, price_per_share, total_value, net_amount)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    security_id,
                    1,
                    "2024-01-01",
                    "buy",
                    10.0,
                    100.0,
                    -1000.0,
                    -1000.0,
                ),
            )

            conn.commit()
            conn.close()

            xirr = calculate_portfolio_xirr(str(db_path))
            self.assertIsNotNone(xirr)
            self.assertTrue(abs(xirr) < 1e-6)


if __name__ == "__main__":
    unittest.main()
