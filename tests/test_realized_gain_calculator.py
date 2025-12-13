import math
import sqlite3
import tempfile
from pathlib import Path
import unittest

from src.etl.realized_gain_calculator import calculate_realized_gains
from src.etl.create_transaction_matches import create_transaction_matches
from src.repository.create_db import (
    create_broker_t,
    create_realized_gain_t,
    create_security_t,
    create_transaction_match_t,
    create_transaction_t,
)


class CalculateRealizedGainsTest(unittest.TestCase):
    def test_handles_dividends_and_fifo(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "realized_gain.db"
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            create_broker_t(cursor)
            create_security_t(cursor)
            create_transaction_t(cursor)
            create_transaction_match_t(cursor)
            create_realized_gain_t(cursor)

            cursor.execute("INSERT INTO broker_t (broker_name) VALUES (?)", ("comdirect",))
            broker_id = cursor.lastrowid
            cursor.execute(
                "INSERT INTO security_t (security_name, asset_type) VALUES (?, ?)",
                ("Sample ETF", "stock"),
            )
            security_id = cursor.lastrowid

            transactions = [
                ("2023-01-31", "buy", 8.934554, -2000.0),
                ("2023-02-03", "buy", 10.0, -2397.0),
                ("2023-03-09", "dividend", 18.934554, 12.11),
                ("2023-05-15", "sell", 18.934554, 5387.8273407),
            ]

            for tx_date, tx_type, shares, amount in transactions:
                cursor.execute(
                    """
                    INSERT INTO transaction_t
                        (security_id, broker_id, transaction_date, transaction_type,
                         shares, total_value, net_amount, used_in_realized_gain)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (security_id, broker_id, tx_date, tx_type, shares, amount, amount),
                )

            conn.commit()
            conn.close()

            stats_first = create_transaction_matches(str(db_path), clear_existing=True)
            self.assertEqual(stats_first["matches_created"], 2)

            stats_second = create_transaction_matches(str(db_path))
            self.assertEqual(stats_second["matches_created"], 0)

            stats = calculate_realized_gains(str(db_path))
            self.assertEqual(stats["positions_created"], 2)
            self.assertEqual(stats["transactions_marked"], 4)

            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute(
                """
                SELECT broker_id, security_id, shares, invested_value, buy_date, sell_date,
                       p_l, total_dividend, dividend_count
                FROM realized_gain_t
                ORDER BY buy_date
                """
            )
            rows = cursor.fetchall()
            self.assertEqual(len(rows), 2)

            total_shares = 18.934554
            dividend_amount = 12.11
            per_share_dividend = dividend_amount / total_shares
            sell_price_per_share = 5387.8273407 / total_shares

            expected = [
                {
                    "shares": 8.934554,
                    "invested_value": 2000.0,
                    "buy_date": "2023-01-31",
                    "realized_pl": (sell_price_per_share - 2000.0 / 8.934554) * 8.934554,
                    "total_dividend": per_share_dividend * 8.934554,
                },
                {
                    "shares": 10.0,
                    "invested_value": 2397.0,
                    "buy_date": "2023-02-03",
                    "realized_pl": (sell_price_per_share - 239.7) * 10.0,
                    "total_dividend": per_share_dividend * 10.0,
                },
            ]

            for row, expected_row in zip(rows, expected):
                self.assertEqual(row["broker_id"], broker_id)
                self.assertEqual(row["security_id"], security_id)
                self.assertTrue(math.isclose(row["shares"], expected_row["shares"], rel_tol=1e-6))
                self.assertTrue(
                    math.isclose(row["invested_value"], expected_row["invested_value"], rel_tol=1e-6)
                )
                self.assertEqual(row["buy_date"], expected_row["buy_date"])
                self.assertEqual(row["sell_date"], "2023-05-15")
                self.assertTrue(math.isclose(row["p_l"], expected_row["realized_pl"], rel_tol=1e-6))
                self.assertTrue(
                    math.isclose(row["total_dividend"], expected_row["total_dividend"], rel_tol=1e-6)
                )
                self.assertEqual(row["dividend_count"], 1)

            cursor.execute("SELECT used_in_realized_gain FROM transaction_t ORDER BY id")
            self.assertEqual([value[0] for value in cursor.fetchall()], [1, 1, 1, 1])

            conn.close()
