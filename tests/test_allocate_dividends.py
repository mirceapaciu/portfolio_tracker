import math
import sqlite3
import tempfile
from pathlib import Path
import unittest

from src.etl.allocate_dividends import allocate_dividends
from src.repository.create_db import (
    create_broker_t,
    create_dividend_allocation_t,
    create_security_t,
    create_transaction_match_t,
    create_transaction_t,
)


class AllocateDividendsTest(unittest.TestCase):
    def test_allocates_only_segments_open_between_dividends(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "dividends.db"
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            create_broker_t(cursor)
            create_security_t(cursor)
            create_transaction_t(cursor)
            create_transaction_match_t(cursor)
            create_dividend_allocation_t(cursor)

            cursor.execute("INSERT INTO broker_t (broker_name) VALUES (?)", ("demo",))
            broker_id = cursor.lastrowid
            cursor.execute(
                "INSERT INTO security_t (security_name, asset_type) VALUES (?, ?)",
                ("Sample Equity", "stock"),
            )
            security_id = cursor.lastrowid

            def insert_transaction(tx_date: str, tx_type: str, shares: float, amount: float) -> int:
                cursor.execute(
                    """
                    INSERT INTO transaction_t
                        (security_id, broker_id, transaction_date, transaction_type,
                         shares, total_value, net_amount, allocated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (security_id, broker_id, tx_date, tx_type, shares, amount, amount),
                )
                return cursor.lastrowid

            def insert_buy_with_sell(buy_date: str, sell_date: str, shares: float) -> int:
                buy_id = insert_transaction(buy_date, "buy", shares, -shares * 10)
                sell_id = insert_transaction(sell_date, "sell", shares, shares * 12)
                cursor.execute(
                    """
                    INSERT INTO transaction_match_t
                        (broker_id, security_id, buy_transaction_id, sell_transaction_id,
                         shares, allocated_cost, allocated_proceeds)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        broker_id,
                        security_id,
                        buy_id,
                        sell_id,
                        shares,
                        shares * 10,
                        shares * 12,
                    ),
                )
                return buy_id

            insert_buy_with_sell("2024-03-20", "2024-09-27", 100.0)
            insert_buy_with_sell("2024-05-07", "2024-09-27", 100.0)
            insert_buy_with_sell("2024-06-17", "2024-09-27", 70.0)
            insert_transaction("2024-08-16", "buy", 100.0, -1200.0)

            insert_transaction("2024-05-04", "dividend", 100.0, 100.0)
            insert_transaction("2025-05-07", "dividend", 350.0, 350.0)

            conn.commit()
            conn.close()

            stats = allocate_dividends(str(db_path))
            self.assertEqual(stats["dividends_processed"], 2)
            self.assertEqual(stats["dividends_allocated"], 2)
            self.assertEqual(stats["dividends_failed"], 0)
            self.assertEqual(stats["allocations_created"], 5)

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT div_tx.transaction_date AS dividend_date,
                       buy_tx.transaction_date AS buy_date,
                       da.shares
                FROM dividend_allocation_t da
                JOIN transaction_t div_tx ON div_tx.id = da.dividend_transaction_id
                JOIN transaction_t buy_tx ON buy_tx.id = da.buy_transaction_id
                ORDER BY div_tx.transaction_date, buy_tx.transaction_date, da.id
                """
            )
            rows = cursor.fetchall()
            conn.close()

            expected = [
                ("2024-05-04", "2024-03-20", 100.0),
                ("2025-05-07", "2024-03-20", 100.0),
                ("2025-05-07", "2024-05-07", 100.0),
                ("2025-05-07", "2024-06-17", 70.0),
                ("2025-05-07", "2024-08-16", 80.0),
            ]

            self.assertEqual(len(rows), len(expected))
            for row, (expected_dividend, expected_buy, expected_shares) in zip(rows, expected):
                self.assertEqual(row[0], expected_dividend)
                self.assertEqual(row[1], expected_buy)
                self.assertTrue(math.isclose(row[2], expected_shares, rel_tol=1e-9))

    def test_allocates_multiple_dividends_without_sells(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            db_path = Path(tmp_dir) / "dividends_long.db"
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()

            create_broker_t(cursor)
            create_security_t(cursor)
            create_transaction_t(cursor)
            create_transaction_match_t(cursor)
            create_dividend_allocation_t(cursor)

            cursor.execute("INSERT INTO broker_t (broker_name) VALUES (?)", ("long",))
            broker_id = cursor.lastrowid
            cursor.execute(
                "INSERT INTO security_t (security_name, asset_type) VALUES (?, ?)",
                ("German ETF", "stock"),
            )
            security_id = cursor.lastrowid

            def insert_transaction(
                tx_date: str,
                tx_type: str,
                shares: float,
                price_per_share: float,
                net_amount: float,
            ) -> int:
                total_value = price_per_share * shares if price_per_share is not None else net_amount
                cursor.execute(
                    """
                    INSERT INTO transaction_t
                        (security_id, broker_id, transaction_date, transaction_type,
                         shares, price_per_share, total_value, net_amount, allocated)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        security_id,
                        broker_id,
                        tx_date,
                        tx_type,
                        shares,
                        price_per_share,
                        total_value,
                        net_amount,
                    ),
                )
                return cursor.lastrowid

            transactions = [
                ("2022-02-01", "buy", 190.0, 28.22, -5396.19),
                ("2022-02-01", "buy", 10.0, 28.21, -286.15),
                ("2022-04-29", "buy", 100.0, 25.61, -2582.48),
                ("2022-05-10", "dividend", 300.0, 1.54, 462.0),
                ("2023-05-10", "dividend", 300.0, 1.70, 510.0),
                ("2024-03-08", "buy", 120.0, 33.215, -4015.12),
                ("2024-05-06", "dividend", 420.0, 1.98, 831.6),
                ("2025-05-07", "dividend", 420.0, 2.15, 903.0),
            ]

            for tx_date, tx_type, shares, price, net in transactions:
                insert_transaction(tx_date, tx_type, shares, price, net)

            conn.commit()
            conn.close()

            stats = allocate_dividends(str(db_path))
            self.assertEqual(stats["dividends_processed"], 4)
            self.assertEqual(stats["dividends_allocated"], 4)
            self.assertEqual(stats["dividends_failed"], 0)
            self.assertEqual(stats["allocations_created"], 14)

            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT div_tx.transaction_date AS dividend_date,
                       buy_tx.transaction_date AS buy_date,
                       da.shares,
                       da.allocated_amount
                FROM dividend_allocation_t da
                JOIN transaction_t div_tx ON div_tx.id = da.dividend_transaction_id
                JOIN transaction_t buy_tx ON buy_tx.id = da.buy_transaction_id
                ORDER BY div_tx.transaction_date, buy_tx.transaction_date, da.buy_transaction_id
                """
            )
            rows = cursor.fetchall()
            conn.close()

            expected = [
                ("2022-05-10", "2022-02-01", 190.0, 190.0 * 1.54),
                ("2022-05-10", "2022-02-01", 10.0, 10.0 * 1.54),
                ("2022-05-10", "2022-04-29", 100.0, 100.0 * 1.54),
                ("2023-05-10", "2022-02-01", 190.0, 190.0 * 1.70),
                ("2023-05-10", "2022-02-01", 10.0, 10.0 * 1.70),
                ("2023-05-10", "2022-04-29", 100.0, 100.0 * 1.70),
                ("2024-05-06", "2022-02-01", 190.0, 190.0 * 1.98),
                ("2024-05-06", "2022-02-01", 10.0, 10.0 * 1.98),
                ("2024-05-06", "2022-04-29", 100.0, 100.0 * 1.98),
                ("2024-05-06", "2024-03-08", 120.0, 120.0 * 1.98),
                ("2025-05-07", "2022-02-01", 190.0, 190.0 * 2.15),
                ("2025-05-07", "2022-02-01", 10.0, 10.0 * 2.15),
                ("2025-05-07", "2022-04-29", 100.0, 100.0 * 2.15),
                ("2025-05-07", "2024-03-08", 120.0, 120.0 * 2.15),
            ]

            self.assertEqual(len(rows), len(expected))
            for row, (exp_dividend, exp_buy, exp_shares, exp_amount) in zip(rows, expected):
                self.assertEqual(row[0], exp_dividend)
                self.assertEqual(row[1], exp_buy)
                self.assertTrue(math.isclose(row[2], exp_shares, rel_tol=1e-9))
                self.assertTrue(math.isclose(row[3], exp_amount, rel_tol=1e-9))


if __name__ == "__main__":
    unittest.main()
