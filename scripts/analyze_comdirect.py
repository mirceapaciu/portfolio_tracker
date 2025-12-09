#!/usr/bin/env python3
"""
Analyze comdirect transaction CSV to find buy/sell transaction pairs for securities.
Handles German number format (comma as decimal separator) and date format (DD.MM.YYYY).
"""

import csv
from datetime import datetime
from decimal import Decimal, InvalidOperation
from collections import defaultdict
from typing import Dict, List, Tuple
import sys


def parse_german_decimal(value: str) -> Decimal:
    """Convert German decimal format (47,53) to Decimal."""
    if not value or value == '0':
        return Decimal('0')
    try:
        # Replace comma with period and remove any thousand separators
        value = value.replace('.', '').replace(',', '.')
        return Decimal(value)
    except (InvalidOperation, ValueError):
        return Decimal('0')


def parse_german_date(date_str: str) -> datetime:
    """Parse German date format DD.MM.YYYY."""
    return datetime.strptime(date_str, '%d.%m.%Y')


def parse_shares(shares_str: str) -> Decimal:
    """Parse share count, handling both integer and decimal values."""
    try:
        return parse_german_decimal(shares_str)
    except:
        return Decimal('0')


class ComdirectTransaction:
    """Represents a single comdirect transaction (buy or sell)."""
    def __init__(self, row: Dict[str, str]):
        self.settlement_date = parse_german_date(row['Abrechnungstag'])
        
        # Handle encoding issue - find execution date column
        exec_key = None
        for key in row.keys():
            if 'hrung' in key or 'Ausf' in key:
                exec_key = key
                break
        self.execution_date = parse_german_date(row[exec_key]) if exec_key else self.settlement_date
        
        self.wkn = row['WKN']
        self.isin = row['ISIN']
        self.security_name = row['Bezeichnung']
        
        # Handle encoding issue with "Geschäftsart"
        transaction_key = None
        for key in row.keys():
            if 'art' in key:
                transaction_key = key
                break
        self.transaction_type = row[transaction_key] if transaction_key else ''
        
        # Handle encoding issue with "Stücke/Nom."
        shares_key = None
        for key in row.keys():
            if 'cke' in key or 'Nom' in key:
                shares_key = key
                break
        self.shares = parse_shares(row[shares_key]) if shares_key else Decimal('0')
        
        self.price = parse_german_decimal(row['Kurs'])
        
        # Handle encoding issue with "Währung"
        currency_key = None
        for key in row.keys():
            if 'hrung' in key and 'W' in key:
                currency_key = key
                break
        self.currency = row[currency_key] if currency_key else 'EUR'
        
        # Kundenendbetrag is the final amount (negative for buy, positive for sell)
        self.customer_amount = parse_german_decimal(row['Kundenendbetrag EUR'])
        
    def is_buy(self) -> bool:
        """Check if this is a buy transaction."""
        return self.transaction_type == 'Kauf'
    
    def is_sell(self) -> bool:
        """Check if this is a sell transaction."""
        return self.transaction_type == 'Verkauf'
    
    def get_price_per_share(self) -> Decimal:
        """Get price per share from the Kurs column."""
        return self.price
    
    def get_total_cost(self) -> Decimal:
        """Get total cost including fees (absolute value)."""
        return abs(self.customer_amount)


def analyze_csv(filepath: str) -> List[Tuple]:
    """
    Parse CSV and match buy/sell transactions for each security.
    Returns list of (security_name, buy_date, buy_price, shares, sell_date, sell_price, realized_pl)
    """
    
    # Dictionary to track transactions by security (ISIN is more reliable than WKN)
    transactions_by_security = defaultdict(lambda: {'buys': [], 'sells': []})
    
    # Read CSV with proper encoding
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            
            for row in reader:
                try:
                    trans = ComdirectTransaction(row)
                    
                    # Use ISIN as key (or WKN if ISIN is empty)
                    key = trans.isin if trans.isin else trans.wkn
                    
                    if trans.is_buy():
                        transactions_by_security[key]['buys'].append(trans)
                    elif trans.is_sell():
                        transactions_by_security[key]['sells'].append(trans)
                        
                except Exception as e:
                    # Skip rows that can't be parsed
                    print(f"Warning: Could not parse row: {e}")
                    continue
                    
    except UnicodeDecodeError:
        # Try Windows-1252 encoding if UTF-8 fails
        with open(filepath, 'r', encoding='windows-1252') as f:
            reader = csv.DictReader(f, delimiter=';')
            
            for row in reader:
                try:
                    trans = ComdirectTransaction(row)
                    key = trans.isin if trans.isin else trans.wkn
                    
                    if trans.is_buy():
                        transactions_by_security[key]['buys'].append(trans)
                    elif trans.is_sell():
                        transactions_by_security[key]['sells'].append(trans)
                        
                except Exception as e:
                    continue
    
    # Match buy/sell pairs using FIFO (First In, First Out)
    results = []
    
    for security_id, trans_dict in transactions_by_security.items():
        buys = sorted(trans_dict['buys'], key=lambda x: x.settlement_date)
        sells = sorted(trans_dict['sells'], key=lambda x: x.settlement_date)
        
        if not sells:
            # No sells for this security
            continue
            
        # For each sell, match with corresponding buys (FIFO)
        for sell in sells:
            if sell.shares == 0:
                continue
                
            remaining_shares = sell.shares
            sell_price_per_share = sell.get_price_per_share()
            sell_total = sell.get_total_cost()
            
            # Track if we matched any buys
            matched_any = False
            
            # Match with buys (FIFO)
            for buy in buys:
                if buy.shares == 0:
                    continue
                    
                if remaining_shares <= 0:
                    break
                
                matched_any = True
                
                # How many shares from this buy are we selling?
                shares_to_match = min(remaining_shares, buy.shares)
                buy_price_per_share = buy.get_price_per_share()
                
                # Calculate invested value and realized P/L
                invested_value = buy_price_per_share * shares_to_match
                realized_pl = (sell_price_per_share - buy_price_per_share) * shares_to_match
                
                results.append((
                    sell.security_name,
                    buy.settlement_date.strftime('%d.%m.%Y'),
                    buy_price_per_share,
                    shares_to_match,
                    invested_value,
                    sell.settlement_date.strftime('%d.%m.%Y'),
                    sell_price_per_share,
                    realized_pl
                ))
                
                # Update remaining shares
                buy.shares -= shares_to_match
                remaining_shares -= shares_to_match
            
            # If we have unmatched shares, it means we're selling shares bought before our data period
            if remaining_shares > 0:
                # For unmatched shares, we can't calculate accurate P/L
                # But we still report the sell
                results.append((
                    sell.security_name,
                    'N/A (before data)',
                    Decimal('0'),  # Unknown buy price
                    remaining_shares,
                    Decimal('0'),  # Unknown invested value
                    sell.settlement_date.strftime('%d.%m.%Y'),
                    sell_price_per_share,
                    Decimal('0')  # Unknown P/L
                ))
    
    return results


def print_results(results: List[Tuple]):
    """Print results in a formatted table."""
    print(f"\n{'Security':<40} {'Buy Date':<15} {'Buy Price':>12} {'Share Count':>12} {'Invested Value':>14} {'Sell Date':<15} {'Sell Price':>12} {'Realized P/L':>15}")
    print("=" * 150)
    
    total_pl = Decimal('0')
    known_pl_only = Decimal('0')
    
    for security, buy_date, buy_price, shares, invested_value, sell_date, sell_price, realized_pl in results:
        print(f"{security:<40} {buy_date:<15} {buy_price:>12.2f} {shares:>12.3f} {invested_value:>14.2f} {sell_date:<15} {sell_price:>12.2f} {realized_pl:>15.2f}")
        total_pl += realized_pl
        if buy_date != 'N/A (before data)':
            known_pl_only += realized_pl
    
    print("=" * 150)
    print(f"{'Total Realized P/L (known transactions):':<133} {known_pl_only:>15.2f}")
    print(f"{'Total Realized P/L (including N/A):':<133} {total_pl:>15.2f}")


def main():
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    else:
        # Default to the CSV in the data directory
        filepath = r'd:\src\portfolio_tracker\data\abrechnungsdaten_comdirect_20251205.csv'
    
    print(f"Analyzing comdirect transactions from: {filepath}")
    
    results = analyze_csv(filepath)
    
    if not results:
        print("\nNo buy/sell transaction pairs found in the CSV.")
        return
    
    print_results(results)
    
    # Also save to CSV
    output_file = r'd:\src\portfolio_tracker\data\comdirect_transaction_analysis.csv'
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Security Name', 'Buy Date', 'Buy Price', 'Share Count', 'Invested Value', 'Sell Date', 'Sell Price', 'Realized P/L'])
        
        for row in results:
            writer.writerow(row)
    
    print(f"\nResults saved to: {output_file}")


if __name__ == '__main__':
    main()
