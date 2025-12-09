#!/usr/bin/env python3
"""
Analyze German tax export CSV to find buy/sell transaction pairs for securities.
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
        # Replace comma with period for Decimal parsing
        return Decimal(value.replace(',', '.'))
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


class Transaction:
    """Represents a single transaction (buy or sell)."""
    def __init__(self, row: Dict[str, str]):
        self.date = parse_german_date(row['Buchungstag'])
        self.tax_date = parse_german_date(row['Steuerliches Datum'])
        self.transaction_type = row['Vorgang']
        
        # Handle encoding issue with "Stück/Nominale" column
        share_key = None
        for key in row.keys():
            if 'ck/Nominale' in key or 'Nominale' in key:
                share_key = key
                break
        
        self.shares = parse_shares(row[share_key]) if share_key else Decimal('0')
        self.security_name = row['Bezeichnung']
        self.wkn = row['WKN']
        self.gross_amount = parse_german_decimal(row['Betrag Brutto'])
        self.realized_pl = parse_german_decimal(row['Gewinn/Verlust'])
        
    def is_buy(self) -> bool:
        """Check if this is a buy transaction."""
        return self.transaction_type in ['Kauf', 'Kauf ausl.m.Ertragsant', 'Kauf inl. m.Ertragsant']
    
    def is_sell(self) -> bool:
        """Check if this is a sell transaction."""
        return self.transaction_type in ['Verkauf', 'Verkauf Investmentfonds']
    
    def get_price_per_share(self) -> Decimal:
        """Calculate price per share (gross amount / shares)."""
        if self.shares == 0:
            return Decimal('0')
        return abs(self.gross_amount / self.shares)


def analyze_csv(filepath: str) -> List[Tuple]:
    """
    Parse CSV and match buy/sell transactions for each security.
    Returns list of (security_name, buy_date, buy_price, shares, sell_date, sell_price, realized_pl)
    
    Note: If no buy transaction is found in the data, buy_date will be 'N/A' and 
    buy_price will be calculated from (sell_price - realized_pl/shares).
    """
    
    # Dictionary to track transactions by security (WKN)
    # WKN is the German securities identifier
    transactions_by_security = defaultdict(lambda: {'buys': [], 'sells': []})
    
    # Read CSV with proper encoding
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=';')
            
            for row in reader:
                try:
                    trans = Transaction(row)
                    
                    # Only track actual buy/sell transactions
                    if trans.is_buy():
                        transactions_by_security[trans.wkn]['buys'].append(trans)
                    elif trans.is_sell():
                        transactions_by_security[trans.wkn]['sells'].append(trans)
                        
                except Exception as e:
                    # Skip rows that can't be parsed
                    continue
                    
    except UnicodeDecodeError:
        # Try Windows-1252 encoding if UTF-8 fails
        with open(filepath, 'r', encoding='windows-1252') as f:
            reader = csv.DictReader(f, delimiter=';')
            
            for row in reader:
                try:
                    trans = Transaction(row)
                    
                    if trans.is_buy():
                        transactions_by_security[trans.wkn]['buys'].append(trans)
                    elif trans.is_sell():
                        transactions_by_security[trans.wkn]['sells'].append(trans)
                        
                except Exception as e:
                    continue
    
    # Match buy/sell pairs using FIFO (First In, First Out)
    results = []
    
    for wkn, trans_dict in transactions_by_security.items():
        buys = sorted(trans_dict['buys'], key=lambda x: x.date)
        sells = sorted(trans_dict['sells'], key=lambda x: x.date)
        
        if not sells:
            # No sells for this security
            continue
            
        # For each sell, match with corresponding buys (FIFO)
        for sell in sells:
            if sell.shares == 0:
                continue
                
            remaining_shares = abs(sell.shares)
            sell_price_per_share = sell.get_price_per_share()
            
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
                
                # Calculate P/L for this portion
                # Note: sell.realized_pl already contains the total P/L from the CSV
                # We'll use it proportionally
                proportion = shares_to_match / abs(sell.shares)
                partial_pl = sell.realized_pl * proportion
                
                results.append((
                    sell.security_name,
                    buy.date.strftime('%d.%m.%Y'),
                    buy_price_per_share,
                    shares_to_match,
                    sell.date.strftime('%d.%m.%Y'),
                    sell_price_per_share,
                    partial_pl
                ))
                
                # Update remaining shares
                buy.shares -= shares_to_match
                remaining_shares -= shares_to_match
            
            # If we didn't match any buys, still report the sell
            # Calculate implied buy price from P/L
            if not matched_any or remaining_shares > 0:
                shares_unmatched = abs(sell.shares) if not matched_any else remaining_shares
                proportion = shares_unmatched / abs(sell.shares)
                partial_pl = sell.realized_pl * proportion
                
                # Calculate implied buy price: sell_price - (pl / shares)
                if shares_unmatched > 0:
                    implied_buy_price = sell_price_per_share - (partial_pl / shares_unmatched)
                else:
                    implied_buy_price = Decimal('0')
                
                results.append((
                    sell.security_name,
                    'N/A (before data)',
                    implied_buy_price,
                    shares_unmatched,
                    sell.date.strftime('%d.%m.%Y'),
                    sell_price_per_share,
                    partial_pl
                ))
    
    return results


def print_results(results: List[Tuple]):
    """Print results in a formatted table."""
    print(f"\n{'Security':<40} {'Buy Date':<20} {'Buy Price':>12} {'Shares':>10} {'Sell Date':<12} {'Sell Price':>12} {'Realized P/L':>15}")
    print("=" * 150)
    
    total_pl = Decimal('0')
    
    for security, buy_date, buy_price, shares, sell_date, sell_price, realized_pl in results:
        print(f"{security:<40} {buy_date:<20} {buy_price:>12.2f} {shares:>10.2f} {sell_date:<12} {sell_price:>12.2f} {realized_pl:>15.2f}")
        total_pl += realized_pl
    
    print("=" * 150)
    print(f"{'Total Realized P/L:':<133} {total_pl:>15.2f}")


def main():
    if len(sys.argv) > 1:
        filepath = sys.argv[1]
    else:
        # Default to the CSV in the data directory
        filepath = r'd:\src\portfolio_tracker\data\steuerlichedetailansichtexport_9772900462_20251205-1606.csv'
    
    print(f"Analyzing transactions from: {filepath}")
    
    results = analyze_csv(filepath)
    
    if not results:
        print("\nNo buy/sell transaction pairs found in the CSV.")
        return
    
    print_results(results)
    
    # Also save to CSV
    output_file = r'd:\src\portfolio_tracker\data\transaction_analysis.csv'
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['Security Name', 'Buy Date', 'Buy Price', 'Share Count', 'Sell Date', 'Sell Price', 'Realized P/L'])
        
        for row in results:
            writer.writerow(row)
    
    print(f"\nResults saved to: {output_file}")


if __name__ == '__main__':
    main()
