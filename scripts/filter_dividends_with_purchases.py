"""
Filter dividends to only include securities with BUY transactions.

Reads dividends_extract.csv and comdirect_transaction_analysis.csv,
and outputs only dividends for securities that have buy transactions
(with available buy date and price) in the transaction analysis.
"""

import csv
from pathlib import Path
from typing import Set


def filter_dividends_with_purchases(
    dividends_file: Path,
    transactions_file: Path,
    output_file: Path
) -> None:
    """
    Filter dividends to only include securities with purchase history.
    
    Args:
        dividends_file: Path to dividends extract CSV
        transactions_file: Path to transaction analysis CSV
        output_file: Path to output filtered CSV
    """
    # Read securities with BUY transactions from transaction analysis
    securities_with_purchases: Set[str] = set()
    
    with open(transactions_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            security_name = row.get('Security Name', '').strip()
            buy_date = row.get('Buy Date', '').strip()
            buy_price = row.get('Buy Price', '').strip()
            
            # Only include if buy date and price are available
            if security_name and buy_date and buy_price:
                securities_with_purchases.add(security_name)
    
    print(f"Found {len(securities_with_purchases)} unique securities with BUY transactions")
    
    # Filter dividends
    filtered_dividends = []
    total_dividends = 0
    
    with open(dividends_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            total_dividends += 1
            security_name = row.get('Security name', '').strip()
            
            # Check if this security has purchase history
            if security_name in securities_with_purchases:
                filtered_dividends.append(row)
    
    # Write filtered output
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['Date', 'Security name', 'Share count', 'Dividend amount']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(filtered_dividends)
    
    print(f"Total dividends: {total_dividends}")
    print(f"Filtered dividends (with purchases): {len(filtered_dividends)}")
    print(f"Output written to: {output_file}")


if __name__ == '__main__':
    # Define file paths
    data_dir = Path(__file__).parent.parent / 'data'
    dividends_file = data_dir / 'dividends_extract.csv'
    transactions_file = data_dir / 'comdirect_transaction_analysis.csv'
    output_file = data_dir / 'dividends_with_purchases.csv'
    
    # Filter dividends
    filter_dividends_with_purchases(dividends_file, transactions_file, output_file)
