"""
Create aggregated investment report with CAGR calculations.

Aggregates transactions by security, buy date, and sell date,
calculates total dividends received during holding period,
and computes CAGR based on total returns.
"""

import csv
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
from collections import defaultdict


def parse_german_date(date_str: str) -> datetime:
    """Parse German date format DD.MM.YYYY."""
    return datetime.strptime(date_str, '%d.%m.%Y')


def calculate_cagr(initial_value: float, final_value: float, years: float) -> float:
    """
    Calculate Compound Annual Growth Rate (CAGR).
    
    CAGR = (Final Value / Initial Value)^(1/years) - 1
    
    Args:
        initial_value: Initial investment
        final_value: Final value (investment + returns)
        years: Holding period in years
    
    Returns:
        CAGR as a percentage
    """
    if initial_value <= 0 or years <= 0:
        return 0.0
    
    if final_value <= 0:
        # For losses, calculate negative CAGR
        return -100.0 * (1 - (abs(final_value) / initial_value) ** (1 / years))
    
    cagr = ((final_value / initial_value) ** (1 / years) - 1) * 100
    return cagr


def load_dividends(dividends_file: Path) -> List[Dict[str, str]]:
    """Load dividend data from CSV."""
    dividends = []
    with open(dividends_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            dividends.append(row)
    return dividends


def aggregate_transactions(
    transactions_file: Path,
    dividends_file: Path,
    output_file: Path
) -> None:
    """
    Aggregate transactions and calculate comprehensive metrics.
    
    Groups by security name, buy date, and sell date.
    Calculates total invested value, P/L, dividends, and CAGR.
    """
    # Load dividends
    dividends = load_dividends(dividends_file)
    
    # Track which dividends have been matched
    matched_dividend_indices = set()
    
    # Aggregate transactions by (security, buy_date, sell_date)
    aggregated: Dict[Tuple[str, str, str], Dict] = defaultdict(lambda: {
        'invested_value': 0.0,
        'realized_pl': 0.0,
        'buy_date': '',
        'sell_date': '',
        'security_name': '',
        'share_count': 0.0
    })
    
    with open(transactions_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            security_name = row['Security Name'].strip()
            buy_date = row['Buy Date'].strip()
            sell_date = row['Sell Date'].strip()
            invested_value = float(row['Invested Value'].replace(',', '.'))
            realized_pl = float(row['Realized P/L'].replace(',', '.'))
            share_count = float(row['Share Count'].replace(',', '.'))
            
            key = (security_name, buy_date, sell_date)
            
            aggregated[key]['security_name'] = security_name
            aggregated[key]['buy_date'] = buy_date
            aggregated[key]['sell_date'] = sell_date
            aggregated[key]['invested_value'] += invested_value
            aggregated[key]['realized_pl'] += realized_pl
            aggregated[key]['share_count'] += share_count
    
    # Calculate dividends and CAGR for each aggregated position
    results = []
    
    for key, data in aggregated.items():
        security_name = data['security_name']
        buy_date_str = data['buy_date']
        sell_date_str = data['sell_date']
        invested_value = data['invested_value']
        realized_pl = data['realized_pl']
        position_share_count = data['share_count']
        
        # Parse dates
        buy_date = parse_german_date(buy_date_str)
        sell_date = parse_german_date(sell_date_str)
        
        # Calculate total dividends received during holding period
        # Match by security name and date range, apply pro-rata based on share count
        total_dividend = 0.0
        dividend_count = 0
        
        for idx, div in enumerate(dividends):
            # Skip if already matched
            if idx in matched_dividend_indices:
                continue
                
            div_security = div['Security name'].strip()
            div_date_str = div['Date'].strip()
            div_amount = float(div['Dividend amount'])
            div_share_count_str = div.get('Share count', '').strip()
            
            # Parse dividend share count
            try:
                div_share_count = float(div_share_count_str) if div_share_count_str else 0.0
            except ValueError:
                div_share_count = 0.0
            
            # Check if security matches (ignore share count for matching)
            if div_security == security_name:
                div_date = parse_german_date(div_date_str)
                # Check if dividend date is within holding period (inclusive)
                if buy_date <= div_date <= sell_date:
                    # Calculate pro-rata dividend based on share count ratio
                    if div_share_count > 0 and position_share_count > 0:
                        # Pro-rata: (position shares / dividend shares) * dividend amount
                        prorata_dividend = (position_share_count / div_share_count) * div_amount
                    else:
                        # If share counts not available, use full amount
                        prorata_dividend = div_amount
                    
                    total_dividend += prorata_dividend
                    dividend_count += 1
                    matched_dividend_indices.add(idx)  # Mark as matched, use only once
        
        # Calculate holding period in years
        holding_days = (sell_date - buy_date).days
        holding_years = holding_days / 365.25
        
        # Calculate CAGR
        # Final value = Initial investment + P/L + Dividends
        total_return = realized_pl + total_dividend
        final_value = invested_value + total_return
        
        cagr = calculate_cagr(invested_value, final_value, holding_years)
        
        results.append({
            'Security name': security_name,
            'Share count': f'{position_share_count:.0f}',
            'Invested_value': f'{invested_value:.2f}',
            'BUY Date': buy_date_str,
            'Sell Date': sell_date_str,
            'P/L': f'{realized_pl:.2f}',
            'Total dividend': f'{total_dividend:.2f}',
            'Dividend count': dividend_count,
            'CAGR (%)': f'{cagr:.2f}'
        })
    
    # Sort by security name, then by buy date
    results.sort(key=lambda x: (x['Security name'], parse_german_date(x['BUY Date'])))
    
    # Print unmatched dividends
    unmatched_dividends = []
    for idx, div in enumerate(dividends):
        if idx not in matched_dividend_indices:
            unmatched_dividends.append(div)
    
    if unmatched_dividends:
        print("\n" + "="*80)
        print(f"UNMATCHED DIVIDENDS: {len(unmatched_dividends)} dividends not matched to positions")
        print("="*80)
        for div in unmatched_dividends:
            print(f"  {div['Date']} | {div['Security name']:40s} | "
                  f"Shares: {div.get('Share count', 'N/A'):>8s} | "
                  f"Amount: {div['Dividend amount']:>10s}")
        print("="*80 + "\n")
    
    # Write output
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['Security name', 'Share count', 'Invested_value', 'BUY Date', 'Sell Date', 
                      'P/L', 'Total dividend', 'Dividend count', 'CAGR (%)']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(results)
    
    print(f"Created aggregated report with {len(results)} positions")
    print(f"Matched {len(matched_dividend_indices)} dividends to positions")
    print(f"Output written to: {output_file}")


if __name__ == '__main__':
    # Define file paths
    data_dir = Path(__file__).parent.parent / 'data'
    transactions_file = data_dir / 'comdirect_transaction_analysis.csv'
    dividends_file = data_dir / 'dividends_with_purchases.csv'
    output_file = data_dir / 'aggregated_investment_report.csv'
    
    # Generate report
    aggregate_transactions(transactions_file, dividends_file, output_file)
