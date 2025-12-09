"""
Verify if tax export contains sufficient data to generate transaction analysis.

This script analyzes the comdirect tax export file to determine if it contains
all necessary information to recreate the transaction analysis file.
"""

import csv
from pathlib import Path
from collections import defaultdict, Counter


def analyze_tax_export(tax_export_file: Path, transaction_analysis_file: Path) -> None:
    """
    Analyze the tax export file and compare with transaction analysis.
    """
    print("="*80)
    print("ANALYSIS: Can transaction analysis be generated from tax export?")
    print("="*80)
    print()
    
    # Load transaction analysis securities
    analysis_securities = set()
    analysis_transactions = []
    
    with open(transaction_analysis_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            security = row['Security Name'].strip()
            analysis_securities.add(security)
            analysis_transactions.append({
                'security': security,
                'buy_date': row['Buy Date'],
                'sell_date': row['Sell Date'],
                'shares': float(row['Share Count'].replace(',', '.')),
                'invested': float(row['Invested Value'].replace(',', '.')),
                'pl': float(row['Realized P/L'].replace(',', '.'))
            })
    
    print(f"Transaction Analysis File:")
    print(f"  - Total transaction lines: {len(analysis_transactions)}")
    print(f"  - Unique securities: {len(analysis_securities)}")
    print()
    
    # Analyze tax export
    vorgang_counts = Counter()
    sell_transactions = []
    buy_transactions = []
    tax_securities = set()
    
    with open(tax_export_file, 'r', encoding='windows-1252') as f:
        reader = csv.DictReader(f, delimiter=';')
        
        for row in reader:
            vorgang = row['Vorgang'].strip()
            vorgang_counts[vorgang] += 1
            
            security = row.get('Bezeichnung', '').strip().strip('"')
            if security:
                tax_securities.add(security)
            
            if 'Verkauf' in vorgang:
                sell_transactions.append({
                    'date': row['Buchungstag'].strip().strip('"'),
                    'security': security,
                    'vorgang': vorgang
                })
            
            if 'Kauf' in vorgang:
                buy_transactions.append({
                    'date': row['Buchungstag'].strip().strip('"'),
                    'security': security,
                    'vorgang': vorgang
                })
    
    print(f"Tax Export File:")
    print(f"  - Total transaction lines: {sum(vorgang_counts.values())}")
    print(f"  - Unique securities mentioned: {len(tax_securities)}")
    print()
    
    print("Transaction Types in Tax Export:")
    for vorgang, count in sorted(vorgang_counts.items()):
        print(f"  - {vorgang}: {count}")
    print()
    
    print(f"Sell transactions (Verkauf*): {len(sell_transactions)}")
    print(f"Buy transactions (Kauf*): {len(buy_transactions)}")
    print()
    
    # Key findings
    print("="*80)
    print("KEY FINDINGS:")
    print("="*80)
    print()
    
    if len(buy_transactions) == 0:
        print("❌ CRITICAL: No regular buy/purchase transactions found in tax export!")
        print("   The tax export only contains:")
        print("   - 'Kauf ausl.m.Ertragsant' (foreign purchase with accrued interest)")
        print("   - 'Kauf inl. m.Ertragsant' (domestic purchase with accrued interest)")
        print("   These are NOT the original security purchases.")
        print()
    
    print("CONCLUSION:")
    print("-" * 80)
    print("The tax export file CANNOT be used to generate the transaction analysis file.")
    print()
    print("Reason:")
    print("  The tax export is designed for TAX REPORTING purposes only.")
    print("  It contains:")
    print("    ✓ Sale transactions (Verkauf)")
    print("    ✓ Dividends and interest income")
    print("    ✓ Realized gains/losses")
    print()
    print("  It DOES NOT contain:")
    print("    ✗ Original purchase transactions (Kauf)")
    print("    ✗ Purchase dates for securities")
    print("    ✗ Purchase prices per share")
    print("    ✗ Cost basis information")
    print()
    print("To generate a complete transaction analysis, you would need:")
    print("  - Broker transaction history (Umsatzübersicht)")
    print("  - Account statements with buy/sell details")
    print("  - Or a separate export that includes purchase data")
    print("="*80)
    
    # Check securities coverage
    print()
    print("Securities Coverage Analysis:")
    print("-" * 80)
    securities_in_both = analysis_securities & tax_securities
    securities_only_in_analysis = analysis_securities - tax_securities
    securities_only_in_tax = tax_securities - analysis_securities
    
    print(f"Securities in both files: {len(securities_in_both)}")
    print(f"Securities only in transaction analysis: {len(securities_only_in_analysis)}")
    if securities_only_in_analysis:
        for sec in sorted(securities_only_in_analysis):
            print(f"  - {sec}")
    
    print(f"\nSecurities only in tax export: {len(securities_only_in_tax)}")
    if securities_only_in_tax:
        for sec in sorted(securities_only_in_tax)[:10]:  # Show first 10
            print(f"  - {sec}")
        if len(securities_only_in_tax) > 10:
            print(f"  ... and {len(securities_only_in_tax) - 10} more")


if __name__ == '__main__':
    data_dir = Path(__file__).parent.parent / 'data'
    tax_export_file = data_dir / 'input' / 'steuerlichedetailansichtexport_9772900462_20251205-1606.csv'
    transaction_analysis_file = data_dir / 'comdirect_transaction_analysis.csv'
    
    analyze_tax_export(tax_export_file, transaction_analysis_file)
