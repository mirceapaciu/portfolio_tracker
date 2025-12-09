"""
Extract dividend transactions from comdirect tax export CSV.

Filters for transactions with 'Vorgang' containing:
- Zinsen
- Dividenden
- Divid.
- Ausschüttung

Output columns: Date, Security name, Dividend amount
"""

import csv
from pathlib import Path
from typing import List, Dict


def extract_dividends(input_file: Path, output_file: Path) -> None:
    """
    Extract dividend transactions from tax export CSV.
    
    Args:
        input_file: Path to input CSV file
        output_file: Path to output CSV file
    """
    # Keywords to identify dividend transactions
    dividend_keywords = ['Zinsen', 'Dividenden', 'Divid.', 'Ausschüttung']
    
    dividends: List[Dict[str, str]] = []
    
    # Read input CSV with semicolon delimiter and Windows-1252 encoding
    with open(input_file, 'r', encoding='windows-1252') as f:
        reader = csv.DictReader(f, delimiter=';')
        
        for row in reader:
            vorgang = row.get('Vorgang', '').strip()
            
            # Check if transaction type contains any dividend keyword
            if any(keyword in vorgang for keyword in dividend_keywords):
                # Extract relevant fields
                buchungstag = row.get('Buchungstag', '').strip().strip('"')
                bezeichnung = row.get('Bezeichnung', '').strip().strip('"')
                betrag_brutto = row.get('Betrag Brutto', '').strip().strip('"')
                
                # Try different possible column names for share count due to encoding
                share_count = row.get('Stück/Nominale', '').strip().strip('"')
                if not share_count:
                    share_count = row.get('St�ck/Nominale', '').strip().strip('"')
                
                # Convert German decimal format (comma) to standard format (dot)
                betrag_brutto = betrag_brutto.replace(',', '.')
                share_count = share_count.replace(',', '.')
                
                dividends.append({
                    'Date': buchungstag,
                    'Security name': bezeichnung,
                    'Share count': share_count,
                    'Dividend amount': betrag_brutto
                })
    
    # Write output CSV
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['Date', 'Security name', 'Share count', 'Dividend amount']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        
        writer.writeheader()
        writer.writerows(dividends)
    
    print(f"Extracted {len(dividends)} dividend transactions")
    print(f"Output written to: {output_file}")


if __name__ == '__main__':
    # Define file paths
    data_dir = Path(__file__).parent.parent / 'data'
    input_file = data_dir / 'steuerlichedetailansichtexport_9772900462_20251205-1606.csv'
    output_file = data_dir / 'dividends_extract.csv'
    
    # Extract dividends
    extract_dividends(input_file, output_file)
