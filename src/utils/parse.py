"""
Parsing utility functions for CSV data import.
"""

from datetime import datetime
from decimal import Decimal


def parse_german_decimal(value: str) -> Decimal | None:
    """
    Convert German decimal format to Decimal.
    
    German format uses comma as decimal separator and dot as thousands separator.
    Examples: "1.234,56" -> Decimal("1234.56"), "47,53" -> Decimal("47.53")
    
    Args:
        value: String in German decimal format
        
    Returns:
        Decimal object or None if parsing fails
    """
    if not value or value.strip() == "":
        return None
    # Remove thousands separator (.) and replace decimal comma with dot
    value = value.replace(".", "").replace(",", ".")
    try:
        return Decimal(value)
    except Exception:
        return None


def parse_german_date(date_str: str) -> str | None:
    """
    Convert German date format DD.MM.YYYY to YYYY-MM-DD.
    
    Args:
        date_str: Date string in DD.MM.YYYY format
        
    Returns:
        Date string in YYYY-MM-DD format or None if parsing fails
    """
    if not date_str or date_str.strip() == "":
        return None
    try:
        dt = datetime.strptime(date_str.strip(), "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None


def parse_date(date_str: str) -> str | None:
    """
    Parse date string with multiple format support.
    
    Tries multiple formats:
    - DD.MM.YY (German format with 2-digit year)
    - DD.MM.YYYY (German format with 4-digit year)
    - YYYY-MM-DD (ISO format)
    - DD/MM/YYYY
    
    Args:
        date_str: Date string in various formats
        
    Returns:
        Date string in YYYY-MM-DD format or None if parsing fails
    """
    if not date_str or date_str.strip() == "":
        return None
    
    date_str = date_str.strip()
    
    # Try DD.MM.YY format first (2-digit year)
    try:
        dt = datetime.strptime(date_str, "%d.%m.%y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    
    # Try DD.MM.YYYY (German format with 4-digit year)
    try:
        dt = datetime.strptime(date_str, "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    
    # Try YYYY-MM-DD format
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    
    # Try DD/MM/YYYY
    try:
        dt = datetime.strptime(date_str, "%d/%m/%Y")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass
    
    return None
