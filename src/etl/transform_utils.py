"""
Transform Utils

This module contains reusable functions for ETL.
"""

import re
import unicodedata


def _normalize_string(value: str) -> str:
    """Return a lowercase ASCII-only variant for robust regex matching."""
    normalized = unicodedata.normalize('NFKD', value)
    normalized = normalized.encode('ascii', 'ignore').decode('ascii')
    return normalized.lower().strip()


def transform_transaction_type(raw_type: str) -> str:
    """
    Normalize transaction type to standard values.
    
    Args:
        raw_type: Raw transaction type from staging table
        
    Returns:
        Normalized transaction type: 'buy', 'sell', 'dividend', 'interest', 'distribution'
    """
    if not raw_type:
        return None
    
    normalized = _normalize_string(raw_type)

    # Map transaction types using ASCII-safe keys
    type_mapping = {
        'buy': 'buy',
        'kauf': 'buy',
        'sell': 'sell',
        'abgang': 'sell',
        'verkauf': 'sell',
        'dividend': 'dividend',
        'dividende': 'dividend',
        'ausl. dividenden': 'dividend',
        'inl. dividenden': 'dividend',
        'ausl dividenden': 'dividend',
        'inl dividenden': 'dividend',
        'interest': 'interest',
        'zinsen': 'interest',
        'ausl. zinsen': 'interest',
        'inl. zinsen': 'interest',
        'ausl zinsen': 'interest',
        'inl zinsen': 'interest',
        'distribution': 'distribution',
        'ausschuettung': 'distribution',
        'ausschuttung': 'distribution',
        'investm. ausschuettung': 'distribution',
        'investm. ausschuttung': 'distribution'
    }

    if normalized in type_mapping:
        return type_mapping[normalized]

    pattern_map = (
        (r'divid', 'dividend'),
        (r'aussch', 'distribution'),
        (r'zins', 'interest'),
    )

    for pattern, mapped in pattern_map:
        if re.search(pattern, normalized):
            return mapped
    
    return normalized

