"""
Repository functions for security_t table operations.
"""

import sqlite3
from datetime import datetime


def get_or_create_security(cursor: sqlite3.Cursor, security_name: str, isin: str = None,
                           symbol: str = None, asset_type: str = None, wkn: str = None) -> int:
    """
    Get security ID, creating the security if it doesn't exist.
    
    Args:
        cursor: Database cursor
        security_name: Name of the security
        isin: International Securities Identification Number (optional)
        symbol: Trading symbol (optional)
        asset_type: Type of asset - e.g., 'stock', 'ETF', 'fund' (optional)
        
    Returns:
        Security ID
    """
    # Try to find existing security by name
    cursor.execute("SELECT id, wkn FROM security_t WHERE security_name = ?", (security_name,))
    row = cursor.fetchone()
    
    if row:
        security_id, existing_wkn = row
        if wkn and not existing_wkn:
            cursor.execute(
                "UPDATE security_t SET wkn = ?, updated_at = ? WHERE id = ?",
                (wkn, datetime.now(), security_id)
            )
        return security_id
    
    # If ISIN provided, check if it exists
    if isin:
        cursor.execute("SELECT id FROM security_t WHERE isin = ?", (isin,))
        row = cursor.fetchone()
        if row:
            return row[0]
    
    # If WKN provided, check if it exists
    if wkn:
        cursor.execute("SELECT id FROM security_t WHERE wkn = ?", (wkn,))
        row = cursor.fetchone()
        if row:
            return row[0]
    
    # Create new security
    cursor.execute(
        """INSERT INTO security_t (security_name, wkn, isin, symbol, asset_type, created_at, updated_at) 
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (security_name, wkn, isin, symbol, asset_type, datetime.now(), datetime.now())
    )
    return cursor.lastrowid


def get_security_by_name(cursor: sqlite3.Cursor, security_name: str) -> dict | None:
    """
    Get security details by name.
    
    Args:
        cursor: Database cursor
        security_name: Name of the security
        
    Returns:
        Dictionary with security details or None if not found
    """
    cursor.execute(
          """SELECT id, security_name, wkn, isin, symbol, asset_type, created_at, updated_at 
           FROM security_t WHERE security_name = ?""",
        (security_name,)
    )
    row = cursor.fetchone()
    
    if not row:
        return None
    
    return {
        'id': row[0],
        'security_name': row[1],
        'wkn': row[2],
        'isin': row[3],
        'symbol': row[4],
        'asset_type': row[5],
        'created_at': row[6],
        'updated_at': row[7]
    }


def get_security_by_isin(cursor: sqlite3.Cursor, isin: str) -> dict | None:
    """
    Get security details by ISIN.
    
    Args:
        cursor: Database cursor
        isin: International Securities Identification Number
        
    Returns:
        Dictionary with security details or None if not found
    """
    cursor.execute(
          """SELECT id, security_name, wkn, isin, symbol, asset_type, created_at, updated_at 
           FROM security_t WHERE isin = ?""",
        (isin,)
    )
    row = cursor.fetchone()
    
    if not row:
        return None
    
    return {
        'id': row[0],
        'security_name': row[1],
        'wkn': row[2],
        'isin': row[3],
        'symbol': row[4],
        'asset_type': row[5],
        'created_at': row[6],
        'updated_at': row[7]
    }


def update_security(cursor: sqlite3.Cursor, security_id: int, isin: str = None,
                   symbol: str = None, asset_type: str = None, wkn: str = None):
    """
    Update security details.
    
    Args:
        cursor: Database cursor
        security_id: Security ID
        isin: International Securities Identification Number (optional)
        symbol: Trading symbol (optional)
        asset_type: Type of asset (optional)
    """
    updates = []
    params = []
    
    if isin is not None:
        updates.append("isin = ?")
        params.append(isin)
    
    if symbol is not None:
        updates.append("symbol = ?")
        params.append(symbol)
    
    if wkn is not None:
        updates.append("wkn = ?")
        params.append(wkn)
    
    if asset_type is not None:
        updates.append("asset_type = ?")
        params.append(asset_type)
    
    if updates:
        updates.append("updated_at = ?")
        params.append(datetime.now())
        params.append(security_id)
        
        sql = f"UPDATE security_t SET {', '.join(updates)} WHERE id = ?"
        cursor.execute(sql, params)


def list_all_securities(cursor: sqlite3.Cursor) -> list[dict]:
    """
    List all securities.
    
    Args:
        cursor: Database cursor
        
    Returns:
        List of dictionaries with security details
    """
    cursor.execute(
        """SELECT id, security_name, wkn, isin, symbol, asset_type, created_at, updated_at 
           FROM security_t ORDER BY security_name"""
    )
    
    securities = []
    for row in cursor.fetchall():
        securities.append({
            'id': row[0],
            'security_name': row[1],
            'wkn': row[2],
            'isin': row[3],
            'symbol': row[4],
            'asset_type': row[5],
            'created_at': row[6],
            'updated_at': row[7]
        })
    
    return securities
