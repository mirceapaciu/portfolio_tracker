"""
Repository functions for broker_t table operations.
"""

import sqlite3


def get_or_create_broker(cursor: sqlite3.Cursor, broker_name: str) -> int:
    """
    Get broker ID, creating the broker if it doesn't exist.
    
    Args:
        cursor: Database cursor
        broker_name: Name of the broker (e.g., 'traderepublic', 'comdirect')
        
    Returns:
        Broker ID
    """
    # Try to find existing broker
    cursor.execute("SELECT id FROM broker_t WHERE broker_name = ?", (broker_name,))
    row = cursor.fetchone()
    
    if row:
        return row[0]
    
    # Create new broker
    cursor.execute("INSERT INTO broker_t (broker_name) VALUES (?)", (broker_name,))
    return cursor.lastrowid


def get_broker_by_name(cursor: sqlite3.Cursor, broker_name: str) -> int | None:
    """
    Get broker ID by name.
    
    Args:
        cursor: Database cursor
        broker_name: Name of the broker
        
    Returns:
        Broker ID or None if not found
    """
    cursor.execute("SELECT id FROM broker_t WHERE broker_name = ?", (broker_name,))
    row = cursor.fetchone()
    return row[0] if row else None


def get_broker_by_id(cursor: sqlite3.Cursor, broker_id: int) -> str | None:
    """
    Get broker name by ID.
    
    Args:
        cursor: Database cursor
        broker_id: Broker ID
        
    Returns:
        Broker name or None if not found
    """
    cursor.execute("SELECT broker_name FROM broker_t WHERE id = ?", (broker_id,))
    row = cursor.fetchone()
    return row[0] if row else None


def list_all_brokers(cursor: sqlite3.Cursor) -> list[tuple[int, str]]:
    """
    List all brokers.
    
    Args:
        cursor: Database cursor
        
    Returns:
        List of (id, broker_name) tuples
    """
    cursor.execute("SELECT id, broker_name FROM broker_t ORDER BY broker_name")
    return cursor.fetchall()
