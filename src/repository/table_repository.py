"""
Repository functions for table_t metadata table operations.
"""

import sqlite3


def get_or_create_table_id(cursor: sqlite3.Cursor, table_name: str) -> int:
    """
    Get table metadata ID, creating the entry if it doesn't exist.
    
    Args:
        cursor: Database cursor
        table_name: Name of the staging table (e.g., 'traderepublic_transactions_staging')
        
    Returns:
        Table ID
    """
    # Try to find existing table
    cursor.execute("SELECT id FROM table_t WHERE table_name = ?", (table_name,))
    row = cursor.fetchone()
    
    if row:
        return row[0]
    
    # Create new table entry
    cursor.execute("INSERT INTO table_t (table_name) VALUES (?)", (table_name,))
    return cursor.lastrowid


def get_table_by_name(cursor: sqlite3.Cursor, table_name: str) -> int | None:
    """
    Get table ID by name.
    
    Args:
        cursor: Database cursor
        table_name: Name of the table
        
    Returns:
        Table ID or None if not found
    """
    cursor.execute("SELECT id FROM table_t WHERE table_name = ?", (table_name,))
    row = cursor.fetchone()
    return row[0] if row else None


def get_table_by_id(cursor: sqlite3.Cursor, table_id: int) -> str | None:
    """
    Get table name by ID.
    
    Args:
        cursor: Database cursor
        table_id: Table ID
        
    Returns:
        Table name or None if not found
    """
    cursor.execute("SELECT table_name FROM table_t WHERE id = ?", (table_id,))
    row = cursor.fetchone()
    return row[0] if row else None


def list_all_tables(cursor: sqlite3.Cursor) -> list[tuple[int, str]]:
    """
    List all registered tables.
    
    Args:
        cursor: Database cursor
        
    Returns:
        List of (id, table_name) tuples
    """
    cursor.execute("SELECT id, table_name FROM table_t ORDER BY table_name")
    return cursor.fetchall()
