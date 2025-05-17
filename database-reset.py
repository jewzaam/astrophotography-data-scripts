"""
This script resets the astrophotography database by deleting all data from specific tables.
It connects to the database and executes SQL commands to clear the data.
"""

import sqlite3

import common

with sqlite3.connect(common.DATABASE_ASTROPHOTGRAPHY) as conn:
    c = conn.cursor()

    c.execute("delete from accepted_data;")
    c.execute("delete from target;")
    conn.commit()