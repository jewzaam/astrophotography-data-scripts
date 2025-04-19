import sqlite3

import common

with sqlite3.connect(common.DATABASE_ASTROPHOTGRAPHY) as conn:
    c = conn.cursor()

    c.execute("delete from accepted_data;")
    c.execute("delete from target;")
    conn.commit()