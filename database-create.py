"""
This script creates the schema and initial data for the astrophotography database.
It uses predefined methods to set up profiles and filters in the database.
"""

import common
import database

db_ap = database.Astrophotgraphy(
    db_filename=common.DATABASE_ASTROPHOTGRAPHY,
    autoCommit=False,
)

try:
    db_ap.open()

    db_ap.CreateSchema()
    profile_stmts = db_ap.CreateProfileStmts(profile_dir=common.DIRECTORY_NINA_PROFILES)
    for stmt in profile_stmts:
        db_ap.execute(stmt)
    db_ap.CreateFilters()
finally:
    db_ap.commit()
    db_ap.close()