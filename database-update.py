"""
This script updates the astrophotography database with new or modified data.
It uses command-line arguments to specify the mode of operation (create, delete, or update).
"""

import argparse

import common
import database


DATETIME_FORMAT="%Y-%m-%dT%H:%M:%S"
DATE_FORMAT="%Y-%m-%d"

parser = argparse.ArgumentParser(description="upsert accepted images to AP database")
parser.add_argument("--fromdir", required=True, type=str, help="directory to search for images")
parser.add_argument("--modeCreate", action='store_true', help="looks for new images only")
parser.add_argument("--modeDelete", action='store_true', help="looks for deleted images only")
parser.add_argument("--modeUpdate", action='store_true', help="looks for deleted images only")
parser.add_argument("--debug", action='store_true')
parser.add_argument("--dryrun", action='store_true')

# treat args parsed as a dictionary
args = vars(parser.parse_args())

user_fromdir = args["fromdir"]
user_modeCreate = args["modeCreate"]
user_modeDelete = args["modeDelete"]
user_modeUpdate = args["modeUpdate"]
user_debug = args["debug"]
user_dryrun = args["dryrun"]

db_ap = database.Astrophotgraphy(
    db_filename=common.DATABASE_ASTROPHOTGRAPHY,
    debug=user_debug,
    dryrun=user_dryrun,
)

try:
    db_ap.open()
    db_ap.UpdateFromDirectory(
        from_dir=user_fromdir, 
        modeCreate=user_modeCreate,
        modeDelete=user_modeDelete,
        modeUpdate=user_modeUpdate,
    )
finally:
    # always commit even if there's an exception
    db_ap.commit()
    db_ap.close()
