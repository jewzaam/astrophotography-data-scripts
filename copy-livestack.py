import argparse
import os
import sys

import common

# find all data in blink dir that needs calibration
# copy calibration data to the data sets
# report any data sets missing calibration data
# [optional] move data with calibration data to next directory

# required properties for a master calibration frame
REQUIRED_PROPERTIES=['type', 'exposureseconds', 'gain', 'offset', 'camera']

parser = argparse.ArgumentParser(description="copy calibration files")
parser.add_argument("--calibration_dir", type=str, help="calibration directory, output of WBPP", default=os.path.join(common.DIRECTORY_ROOT_WBPP, common.DIRECTORY_CALIBRATION))
parser.add_argument("--darklibrary_dir", type=str, help="darks library directory", default=common.DIRECTORY_ROOT_DARKLIBRARY,)
parser.add_argument("--livestack_dir", type=str, help="livestack directory", default=common.DIRECTORY_ROOT_LIVESTACK,)
parser.add_argument("--debug", action='store_true')
parser.add_argument("--dryrun", action='store_true')

# treat args parsed as a dictionary
args = vars(parser.parse_args())

user_calibration_dir = args["calibration_dir"]
user_darklibrary_dir = args["darklibrary_dir"]
user_livestack_dir = args["livestack_dir"]
user_debug = args["debug"]
user_dryrun = args["dryrun"]

print("ERROR this script needs updated.")
sys.exit(-1)

# copy darks to live stack
common.copy_calibration_to_library(
    type="MASTER DARK",
    calibration_dir=user_darklibrary_dir, 
    library_dir=user_livestack_dir,
    # exclude settemp from group by for livestack
    group_by=['exposureseconds', 'camera', 'gain', 'offset', 'type'],
    delete_after_copy=False,
    debug=user_debug, 
    dryrun=user_dryrun,
)

# copy flats to live stack
common.copy_calibration_to_library(
    type="MASTER FLAT",
    calibration_dir=user_calibration_dir, 
    library_dir=user_livestack_dir,
    # exclude date and settemp from group by for livestack
    group_by=['filter', 'optic', 'camera', 'gain', 'offset', 'type'],
    delete_after_copy=False,
    debug=user_debug, 
    dryrun=user_dryrun,
)
