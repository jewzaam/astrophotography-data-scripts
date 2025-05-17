"""
This script deletes calibration files from specified directories.
It uses command-line arguments to specify input directories and enable debugging or dry-run modes.
"""

import argparse
import os
import shutil
import pathlib

import common
import filesystem

parser = argparse.ArgumentParser(description="delete calibration files")
parser.add_argument("--input_dir", type=str, help="directory to search for calibration images", default=common.DIRECTORY_ROOT_RAW)
parser.add_argument("--calibration_dir", type=str, help="calibration directory", default=os.path.join(common.DIRECTORY_ROOT_WBPP, common.DIRECTORY_CALIBRATION))
parser.add_argument("--debug", action='store_true')
parser.add_argument("--dryrun", action='store_true')

# treat args parsed as a dictionary
args = vars(parser.parse_args())

user_input_dir = args["input_dir"]
user_calibration_dir = args["calibration_dir"]
user_debug = args["debug"]
user_dryrun = args["dryrun"]

p = filesystem.Delete(
    input_dir=user_input_dir,
    input_pattern=".*\.(fits|xisf)$",
    debug=user_debug,
    dryrun=user_dryrun,
)

p.bias()
p.dark()
p.flat()

# special case, delete the calibration dir recursively and recreate it empty
if not user_dryrun:
    shutil.rmtree(user_calibration_dir)
    pathlib.Path(user_calibration_dir).mkdir(parents=True, exist_ok=True)