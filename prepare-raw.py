"""
This script prepares raw astrophotography data by organizing files into specific directories.
It uses command-line arguments to specify input and output locations.
"""

import argparse

import common
import filesystem

parser = argparse.ArgumentParser(description="copy files")
parser.add_argument("--input_dir", type=str, help="directory to search for images", default=common.DIRECTORY_ROOT_RAW)
parser.add_argument("--input_pattern", type=str, help="image filename pattern", default=common.INPUT_PATTERN_ALL)
parser.add_argument("--output_bias_dir", type=str, help="directory to search for images", default=common.DIRECTORY_ROOT_RAW)
parser.add_argument("--output_dark_dir", type=str, help="directory to search for images", default=common.DIRECTORY_ROOT_RAW)
parser.add_argument("--output_flat_dir", type=str, help="directory to search for images", default=common.DIRECTORY_ROOT_RAW)
parser.add_argument("--output_light_dir", type=str, help="directory to search for images", default=common.DIRECTORY_ROOT_DATA)
parser.add_argument("--debug", action='store_true')
parser.add_argument("--dryrun", action='store_true')

# treat args parsed as a dictionary
args = vars(parser.parse_args())

user_input_dir = args["input_dir"]
user_input_pattern = args["input_pattern"]
user_output_bias_dir = args["output_bias_dir"]
user_output_dark_dir = args["output_dark_dir"]
user_output_flat_dir = args["output_flat_dir"]
user_output_light_dir = args["output_light_dir"]
user_debug = args["debug"]
user_dryrun = args["dryrun"]

p = filesystem.Prepare(
    input_dir=user_input_dir,
    input_pattern=user_input_pattern,
    output_dir_bias=user_output_bias_dir,
    output_dir_dark=user_output_dark_dir,
    output_dir_flat=user_output_flat_dir,
    output_dir_light=user_output_light_dir,
    debug=user_debug,
    dryrun=user_dryrun,
)

p.bias()
p.dark()
p.flat()
p.light(printStatus=True)
