import argparse
import os

import common
import filesystem

parser = argparse.ArgumentParser(description="output data about sky flats to csv for analysis")
parser.add_argument("--input_dir", type=str, help="directory to search for images", default=common.DIRECTORY_ROOT_RAW_FLAT)
parser.add_argument("--output_csv", type=str, help="csv file to output results", default=common.DIRECTORY_ROOT_RAW_FLAT+os.path.sep+"sky-flats-analysis.csv")
parser.add_argument("--debug", action='store_true')
parser.add_argument("--dryrun", action='store_true')

# treat args parsed as a dictionary
args = vars(parser.parse_args())

user_input_dir = args["input_dir"]
user_output_csv = args["output_csv"]
user_debug = args["debug"]
user_dryrun = args["dryrun"]


print(f"Reading data for sky flats...")

data_flats = common.get_filtered_metadata(
    dirs=[user_input_dir],
    patterns=[".*\.fits$"],
    recursive=True,
    filters={"type": "FLAT"},
    profileFromPath=True,
)

if user_debug:
    print(data_flats)

# convert data to array, drop key since it's already in the 'filename' attribute
data_flattened = [] # yes, I get this is a bit ironic and possibly confusing. I like it, move along.
for key in data_flats:
    datum = data_flats[key]
    # keep a subset of attributes to make the data easier to work with
    keep_keys = [
        "filter",
        "camera",
        "optic",
        "focal_ratio",
        "date-loc",
        "sunangle",
        "centalt",
        "exposureseconds",
        "gain",
        "offset",
        "moonangl",
        "settemp",
        "temp",
        "ra",
        "dec",
        "filename",
    ]
    datum_filtered = {}
    for k in keep_keys:
        datum_filtered[k] = datum[k]
    data_flattened.append(datum_filtered)

if user_debug:
    print(data_flattened)


print(f"Writing CSV for sky flats...")
filename_csv = user_output_csv
data_csv = common.simpleObject_to_csv(data_flattened, output_headers=True)
with open(filename_csv, "w") as f:
    f.write(data_csv)
