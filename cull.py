"""
This script identifies and rejects astrophotography images that do not meet quality criteria.
It organizes rejected images into a specified directory for further review.
"""

import os
import argparse
import statistics
import sys

import common

def reject_image(dir, file):
    # create target directory (include os.sep to strip it!)
    relativedir=dir.replace(f"{user_srcdir}{os.sep}", "")
    filename_only=file.split(os.sep)[-1]

    from_file=os.path.join(dir, file)
    to_dir=os.path.join(user_rejectdir, relativedir)
    to_file=os.path.join(to_dir, filename_only)

    # had some mistakes with paths early on..
    # verify to_dir contains user_rejectdir
    if user_rejectdir not in to_dir:
        print("ERROR attempting to move file to invalid location!  See following debug information:")
        print(f"relativedir={relativedir}")
        print(f"filename_only={filename_only}")
        print(f"user_rejectdir={user_rejectdir}")
        print(f"from_file={from_file}")
        print(f"to_dir={to_dir}")
        print(f"to_file={to_file}")
        sys.exit(1)

    if user_dryrun:
        if user_debug:
            print(f"relativedir={relativedir}")
            print(f"filename_only={filename_only}")
            print(f"user_rejectdir={user_rejectdir}")
            print(f"from_file={from_file}")
            print(f"to_dir={to_dir}")
            print(f"to_file={to_file}")
            print("DEBUG would move")
            print(f"   from: {from_file}")
            print(f"   to:   {to_file}")
    else:
        # move the file
        common.move_file(from_file=from_file, to_file=to_file)
    print(f"REJECTED: {relativedir}{os.sep}{filename_only}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="initial culling of images")
    parser.add_argument("--srcdir", type=str, help="source directory for input images")
    parser.add_argument("--rejectdir", type=str, help="directory to put all rejected images into (relative dir struture is preserved)")
    parser.add_argument("--max_hfr", type=float, help="maximum HFR to keep")
    parser.add_argument("--max_rms", type=float, help="maximum RMS in arcsec to keep")
    parser.add_argument("--auto_yes_percent", type=float, help="automatic accept rejection if is less than this percent of images")
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--dryrun", action='store_true')

    # treat args parsed as a dictionary
    args = vars(parser.parse_args())

    user_srcdir = args["srcdir"]
    user_rejectdir = args["rejectdir"]
    user_maxhfr = args["max_hfr"]
    user_maxrms = args["max_rms"]
    user_autoyespercent = args["auto_yes_percent"]
    user_debug = args["debug"]
    user_dryrun = args["dryrun"]

    data = common.get_metadata(
        dirs=[user_srcdir],
        patterns=[".*\.fits$"],
        recursive=True,
        required_properties=[],
        debug=user_debug,
        profileFromPath=True,
    )

    data_groups = {}

    # do file grouping
    for filename in data.keys():
        # skip anything already accepted
        if common.DIRECTORY_ACCEPT in filename:
            continue
        # strip filename
        directory = os.sep.join(filename.split(os.sep)[:-1])
        if directory not in data_groups:
            data_groups[directory] = []
        data_groups[directory].append(data[filename])

    if user_debug:
        print("DEBUG directory groupings:")
        for dir in data_groups.keys():
            print(f"    {dir}: {len(data_groups[dir])}")

    # loop over data sets
    overall_count_reject = 0
    overall_count_total = 0
    for directory in data_groups.keys():


        count_reject_hfr = 0
        count_reject_rms = 0
        count_reject = 0
        count_total = len(data_groups[directory])
        overall_count_total += count_total
        datum_reject = []

        for datum in data_groups[directory]:
            # check hfr
            key = "hfr"
            if key in datum and datum[key] is not None:
                if float(datum[key]) > user_maxhfr:
                    count_reject_hfr += 1
                    count_reject += 1
                    datum_reject.append(datum)
                    continue

            key = "rmsac"
            if key in datum and datum[key] is not None:
                if float(datum[key]) > user_maxrms:
                    count_reject_rms += 1
                    count_reject += 1
                    datum_reject.append(datum)
                    continue

        if count_reject > 0:
            print("==============================")
            dir_short = directory.replace(user_srcdir, "")
            print(f"Reject for '{dir_short}'.")
            question = f"OK hfr={count_reject_hfr}, rms={count_reject_rms} to reject ({count_reject}/{count_total}, {int(1000*count_reject/count_total)/10} %)? (y/N)"

            if (100*count_reject/count_total) < user_autoyespercent:
                # don't prompt, just auto accept
                print(f"{question} y (automatic)")
                answer = "y"
            elif user_dryrun:
                # don't prompt in dry run, just force "yes"
                print(f"{question} y (dryrun)")
                answer = "y"
            else:
                answer = input(question)

            if answer == "y":
                overall_count_reject += count_reject
                for datum in datum_reject:
                    reject_image(directory, datum["filename"])
            # else don't reject
            print("==============================")

    if overall_count_total > 0:
        print(f"Total Rejected: {overall_count_reject} of {overall_count_total} ({int(1000*overall_count_reject/overall_count_total)/10} %)")
print(f"Done with {user_srcdir}")