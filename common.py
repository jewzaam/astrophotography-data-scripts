from codeop import CommandCompiler
import os
import shutil
import re
import json
import xisf

import zipfile
from astropy.io import fits
from io import BytesIO

from astropy.io import fits
from pathlib import Path
from datetime import datetime, timedelta

CONSTANT_NORMALIZATION_DATA = {
    "camera": {
        "DWARFIII": {
            "focal_ratio": "4.3",
            "type": "LIGHT",
        }
    }
}

FILTER_NORMALIZATION_DATA = {
    "DATE-OBS": {
        "date": (lambda x: normalize_date(x)),
        "datetime": (lambda x: normalize_datetime(x)),
    },
    "FILTER": {
        "filter": (lambda x: normalize_filterName(x))
    },
    "EXPOSURE": { # preferred key for exposureseconds
        "exposureseconds": (lambda x: "{:.2f}".format(float(x)))
    },
    "EXPTIME": {
        "exposureseconds": (lambda x: "{:.2f}".format(float(x)))
    },
    "EXP": {
        "exposureseconds": (lambda x: "{:.2f}".format(float(x)))
    },
    "CCD-TEMP": {
        "temp": (lambda x: "{:.2f}".format(float(x)))
    },
    "SETTEMP": { # preferred key for settemp
        "settemp": (lambda x: "{:.2f}".format(float(x)))
    },
    "SET-TEMP": {
        "settemp": (lambda x: "{:.2f}".format(float(x)))
    },
    "IMAGETYP": {
        "type": (lambda x: str(x).upper())
    },
    "TELESCOP": {
        "optic": str
    },
    "FOCRATIO": {
        "focal_ratio": str
    },
    "INSTRUME": {
        "camera": str
    },
    "OBJECT": {
        "targetname": str
    },
    "SITELAT": { # preferred key for latitude
        "latitude": (lambda x: "{0:.1f}".format(float(x)))
    },
    "OBSGEO-B": {
        "latitude": (lambda x: "{0:.1f}".format(float(x)))
    },
    "SITELONG": { # preferred key for longitude
        "longitude": (lambda x: "{0:.1f}".format(float(x)))
    },
    "OBSGEO-L": {
        "longitude": (lambda x: "{0:.1f}".format(float(x)))
    },
    "READOUTM": {
        "readoutmode": str
    },
    # M 42_15s60_Astro_20250413-193110677_27C.fits
    "astro": {
        "filter": (lambda x: "Astro")
    },
    # M 42_15s60_Duo-Band_20250413-193110677_27C.fits
    "duo-band": {
        "filter": (lambda x: "Duo-Band")
    },
}

# https://stackoverflow.com/questions/8347048/how-to-convert-string-to-title-case-in-python
def camelCase(st):
    """
    Convert a string to camelCase, removing non-alphanumeric characters and capitalizing each word except the first.
    """
    output = ''.join(x for x in st.title() if x.isalnum())
    return output[0].lower() + output[1:]


def get_file_headers(filename: str, profileFromPath: bool, objectFromPath=True, normalize=True):
    """
    Extracts headers from a filename, optionally normalizing and extracting profile/object information from the path.
    Handles special cases for certain header keys and file types.
    """
    output = {
        "filename": filename, # before any name manipulations
    }

    # SPECIAL CASES:
    # SET-TEMP: A key with a dash.  Thanks NINA.  Handle it before parsing by removing the dash.
    if "SET-TEMP" in filename:
        filename = filename.replace("SET-TEMP", "SETTEMP")

    # Hate special cases so it is optional.
    # Pick OBJECT from the path.  Is the beginning of the dir that is _parent_ of "accept".
    if objectFromPath and DIRECTORY_ACCEPT in filename:
        # inject OBJECT_ at beginning of path before accept.
        # do this by splitting path and finding accept
        filename_split = filename.split(os.sep)
        filename_new = []
        for i in range(0, len(filename_split)-1):
            icurr = filename_split[i]
            inext = filename_split[i+1]
            if inext == DIRECTORY_ACCEPT:
                filename_new.append(f"OBJECT_{icurr}")
            else:
                filename_new.append(icurr)
        filename_new.append(filename_split[-1])
        filename=os.sep.join(filename_new)

    # Pick Profile (optic, focal ratio, camera) from path.
    if profileFromPath:
        m = re.match("(.*[\\\\\\/])([^@]*)@f([^+]*)[+]([^\\\\\\/]*)([\\\\\\/].*)", filename)
        if m and m.groups() and len(m.groups()) == 5:
            # rebuild path by injecting property prefixes
            p = m.groups()
            filename=f"{p[0]}TELESCOP_{p[1]}_FOCRATIO_{p[2]}_INSTRUME_{p[3]}{p[4]}"

    # just get the headers from the filename itself
    # don't be picky.  get EVERYTHING that could match
    for chunk in os.path.splitext(filename)[0].split(os.sep):
        #print(f"chunk={chunk}")
        m1 = re.split("[_]", chunk)
        for i in range(1, len(m1)):
            k = m1[i-1]
            v = m1[i]
            #print(f"k={k}, v={v}")
            if not str.isnumeric(k) and k not in output:
                output[k] = v
        for x in m1:
            #print(f"x={x}")
            if "-" in x:
                m2 = re.split("[-]", x)
                k = m2[0]
                v = "-".join(m2[1:])
                if not str.isnumeric(k) and k not in output and v is not None:
                    #print(f"m2: k={k}, v={v}")
                    output[k] = v

    # SPECIAL CASES:
    # .CR2: If extension is .cr2 and TYPE is not already set, default TYPE to LIGHT
    if filename.endswith(".cr2") and "TYPE" not in output:
        output["TYPE"] = "LIGHT"

    # SET-TEMP: The dash was previously removed.  Add it back.
    if "SETTEMP" in output:
        output['SET-TEMP'] = output['SETTEMP']

    # EXPOSURE: Value may end in "s", starting Dec 2023.  Thanks NINA.
    if "EXPOSURE" in output and "s" in output['EXPOSURE']:
        output['EXPOSURE'] = output['EXPOSURE'].replace("s", "")

    if normalize:
        output = normalize_headers(output)

    return output


def get_fits_headers(filename: str, profileFromPath: bool, normalize=True, file_naming_override=False):
    """
    Extracts and normalizes FITS headers from a file, optionally overriding with headers from the filename.
    """
    file_output = {}
    output = {}

    if file_naming_override:
        file_output=get_file_headers(filename, normalize=normalize, profileFromPath=profileFromPath)

    with fits.open(filename) as fits_file:
        # get all headers (key/value) as dict from primary image
        output = dict(fits_file[0].header)
    # convert all values to string
    for k in output:
        if output[k] is not None and type(output[k]) is not str:
            output[k] = str(output[k])

    # file naming is higher priority but might be empty
    output = dict(list(output.items()) + list(file_output.items()))

    # normalize if required
    if normalize:
        output = normalize_headers(output)
    return output


def get_xisf_headers(filename: str, profileFromPath: bool, normalize=True, file_naming_override=False):
    """
    Extracts and normalizes XISF headers from a file, optionally overriding with headers from the filename.
    """
    output = {}

    if file_naming_override:
        output=get_file_headers(filename, normalize=normalize, profileFromPath=profileFromPath)

    xisf_file = xisf.XISF(filename)
    metadata = xisf_file.get_images_metadata()
    # get all fits headers from metadata, converted to string
    for k in metadata[0]['FITSKeywords'].keys():
        # don't overwrite any headers that already exist (only happens if loaded from filename)
        if k in output or k == "HISTORY":
            continue
        if len(metadata[0]['FITSKeywords'][k]) > 0 and 'value' in metadata[0]['FITSKeywords'][k][0]:
            v = metadata[0]['FITSKeywords'][k][0]['value']
            if v is not None and str(v) != "":
                output[k] = str(v)
    # normalize if required
    if normalize:
        output = normalize_headers(output)
    return output


def normalize_filename(output_directory: str, input_filename: str, headers: dict, statedir: str):
    """
    Constructs a normalized filename based on output directory, input filename, headers, and state directory.
    Ensures required headers are present and builds a path with relevant metadata.
    """

    file_extension = os.path.splitext(input_filename)[1]

    # the absolute bare minimum required headers
    required_headers = ['type', 'optic', 'camera', 'date', 'exposureseconds', 'datetime', 'filter']

    # check that all required headers are available.
    for rh in required_headers:
        if rh not in headers:
            raise Exception(f"missing required header '{rh}' for file: {input_filename}")

    # collect output as an array.  will join it at the end
    output = [output_directory]
    type = headers['type']

    if type == "BIAS" or type == "DARK" or type == "FLAT":
        # technically we care about focal ratio for flats
        # BUT this is used to repair raw data which does not include focal ratio
        # so we leave focal ratio out for flats.
        output.append(f"{headers['optic']}+{headers['camera']}")
    elif type == "LIGHT":
        output.append(f"{headers['optic']}@f{headers['focal_ratio']}+{headers['camera']}")

    if type == "LIGHT":
        if statedir is not None and len(statedir) > 0:
            output.append(statedir)
        output.append(headers['targetname'])

    # for all types...
    output.append(f"DATE_{headers['date']}")

    # while we don't care about filter for bias and darks, it is used generally included out of NINA
    p = f"FILTER_{headers['filter']}_EXP_{headers['exposureseconds']}"
    if 'settemp' in headers:
        p = f"{p}_SETTEMP_{headers['settemp']}"
    if type == "LIGHT":
        if 'panel' in headers and headers['panel'] is not None and len(headers['panel']) > 0:
            p += f"_PANEL_{headers['panel']}"
    output.append(p)

    # for all types...
    p = f"{headers['datetime']}"
    for opt in ['hfr', 'stars', 'rmsac', 'temp']:
        if opt in headers and headers[opt] is not None and len(headers[opt]) > 0:
            p += f"_{opt.upper()}_{headers[opt]}"
    p += f"{file_extension}"
    output.append(p)

    # create the output filename
    output_filename = os.path.normpath(os.sep.join(output))

    return output_filename


def denormalize_header(header: str):
    """
    Converts a normalized header name back to its original FITS header form if possible.
    """
    for dheader in FILTER_NORMALIZATION_DATA.keys():
        nheader = list(FILTER_NORMALIZATION_DATA[dheader].keys())[0]
        if header == nheader:
            return dheader

    # didn't find it..
    return None


def normalize_target_name(input: str):
    """
    Splits a target name into the main target and panel if present, removing single quotes.
    Returns a list [target, panel].
    """
    target = input
    panel = ""
    m = re.match("(.*) Panel (.*)", target)
    if m is not None and m.groups() is not None and len(m.groups()) == 2:
        target = m.groups()[0]
        panel = m.groups()[1]
    else:
        panel = ""
    # strip single quote
    target = target.replace("'", "")
    return [target, panel]


def normalize_headers(input: dict):
    """
    Normalizes a dictionary of headers using FILTER_NORMALIZATION_DATA and CONSTANT_NORMALIZATION_DATA.
    Converts keys to lower case if not found in normalization data.
    Handles special cases for target name and constants.
    """

    output = {}
    for key in input.keys():
        value = input[key]
        if value is not None and key in FILTER_NORMALIZATION_DATA.keys():
            for normalized_keyword in FILTER_NORMALIZATION_DATA[key]:
                conversion_function = FILTER_NORMALIZATION_DATA[key][normalized_keyword]
                output[normalized_keyword] = conversion_function(value)
        else:
            # simply convert to lower case
            output[key.lower()] = value
    # final special case, strip panel out of target name
    if 'panel' not in output and 'targetname' in output and output['targetname']:
        x = normalize_target_name(output['targetname'])
        output['targetname'] = x[0]
        output['panel'] = x[1]
    # handle constants
    for key in CONSTANT_NORMALIZATION_DATA.keys():
        for value in CONSTANT_NORMALIZATION_DATA[key].keys():
            if key in output and output[key] == value:
                for ckey in CONSTANT_NORMALIZATION_DATA[key][value].keys():
                    cvalue=CONSTANT_NORMALIZATION_DATA[key][value][ckey]
                    if ckey not in output:
                        output[ckey] = cvalue
    return output


def replace_env_vars(input: str):
    """
    Replaces environment variable placeholders in a string with their actual values from the OS environment.
    """
    if input is None:
        return None
    output = input
    output_uc = input.upper()
    for e in os.environ.items():
        k = f"%{e[0]}%"
        v = e[1]
        while k in output_uc:
            # env vars are uppercase but ignore case when used.  use slices to do replacing.
            k_start = output_uc.find(k)
            output = output[:k_start] + v + output[k_start + len(k):]
            output_uc = output.upper()
    return output


# 0=draft, 1=active, 2=inactive, 3=closed
def project_status_from_path(path: str):
    """
    Determines the project status code based on directory names in the path.
    Returns 0=draft, 1=active, 2=inactive, 3=closed.
    """
    status = 0

    if DIRECTORY_BLINK in path or DIRECTORY_DATA in path:
        status = 1
    elif DIRECTORY_MASTER in path or DIRECTORY_PROCESS in path or DIRECTORY_BAKE in path:
        status = 2
    elif DIRECTORY_DONE in path:
        status = 3

    #print(f"{status} : {path}")

    return status


def backup_scheduler_database(): # pragma: no cover
    """
    Creates a backup of the NINA Scheduler database to Dropbox.
    """
    # create backup path
    Path(os.sep.join(BACKUP_TARGET_SCHEDULER.split(os.sep)[:-1])).mkdir(parents=True, exist_ok=True)

    # _copy_ the file
    shutil.copy2(DATABASE_TARGET_SCHEDULER, BACKUP_TARGET_SCHEDULER)

    print("Target Scheduler database file backed up to Dropbox.")


def simpleObject_to_csv(data: list, output_headers=True):
    """
    Converts a list of dictionaries to a CSV string, optionally including headers.
    Ensures deterministic key order for testing.
    """
    output = ""
    header_printed=not output_headers
    # collect all keys, order is important for deterministic output (aka testing)
    keys = []
    for datum in data:
        for key in datum.keys():
            if key not in keys:
                keys.append(key)
    # process all the data
    for datum in data:
        if not header_printed:
            # print headers from the ordered list of keys
            output += ",".join(str(x) for x in keys)
            output += "\n"
            header_printed=True

        # create array of values, filling in empty values (missing key) with empty string
        values = []
        for key in keys:
            if key in datum:
                values.append(datum[key])
            else:
                values.append("")

        # create output by simply joining all the values
        output += ",".join(str(x) for x in values)
        # always add a newline
        output += "\n"
    return output


def normalize_filterName(name: str):
    """
    Normalizes filter names to standard short forms for known filters.
    """
    output = name
    if name == "BaaderUVIRCut":
        output = "UVIR"
    elif name == "OptolongLeXtreme":
        output = "LeXtr"
    elif name == "S2":
        output = "S"
    elif name == "Ha":
        output = "H"
    elif name == "O3":
        output = "O"
    elif name == "":
        output = "RGB"
    return output


def normalize_date(date: str):
    """
    Converts a date string to the standard output date format, adjusting for timezone offset.
    """
    # TODO fix the timezone offset, it's hardcoded to account for UTC.  but it depends on where the data was acquired
    return datetime.strftime(datetime.strptime(date[:-4], INPUT_FORMAT_DATETIME) - timedelta(hours=16), OUTPUT_FORMAT_DATE)


def normalize_datetime(date: str):
    """
    Converts a date string to the standard output datetime format.
    """
    return datetime.strftime(datetime.strptime(date[:-4], INPUT_FORMAT_DATETIME), OUTPUT_FORMAT_DATETIME)


def move_file(from_file: str, to_file: str, debug=False, dryrun=False): # pragma: no cover
    """
    Moves a file from one location to another, optionally printing debug info and supporting dry run mode.
    """
    copy_file(
        from_file=from_file,
        to_file=to_file,
        debug=debug,
        dryrun=dryrun,
    )

    if debug:
        print(f"DEBUG: delete file after copy in move_file:\n    from_file={from_file}")

    if not dryrun:
        # then delete old file
        os.remove(from_file)


def copy_file(from_file: str, to_file: str, debug=False, dryrun=False): # pragma: no cover
    """
    Copies a file from one location to another, creating directories as needed.
    Supports debug and dry run modes.
    """
    to_dir = os.sep.join(to_file.split(os.sep)[:-1])

    if debug:
        print(f"DEBUG: copy_file:\n    from_file={from_file},\n    to_file={to_file}")

    if not dryrun:
        # create new path
        Path(to_dir).mkdir(parents=True, exist_ok=True)

        # copy the file
        shutil.copy2(from_file, to_file)


def get_filtered_metadata(dirs: list, filters: dict, profileFromPath: bool, patterns=[".*\.fits$"], recursive=False, required_properties=[], debug=False, printStatus=False):
    """
    Loads metadata for files in given directories, then filters the metadata based on provided filters and required properties.
    """

    if required_properties is None:
        required_properties = []

    for filter in filters.keys():
        if filter not in required_properties:
            required_properties.append(filter)

    metadata = get_metadata(
        dirs=dirs,
        patterns=patterns,
        recursive=recursive,
        required_properties=required_properties,
        debug=debug,
        printStatus=printStatus,
        profileFromPath=profileFromPath,
    )

    metadata = filter_metadata(
        data=metadata,
        filters=filters,
        debug=debug,
    )

    return metadata


def get_filenames(dirs: list, patterns=[".*\.fits$"], recursive=False, zips=False):
    """
    Returns a list of filenames in the given directories matching the provided patterns.
    Supports recursive search and ZIP archive extraction.
    """
    filenames = []
    for pattern in patterns:
        for dir in dirs:
            dir=replace_env_vars(dir)
            if not recursive:
                print(os.listdir(dir))
                for filename in (filename for filename in os.listdir(dir) if re.search(pattern, filename) or (zips and zipfile.is_zipfile(filename))):
                    # found a matching file or found a zip file
                    filename_path = os.path.join(dir, filename)
                    if zips and zipfile.is_zipfile(filename_path):
                        # Process ZIP archive
                        with zipfile.ZipFile(filename_path, 'r') as archive:
                            for zip_filename in (filename for filename in archive.filelist if re.search(pattern, filename)):
                                # add each contained filename that matches the pattern
                                filenames.append(os.path(filename_path, zip_filename))
                    else:
                        # not a zip, simply add it
                        filenames.append(filename_path)
            else:
                for root, _, f_names in os.walk(dir):
                    for filename in (filename for filename in f_names if re.search(pattern, filename)):
                        # special cases to ignore...
                        if "_stash" in root:
                            continue
                        filenames.append(os.path.join(root, filename))
    return filenames


def get_metadata(dirs: list, profileFromPath: bool, patterns=[".*\.fits$"], recursive=False, required_properties=[], debug=False, printStatus=False):
    """
    Loads metadata for files in the given directories, ensuring all required properties are present.
    Optionally prints status updates.
    """
    _required_properties = list(required_properties)
    # 'targetname' is always required, simply to have a value of None...
    if 'targetname' not in _required_properties:
        _required_properties.append('targetname')

    # key of 'data' is the full path of the file
    data = {}

    # find files and load metadata from path+name.
    count_files=0 # could use len but assuming this is faster
    if printStatus:
        print("Loading data..", end=".", flush=True)

    filenames = get_filenames(
        dirs=dirs,
        patterns=patterns,
        recursive=recursive,
    )

    for filename in filenames:
        d = get_file_headers(filename, profileFromPath=profileFromPath)
        data[d['filename']] = d
        count_files += 1
        if printStatus and count_files % 1000 == 0:
            print(".", end="", flush=True)

    if printStatus:
        # need to complete the line!
        print("")

    # make sure all required properties are at least None
    for f in data.keys():
        for p in _required_properties:
            if p not in data[f]:
                data[f][p] = None

    return enrich_metadata(
        data=data,
        required_properties=_required_properties,
        debug=debug,
        printStatus=printStatus,
        profileFromPath=profileFromPath,
    )


def enrich_metadata(data: dict, profileFromPath: bool, required_properties=[], debug=False, printStatus=False):
    """
    Enriches metadata for files missing required properties by extracting additional headers from the files themselves.
    Optionally prints status updates.
    """
    # list of filenames (key of data dict) that need enrichment
    to_enrich = []

    # check each datum fo enrichment
    for datum in data.values():
        # check if we have all required properties
        for rp in required_properties:
            if rp not in datum or datum[rp] is None or len(datum[rp]) == 0:
                # required property is missing, must enrich.
                to_enrich.append(datum['filename'])
                continue

    to_enrich.sort()

    # variables for printing status, if desired
    last_profilename = None
    last_targetname = None
    last_target_count = 0

    # enrich things that need it
    for filename in to_enrich:
        datum = data[filename]
        # get headers from metadata.  normalize and use file naming override.
        enriched = None
        if filename.endswith(".fits"):
            enriched = get_fits_headers(filename, normalize=True, file_naming_override=True, profileFromPath=profileFromPath)
        elif filename.endswith(".xisf"):
            enriched = get_xisf_headers(filename, normalize=True, file_naming_override=True, profileFromPath=profileFromPath)
        else:
            # some other file type, probably cr2
            # can only default the location to "home"
            # TODO add some actual default to locations in ap database?
            datum['latitude'] = "35.6"
            datum['longitude'] = "-78.8"
            # we can do no more, treat datum as if it were enriched.
            enriched = datum

        if printStatus and 'targetname' in enriched and last_targetname != enriched['targetname']:
            last_targetname=enriched['targetname']
            profilename=f"{enriched['optic']}@f{enriched['focal_ratio']}+{enriched['camera']}"
            if last_profilename is not None:
                # we have already printed something, so we need a newline for the next target.
                print("")
            if profilename != last_profilename:
                last_profilename = profilename
                print(f"{last_profilename}...")
            print(f"\t{last_targetname}..", end=".", flush=True)

        last_target_count += 1
        if printStatus and 'targetname' in enriched and enriched['targetname'] is not None and last_target_count % 50 == 0:
            # print a period every 50 files just to have a visual.
            print("", end=".", flush=True)

        # store the now-enriched data
        data[filename] = enriched

    if printStatus:
        print("")

    # make sure 'filename' is always set.  enrich will strip it.
    for filename in data.keys():
        data[filename]['filename'] = filename

    return data


def filter_metadata(data: dict, filters: dict, debug=False):
    """
    Filters a metadata dictionary based on provided filter key/value pairs or functions.
    Returns a new dictionary with only matching entries.
    """
    # validate input filter data
    if filters is None or filters.keys() is None or len(filters.keys()) == 0:
        raise(Exception("Invalid filter data"))

    # validate filter values
    for filter_key in filters.keys():
        filter_value = filters[filter_key]

        if filter_value is None:
            # unexpected.  bad input, but it should be rejected.
            print(f"ERROR filter: key '{filter_key}' has no value '{filter_value}'")
            raise(Exception(f"filter key '{filter_key}' has no value '{filter_value}'"))

    # filters are good.  process the data and build a new output data set
    output = {}

    # for each datum, check filter.  if it matches all filters, add the datum to 'new_data'
    for filename in data.keys():
        datum = data[filename]
        # process each filter for this datum
        # is_match will be False if at least one filter does not match
        is_match = True

        # loop through each filter.  if any filter does not match set is_match False and break the loop
        for filter_key in filters.keys():
            filter_value = filters[filter_key]

            # if we don't have the filter in the datum it's ok, just treat it as "OK"
            if filter_key not in datum:
                continue

            # filter exists in datum, check value
            if callable(filter_value):
                try:
                    # assumes the function returns bool...
                    if not filter_value(datum[filter_key]):
                        # not a match
                        is_match = False
                        break
                except:
                    # no idea, bad function? bail!
                    raise Exception(f"WARNING failed to call function '{filter_value}' with argument '{datum[filter_key]}'")

            elif type(filter_value) is int:
                try:
                    # convert to float first because "90.00" won't convert to int directly
                    if int(float(datum[filter_key])) != filter_value:
                        # not a match
                        is_match = False
                        break
                except:
                    # cannot convert to int probably, so it's not a match
                    is_match = False
                    break

            elif type(filter_value) is float:
                try:
                    # set to match if
                    if float(datum[filter_key]) != filter_value:
                        # not a match
                        is_match = False
                        break
                except:
                    # cannot convert to float probably, so it's not a match
                    is_match = False
                    break

            else:
                # default, treat as string
                if str(datum[filter_key]) != filter_value:
                    # not a match
                    is_match = False
                    break

        # all filters have been checked, did we find a match?
        if is_match:
            # found a match for all filters.  add datum to 'output'
            output[filename] = datum

    return output


def get_copy_list(data: dict, output_dir: str, filters: dict, debug=False):
    """
    Returns a list of (source, destination) tuples for copying files to an output location based on filters.
    Constructs destination filenames using metadata and filter values.
    """

    if data is None or len(data) == 0:
        # no calibration data found!  nothing to do...
        return []

    if filters is None or (len(filters.keys())) == 0:
        raise Exception("no filters provided")

    data_filtered = filter_metadata(
        data=data,
        filters=filters,
        debug=debug,
    )

    # build destination dir/name for calibration data based on filters
    output = []
    for from_file in data_filtered.keys():
        datum = data_filtered[from_file]

        output_filename_only = f"{camelCase(datum['type'])}"
        for key in filters.keys():
            # skip 'type', used for filtering
            if key == 'type':
                continue
            # skip 'camera', used in directory structure
            if key == 'camera':
                continue
            # skip 'optic', used in directory structure
            if key == 'optic':
                continue

            # have to map some human readable names to the fits header names.. sigh.
            p = denormalize_header(key)
            if p is None:
                p = str(key).upper()

            # add the property to the filename (if we have it!)
            if key in datum:
                output_filename_only += f"_{p}_{datum[key]}"

        output_filename_only += os.path.splitext(from_file)[1] # file extension

        # output_filename_only = {type}[_filterKey_{filterValue}]*.{extension}
        # to_file = {output_dir}/{camera}[/{optic}]/{output_filename_only}
        p = [output_dir]
        p.append(datum['camera'])
        if 'optic' in filters and 'optic' in datum and datum['optic'] is not None and len(datum['optic']) > 0:
            p.append(datum['optic'])
        if 'focal_ratio' in filters and 'focal_ratio' in datum and datum['focal_ratio'] is not None and len(datum['focal_ratio']) > 0:
            p.append(f"@f{datum['focal_ratio']}")
        p.append(output_filename_only)
        to_file = os.path.normpath(os.sep.join(p))

        output.append([from_file, to_file])

    return output


# TODO delete
def copy_calibration_to_library(type="", calibration_dir="", library_dir="", group_by=[], delete_after_copy=False, debug=False, dryrun=False):
    """
    Copies calibration files from a calibration directory to a library directory, grouping by specified metadata fields.
    Optionally deletes source files after copying.
    """
    # find all masters in calibration directory
    data_calibration = get_filtered_metadata(
        dirs=[calibration_dir],
        patterns=[".*\.xisf$"],
        recursive=True,
        required_properties=group_by,
        filters={"type": type},
        profileFromPath=False,
    )

    if debug:
        print("MASTERS")
        print(data_calibration)

    if data_calibration is not None and len(data_calibration) > 0:
        # move to darks library
        for filename in data_calibration.keys():
            datum = data_calibration[filename]

            calibration_filename_only = f"{camelCase(datum['type'])}"
            for key in group_by:
                # have to map some human readable names to the fits header names.. sigh.
                p = str(key).upper()
                if key == 'camera':
                    p = 'INSTRUME'
                elif key == 'exposureseconds':
                    p = 'EXPOSURE'

                # skip 'type', used for filtering
                if key == 'type':
                    continue
                # skip 'camera', used in directory structure
                if key == 'camera':
                    continue
                # skip 'optic', used in directory structure
                if key == 'optic':
                    continue

                calibration_filename_only += f"_{p}_{datum[key]}"

            calibration_filename_only += ".xisf"

            # add optic in path if we have it (i.e. for flats)
            optic_path = ""
            if 'optic' in group_by and 'optic' in datum and datum['optic'] is not None and len(datum['optic']) > 0:
                optic_path = f"{datum['optic']}{os.sep}"

            from_file = filename
            to_file = os.path.join(library_dir, f"{datum['camera']}{os.sep}{optic_path}{calibration_filename_only}")
            print(f"Copying...\nfrom: {from_file}\nto:   {to_file}")

            copy_file(
                from_file=from_file,
                to_file=to_file,
                debug=debug,
                dryrun=dryrun,
            )

            if not dryrun and delete_after_copy:
                Path(from_file).unlink()


def copy_calibration_to_lights(type="", calibration_dir="", lights_dir="", group_by=[], debug=False, dryrun=False):
    """
    Copies calibration files from a calibration directory to the appropriate light frame directories based on metadata.
    Handles missing calibrations and prints warnings as needed.
    """
    # copy calibration to lights
    # find all lights
    data_lights=get_filtered_metadata(
        dirs=[lights_dir],
        patterns=[".*\.cr2$", ".*\.fits$"],
        recursive=True,
        required_properties=group_by,
        filters={"type": "LIGHT"},
        debug=debug,
        profileFromPath=True,
    )

    # for each light, collect master calibration filter and target directory, key is unique hash of value
    calibration_filters = {}
    missing = set()

    for light_filename in data_lights.keys():
        datum = data_lights[light_filename]
        m = re.match("(.*[\\\\\\/]DATE_[^\\\\\\/]*[\\\\\\/]).*", light_filename)
        directory = None
        if m is not None and m.groups() is not None and len(m.groups()) == 1:
            directory = m.groups()[0]
        else:
            print(f"WARNING unable to find 'DATE' directory for light: {light_filename}")
            continue

        calibration_filter = {}

        # create filter, don't include type
        for key in group_by:
            if key != 'type':
                calibration_filter[key] = datum[key]

        # add directory, which is handled special later
        calibration_filter['directory'] = directory

        # add type from function input
        calibration_filter['type'] = type

        if debug:
            print(f"calibration_filter={calibration_filter}")

        key = hash(json.dumps(calibration_filter, sort_keys=True))

        calibration_filters[key] = calibration_filter

    for key in calibration_filters.keys():
        calibration_filter = calibration_filters[key]
        directory = calibration_filter['directory']

        # remove directory from the filter dict before using it
        del calibration_filter['directory']

        # is the master already in the target directory?
        data_calibration_in_lights = get_filtered_metadata(
            dirs=[directory],
            patterns=[".*\.xisf$"],
            recursive=True,
            filters=calibration_filter,
            debug=debug,
            profileFromPath=True,
        )

        if data_calibration_in_lights is None or len(data_calibration_in_lights) == 0:
            # no master found, copy it
            data_calibration_in_library = get_filtered_metadata(
                dirs=[calibration_dir],
                patterns=[".*\.xisf$"],
                recursive=True,
                filters=calibration_filter,
                debug=debug,
                profileFromPath=False,
            )

            if debug:
                print(f"data_calibration_in_library={data_calibration_in_library}")

            if data_calibration_in_library is not None and len(data_calibration_in_library) > 0:
                # copy from calibration dir to destination
                for data_calibration_filename in data_calibration_in_library.keys():
                    # in order to ensure all attributes are available in the FILENAME for the copied master dark, include them directly

                    calibration_filename_only = f"{camelCase(type)}"
                    for key in group_by:
                        # have to map some human readable names to the fits header names.. sigh.
                        p = str(key).upper()
                        if key == 'camera':
                            p = 'INSTRUME'
                        elif key == 'exposureseconds':
                            p = 'EXPOSURE'

                        # skip 'type'
                        if key == 'type':
                            continue

                        calibration_filename_only += f"_{p}_{data_calibration_in_library[data_calibration_filename][key]}"

                    calibration_filename_only += ".xisf"

                    from_file = data_calibration_filename
                    to_file = os.path.join(directory, calibration_filename_only)
                    # in case want to test without renaming the file, use the following instead:
                    #to_file = os.path.join(directory, data_calibration_filename.split(os.sep)[-1])

                    print(f"Copying...\nfrom: {from_file}\nto:   {to_file}")

                    copy_file(
                        from_file=from_file,
                        to_file=to_file,
                        debug=debug,
                        dryrun=dryrun
                    )
            else:
                # didn't find dark to copy
                missing.add(tuple(calibration_filter.items()))

    # did we miss anything?
    if len(missing) > 0:
        print(f"MISSING {type.lower()}s:")
        for d in missing:
            print(f"    {dict(d)}")


def delete_empty_directories(root_dir: str, dryrun=False): # pragma: no cover
    """
    Recursively deletes empty directories under the given root directory.
    Supports dry run mode.
    """
    root_dir=replace_env_vars(root_dir)
    print(f"delete_empty_directories({root_dir})")
    done = False
    while not done and not dryrun:
        done = True
        for root, d_names, f_names in os.walk(root_dir):
            for d in d_names:
                dir = f"{root}{os.sep}{d}"
                try:
                    os.rmdir(dir)
                    done = False # parent may need deleted
                except:
                    # ignore if cannot delete dir, means it is not empty.  this is OK.
                    pass


DIRECTORY_NINA_PROFILES=replace_env_vars(r"%Dropbox%\Family Room\Astrophotography\Configuration\NINA\Profiles")
DATABASE_ASTROPHOTGRAPHY=replace_env_vars(r"%Dropbox%\Family Room\Astrophotography\Data\astrophotography.sqlite")
DATABASE_TARGET_SCHEDULER=replace_env_vars(r"%LocalAppData%\NINA\SchedulerPlugin\schedulerdb.sqlite")
BACKUP_TARGET_SCHEDULER=replace_env_vars(r"%Dropbox%\Family Room\Astrophotography\Configuration\NINA\SchedulerPlugin\schedulerdb.sqlite")

DIRECTORY_CSV=replace_env_vars(r"%Dropbox%\Family Room\Astrophotography\Data")

DIRECTORY_BLINK=r"10_Blink"
DIRECTORY_DATA=r"20_Data"
DIRECTORY_MASTER=r"30_Master"
DIRECTORY_PROCESS=r"40_Process"
DIRECTORY_BAKE=r"50_Bake"
DIRECTORY_DONE=r"60_Done"
DIRECTORY_ACCEPT=r"accept"
DIRECTORY_CALIBRATION=r"_calibration"

INPUT_PATTERN_ALL=".*"

DIRECTORY_ROOT_RAW=replace_env_vars(r"%Dropbox%\Family Room\Astrophotography\RAW")
DIRECTORY_ROOT_RAW_FLAT=replace_env_vars(r"%Dropbox%\Family Room\Astrophotography\RAW\FLAT")
DIRECTORY_ROOT_DATA=replace_env_vars(r"F:\Astrophotography\Data")
DIRECTORY_ROOT_BIASLIBRARY=replace_env_vars(r"%Dropbox%\Family Room\Astrophotography\Data\_Bias Library")
DIRECTORY_ROOT_DARKLIBRARY=replace_env_vars(r"%Dropbox%\Family Room\Astrophotography\Data\_Dark Library")
DIRECTORY_ROOT_FLATLIBRARY=replace_env_vars(r"%Dropbox%\Family Room\Astrophotography\Data\_Flats Stash")
DIRECTORY_ROOT_LIVESTACK=replace_env_vars(r"%Dropbox%\Family Room\Astrophotography\Data\_Live Stack Data")

DIRECTORY_ROOT_WBPP=r"E:\temp\PI_WBPP"

INPUT_FORMAT_DATETIME=r"%Y-%m-%dT%H:%M:%S"
OUTPUT_FORMAT_DATE=r"%Y-%m-%d"
OUTPUT_FORMAT_DATETIME=r"%Y-%m-%d_%H-%M-%S"

# when assessing if something is "done", this is the % of desired data required to have been accepted
MASTER_READY_PERCENT=0.95