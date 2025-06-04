import argparse
import json
import os
import sys

import common


# find all data in blink dir that needs calibration
# copy calibration data to the data sets
# report any data sets missing calibration data
# [optional] move data with calibration data to next directory

class CopyCalibration:
    #darks_required_properties=('exposureseconds', 'settemp', 'camera', 'gain', 'offset', 'type', 'readoutmode') # SQA55
    #darks_required_properties=('exposureseconds', 'settemp', 'camera', 'gain', 'offset', 'type') # C8E
    #darks_required_properties=('exposureseconds', 'camera', 'gain', 'type') # Dwarf 3
    #flats_required_properties=('date', 'optic', 'filter', 'settemp', 'camera', 'gain', 'offset', 'type', 'readoutmode') # SQA55
    #flats_required_properties=('date', 'optic', 'filter', 'settemp', 'camera', 'gain', 'offset', 'type') # C8E
    #flats_required_properties=('date', 'optic', 'filter', 'camera', 'gain', 'type') # Dwarf 3

    darks_required_properties=[]
    flats_required_properties=[]

    dest_light_dir=""
    src_bias_dir=""
    src_dark_dir=""
    src_flat_dir=""
    dest_bias_dir=""
    dest_dark_dir=""
    dest_flat_dir=""

    debug = False
    dryrun = False

    def __init__(self, 
                 dest_light_dir:str,
                 src_bias_dir:str,
                 src_dark_dir:str,
                 src_flat_dir:str,
                 dest_bias_dir:str,
                 dest_dark_dir:str,
                 dest_flat_dir:str,
                 debug:bool, 
                 dryrun:bool,
                 darks_required_properties:list,
                 flats_required_properties:list
                ):
        """
        Initializes the CopyCalibration class with source and destination directories for calibration files.

        Parameters:
        - dest_light_dir (str): Directory for light frames.
        - src_bias_dir (str): Source directory for bias frames.
        - src_dark_dir (str): Source directory for dark frames.
        - src_flat_dir (str): Source directory for flat frames.
        - dest_bias_dir (str): Destination directory for bias frames.
        - dest_dark_dir (str): Destination directory for dark frames.
        - dest_flat_dir (str): Destination directory for flat frames.
        - debug (bool): Enable debug mode.
        - dryrun (bool): Enable dry-run mode.
        - darks_required_properties (list): Required properties for darks.
        - flats_required_properties (list): Required properties for flats.
        """
        self.dest_light_dir=common.replace_env_vars(dest_light_dir)
        self.src_bias_dir=common.replace_env_vars(src_bias_dir)
        self.src_dark_dir=common.replace_env_vars(src_dark_dir)
        self.src_flat_dir=common.replace_env_vars(src_flat_dir)
        self.dest_bias_dir=common.replace_env_vars(dest_bias_dir)
        self.dest_dark_dir=common.replace_env_vars(dest_dark_dir)
        self.dest_flat_dir=common.replace_env_vars(dest_flat_dir)
        self.debug=debug
        self.dryrun=dryrun
        self.darks_required_properties = darks_required_properties
        self.flats_required_properties = flats_required_properties


    def CopyFiles(self, copy_list:list): # pragma: no cover
        """
        Copies multiple files from source to destination based on the provided list.

        Parameters:
        - copy_list (list): A 2D list where each sublist contains [from_file, to_file].
        """

        if copy_list is None or len(copy_list) == 0:
            # nothing to do, skip quietly
            return

        # do the move
        for from_file, to_file in copy_list:
            # special case if from_file is None, means missing calibration data
            if from_file is None:
                print(f"MISSING calibration for: {to_file}")
            else:
                # don't copy file if it already exists.
                if os.path.isfile(to_file):
                    if self.debug:
                        print(f"DEBUG skipping file that exists: {to_file}")
                    continue
                common.copy_file(
                    from_file=from_file,
                    to_file=to_file,
                    debug=self.debug,
                    dryrun=self.dryrun,
                )

    def GetCopyList_to_dest_bias(self): # pragma: no cover
        """
        Generates a list of bias frames to copy from the source directory to the destination directory.

        Returns:
        - list: A list of files to copy, or None if source or destination directories are missing.
        """

        if self.src_bias_dir is None or len(self.src_bias_dir) == 0 or self.dest_bias_dir is None or len(self.dest_bias_dir) == 0:
            # missing src or dest dirs, skip
            if self.debug:
                print(f"DEBUG skipping copy to bias. src_bias_dir={self.src_bias_dir}, dest_bias_dir={self.dest_bias_dir}")
            return
        
        filter_any = (lambda x: True)

        # find new bias to copy from src to dest
        src_data = common.get_filtered_metadata(
            dirs=[self.src_bias_dir],
            patterns=[".*\.xisf$"],
            recursive=True,
            required_properties=[],
            filters={"type": "MASTER BIAS"},
            debug=self.debug,
            profileFromPath=False,
        )

        copy_list=common.get_copy_list(
            data=src_data,
            output_dir=self.dest_bias_dir,
            filters={
                "type":"MASTER BIAS",
                "exposureseconds": filter_any,
                "settemp": filter_any,
                "camera": filter_any,
                "gain": filter_any,
                "offset": filter_any,
                "readoutmode": filter_any,
            },
            debug=self.debug,
        )

        return copy_list

    def GetCopyList_to_dest_dark(self): # pragma: no cover
        """
        Generates a list of dark frames to copy from the source directory to the destination directory.

        Returns:
        - list: A list of files to copy, or None if source or destination directories are missing.
        """

        if self.src_dark_dir is None or len(self.src_dark_dir) == 0 or self.dest_dark_dir is None or len(self.dest_dark_dir) == 0:
            # missing src or dest dirs, skip
            if self.debug:
                print(f"DEBUG skipping copy to dark. src_dark_dir={self.src_dark_dir}, dest_dark_dir={self.dest_dark_dir}")
            return
        
        filter_any = (lambda x: True)

        # find new dark to copy from src to dest
        src_data = common.get_filtered_metadata(
            dirs=[self.src_dark_dir],
            patterns=[".*\.xisf$"],
            recursive=True,
            required_properties=[],
            filters={"type": "MASTER DARK"},
            debug=self.debug,
            profileFromPath=False,
        )

        copy_list=common.get_copy_list(
            data=src_data,
            output_dir=self.dest_dark_dir,
            filters={
                "type":"MASTER DARK",
                "exposureseconds": filter_any,
                "settemp": filter_any,
                "camera": filter_any,
                "gain": filter_any,
                "offset": filter_any,
                "readoutmode": filter_any,
            },
            debug=self.debug,
        )

        return copy_list

    def GetCopyList_to_dest_flat(self): # pragma: no cover
        """
        Generates a list of flat frames to copy from the source directory to the destination directory.

        Returns:
        - list: A list of files to copy, or None if source or destination directories are missing.
        """

        if self.src_flat_dir is None or len(self.src_flat_dir) == 0 or self.dest_flat_dir is None or len(self.dest_flat_dir) == 0:
            # missing src or dest dirs, skip
            if self.debug:
                print(f"DEBUG skipping copy to flat. src_flat_dir={self.src_flat_dir}, dest_flat_dir={self.dest_flat_dir}")
            return
        
        filter_any = (lambda x: True)

        # find new flat to copy from src to dest
        src_data = common.get_filtered_metadata(
            dirs=[self.src_flat_dir],
            patterns=[".*\.xisf$"],
            recursive=True,
            required_properties=[],
            filters={"type": "MASTER FLAT"},
            debug=self.debug,
            profileFromPath=False,
        )

        copy_list=common.get_copy_list(
            data=src_data,
            output_dir=self.dest_flat_dir,
            filters={
                "type":"MASTER FLAT",
                "camera": filter_any,
                "optic": filter_any,
                "date": filter_any,
                "filter": filter_any,
                "settemp": filter_any,
                "gain": filter_any,
                "offset": filter_any,
                "focallen": filter_any,
                "readoutmode": filter_any,
            },
            debug=self.debug,
        )

        # Replace "DATE-OBS" with "DATE".  This is set because of reverse filter name mappings.
        output = []
        for flat in copy_list:
            src=flat[0]
            dest=flat[1].replace("DATE-OBS", "DATE")
            output.append([src,dest])

        return output

    def GetCopyList_darks_to_lights(self, required_properties:list): # pragma: no cover
        """
        Generates a list of dark frames to copy to light frame directories based on required properties.

        Parameters:
        - required_properties (list): A list of required metadata properties for matching dark frames to light frames.

        Returns:
        - list: A list of files to copy, or None if source or destination directories are missing.
        """

        if self.src_dark_dir is None or len(self.src_dark_dir) == 0 or self.dest_light_dir is None or len(self.dest_light_dir) == 0:
            # missing src or dest dirs, skip
            if self.debug:
                print(f"DEBUG skipping copy dark to lights. src_dark_dir={self.src_dark_dir}, dest_light_dir={self.dest_light_dir}")
            return

        # find dark
        src_dark = common.get_filtered_metadata(
            dirs=[self.src_dark_dir],
            patterns=[".*\.xisf$"],
            recursive=True,
            required_properties=required_properties,
            filters={"type": "MASTER DARK"},
            debug=self.debug,
            profileFromPath=False,
        )

        # find lights
        dest_light = common.get_filtered_metadata(
            dirs=[self.dest_light_dir],
            patterns=[".*\.cr2$", ".*\.fits$"],
            recursive=True,
            required_properties=required_properties,
            filters={"type": "LIGHT"},
            debug=self.debug,
            profileFromPath=True,
        )

        return self._getCopyList_to_lights(
            data_calibration=src_dark,
            data_lights=dest_light,
            required_properties=required_properties,
        )


    def GetCopyList_flats_to_lights(self, required_properties=flats_required_properties): # pragma: no cover
        """
        Generates a list of flat frames to copy to light frame directories based on required properties.

        Parameters:
        - required_properties (list): A list of required metadata properties for matching flat frames to light frames.

        Returns:
        - list: A list of files to copy, or None if source or destination directories are missing.
        """

        if self.src_flat_dir is None or len(self.src_flat_dir) == 0 or self.dest_light_dir is None or len(self.dest_light_dir) == 0:
            # missing src or dest dirs, skip
            if self.debug:
                print(f"DEBUG skipping copy flat to lights. src_flat_dir={self.src_flat_dir}, dest_light_dir={self.dest_light_dir}")
            return


        # find flats in calibration
        src_flat = common.get_filtered_metadata(
            dirs=[self.src_flat_dir],
            patterns=[".*\.fits$", ".*\.xisf$"],
            recursive=True,
            required_properties=required_properties,
            filters={"type": "MASTER FLAT"},
            debug=self.debug,
            profileFromPath=False,
        )

        # find lights
        dest_light = common.get_filtered_metadata(
            dirs=[self.dest_light_dir],
            patterns=[".*\.cr2$", ".*\.fits$"],
            recursive=True,
            required_properties=required_properties,
            filters={"type": "LIGHT"},
            debug=self.debug,
            profileFromPath=True,
        )

        return self._getCopyList_to_lights(
            data_calibration=src_flat,
            data_lights=dest_light,
            required_properties=required_properties,
        )

    def _getCopyList_to_lights(self, data_calibration:dict, data_lights:dict, required_properties:list):
        """
        Internal method to generate a list of calibration files to copy to light frame directories.

        Parameters:
        - data_calibration (dict): Metadata of calibration files.
        - data_lights (dict): Metadata of light frames.
        - required_properties (list): A list of required metadata properties for matching calibration files to light frames.

        Returns:
        - list: A list of files to copy.
        """

        # walk the lights and build set of filters required to search for calibration
        # TODO maybe move this into something like a "group_by_filter" function
        filters={}

        # strip out filters that we know are unaccepable (not in reference darks and flats rp arrays)
        # and strip out 'type'
        cleansed_required_properties = []
        for rp in required_properties:
            if rp not in cleansed_required_properties and rp != "type":
                if rp in self.darks_required_properties or rp in self.flats_required_properties:
                    cleansed_required_properties.append(rp)

        for filename in data_lights.keys():
            datum = data_lights[filename]
            light_dir = os.sep.join(filename.split(os.sep)[:-1]) # get just directory

            if light_dir in filters:
                # optimizaiton, don't build filter if we already have it
                continue

            filters[light_dir] = {}
            for rp in cleansed_required_properties:
                filters[light_dir][rp] = datum[rp]

        #print(json.dumps(filters, indent=4))
        #print(json.dumps(data_calibration, indent=4))
        #sys.exit(0)

        # we have calibration, lights, and a pile of filters
        # find the calibration files to copies
        copy_list=[]
        for light_dir in filters.keys():
            f = filters[light_dir]

            filtered_data_calibration = common.filter_metadata(
                data=data_calibration,
                filters=f,
                debug=self.debug,
            )

            if filtered_data_calibration is not None:
                # sanity check.  expect 0 or 1 calibration matches
                if len(filtered_data_calibration) > 1:
                    raise Exception(f"expected zero or one calibration to match, found {len(filtered_data_calibration)}")

                if len(filtered_data_calibration) == 0:
                    # missing! represent as None in the source filename
                    copy_list.append(
                        [
                            None,
                            light_dir,
                        ]
                    )
                else:
                    for fdc_filename in filtered_data_calibration.keys():
                        # fdc_filename is the calibration filename

                        # strip all bits from `light_dir` after the DATE folder
                        output_dir = []
                        for d in light_dir.split(os.sep):
                            # all up to and including whatever has "DATE" in it
                            # TODO consider a better way, it really should be the _last_ folder with "DATE"
                            output_dir.append(d)
                            if "DATE" in d:
                                break
                        copy_list.append(
                            [
                                fdc_filename,
                                os.path.join(os.sep.join(output_dir), fdc_filename.split(os.sep)[-1]),
                            ]
                        )

        return copy_list


if __name__ == '__main__': # pragma: no cover
    parser = argparse.ArgumentParser(description="copy calibration files")

    parser.add_argument("--src_bias_dir", type=str, help="source for bias masters, optional")
    parser.add_argument("--src_dark_dir", type=str, help="source for dark masters, optional")
    parser.add_argument("--src_flat_dir", type=str, help="source for flat masters, optional")

    parser.add_argument("--dest_bias_dir", type=str, help="destination for bias masters, optional")
    parser.add_argument("--dest_dark_dir", type=str, help="destination for dark masters, optional")
    parser.add_argument("--dest_flat_dir", type=str, help="destination for flat masters, optional")

    parser.add_argument("--dest_light_dir", type=str, help="directory of lights, optional")

    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--dryrun", action='store_true')

    parser.add_argument("--darks_required_properties", type=str, help="Comma-delimited string of required properties for darks, optional")
    parser.add_argument("--flats_required_properties", type=str, help="Comma-delimited string of required properties for flats, optional")

    # Parse the arguments
    args = vars(parser.parse_args())

    # Convert comma-delimited strings to lists if provided, otherwise set to None
    darks_required_properties = args["darks_required_properties"].split(",") if args["darks_required_properties"] else None
    flats_required_properties = args["flats_required_properties"].split(",") if args["flats_required_properties"] else None

    cc = CopyCalibration(
        src_bias_dir=args["src_bias_dir"],
        src_dark_dir=args["src_dark_dir"],
        src_flat_dir=args["src_flat_dir"],
        dest_bias_dir=args["dest_bias_dir"],
        dest_dark_dir=args["dest_dark_dir"],
        dest_flat_dir=args["dest_flat_dir"],
        dest_light_dir=args["dest_light_dir"],
        debug=args["debug"],
        dryrun=args["dryrun"],
        darks_required_properties=darks_required_properties,
        flats_required_properties=flats_required_properties
    )

    # src bias to dest bias
    cc.CopyFiles(cc.GetCopyList_to_dest_bias())

    # src dark to dest dark
    cc.CopyFiles(cc.GetCopyList_to_dest_dark())

    # src flat to dest flat
    cc.CopyFiles(cc.GetCopyList_to_dest_flat())

    # src dark to lights
    cc.CopyFiles(cc.GetCopyList_darks_to_lights(required_properties=cc.darks_required_properties))

    # src flat to lights
    cc.CopyFiles(cc.GetCopyList_flats_to_lights(required_properties=cc.flats_required_properties))