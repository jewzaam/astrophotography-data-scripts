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
    #darks_required_properties=('exposureseconds', 'settemp', 'camera', 'gain', 'offset', 'type', 'readoutmode')
    darks_required_properties=('exposureseconds', 'camera', 'gain', 'type')
    #flats_required_properties=('date', 'optic', 'filter', 'settemp', 'camera', 'gain', 'offset', 'type', 'readoutmode')
    flats_required_properties=('date', 'optic', 'filter', 'camera', 'gain', 'type')

    lights_dir = ""
    calibration_dir = ""
    biaslibrary_dir = ""
    darklibrary_dir = ""
    flatlibrary_dir = ""
    debug = False
    dryrun = False

    def __init__(self, lights_dir, calibration_dir, biaslibrary_dir, darklibrary_dir, flatlibrary_dir, debug, dryrun):
        self.lights_dir=common.replace_env_vars(lights_dir)
        self.calibration_dir=common.replace_env_vars(calibration_dir)
        self.biaslibrary_dir=common.replace_env_vars(biaslibrary_dir)
        self.darklibrary_dir=common.replace_env_vars(darklibrary_dir)
        self.flatlibrary_dir=common.replace_env_vars(flatlibrary_dir)
        self.debug=debug
        self.dryrun=dryrun


    def CopyFiles(self, copy_list:[]): # pragma: no cover
        """
        Copies multiple files.  List is 2 dimensional.
        [
            [
                from_file,
                to_file
            ]
        ]
        """
        # do the move for darks
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

    def GetCopyList_calibration_to_biaslibrary(self): # pragma: no cover
        # find new bias to copy from calibration to bias library
        data_bias = common.get_filtered_metadata(
            dirs=[self.calibration_dir],
            patterns=[".*\.xisf$"],
            recursive=True,
            required_properties=[],
            filters={"type": "MASTER BIAS"},
            debug=self.debug,
            profileFromPath=False,
        )

        return self._getCopyList_to_biaslibrary(
            data_bias=data_bias,
        )

    def _getCopyList_to_biaslibrary(self, data_bias:{}):
        # need to match anything for some dimension to copy master bias to the bias library
        filter_any = (lambda x: True)

        # find master bias
        biaslibrary_list=common.get_copy_list(
            data=data_bias,
            output_dir=self.biaslibrary_dir,
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

        return biaslibrary_list

    def GetCopyList_calibration_to_darkslibrary(self): # pragma: no cover
        # find new darks to copy from calibration to darks library
        data_darks = common.get_filtered_metadata(
            dirs=[self.calibration_dir],
            patterns=[".*\.xisf$"],
            recursive=True,
            required_properties=[],
            filters={"type": "MASTER DARK"},
            debug=self.debug,
            profileFromPath=False,
        )

        return self._getCopyList_to_darkslibrary(
            data_darks=data_darks,
        )

    def _getCopyList_to_darkslibrary(self, data_darks:{}):
        # need to match anything for some dimension to copy master darks to the darks library
        filter_any = (lambda x: True)

        # find master darks
        darkslibrary_list=common.get_copy_list(
            data=data_darks,
            output_dir=self.darklibrary_dir,
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

        return darkslibrary_list

    def GetCopyList_calibration_to_flatlibrary(self): # pragma: no cover
        # find new flats to copy from calibration to flats library
        data_flat = common.get_filtered_metadata(
            dirs=[self.calibration_dir],
            patterns=[".*\.xisf$"],
            recursive=True,
            required_properties=[],
            filters={"type": "MASTER FLAT"},
            debug=self.debug,
            profileFromPath=False,
        )

        return self._getCopyList_to_flatlibrary(
            data_flat=data_flat,
        )

    def _getCopyList_to_flatlibrary(self, data_flat:{}):
        # need to match anything for some dimension to copy master bias to the flat library
        filter_any = (lambda x: True)

        # find master flats
        flatlibrary_list=common.get_copy_list(
            data=data_flat,
            output_dir=self.flatlibrary_dir,
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
        for flat in flatlibrary_list:
            src=flat[0]
            dest=flat[1].replace("DATE-OBS", "DATE")
            output.append([src,dest])

        return output

    def GetCopyList_darks_to_lights(self, required_properties:[str]): # pragma: no cover
        # find darks in dark library
        data_darkslibrary = common.get_filtered_metadata(
            dirs=[self.darklibrary_dir],
            patterns=[".*\.xisf$"],
            recursive=True,
            required_properties=required_properties,
            filters={"type": "MASTER DARK"},
            debug=self.debug,
            profileFromPath=False,
        )

        # find lights
        data_lights = common.get_filtered_metadata(
            dirs=[self.lights_dir],
            patterns=[".*\.cr2$", ".*\.fits$"],
            recursive=True,
            required_properties=required_properties,
            filters={"type": "LIGHT"},
            debug=self.debug,
            profileFromPath=True,
        )

        return self._getCopyList_to_lights(
            data_calibration=data_darkslibrary,
            data_lights=data_lights,
            required_properties=required_properties,
        )


    def GetCopyList_flats_to_lights(self, required_properties=flats_required_properties): # pragma: no cover
        # find flats in calibration
        data_flats = common.get_filtered_metadata(
            dirs=[self.calibration_dir],
            patterns=[".*\.xisf$"],
            recursive=True,
            required_properties=required_properties,
            filters={"type": "MASTER FLAT"},
            debug=self.debug,
            profileFromPath=False,
        )

        # find lights
        data_lights = common.get_filtered_metadata(
            dirs=[self.lights_dir],
            patterns=[".*\.cr2$", ".*\.fits$"],
            recursive=True,
            required_properties=required_properties,
            filters={"type": "LIGHT"},
            debug=self.debug,
            profileFromPath=True,
        )

        return self._getCopyList_to_lights(
            data_calibration=data_flats,
            data_lights=data_lights,
            required_properties=required_properties,
        )

    def _getCopyList_to_lights(self, data_calibration:{}, data_lights:{}, required_properties:[]):
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
    parser.add_argument("--lights_dir", required=True, type=str, help="directory to search for images")
    parser.add_argument("--calibration_dir", type=str, help="calibration directory, output of WBPP", default=os.path.join(common.DIRECTORY_ROOT_WBPP, common.DIRECTORY_CALIBRATION))
    parser.add_argument("--biaslibrary_dir", type=str, help="bias library directory", default=common.DIRECTORY_ROOT_BIASLIBRARY,)
    parser.add_argument("--darklibrary_dir", type=str, help="darks library directory", default=common.DIRECTORY_ROOT_DARKLIBRARY,)
    parser.add_argument("--flatlibrary_dir", type=str, help="flats stash directory", default=common.DIRECTORY_ROOT_FLATLIBRARY,)

    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--dryrun", action='store_true')

    # treat args parsed as a dictionary
    args = vars(parser.parse_args())

    cc = CopyCalibration(
        lights_dir=args["lights_dir"],
        calibration_dir=args["calibration_dir"],
        biaslibrary_dir=args["biaslibrary_dir"],
        darklibrary_dir=args["darklibrary_dir"],
        flatlibrary_dir=args["flatlibrary_dir"],
        debug=args["debug"],
        dryrun=args["dryrun"],
    )

    # bias to bias library
    cc.CopyFiles(cc.GetCopyList_calibration_to_biaslibrary())

    # darks to darks library
    cc.CopyFiles(cc.GetCopyList_calibration_to_darkslibrary())

    # flats to flats stash
    cc.CopyFiles(cc.GetCopyList_calibration_to_flatlibrary())

    # darks to lights
    cc.CopyFiles(cc.GetCopyList_darks_to_lights(required_properties=cc.darks_required_properties))

    # flats to lights
    cc.CopyFiles(cc.GetCopyList_flats_to_lights(required_properties=cc.flats_required_properties))