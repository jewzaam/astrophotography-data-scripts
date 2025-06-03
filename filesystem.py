"""
This module provides classes and methods for preparing and organizing astrophotography files.
It includes functionality for handling input and output directories and file patterns.
"""

import os
import pathlib
import re

import common

class Prepare():
    input_dir = None
    input_pattern = None
    output_dir_bias = None
    output_dir_dark = None
    output_dir_flat = None
    output_dir_light = None
    debug = False
    dryrun = False

    def __init__(self, input_dir:str, input_pattern:str,
                 output_dir_bias:str, output_dir_dark:str, output_dir_flat:str, output_dir_light:str, 
                 debug:bool, dryrun:bool):
        self.input_dir = input_dir
        self.input_pattern = input_pattern
        self.output_dir_bias = output_dir_bias
        self.output_dir_dark = output_dir_dark
        self.output_dir_flat = output_dir_flat
        self.output_dir_light = output_dir_light
        self.debug=debug
        self.dryrun=dryrun

    def _prepare(self, type:str, output_dir:str, recursive=False, printStatus=False):
        # set required properties based on the image type
        rp = []
        if type == "BIAS" or type == "DARK":
            rp = ['camera', 'type', 'date', 'exposureseconds', 'datetime', 'filename']
        elif type == "FLAT":
            rp = ['camera', 'type', 'date', 'exposureseconds', 'datetime', 'filename', 'optic', 'focal_ratio', 'filter']
        elif type == "LIGHT":
            rp = ['camera', 'type', 'date', 'exposureseconds', 'datetime', 'filename', 'optic', 'focal_ratio', 'filter', 'targetname']
        else:
            raise Exception(f"unexpected image type: {type}")

        data = common.get_filtered_metadata(
            dirs=[self.input_dir],
            patterns=[self.input_pattern],
            recursive=recursive,
            required_properties=rp,
            filters={"type": type.upper()},
            debug=self.debug,
            profileFromPath=False,
            printStatus=printStatus,
        )

        if printStatus:
            print("Moving files..", end=".", flush=True)

        # collect all "target" directories (parent of DATE) so can create "accept" sub-dirs
        target_dirs = set()
        count_files=0 # could use len but assuming this is faster

        for datum in data.values():
            filename_src=datum['filename']
            statedir=None
            if 'type' not in datum:
                print(f"WARNING: type not set in datum, skipping: {datum}")
                continue
            if datum['type'] == 'LIGHT':
                statedir = common.DIRECTORY_BLINK
            filename_dest=common.normalize_filename(
                output_directory=output_dir,
                input_filename=filename_src,
                headers=datum,
                statedir=statedir,
            )

            common.move_file(
                from_file=filename_src, 
                to_file=filename_dest,
                debug=self.debug,
                dryrun=self.dryrun,
            )

            count_files += 1
            if printStatus and count_files % 50 == 0:
                print(".", end="", flush=True)

            for t in re.findall("(.*)[\\\\\\/]DATE.*", filename_dest):
                if t not in target_dirs and not self.dryrun:
                    # create the accept directory as we go, more idempotent overall (resiliant to failures)
                    pathlib.Path(t+os.sep+common.DIRECTORY_ACCEPT).mkdir(parents=True, exist_ok=True)
                target_dirs.add(t)
        
        if printStatus:
            print("\n")
        
    def bias(self):
        self._prepare('BIAS', self.output_dir_bias, recursive=False, printStatus=False)
    
    def dark(self):
        self._prepare('DARK', self.output_dir_dark, recursive=False, printStatus=False)
    
    def flat(self):
        self._prepare('FLAT', self.output_dir_flat, recursive=False, printStatus=False)
    
    def light(self, printStatus=True):
        self._prepare('LIGHT', self.output_dir_light, recursive=True, printStatus=printStatus)
        # Place a ".keep" file in any directories to retain for management purposes.
        common.delete_empty_directories(os.path.join(self.input_dir), dryrun=self.dryrun)

class Delete():
    """
    Handles the deletion of astrophotography files based on their type and metadata.

    Attributes:
        input_dir (str): The directory containing the input files.
        input_pattern (str): The pattern to match input files.
        debug (bool): Whether to enable debug mode.
        dryrun (bool): Whether to perform a dry run without making actual changes.
    """

    input_dir = None
    input_pattern = None
    debug = False
    dryrun = False

    def __init__(self, input_dir: str, input_pattern: str, debug: bool, dryrun: bool):
        """
        Initialize the Delete class with input directory, file pattern, debug mode, and dry run mode.

        Args:
            input_dir (str): The directory containing files to be deleted.
            input_pattern (str): The pattern to match files for deletion.
            debug (bool): If True, enables debug mode for verbose output.
            dryrun (bool): If True, simulates deletion without actually removing files.
        """
        self.input_dir = input_dir
        self.input_pattern = input_pattern
        self.debug=debug
        self.dryrun=dryrun

    def _delete(self, type: str, recursive: bool = False, printStatus: bool = False):
        """
        Delete files of a specific type based on metadata filters.

        Args:
            type (str): The type of files to delete (e.g., 'BIAS', 'DARK', 'FLAT', 'LIGHT').
            recursive (bool, optional): If True, searches directories recursively. Defaults to False.
            printStatus (bool, optional): If True, prints status messages during deletion. Defaults to False.

        Raises:
            Exception: If an unexpected image type is provided.
        """
        # set required properties based on the image type
        rp = []
        if type == "BIAS" or type == "DARK":
            rp = ['camera', 'type', 'date', 'exposureseconds', 'datetime', 'filename']
        elif type == "FLAT":
            rp = ['camera', 'type', 'date', 'exposureseconds', 'datetime', 'filename', 'optic', 'focal_ratio', 'filter']
        elif type == "LIGHT":
            rp = ['camera', 'type', 'date', 'exposureseconds', 'datetime', 'filename', 'optic', 'focal_ratio', 'filter', 'targetname']
        else:
            raise Exception(f"unexpected image type: {type}")

        data = common.get_filtered_metadata(
            dirs=[self.input_dir],
            patterns=[self.input_pattern],
            recursive=recursive,
            required_properties=rp,
            filters={"type": type.upper()},
            debug=self.debug,
            profileFromPath=False,
            printStatus=printStatus,
        )

        if printStatus:
            print("Deleting files...")

        for datum in data.values():
            filename_src=datum['filename']
            if printStatus:
                print(f"    {filename_src}")
            if not self.dryrun:
                pathlib.Path(filename_src).unlink()
        
        # Place a ".keep" file in any directories to retain for management purposes.
        common.delete_empty_directories(os.path.join(self.input_dir), dryrun=self.dryrun)


    def bias(self):
        """
        Delete all files of type 'BIAS' in the input directory.
        """
        self._delete('BIAS', recursive=True, printStatus=self.debug)
    
    def dark(self):
        """
        Delete all files of type 'DARK' in the input directory.
        """
        self._delete('DARK', recursive=True, printStatus=self.debug)
    
    def flat(self):
        """
        Delete all files of type 'FLAT' in the input directory.
        """
        self._delete('FLAT', recursive=True, printStatus=self.debug)
