"""
This script provides functionality to summarize astrophotography data.
It includes classes and methods to process directories and databases for generating summaries.
"""

import argparse
import json
import os
import traceback
import common
import database

FILENAME_CSV="astrobin_csv.txt"
FILENAME_TOTALS="totals.txt"

class SummaryData():
    from_dir = None
    db_ap:database.Astrophotgraphy = None
    debug:bool = False
    dryrun:bool = False

    def __init__(self, db_ap: database.Astrophotgraphy, from_dir: str, debug: bool, dryrun: bool):
        """
        Initialize the SummaryData class.

        Args:
            db_ap (database.Astrophotgraphy): The astrophotography database instance.
            from_dir (str): The directory to process for summaries.
            debug (bool): If True, enables debug mode for verbose output.
            dryrun (bool): If True, simulates the process without making changes.
        """
        self.db_ap = db_ap
        self.from_dir = from_dir
        self.debug = debug
        self.dryrun = dryrun

    def prepare_data(self) -> dict[str, str]:
        """
        Prepare summary data by querying the database and organizing it by target directory.

        Returns:
            dict: A dictionary where the key is the target directory and the value is a list of key-value pairs.
        """

        output = {}

        try:
            self.db_ap.open()

            # for every target (unique per optic/camera!), build csv data and write to target's root directory (parent of 'accept')
            d = self.from_dir.replace("'", "''")
            stmt=f"""
                select o.id, c.id, t.id, t.name, a.panel_name, a.raw_directory, a.date, f.name, f.astrobin_id, a.accepted_count, a.shutter_time_seconds, o.focal_ratio, l.bortle, p.id
                from target t, accepted_data a, filter f, optic o, location l, camera c, profile p
                where t.id=a.target_id
                and f.id=a.filter_id
                and o.id=a.optic_id
                and l.id=a.location_id
                and c.id=a.camera_id
                and p.optic_id=o.id
                and p.camera_id=c.id
                and a.raw_directory like '{d}%'
                order by p.id, f.name, a.raw_directory, a.panel_name, f.astrobin_id
                ;"""
            data = self.db_ap.select(
                stmt=stmt,
                columns=['optic_id', 'camera_id', 'target_id', 'targetname', 'panelname', 'raw_directory', 'date', 'filter_name', 'filter_astrobinid', 'accepted_count', 'exposureseconds', 'focal_ratio', 'bortle', 'profile_id'],
            )
            # NOTE the columns are named because of what Astrobin wants!

            # create a dict representing the last target that contains
            last_key:str=None
            last_raw_directory:str=None

            for datum in data:
                this_key = f"{datum['profile_id']}+{datum['optic_id']}+{datum['camera_id']}+{datum['target_id']}+{datum['panelname']}"
                if last_key is None or this_key != last_key:
                    # new target
                    last_key = this_key
                    # base dir is everything up to but excluding "accept"
                    accept_start = datum['raw_directory'].find(common.DIRECTORY_ACCEPT)
                    #if last_raw_directory and self.debug:
                    #    print(f"len(output[this_raw_directory])={len(output[this_raw_directory])}")
                    #    print(f"output[this_raw_directory]={output[this_raw_directory]}")
                    last_raw_directory = datum['raw_directory'][:accept_start-1]
                    if self.debug:
                        print(f"last_key={last_key}")
                        print(f"last_raw_directory={last_raw_directory}")

                accept_start = datum['raw_directory'].find(common.DIRECTORY_ACCEPT)
                this_raw_directory = datum['raw_directory'][:accept_start-1]
                #if self.debug:
                #    print(f"this_raw_directory={this_raw_directory}")

                if last_raw_directory != this_raw_directory:
                    print(f"WARNING last_raw_directory={last_raw_directory}")
                    print(f"WARNING this_raw_directory={this_raw_directory}")
                    print(f"WARNING multiple directories for '{datum['targetname']}'")

                if this_raw_directory not in output:
                    output[this_raw_directory] = []
                output[this_raw_directory].append(datum)
        except Exception as e:
            print(e)
            traceback.print_exc()
        finally:
            self.db_ap.close()

        if self.debug:
            print(f"output={json.dumps(output, indent=4)}")

        return output

class Astrobin(SummaryData):

    def prepare_csv(self) -> dict[str, str]:
        """
        Prepare CSV data for Astrobin by translating metadata into CSV format.

        Returns:
            dict: A dictionary where the key is the directory and the value is the CSV string.
        """

        output = {}

        grouped_data = self.prepare_data()
        for target_directory in grouped_data.keys():
            translated_data = []
            for d in grouped_data[target_directory]:
                # translate data..
                translated_data.append({
                    "date": d['date'],
                    "filter": d['filter_astrobinid'],
                    "number": d['accepted_count'],
                    "duration": d['exposureseconds'],
                    "fNumber": d['focal_ratio'],
                    "bortle": d['bortle'],
                })
            data_csv = common.simpleObject_to_csv(translated_data, output_headers=True)
            output[target_directory] = data_csv

        return output

    def write_csv(self, data: dict[str, str]):
        """
        Write the prepared CSV data to files in the corresponding directories.

        Args:
            data (dict): A dictionary where the key is the directory and the value is the CSV string.
        """

        for directory in data.keys():
            data_csv = data[directory]
            filename_csv=os.path.join(directory, FILENAME_CSV)
            if not self.dryrun:
                try:
                    with open(filename_csv, "w") as f:
                        f.write(data_csv)
                except Exception as e:
                    print(e)
                    pass
            else:
                print("--------------")
                print(filename_csv)
                print(data_csv)

class Totals(SummaryData):
    db_ts:database.Scheduler = None
    db_ap:database.Astrophotgraphy = None

    def __init__(self, db_ap: database.Astrophotgraphy, db_ts: database.Scheduler, from_dir: str, debug: bool, dryrun: bool):
        """
        Initialize the Totals class.

        Args:
            db_ap (database.Astrophotgraphy): The astrophotography database instance.
            db_ts (database.Scheduler): The scheduler database instance.
            from_dir (str): The directory to process for summaries.
            debug (bool): If True, enables debug mode for verbose output.
            dryrun (bool): If True, simulates the process without making changes.
        """
        self.db_ap = db_ap
        self.db_ts = db_ts
        self.from_dir = from_dir
        self.debug = debug
        self.dryrun = dryrun


    def prepare_totals(self) -> dict[str, str]:
        """
        Prepare totals data by calculating desired, available, and needed hours for each target.

        Returns:
            dict: A dictionary where the key is the directory and the value is a dictionary of totals.
        """

        try:
            self.db_ts.open() # to get desired hours

            output = {}
            grouped_data = self.prepare_data()
            for target_directory in grouped_data.keys():
                totals = {
                    'have': {
                        'uom': "hours",
                        'total': 0.0,
                    },
                    'want': {
                        'uom': "hours",
                        'total': 0.0,
                    },
                    'need': {
                        'uom': "hours",
                        'total': 0.0,
                    },
                }
                # want
                wanted = self.db_ts.GetDesiredHours(grouped_data[target_directory][0]['profile_id'], grouped_data[target_directory][0]['targetname'])
                for filtername in wanted.keys():
                    totals['want'][filtername] = wanted[filtername]
                    totals['want']['total'] += totals['want'][filtername]

                # have
                for datum in grouped_data[target_directory]:
                    h = round(int(datum['accepted_count']) * float(datum['exposureseconds']) / 60 / 60, 1)
                    k = f"{datum['filter_name']}"
                    if 'panelname' in datum and len(datum['panelname']) > 0:
                        k = f"{datum['filter_name']} Panel {datum['panelname']}"
                    if k not in totals['have']:
                        totals['have'][k] = h
                    else:
                        totals['have'][k] += h
                    totals['have']['total'] += h

                # need
                for k in totals['want']:
                    if k in ['total', 'uom']:
                        continue
                    if k in totals['have'] and totals['want'][k] > totals['have'][k]:
                        totals['need'][k] = round(totals['want'][k] - totals['have'][k], 1)
                        totals['need']['total'] += totals['need'][k]
                    elif k not in totals['have']:
                        totals['need'][k] = round(totals['want'][k], 1)
                        totals['need']['total'] += totals['need'][k]

                output[target_directory] = totals
        finally:
            self.db_ts.close()
            self.db_ap.close()

        return output

    def write_totals(self, data: dict[str, str]):
        """
        Write the prepared totals data to files in the corresponding directories.

        Args:
            data (dict): A dictionary where the key is the directory and the value is the totals data.
        """

        for directory in data.keys():
            totals = data[directory]
            filename_total=os.path.join(directory, FILENAME_TOTALS)
            data_total = ""
            for key in totals.keys():
                value = totals[key]
                if type(value) is float:
                    value = '{:.2f}'.format(value)
                data_total += f"{key} = {value}\n"
            if not self.dryrun:
                try:
                    with open(filename_total, "w") as f:
                        f.write(json.dumps(totals, indent=4))
                except Exception as e:
                    print(e)
                    pass
            else:
                print("--------------")
                print(filename_total)
                print(data_total)

class Metadata():
    pass

if __name__ == "__main__":
    """
    Main script execution for summarizing astrophotography data.

    This script processes directories and databases to generate summaries and write them to files.
    It supports command-line arguments for specifying the input directory, debug mode, and dry run mode.
    """

    parser = argparse.ArgumentParser(description="upsert accepted images to AP database")
    parser.add_argument("--fromdir", required=True, type=str, help="directory to search for images")
    parser.add_argument("--debug", action='store_true')
    parser.add_argument("--dryrun", action='store_true')

    # treat args parsed as a dictionary
    args = vars(parser.parse_args())

    user_fromdir = args["fromdir"]
    user_debug = args["debug"]
    user_dryrun = args["dryrun"]

    a = Astrobin(
        db_ap=database.Astrophotgraphy(common.DATABASE_ASTROPHOTGRAPHY),
        from_dir=user_fromdir,
        debug=user_debug,
        dryrun=user_dryrun,
    )
    data=a.prepare_csv()
    a.write_csv(data)

    t = Totals(
        db_ap=database.Astrophotgraphy(common.DATABASE_ASTROPHOTGRAPHY),
        db_ts=database.Scheduler(common.DATABASE_TARGET_SCHEDULER),
        from_dir=user_fromdir,
        debug=user_debug,
        dryrun=user_dryrun,
    )
    data=t.prepare_totals()
    t.write_totals(data)