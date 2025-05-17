"""
This script generates reports based on data from the astrophotography and scheduler databases.
It includes methods to query and process data for reporting purposes.
"""

import os
import re
import traceback
import common
import database

class Report():
    db_astrophotography = None
    db_scheduler = None

    def __init__(self):
        """
        Initialize the Report class.

        This class connects to the astrophotography and scheduler databases
        to generate reports and process data for various purposes.
        """
        self.db_astrophotography = database.Database(common.DATABASE_ASTROPHOTGRAPHY)
        self.db_scheduler = database.Database(common.DATABASE_TARGET_SCHEDULER)

    def _findData(self, like: str):
        """
        Query and aggregate data from the astrophotography database.

        Args:
            like (str): A string pattern to filter raw directories.

        Returns:
            dict: A dictionary where keys are directory paths and values are aggregated counts.
        """
        try:
            self.db_astrophotography.open()
            stmt = f"select sum(accepted_count), raw_directory from accepted_data where raw_directory like '%{like}%' group by raw_directory;"
            data = self.db_astrophotography.select(
                stmt=stmt,
                columns=["accepted_count", "raw_directory"],
            )
            # raw directory is full path.  strip off everything "\accept\.*$".
            output = {}
            for datum in data:
                m = re.match("(.*)[\\\\\\/]accept[\\\\\\/].*", datum['raw_directory'])
                if m and len(m.groups()) == 1:
                    dir = m.groups()[0]
                    count = int(datum['accepted_count'])
                    if dir in output:
                        # existing data, add to accepted_count
                        output[dir] += count
                    else:
                        # new data to aggregate, set to accepted_count
                        output[dir] = count
            return output
        except Exception as e:
            print(e)
            traceback.print_exc()
        finally:
            self.db_astrophotography.close()

    def data(self):
        """
        Retrieve data from the astrophotography database for the data acquisition folder.

        Returns:
            dict: Aggregated data for the data acquisition folder.
        """
        return self._findData(common.DIRECTORY_DATA)

    def master(self):
        """
        Retrieve data from the astrophotography database for the master folder.

        Returns:
            dict: Aggregated data for the master folder.
        """
        return self._findData(common.DIRECTORY_MASTER)

    def process(self):
        """
        Retrieve data from the astrophotography database for the process folder.

        Returns:
            dict: Aggregated data for the process folder.
        """
        return self._findData(common.DIRECTORY_PROCESS)

    def bake(self):
        """
        Retrieve data from the astrophotography database for the bake folder.

        Returns:
            dict: Aggregated data for the bake folder.
        """
        return self._findData(common.DIRECTORY_BAKE)

    def done(self):
        """
        Retrieve data from the astrophotography database for the done folder.

        Returns:
            dict: Aggregated data for the done folder.
        """
        return self._findData(common.DIRECTORY_DONE)

    def data_ready_for_master(self):
        """
        Identify data in the acquisition folder that is ready to be moved to the master folder.

        Returns:
            list: A list of directory paths ready to be moved to the master folder.
        """
        data_dirs = self.data()

        try:
            self.db_scheduler.open()

            ready2move_dirs = []

            for data_dir in data_dirs:
                # NOTE append "accept" so objectFromPath works
                metadata = common.get_file_headers(
                    filename=data_dir+os.sep+common.DIRECTORY_ACCEPT,
                    profileFromPath=True,
                    objectFromPath=True,
                    normalize=True,
                )

                # for the target, find all acquired data
                stmt = f"""
                    select ep.profileid, t.name, ep.desired, ep.acquired, ep.accepted
                    from exposureplan ep, target t
                    where ep.targetid = t.id
                    and t.name like "{metadata['targetname']}%"
                    ;"""
                data = self.db_scheduler.select(
                    stmt=stmt,
                    columns=['profile_id', 'name', 'desired', 'acquired', 'accepted'],
                )
                # it is ready to move if all the exposure plans (datum) returned have accepted * 95% >= desired
                is_ready = len(data) > 0
                for datum in data:
                    if datum['desired'] > 0 and not (datum['accepted'] > datum['desired'] * common.MASTER_READY_PERCENT):
                        is_ready = False
                        break
                if is_ready:
                    ready2move_dirs.append(data_dir)

            return ready2move_dirs
        except Exception as e:
            print(e)
            traceback.print_exc()
        finally:
            self.db_scheduler.close()

if __name__ == "__main__":
    """
    Main script execution for generating reports and processing data.

    This script initializes the Report class, retrieves data for specific folders,
    and prints the results to the console.
    """
    r = Report()
    loop = {
        #"master": r.master,
        #"process": r.process,
        #"bake": r.bake,
        "ready for master": r.data_ready_for_master,
    }

    for key in loop.keys():
        values = loop[key]()
        print(f"\n{key.upper()}:")
        print("\n".join(values))
