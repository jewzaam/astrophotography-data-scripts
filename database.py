"""
This module provides a class for interacting with SQLite databases used in astrophotography data management.
It includes methods for opening, querying, and managing database connections.
"""

import json
import os
import re
import sys
import traceback
import xmltodict
import sqlite3
import yaml

import common

class Database():
    db_filename = ""
    conn = None
    curr = None
    autoCommit = False
    debug = False
    dryrun = False

    def __init__(self, db_filename:str, autoCommit=False, debug=False, dryrun=False):
        self.db_filename = db_filename
        self.autoCommit = autoCommit
        self.debug = debug
        self.dryrun = dryrun

    def isOpen(self):
        return self.curr

    def open(self):
        if not self.dryrun and not self.isOpen():
            self.conn = sqlite3.connect(self.db_filename)
            self.curr = self.conn.cursor()
            self.conn.execute("PRAGMA case_sensitive_like=ON")

    def execute(self, stmt:str):
        if not self.dryrun:
            try:
                self.curr.execute(stmt)
            except Exception as e:
                print(f"ERROR executing statement\n{stmt}")
                raise e
            return self.curr.fetchall()
        else:
            return stmt

    def commit(self):
        if self.conn:
            self.conn.commit()

    def rollback(self):
        if self.conn:
            self.conn.rollback()

    def close(self):
        if self.autoCommit:
            self.commit()
        if self.conn:
            if self.conn.in_transaction:
                print("WARNING closing connection while IN TRANSACTION")
            self.conn.close()
            self.conn = None
            self.curr = None

    def _make_value(self, values: dict[str, str]):
        """
        For value clause, create a comma-separated list of values in quotes.

        Args:
            values (dict): A dictionary of key-value pairs to process.

        Returns:
            str: A string of comma-separated values in quotes.
        """
        values_cleansed = []
        for v in values.values():
            if v and type(v) is str:
                values_cleansed.append(self.normalize_str(v))
            elif v is None:
                values_cleansed.append("")
            else:
                values_cleansed.append(str(v))
        j="','" # can't do in-line with f"" formatting, so use a var
        try:
            return f"'{j.join(values_cleansed)}'"
        except Exception as e:
            print(f"ERROR values={values}")
            print(f"ERROR values_cleansed={values_cleansed}")
            raise e

    def _make_where(self, where: dict[str, str]):
        """
        For where clause, create an "and"-separated list of key='value'.

        Args:
            where (dict): A dictionary of key-value pairs for the WHERE clause.

        Returns:
            str: A string representing the WHERE clause.
        """
        where_stmts = []
        for key in where.keys():
            value = where[key]
            if type(value) is str:
                value = self.normalize_str(value)
            if type(value) is str and "%" in value:
                where_stmts.append(f"{key} like '{value}'")
            else:
                where_stmts.append(f"{key}='{value}'")
        return f"{' and '.join(where_stmts)}"

    def _make_set(self, where: dict[str, str]):
        """
        For ON CONFLICT SET clause, create a comma-separated list of key='value'.

        Args:
            where (dict): A dictionary of key-value pairs for the SET clause.

        Returns:
            str: A string representing the SET clause.
        """
        set_stmts = []
        for key in where.keys():
            value = where[key]
            if type(value) is str:
                value = self.normalize_str(value)
            set_stmts.append(f"{key}='{value}'")
        return f"{','.join(set_stmts)}"

    def select_stmt(self, columns: list[str], table: str, where: dict[str, str]):
        """
        Generate a SELECT statement.

        Args:
            columns (list): A list of column names to select.
            table (str): The table name.
            where (dict): A dictionary of key-value pairs for the WHERE clause.

        Returns:
            str: The generated SELECT statement.
        """
        return f"select {','.join(columns)} from {table} where {self._make_where(where)};"

    def select(self, stmt: str, columns: list[str]) -> list[dict]:
        """
        Execute a SELECT statement and return the results as a list of dictionaries.

        Args:
            stmt (str): The SELECT statement to execute.
            columns (list): A list of column names.

        Returns:
            list: A list of dictionaries representing the query results.
        """
        rows = self.execute(stmt)
        if self.dryrun:
            # simply return the raw response, which will be the generated statement
            return rows
        output=[]
        if rows:
            for row in rows:
                f = {}
                for i, c in enumerate(columns):
                    f[c] = row[i]
                output.append(f)
        return output

    def insert_stmt(self, table: str, values: dict[str, str], ignoreErrors=False):
        """
        Generate an INSERT statement.

        Args:
            table (str): The table name.
            values (dict): A dictionary of key-value pairs to insert.
            ignoreErrors (bool): Whether to ignore errors during insertion.

        Returns:
            str: The generated INSERT statement.
        """
        ignore = ""
        if ignoreErrors:
            ignore = "or ignore "
        return f"insert {ignore}into {table} ({','.join(values.keys())}) values ({self._make_value(values)});"

    def insert(self, table: str, values: dict[str, str], ignoreErrors=False):
        """
        Execute an INSERT statement.

        Args:
            table (str): The table name.
            values (dict): A dictionary of key-value pairs to insert.
            ignoreErrors (bool): Whether to ignore errors during insertion.

        Returns:
            Any: The result of the INSERT operation.
        """
        return self.execute(self.insert_stmt(table, values, ignoreErrors))

    def upsert_stmt(self, table: str, insert_values: dict[str, str], update_values: dict[str, str], conflictColumns: list[str]):
        """
        Generate an UPSERT statement.

        Args:
            table (str): The table name.
            insert_values (dict): A dictionary of key-value pairs to insert.
            update_values (dict): A dictionary of key-value pairs to update on conflict.
            conflictColumns (list): A list of columns to check for conflicts.

        Returns:
            str: The generated UPSERT statement.
        """
        return f"insert into {table} ({','.join(insert_values.keys())}) values ({self._make_value(insert_values)}) on conflict ({','.join(conflictColumns)}) do update set {self._make_set(update_values)},last_updated_date=CURRENT_TIMESTAMP;"

    def upsert(self, table: str, insert_values: dict[str, str], update_values: dict[str, str], conflictColumns: list[str]):
        """
        Execute an UPSERT statement.

        Args:
            table (str): The table name.
            insert_values (dict): A dictionary of key-value pairs to insert.
            update_values (dict): A dictionary of key-value pairs to update on conflict.
            conflictColumns (list): A list of columns to check for conflicts.

        Returns:
            Any: The result of the UPSERT operation.
        """
        return self.execute(self.upsert_stmt(table, insert_values, update_values, conflictColumns))
    
    def delete(self, table: str, where: dict[str, str]):
        """
        Execute a DELETE statement.

        Args:
            table (str): The table name.
            where (dict): A dictionary of key-value pairs for the WHERE clause.

        Returns:
            Any: The result of the DELETE operation.
        """
        stmt = f"delete from {table} where {self._make_where(where)};"
        return self.execute(stmt)

    def normalize_str(self, value):
        if type(value) is not str:
            return value
        return value.replace("'", "''")

class Astrophotgraphy(Database):
    createSchema = [
        """CREATE TABLE IF NOT EXISTS camera (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
                                );""",
        "CREATE UNIQUE INDEX IF NOT EXISTS camera1 ON camera(name);",
        """CREATE TABLE IF NOT EXISTS optic (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    focal_ratio text NOT NULL,
                                    creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
                                );""",
        "CREATE UNIQUE INDEX IF NOT EXISTS optic1 ON optic(name,focal_ratio);",
        """CREATE TABLE IF NOT EXISTS profile (
                                    id text PRIMARY KEY,
                                    name text NOT NULL,
                                    filter_names text NOT NULL,
                                    optic_id integer NOT NULL,
                                    camera_id integer NOT NULL,
                                    creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
                                );""",
        "CREATE UNIQUE INDEX IF NOT EXISTS profile1 ON profile(optic_id,camera_id);",
        "CREATE UNIQUE INDEX IF NOT EXISTS profile2 ON profile(name);",
        """CREATE TABLE IF NOT EXISTS location (
                                    id integer PRIMARY KEY,
                                    name text,
                                    latitude text NOT NULL,
                                    longitude text NOT NULL,
                                    magnitude text,
                                    bortle integer,
                                    brightness_mcd_m2 text,
                                    artifical_brightness_ucd_m2 text,
                                    creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
                                );""",
        "CREATE UNIQUE INDEX IF NOT EXISTS location1 ON location(latitude,longitude);",
        """CREATE TABLE IF NOT EXISTS target (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    profile_id integer NOT NULL,
                                    creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
                                );""",
        "CREATE UNIQUE INDEX IF NOT EXISTS target1 ON target(name);",
        """CREATE TABLE IF NOT EXISTS filter (
                                    id integer PRIMARY KEY,
                                    name text NOT NULL,
                                    astrobin_id integer,
                                    creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP
                                );""",
        "CREATE UNIQUE INDEX IF NOT EXISTS filter1 ON filter(name);",
        """CREATE TABLE IF NOT EXISTS accepted_data (
                                    id integer PRIMARY KEY,
                                    date text NOT NULL,
                                    panel_name text,
                                    shutter_time_seconds integer NOT NULL,
                                    accepted_count integer NOT NULL,
                                    raw_directory text,
                                    camera_id integer NOT NULL,
                                    optic_id integer NOT NULL,
                                    location_id integer NOT NULL,
                                    target_id integer NOT NULL,
                                    filter_id integer NOT NULL,
                                    creation_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    last_updated_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                                    FOREIGN KEY (camera_id) REFERENCES camera (id),
                                    FOREIGN KEY (optic_id) REFERENCES optic (id),
                                    FOREIGN KEY (target_id) REFERENCES target (id),
                                    FOREIGN KEY (filter_id) REFERENCES filter (id),
                                    FOREIGN KEY (location_id) REFERENCES location (id)
                                );""",
        "CREATE UNIQUE INDEX IF NOT EXISTS accepted_data1 ON accepted_data(camera_id,optic_id,location_id,target_id,filter_id,date,panel_name,shutter_time_seconds,raw_directory);",
        "CREATE UNIQUE INDEX IF NOT EXISTS accepted_data2 ON accepted_data(raw_directory);",
    ]

    defaultLocations = [
        {"name": "RL", "latitude": "35.6", "longitude": "-78.8"},
        {"name": "BW", "latitude": "35.8", "longitude": "-79.0"},
        {"name": "3BA", "latitude": "36.1", "longitude": "-78.7"},
        {"name": "SRSP", "latitude": "36.7", "longitude": "-78.7"},
        {"name": "HW", "latitude": "35.4", "longitude": "-78.3"},
        {"name": "KDDS", "latitude": "39.6", "longitude": "-104.0"}
    ]

    defaultFilters = {
        "L": {"name": "L", "astrobin_id": "2625"},
        "R": {"name": "R", "astrobin_id": "2627"},
        "G": {"name": "G", "astrobin_id": "2626"},
        "B": {"name": "B", "astrobin_id": "2628"},
        "S": {"name": "S", "astrobin_id": "2629"},
        "H": {"name": "H", "astrobin_id": "2631"},
        "O": {"name": "O", "astrobin_id": "2630"},
        "UVIR": {"name": "UVIR", "astrobin_id": "2411"},
        "LenHa": {"name": "LenHa", "astrobin_id": "5500"},
        "LeXtr": {"name": "LeXtr", "astrobin_id": "2618"},
        "ALPT": {"name": "ALPT", "astrobin_id": "5678"},
    }

    # do not include "id" column.  it is added as needed.
    columns = {
        "filter": ['id', 'name', 'astrobin_id'],
        "location": ['id', 'name', 'latitude', 'longitude', 'magnitude', 'bortle', 'brightness_mcd_m2', 'artifical_brightness_ucd_m2'],
    }

    # for upsert, the columns in the 'on conflict' clause. must be a unique key
    conflicts = {
        "location": ['latitude', 'longitude'],
        "profile": ['id'],
        "filter": ['name'],
        "accepted_data": ['date', 'shutter_time_seconds', 'panel_name', 'camera_id', 'optic_id', 'location_id', 'target_id', 'filter_id'],
    }

    def CreateSchema(self):
        for stmt in self.createSchema:
            self.execute(stmt)

    def CreateProfileStmts(self, profile_dir:str) -> list[str]:
        """
        Generate the statements to create profile data for all profiles in the given directory.
        Statements must be executed seperately.
        """
        output_stmts = []
        filenames = []
        for root, _, f_names in os.walk(profile_dir):
            for f in f_names:
                if not f.endswith(".profile"):
                    continue
                filenames.append(f"{root}{os.sep}{f}")
        for filename in filenames:
            with open(filename, "r") as fd:
                try:
                    xml_dump = json.dumps(xmltodict.parse(fd.read()), indent=4)
                    xml_data = yaml.safe_load(xml_dump)
                    profile_id = xml_data["Profile"]["Id"]
                    profile_name = xml_data["Profile"]["Name"]

                    # write profile as json file for debugging
                    #with open(f"C:\Users\jewza\Dropbox\Family Room\Astrophotography\Data\profile-{profile_name}.json", "w") as f:
                    #    f.write(xml_dump)

                    # find all filters
                    filters = []
                    if "FilterWheelSettings" in xml_data["Profile"] and "FilterWheelFilters" in xml_data["Profile"]["FilterWheelSettings"] and "a:FilterInfo" in xml_data["Profile"]["FilterWheelSettings"]["FilterWheelFilters"]:
                        # if there is only one filter it  will not be an array. annoying..
                        infos = xml_data["Profile"]["FilterWheelSettings"]["FilterWheelFilters"]["a:FilterInfo"]
                        if type(infos) is not list:
                            infos = [infos]

                        for info in infos:
                            f = common.normalize_filterName(info['a:_name'])
                            # skip any "DARK" filter
                            if f.startswith("DARK"):
                                continue
                            # skip any "BLANK" filter
                            if f.startswith("BLANK"):
                                continue
                            if f not in self.defaultFilters:
                                print(f"WARNING found unknown filter '{f}' in profile '{profile_name}'")
                            filters.append(f)

                    # special handling for filter names, order is priority.
                    filter_names=",".join(filters)
                    if filter_names == "L,R,G,B,S,H,O":
                        # want to prioritize O over S and S over H
                        filter_names="L,R,G,B,O,S,H"

                    # profile names are following a standard now... <optic>@<f-ratio>+<camera>
                    m = re.match("([^@]*)@f([^+]*)[+](.*)", profile_name)
                    print(f"DEBUG: {profile_name}")
                    if m is not None and m.groups() is not None and len(m.groups()) == 3:
                        optic = m.groups()[0]
                        focal_ratio = m.groups()[1]
                        camera = m.groups()[2]
                        print(f"DEBUG: {optic}, {focal_ratio}, {camera}")
                        # insert optic (have to insert for lookup)
                        self.execute(self.insert_stmt("optic", {"name": optic, "focal_ratio": focal_ratio}, ignoreErrors=True))
                        # insert camera (have to insert for lookup)
                        self.execute(self.insert_stmt("camera", {"name": camera}, ignoreErrors=True))
                        # insert profile, allow for only "filter_names" to be updated
                        output_stmts.append(
                            self.upsert_stmt(
                                table="profile",
                                insert_values={
                                    "id": profile_id,
                                    "name": profile_name,
                                    "filter_names": filter_names,
                                    "optic_id": self.select(self.select_stmt(['id'], "optic", {"name": optic, "focal_ratio": focal_ratio}), ['id'])[0]['id'],
                                    "camera_id": self.select(self.select_stmt(['id'], "camera", {"name": camera}), ['id'])[0]['id'],
                                },
                                update_values={
                                    "name": profile_name,
                                    "filter_names": filter_names,
                                    "optic_id": self.select(self.select_stmt(['id'], "optic", {"name": optic, "focal_ratio": focal_ratio}), ['id'])[0]['id'],
                                    "camera_id": self.select(self.select_stmt(['id'], "camera", {"name": camera}), ['id'])[0]['id'],
                                },
                                conflictColumns=["id"],
                            )
                        )
                except Exception as e:
                    print(f"error processing '{filename}")
                    raise e
        return output_stmts

    def CreateFilters(self):
        for f in self.defaultFilters.keys():
            astrobin_id = self.defaultFilters[f]['astrobin_id']
            self.execute(
                f"""INSERT INTO filter (name,astrobin_id)
                    values (
                        '{f}',
                        '{astrobin_id}'
                    )
                    ON CONFLICT (name)
                    DO UPDATE SET
                    last_updated_date = CURRENT_TIMESTAMP,
                    astrobin_id = '{astrobin_id}'
                    ;"""
            )


    def UpdateFromDirectory(self, from_dir:str, modeDelete, modeCreate, modeUpdate):
        """
        modeDelete - delete any accepted_data where the directory is missing (done first)
        modeCreate - create accepted_data where the directory doesn't exist in the database
        modeUpdate - create and/or update accepted_data
        """

        print("Modes enabled:")
        print(f"    Delete = {modeDelete}")
        print(f"    Create = {modeCreate}")
        print(f"    Update = {modeUpdate}")

        if not modeDelete and not modeCreate and not modeUpdate:
            print("ERROR: at least one mode must be enabled.  Exiting!")
            return

        deleted_count = 0
        accepted_count = 0
        total_count = 0
        accepted_data = {}

        if modeDelete:
            print("Delete...")

            # get all raw_directories
            c = ['id', 'raw_directory']
            stmt = self.select_stmt(
                columns=c,
                table='accepted_data',
                where={
                    "raw_directory": f"{from_dir}%", # will add like statement...
                },
            )
            data_dirs = self.select(
                stmt=stmt,
                columns=c,
            )
            for datum in data_dirs:
                raw_dir = datum['raw_directory']
                # does the directory exist?
                if not os.path.isdir(raw_dir):
                    # it does not! delete the accept_data
                    if self.debug:
                        print(f"    Deleting: {raw_dir}")
                    if not self.dryrun:
                        self.delete(
                            table='accepted_data',
                            where={"id": datum['id']},
                        )
                    deleted_count += 1

        # support searching an array of directories
        from_dirs = [from_dir]

        if modeCreate:
            # get all directories in filesystem
            # check if directory exists in database
            # if not, queue up for fetching metadata
            # else, ignore
            filenames = common.get_filenames(
                dirs=[from_dir],
                patterns=[".*\.fits$", ".*\.cr2$"],
                recursive=True,
            )

            # walk through all accepted data and remove anything that already exists in the database
            missing_dirs=set()
            existing_dirs=set()
            for filename in filenames:
                directory = os.sep.join(filename.split(os.sep)[:-1])
                if directory not in existing_dirs and directory not in missing_dirs:
                    # look up the directory
                    # cache in either missing_dirs or existing_dirs
                    stmt = self.select_stmt(
                        columns=['count(id)'],
                        table="accepted_data",
                        where={"raw_directory": f"{directory}%"}
                    )
                    exists = self.select(
                        stmt=stmt,
                        columns=['count']
                    )
                    if len(exists) == 1 and exists[0]['count'] == 0:
                        missing_dirs.add(directory)
                    else:
                        existing_dirs.add(directory)
            # since this is createOnly, we'll only look at missing directories
            if self.debug:
                for d in missing_dirs:
                    print(f"    Creating: {d}")
            # override from_dirs with only what needs to be created.
            from_dirs = missing_dirs
    
        if modeCreate or modeUpdate:
            print("Create or Update...")
            terminated = False

            required_properties=['type', 'targetname', 'panel', 'date', 'optic', 'focal_ratio',
                                'filter', 'camera', 'exposureseconds', 'latitude', 'longitude',
                                'filename']

            # fetch all lights metadata for cr2 and fits image files
            data = common.get_filtered_metadata(
                dirs=from_dirs, 
                patterns=[".*\.cr2$",".*\.fits$"], 
                recursive=True, 
                required_properties=required_properties, 
                filters={"type": "LIGHT"},
                debug=self.debug,
                printStatus=True,
                profileFromPath=True,
            )

            # the data after manipulation and aggregation
            accepted_data = {}

            accepted_count = 0
            total_count = 0

            try:
                for filename in data.keys():
                    # collect count of all files
                    total_count += 1

                    # skip if not in an 'accept' directory
                    if common.DIRECTORY_ACCEPT not in filename:
                        #print(f"SKIP: {filename}\n")
                        continue

                    # collect count of accepted files
                    accepted_count += 1

                    directory = os.sep.join(filename.split(os.sep)[:-1])

                    if self.debug:
                        print(f"data[filename]={data[filename]}")

                    # some setups (Dwarf 3) don't have lat/long, default to first default location
                    if 'latitude' not in data[filename] or 'longitude' not in data[filename]:
                        data[filename]['latitude'] = Astrophotgraphy.defaultLocations[0]['latitude']
                        data[filename]['longitude'] = Astrophotgraphy.defaultLocations[0]['longitude']

                    # extract bits needed for hash
                    datum = {
                        "date": data[filename]['date'],
                        "optic": data[filename]['optic'],
                        "focal_ratio": data[filename]['focal_ratio'],
                        "filter": data[filename]['filter'],
                        "camera": data[filename]['camera'],
                        "targetname": data[filename]['targetname'],
                        "panel": data[filename]['panel'],
                        "latitude": data[filename]['latitude'],
                        "longitude": data[filename]['longitude'],
                        "exposureseconds": data[filename]['exposureseconds'],
                        "directory": directory,
                    }

                    key = hash(json.dumps(datum, sort_keys=True))

                    # have we already processed this accepted_data?
                    if key in accepted_data:
                        # yes, increment count
                        accepted_data[key]["count"] += 1
                    else:
                        # no, set count to 1 and add to accepted_data
                        datum["count"] = 1
                        accepted_data[key] = datum

            except KeyboardInterrupt as e:
                print(f"User terminated!")
                terminated = True
            except:
                print(f"ERROR processing file {filename}")
                traceback.print_exc()
                terminated = True

            if terminated:
                raise Exception("user terminated")

        print("\nCounts:")
        print(f"\tDelete: {deleted_count}")
        print(f"\tAccept: {accepted_count}")
        print(f"\tTotal:  {total_count}")

        if not self.dryrun:
            print("Updating database...")
            for datum in accepted_data.values():
                # create rows as needed.
                # NOTE this is not optimized on purpose, it's always a relatively small data set so repetition on upsert reference data is fine
                
                # make sure reference data exists.  assume there is a unique key constraint for OR IGNORE to work
                # NOTE use double quotes to wrap values, we'll replace double quotes with single quotes in metadata
                self.insert(ignoreErrors=True, table="optic",
                    values={
                        "name": datum['optic'],
                        "focal_ratio": datum['focal_ratio'],
                    })
                self.insert(ignoreErrors=True, table="camera",
                    values={
                        "name": datum['camera'],
                    })
                self.insert(ignoreErrors=True, table="location",
                    values={
                        "latitude": datum['latitude'],
                        "longitude": datum['longitude'],
                    })
                self.insert(ignoreErrors=True, table="target",
                    values={
                        "name": datum['targetname'],
                    })
                self.insert(ignoreErrors=True, table="filter",
                    values={
                        "name": datum['filter'],
                    })

                # insert accepted data, update accepted_count if it already exists
                insert_stmt=f"""
                    INSERT INTO accepted_data(date, shutter_time_seconds, accepted_count, panel_name, raw_directory,
                    camera_id, optic_id, location_id, target_id, filter_id)
                    VALUES (\"{datum['date']}\", \"{datum['exposureseconds']}\", \"{datum['count']}\", \"{datum['panel']}\", \"{datum['directory']}\",
                    (select id from camera where name=\"{datum['camera']}\"),
                    (select id from optic where name=\"{datum['optic']}\" and focal_ratio=\"{datum['focal_ratio']}\"),
                    (select id from location where latitude=\"{datum['latitude']}\" and longitude=\"{datum['longitude']}\"),
                    (select id from target where name=\"{datum['targetname']}\"),
                    (select id from filter where name=\"{datum['filter']}\")
                    )
                    ON CONFLICT (raw_directory) 
                    DO UPDATE SET 
                    last_updated_date = CURRENT_TIMESTAMP,
                    accepted_count = \"{datum['count']}\" 
                    ;"""
                self.execute(insert_stmt)
        else:
            print("DRYRUN: not(Updating database)")

class Scheduler(Database):
    def GetDesiredHours(self, profile_id:str, targetname:str) -> float:
        output = {}

        select_stmt = f"""
            select et.defaultexposure, ep.desired, et.name
            from exposuretemplate et, exposureplan ep, target t
            where et.id=ep.exposuretemplateid
            and ep.targetid=t.id
            and t.name='{self.normalize_str(targetname)}'
            and et.profileid='{profile_id}'
            order by et.name
            ;"""
        data = self.select(
            stmt=select_stmt,
            columns=['defaultexposure', 'desired', 'filtername']
        )
        for datum in data:
            output[datum['filtername']] = datum['defaultexposure'] * datum['desired'] / 60 / 60

        return output