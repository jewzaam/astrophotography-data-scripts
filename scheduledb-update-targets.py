"""
This script updates target data in the NINA Scheduler database.
It processes profile files and integrates them into the database.

The script performs the following tasks:
- Collects all profile file names from a specified directory.
- Connects to the astrophotography and scheduler databases.
- Processes target data from profile files and updates the database.
- Handles specific profiles and their associated projects, targets, and exposure plans.

Exceptions:
    sqlite3.Error: Handles SQLite errors and ensures database connections are closed properly.
"""

import os
import re
import sqlite3
import traceback
import yaml

import common


# collected data
data = {}

# collect all file names
filenames = []

for root, d_names, f_names in os.walk(common.DIRECTORY_NINA_PROFILES):
    for f in f_names:
        filenames.append(os.path.normpath(f"{root}\{f}"))


# connect to the 2 databases
try:
    conn_ts = sqlite3.connect(common.DATABASE_TARGET_SCHEDULER)
    c_ts = conn_ts.cursor()
    conn_ap = sqlite3.connect(common.DATABASE_ASTROPHOTGRAPHY)
    c_ap = conn_ap.cursor()

    # find targets
    for filename in (filename for filename in filenames if "Targets" in filename and filename.endswith(".json")):
        with open(filename, "r") as stream:
            try:
                raw_data = yaml.safe_load(stream)
            except yaml.YAMLError as ex:
                print(f"ERROR reading file {filename}")
                traceback.print_exc()

        targetname_with_panel=raw_data["Target"]["TargetName"].replace("\"", "")
        targetname_with_panel=targetname_with_panel.replace("'", "")

        # find panel name (if it exists)
        m = re.match("(.*) Panel (.*)", targetname_with_panel)
        targetname = targetname_with_panel
        panelname = ""
        if m is not None and m.groups() is not None and len(m.groups()) == 2:
            targetname = m.groups()[0]
            panelname = m.groups()[1]
        
        coord_ra=(
            float(raw_data["Target"]["InputCoordinates"]["RAHours"]) +
            float((raw_data["Target"]["InputCoordinates"]["RAMinutes"]) / 60) +
            float((raw_data["Target"]["InputCoordinates"]["RASeconds"]) / 60 / 60)
        )

        coord_dec = (
            abs(float(raw_data["Target"]["InputCoordinates"]["DecDegrees"])) +
            abs(float(raw_data["Target"]["InputCoordinates"]["DecMinutes"]) / 60) +
            abs(float(raw_data["Target"]["InputCoordinates"]["DecSeconds"]) / 60 / 60)
        )

        rotation = 0
        if "PositionAngle" in raw_data["Target"]:
            rotation = float(raw_data["Target"]["PositionAngle"])

        if raw_data["Target"]["InputCoordinates"]["NegativeDec"]:
            coord_dec *= -1
        
        # don't like this, but get profile _name_ from the parent dir of Targets.. so many brittle "standards"
        # monkey with filename as the path separator is messing up regex.
        filename = "/".join(filename.split(os.sep))
        m = re.match(f".*/([^/]*)/Targets.*", filename)
        profile_name = ""

        if m is not None and m.groups() is not None and len(m.groups()) == 1:
            profile_name = m.groups()[0]

        # FIX profile that was incorrectly named..
        if profile_name == "C8E@f7+ZWO ASI2600MM Pro":
            profile_name = "C8E@f7.0+ZWO ASI2600MM Pro"

        c_ap.execute(f"select id from profile where name='{profile_name}'")
        row_ap = c_ap.fetchone() # UniqueKey on name, there can be only one
        if row_ap is not None:
            profile_id = row_ap[0]
        else:
            print(f"ERROR: unable to find profile id for '{profile_name}'")
            break
        
        # HACK: dirty way to handle the profiles and initial creation :(
        isMosaic = "0"
        if panelname != "":
            isMosaic = "1"

        project_data = {}
        if profile_name.endswith("+ZWO ASI2600MM Pro"):
            project_data["LRGB"] = {
                "priority": 0,
                "ditherevery": 15,
                "isMosaic": isMosaic,
                "filters": [
                    {
                        "filtername": "L",
                    },
                    {
                        "filtername": "R",
                    },
                    {
                        "filtername": "G",
                    },
                    {
                        "filtername": "B",
                    },
                ]
            }
            project_data["SHO"] = {
                "priority": 0,
                "ditherevery": 5,
                "isMosaic": isMosaic,
                "filters": [
                    {
                        "filtername": "O",
                        "filtername": "S",
                        "filtername": "H",
                    },
                ]
            }
            if profile_name.startswith("C8@f6.3+"):
                project_data["LRGB"]["ditherevery"] = 15
                project_data["SHO"]["ditherevery"] = 5
            if profile_name.startswith("C8E@f7.0+"):
                project_data["LRGB"]["ditherevery"] = 15
                project_data["SHO"]["ditherevery"] = 2
            elif profile_name.startswith("E120@f7.0+"):
                project_data["LRGB"]["ditherevery"] = 7
                project_data["SHO"]["ditherevery"] = 1
        elif profile_name.endswith("+ATR585M"):
            project_data["LRGB"] = {
                "priority": 0,
                "ditherevery": 10,
                "isMosaic": isMosaic,
                "filters": [
                    {
                        "filtername": "L",
                    },
                    {
                        "filtername": "R",
                    },
                    {
                        "filtername": "G",
                    },
                    {
                        "filtername": "B",
                    },
                ]
            }
            project_data["SHO"] = {
                "priority": 0,
                "ditherevery": 5,
                "isMosaic": isMosaic,
                "filters": [
                    {
                        "filtername": "O",
                        "filtername": "S",
                        "filtername": "H",
                    },
                ]
            }
            if profile_name.startswith("C8@f6.3+"):
                project_data["LRGB"]["ditherevery"] = 15
                project_data["SHO"]["ditherevery"] = 5
            if profile_name.startswith("C8E@f7.0+"):
                project_data["LRGB"]["ditherevery"] = 15
                project_data["SHO"]["ditherevery"] = 2
            elif profile_name.startswith("E120@f7.0+"):
                project_data["LRGB"]["ditherevery"] = 7
                project_data["SHO"]["ditherevery"] = 1
        elif profile_name.endswith("+AP26CC"):
            project_data["UVIR"] = {
                "priority": 0,
                "ditherevery": 5,
                "isMosaic": isMosaic,
                "filters": [
                    {
                        "filtername": "UVIR",
                    },
                ]
            }
            project_data["LeXtr"] = {
                "priority": 0,
                "ditherevery": 4,
                "isMosaic": isMosaic,
                "filters": [
                    {
                        "filtername": "LeXtr",
                    },
                ]
            }
            project_data["ALPT"] = {
                "priority": 0,
                "ditherevery": 4,
                "isMosaic": isMosaic,
                "filters": [
                    {
                        "filtername": "ALPT",
                    },
                ]
            }
        elif profile_name.endswith("+DWARFIII"):
            project_data["Astro"] = {
                "priority": 0,
                "ditherevery": 0,
                "isMosaic": isMosaic,
                "filters": [
                    {
                        "filtername": "Astro",
                    },
                ]
            }
            project_data["Dual-Band"] = {
                "priority": 0,
                "ditherevery": 0,
                "isMosaic": isMosaic,
                "filters": [
                    {
                        "filtername": "Dual-Band",
                    },
                ]
            }
        else:
            print(f"WARNING: profile not handled!  '{profile_name}'")

        
        #print(f"{profile_name}: {targetname}/{panelname} @ {coord_ra} / {coord_dec}")
        for key in project_data.keys():
            # don't create duplicates
            select_project = f"""select id 
                                from project
                                where profileid='{profile_id}'
                                and name=\"{targetname}+{key}\"
                                ;
                                """
            c_ts.execute(select_project)
            row_p=c_ts.fetchone()
            if row_p is None or len(row_p) == 0:
                print(f"CREATE profile: {profile_name}/{targetname}+{key}")
                # NOTE create all projects as "Active" (state=1)
                insert_project = f"""insert into project (
                                    profileid, name, state, priority, createdate, minimumtime, minimumaltitude, 
                                    usecustomhorizon, horizonoffset, meridianwindow, filterswitchfrequency,
                                    ditherevery, enablegrader, isMosaic
                                )
                                values (
                                    '{profile_id}',
                                    \"{targetname}+{key}\",
                                    1,
                                    {project_data[key]["priority"]},
                                    1700839363,
                                    30, 0, 1, 0, 0, 0,
                                    {project_data[key]["ditherevery"]},
                                    0,
                                    {project_data[key]["isMosaic"]}
                                );"""
                c_ts.execute(insert_project)

            select_target = f"""select id, ra, dec, rotation
                                from target
                                where name=\"{targetname_with_panel}\"
                                and projectid in (select id 
                                    from project
                                    where profileid='{profile_id}'
                                    and name=\"{targetname}+{key}\")
                                ;
                                """
            c_ts.execute(select_target)
            row_p=c_ts.fetchone()
            if row_p is None or len(row_p) == 0:
                print(f"CREATE target: {profile_name}/{targetname_with_panel}")
                insert_target = f"""insert into target (
                                    name, active, ra, dec, epochcode, rotation, roi, projectid
                                )
                                values (
                                    \"{targetname_with_panel}\",
                                    1,
                                    {coord_ra},
                                    {coord_dec},
                                    2,
                                    {rotation},
                                    100,
                                    (select id 
                                        from project
                                        where profileid='{profile_id}'
                                        and name=\"{targetname}+{key}\")
                                );
                                """
                c_ts.execute(insert_target)
            else:
                precision = 6
                t_id = row_p[0]
                old_ra = round(row_p[1], precision)
                old_dec = round(row_p[2], precision)
                old_rotation = row_p[3]
                if old_ra != round(coord_ra, precision) or old_dec != round(coord_dec, precision) or old_rotation != rotation:
                    print(f"UPDATE target: {profile_name}/{targetname_with_panel}")
                    print(f"\tra     ({row_p[1]} --> {coord_ra})")
                    print(f"\tdec    ({row_p[2]} --> {coord_dec})")
                    print(f"\rotation({row_p[3]} --> {rotation})")
                    # update coordinates..
                    update_target = f"""update target
                                        set ra={coord_ra},
                                        dec={coord_dec},
                                        rotation={rotation}
                                        where id={t_id};"""
                    c_ts.execute(update_target)

            '''
            for filter in project_data[key]["filters"]:
                select_exposureplan = f"""select id 
                                        from exposureplan
                                        where profileid='{profile_id}'
                                        and targetid in (
                                            select id 
                                            from target
                                            where name=\"{targetname_with_panel}\"
                                            and projectid in (
                                                select id 
                                                from project
                                                where profileid='{profile_id}'
                                                and name=\"{targetname}+{key}\"
                                            )
                                        )
                                        and exposureTemplateId in (
                                            select et.id 
                                            from exposuretemplate et, project p 
                                            where et.profileid=p.profileid 
                                            and et.profileid='{profile_id}'
                                            and p.name=\"{targetname}+{key}\" 
                                            and et.name='{filter["filtername"]}'
                                        )
                                        ;
                                        """
                c_ts.execute(select_exposureplan)
                row_p=c_ts.fetchall()
                if row_p is None or len(row_p) == 0:
                    print(f"CREATE exposureplan: {profile_name}/{targetname}+{key}/{filter['filtername']}")
                    insert_exposureplan = f"""insert into exposureplan (
                                                profileid, exposure, desired, acquired, accepted, targetid, exposureTemplateId
                                            )
                                            values (
                                                '{profile_id}',
                                                -1,
                                                {filter["desired"]},
                                                0, 0,
                                                (
                                                    select t.id 
                                                    from target t, project p 
                                                    where t.projectid=p.id 
                                                    and t.name=\"{targetname_with_panel}\"
                                                    and p.name=\"{targetname}+{key}\"
                                                ),
                                                (
                                                    select et.id 
                                                    from exposuretemplate et, project p 
                                                    where et.profileid=p.profileid 
                                                    and et.profileid='{profile_id}'
                                                    and p.name=\"{targetname}+{key}\" 
                                                    and et.name='{filter["filtername"]}'
                                                )
                                            );
                                            """
                    c_ts.execute(insert_exposureplan)
                '''
            
            conn_ts.commit()

    common.backup_scheduler_database()

except sqlite3.Error as e:
    """
    Handle SQLite errors and ensure database connections are closed properly.

    Args:
        e (sqlite3.Error): The SQLite error encountered during execution.
    """
    if conn_ts is not None:
        conn_ts.close()
    if conn_ap is not None:
        conn_ap.close()
    print(e)
    traceback.print_exc()

# RESET for testing
# delete from exposureplan; delete from target; delete from project;
