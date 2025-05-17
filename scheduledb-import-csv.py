"""
This script imports CSV data into the NINA Scheduler database.
It connects to both the astrophotography and scheduler databases for data synchronization.
"""

import os
import sqlite3
import sys
import traceback

import common

try:
    conn_ts = sqlite3.connect(common.DATABASE_TARGET_SCHEDULER)
    c_ts = conn_ts.cursor()
    c_ts.execute("PRAGMA case_sensitive_like=ON")
    conn_ap = sqlite3.connect(common.DATABASE_ASTROPHOTGRAPHY)
    c_ap = conn_ap.cursor()
    c_ap.execute("PRAGMA case_sensitive_like=ON")

    update_count = 0

    # fetch profile data from the astrophotography database
    c_ap.execute("select id, name, filter_names from profile;")
    rows_ap = c_ap.fetchall()
    if rows_ap is None or len(rows_ap) == 0:
        print("ERROR no profiles found. Aborting.")
        sys.exit(1)

    for row_ap in rows_ap:
        data = []

        profile_id = row_ap[0]
        profile_name = row_ap[1]
        filter_names = row_ap[2].split(",")

        # open CSV for the profile and start processing
        filename_csv = f"{common.DIRECTORY_CSV}{os.sep}desired-{profile_id}.csv"

        if not os.path.isfile(filename_csv):
            # print(f"WARNING no CSV for profile {profile_id}")
            continue

        with open(filename_csv, "r") as f:
            # build lookup of header name to index
            h = f.readline().split(",")
            headers = {}
            for i in range(0, len(h)):
                headers[h[i].strip()] = i
            
            while True:
                datum = {}
                l = f.readline()
                if not l:
                    break

                items = l.split(",")
                
                # build datum from headers & items
                for k in headers.keys():
                    i = headers[k]
                    datum[k] = items[i].strip()
                
                data.append(datum)
        
        # all data for the profile is collected
        # walk through it and update exposure plans
        # NOTE: exposureplan may not exist but all targets have already been created, project.name encodes filter name

        for datum in data:
            target_name = datum["target_name"]

            for filter_name in filter_names:
                # find project and target data
                select_project = f"""select p.id, t.id, p.priority
                                    from project p, target t
                                    where p.profileid='{profile_id}'
                                    and p.id=t.projectid
                                    and t.name=\"{target_name}\"
                                    and p.name like '%+%{filter_name}%'
                                    ;"""
                c_ts.execute(select_project)
                row_project = c_ts.fetchall()
                if row_project is None or len(row_project) != 1:
                    project_count = 0
                    if row_project is not None:
                        project_count = len(row_project)
                    print(f"WARNING for target/filter '{target_name}/{filter_name}' found '{project_count}' projects, expected '1'. Skipping.\n{select_project}")
                    continue

                project_id = row_project[0][0]
                target_id = row_project[0][1]
                priority = row_project[0][2]

                weight_meridian = 75
                weight_mosaic = 0
                weight_complete = 20
                weight_priority = 100
                weight_soonest = 50
                weight_switch = 33

                new_priority = datum['priority']

                # overwrite priority and weights if less than zero
                if int(datum['priority']) < 0:
                    weight_meridian = 0
                    weight_mosaic = 0
                    weight_complete = 0
                    weight_priority = 0
                    weight_soonest = 0
                    weight_switch = 0
                    new_priority = 0

                # update the priority of the project
                update_project = f"""update project
                                    set priority='{new_priority}'
                                    where id='{project_id}'
                                    ;"""
                c_ts.execute(update_project)

                conn_ts.commit()
                
                # upsert ruleweight so we can customize
                c_ts.execute(f"DELETE FROM ruleweight WHERE projectid={project_id};")
                c_ts.execute(f"INSERT INTO ruleweight(name, weight, projectid) VALUES ('Meridian Window Priority', {weight_meridian}, {project_id});")
                c_ts.execute(f"INSERT INTO ruleweight(name, weight, projectid) VALUES ('Mosaic Completion', {weight_mosaic}, {project_id});")
                c_ts.execute(f"INSERT INTO ruleweight(name, weight, projectid) VALUES ('Percent Complete', {weight_complete}, {project_id});")
                c_ts.execute(f"INSERT INTO ruleweight(name, weight, projectid) VALUES ('Project Priority', {weight_priority}, {project_id});")
                c_ts.execute(f"INSERT INTO ruleweight(name, weight, projectid) VALUES ('Setting Soonest', {weight_soonest}, {project_id});")
                c_ts.execute(f"INSERT INTO ruleweight(name, weight, projectid) VALUES ('Target Switch Penalty', {weight_switch}, {project_id});")

                conn_ts.commit()
                
                # find exposuretemplate (needed for multiple things so just fetch it)
                select_exposuretemplate = f"""select et.id, et.defaultexposure
                                            from exposuretemplate et
                                            where et.profileid='{profile_id}'
                                            and et.name='{filter_name}'
                                            ;"""
                c_ts.execute(select_exposuretemplate)
                row_et = c_ts.fetchall()
                if row_et is None or len(row_et) != 1:
                    et_count = 0
                    if row_et is not None:
                        et_count = len(row_et)
                    print(f"WARNING for target/filter '{target_name}/{filter_name}' found '{et_count}' exposuretemplates, expected '1'.  Skipping\n{select_exposuretemplate}")
                    continue

                exposuretemplate_id = row_et[0][0]
                exposuretemplate_defaultexposure = row_et[0][1]

                # find exposureplan
                select_exposureplan = f"""select ep.id, ep.desired, ep.exposure
                                        from exposureplan ep
                                        where ep.exposuretemplateid='{exposuretemplate_id}'
                                        and ep.targetid='{target_id}'
                                        and ep.exposure<0
                                        ;"""
                c_ts.execute(select_exposureplan)
                row_ep = c_ts.fetchall()
                if row_ep is not None and len(row_ep) > 1:
                    # it is OK if no row is found, but must be 0..1 rows
                    print(f"ERROR found '{len(row_ep)}' exposureplans, expected '0' or '1'.\n{select_exposureplan}")
                    sys.exit(1)
                
                exposureplan_id = None
                exposureplan_desired = 0
                exposureplan_exposure = -1
                if row_ep is not None and len(row_ep) == 1:
                    exposureplan_id = row_ep[0][0]
                    exposureplan_desired = row_ep[0][1]
                    exposureplan_exposure = row_ep[0][2]

                # calculate desired count
                # NOTE exposure time in database is seconds but it's hours in the csv
                actual_exposure = exposuretemplate_defaultexposure  # default
                if exposureplan_exposure > 0:
                    actual_exposure = exposureplan_exposure         # override
                desired_h = float(datum[f"{filter_name}_h"])
                desired_count = int(desired_h * 60 * 60 / int(actual_exposure))

                if exposureplan_desired == desired_count:
                    # nothing to do, carry on
                    continue

                # insert or update exposureplan
                # NOTE Use "-1" for exposure as it will then use the defaultexposure from exposuretemplate.
                # TODO Add support for HDR where additional plans are created with different non-default exposure.
                # TODO Consider adding unique constraint on exposuretemplate(profileid, filtername)!
                update_exposureplan = ""
                if row_ep is None or len(row_ep) == 0:
                     # insert, doesn't exist
                    update_exposureplan = f"""insert into exposureplan (profileid, exposure, desired, acquired, accepted, targetid, exposuretemplateid)
                                            values ('{profile_id}', -1, '{desired_count}', '0', '0', '{target_id}', 
                                                (
                                                    select et.id
                                                    from exposuretemplate et
                                                    where et.profileid='{profile_id}'
                                                    and et.name='{filter_name}'
                                                )
                                            );"""
                else:
                    # update, does exist
                    update_exposureplan = f"""update exposureplan
                                            set desired='{desired_count}'
                                            where id='{exposureplan_id}'
                                            ;"""

                c_ts.execute(update_exposureplan)

                conn_ts.commit()

                update_count += 1

        print(f"Updated '{update_count}' for {profile_name}")

    common.backup_scheduler_database()

except sqlite3.Error as e:
    if conn_ts is not None:
        conn_ts.close()
    print(e)
    traceback.print_exc()