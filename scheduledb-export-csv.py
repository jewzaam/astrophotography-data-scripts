"""
This script exports data from the NINA Scheduler database to a CSV file.
It skips specific profiles based on predefined identifiers.
"""

import os
import sqlite3
import subprocess
import sys
import traceback

import common

SKIP_PROFILES=[
    "7c504d1b-6d2d-4e1e-ba80-5615fcdfc814", # C8@f6.3+ZWO ASI2600MM Pro
    "f8cf2e6c-edc8-40bf-a5d4-c1d1edb05fd3", # 150PDS@f4.75+ZWO ASI2600MM Pro
    "e3438b85-2196-4cde-ad8e-5492e3ca83c9", # E120@f7.0+ZWO ASI2600MM Pro
    "bf009667-c0bc-4c11-a7ab-6dfa8bdc71e2", # C8E@f7.0+ZWO ASI2600MM Pro
    "491c0f8e-d48a-4b81-8674-355bd37a30e2", # R135@f2.8+AP26CC
    "cbd7c275-a041-4db8-911a-0008b67f7593", # SQA55@f5.3+AP26CC
    "eae6ca9c-dc72-457f-9234-55b6e6b057c1", # SQA55@f4.8+ZWO ASI2600MM Pro
    "0b09bb0b-4bd8-40f1-b782-a1589f2a58cc", # SQA55@f4.8+SL2
]

try:
    conn_ts = sqlite3.connect(common.DATABASE_TARGET_SCHEDULER)
    c_ts = conn_ts.cursor()
    conn_ap = sqlite3.connect(common.DATABASE_ASTROPHOTGRAPHY)
    c_ap = conn_ap.cursor()

    # output the following:
    # profile id, profile name, target name, filter, desired hours, accepted hours, exposureplan id

    # fetch profile data from the astrophotography database
    c_ap.execute("select id, name, filter_names from profile;")
    rows_ap = c_ap.fetchall()
    if rows_ap is None or len(rows_ap) == 0:
        print("ERROR no profiles found. Aborting.")
        sys.exit(1)

    for row_ap in rows_ap:
        profile_id = row_ap[0]
        profile_name = row_ap[1]
        filter_names = row_ap[2].split(",")

        if profile_id in SKIP_PROFILES:
            #print(f"SKIPPING {profile_id}, {profile_name}")
            continue

        # get all draft and active target data where also have an exposureplan
        c_ts.execute(f"""select distinct p.profileid, t.name, p.priority
                        from target t, project p
                        where t.projectid=p.id
                        and p.profileid='{profile_id}'
                        and (p.state = 0 or p.state = 1)
                        order by p.profileid, t.name
                    ;""")

        rows_ts = c_ts.fetchall()
        if rows_ts is None or len(rows_ts) == 0:
            print(f"ERROR no exposure plans found for profile '{profile_id} / {profile_name}'. Aborting.")
            sys.exit(1)

        data = []

        for row_ts in rows_ts:
            profile_id = row_ts[0]
            target_name = row_ts[1].replace("\"", "'")
            priority = row_ts[2]

            # get the name of target without "panel" suffix
            project_name_prefix=common.normalize_target_name(target_name)[0]

            # if priority is 0 then check ruleweights
            # if all are "0" then set priority to -1 (yes, a magical number. too bad.)
            if priority == 0:
                select_ruleweight = f"""select count(*)
                                    from ruleweight
                                    where weight > 0
                                    and projectid in (select id from project where name like '{project_name_prefix}%')
                                    ;"""
                c_ts.execute(select_ruleweight)
                row_ruleweight_count = c_ts.fetchall()
                if row_ruleweight_count is not None and len(row_ruleweight_count) > 0 and row_ruleweight_count[0][0] == 0:
                    # all rule weights are 0
                    #print(f"{target_name}: all rule weights are 0: {row_ruleweight_count}")
                    priority = -1

            datum = {
                "profile_id": profile_id,
                "target_name": target_name,
                "priority": priority,
            }

            # initialize filters
            for filter_name in filter_names:
                select_filter = f"""
                            select et.defaultexposure, ep.desired, ep.accepted
                            from exposureplan ep, exposuretemplate et, target t
                            where et.profileid=ep.profileid
                            and et.id=ep.exposureTemplateId
                            and ep.exposure<0
                            and et.name='{filter_name}'
                            and et.profileid='{profile_id}'
                            and ep.targetid=t.id
                            and t.name=\"{target_name}\"
                            ;"""
                #print(select_filter)
                c_ts.execute(select_filter)
                row_filter = c_ts.fetchone()
                if row_filter is not None and len(row_filter) > 0:
                    exposure_s = row_filter[0]
                    desired_count = row_filter[1]
                    accepted_count = row_filter[2]
                    desired_h = desired_count * exposure_s / 60 / 60
                    datum[f"{filter_name}_h"] = str(desired_h)
                    datum[f"{filter_name}_%"] = ""
                    if desired_count > 0:
                        datum[f"{filter_name}_%"] = str('{:.1f}'.format(accepted_count / desired_count))
                else:
                    datum[f"{filter_name}_h"] = "0"
                    datum[f"{filter_name}_%"] = ""

            data.append(datum)

        # write the profile's csv
        print(f"Writing CSV for profile '{profile_id}' / {profile_name}")
        filename_csv = f"{common.DIRECTORY_CSV}{os.sep}desired-{profile_id}.csv"
        data_csv = common.simpleObject_to_csv(data, output_headers=True)
        with open(filename_csv, "w") as f:
            f.write(data_csv)

        # open the csv.. assume since we created it we want to edit
        print(f"Opening CSV for profile '{profile_id}' / {profile_name}")
        p = subprocess.Popen(["C:\Program Files\LibreOffice\program\scalc.exe", filename_csv])
        # wait for it to finish, can then chain import
        p.wait()

except sqlite3.Error as e:
    if conn_ts is not None:
        conn_ts.close()
    if conn_ap is not None:
        conn_ap.close()
    print(e)
    traceback.print_exc()

