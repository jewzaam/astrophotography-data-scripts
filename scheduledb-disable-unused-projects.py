"""
This script disables unused projects in the NINA Scheduler database.
It uses command-line arguments to enable debugging and dry-run modes.
"""

import argparse
import sqlite3
import sys
import traceback

import common

parser = argparse.ArgumentParser(description="disable unused projects")
parser.add_argument("--debug", action='store_true')
parser.add_argument("--dryrun", action='store_true')

# treat args parsed as a dictionary
args = vars(parser.parse_args())

user_debug = args["debug"]
user_dryrun = args["dryrun"]


try:
    conn_ts = sqlite3.connect(common.DATABASE_TARGET_SCHEDULER)
    c_ts = conn_ts.cursor()

    # simple script, disable all projects that have no EP attached
    c_ts.execute("""
            select distinct p.id, p.name, t.name, p.state, p.profileid
            from target t, project p
            where t.projectid=p.id
            and p.state<>0
            and t.id not in (
                 select ep.targetid
                 from exposureplan ep
            )
            group by t.id;
        """)
    rows_ts = c_ts.fetchall()
    if rows_ts is None or len(rows_ts) == 0:
        print("No rows found, done.")
        sys.exit(0)

    # disable every project found.  NOTE this only works with the assumption that every project has one target.. this is valid at time of writing!

    for row_ts in rows_ts:
        project_id=row_ts[0]
        project_name=row_ts[1]
        targetname=row_ts[2]
        project_state=row_ts[3]
        profile_id=row_ts[4]
        new_project_state=0
        print(f"update project state: {profile_id}/{project_name}/{targetname}: {project_state} --> {new_project_state}")
        if not user_dryrun:

            c_ts.execute(f"""update project
                            set state='{new_project_state}'
                            where id='{project_id}'
                            ;""")

    conn_ts.commit()

    common.backup_scheduler_database()

except sqlite3.Error as e:
    if conn_ts is not None:
        conn_ts.close()
    print(e)
    traceback.print_exc()

