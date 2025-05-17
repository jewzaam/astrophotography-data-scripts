"""
This script updates the accepted counts in the NINA Scheduler database.
It uses command-line arguments to enable debugging and dry-run modes.
"""

import argparse
import re
import sqlite3
import traceback

import common

parser = argparse.ArgumentParser(description="update accepted counts")
parser.add_argument("--debug", action='store_true')
parser.add_argument("--dryrun", action='store_true')

# treat args parsed as a dictionary
args = vars(parser.parse_args())

user_debug = args["debug"]
user_dryrun = args["dryrun"]


# connect to the 2 databases
try:
    conn_ts = sqlite3.connect(common.DATABASE_TARGET_SCHEDULER)
    c_ts = conn_ts.cursor()
    conn_ap = sqlite3.connect(common.DATABASE_ASTROPHOTGRAPHY)
    c_ap = conn_ap.cursor()

    '''
     for each filter+target+project+profile
       find accepted count from ap DB
       update accepted in scheduler DB
    '''

    c_ts.execute("""select ep.id, ep.profileid, t.name as targetname, et.name, ep.accepted, ep.desired,
                    et.defaultexposure, ep.exposure, p.state, p.id, p.name
                    from exposuretemplate et, exposureplan ep, target t, project p
                    where ep.profileid=et.profileid
                    and ep.profileid=p.profileid
                    and ep.targetid=t.id
                    and ep.exposuretemplateid=et.id
                    and t.projectid=p.id
                    ;""")

    rows_ts = c_ts.fetchall()

    for row_ts in rows_ts:
        exposureplan_id = row_ts[0]
        profile = row_ts[1]
        targetname = row_ts[2]
        filtername = row_ts[3]
        old_accepted_count = row_ts[4]
        desired_count = row_ts[5]
        defaultexposure = row_ts[6]
        exposure = row_ts[7]
        project_state = row_ts[8]
        project_id = row_ts[9]
        project_name = row_ts[10]

        exposure_duration_s = defaultexposure
        if exposure > 0:
            exposure_duration_s = exposure
        # find panel name (if it exists)
        m = re.match("(.*) Panel (.*)", targetname)
        panelname = ""
        if m is not None and m.groups() is not None and len(m.groups()) == 2:
            targetname = m.groups()[0]
            panelname = m.groups()[1]

        # figure out the status from the location of the accepted data, using the HIGHEST value in case there are multiple found
        # NOTE on multiples found, common if data doesn't have master calibration frames yet and is split across multiple dirs
        select_status=f"""select distinct a.raw_directory
                        from target t, accepted_data a, profile p
                        where a.target_id=t.id
                        and a.camera_id=p.camera_id
                        and a.optic_id=p.optic_id
                        and p.id='{profile}'
                        and t.name=\"{targetname}\"
                        and a.panel_name=\"{panelname}\"
                        ;"""
        c_ap.execute(select_status)
        rows_dir=c_ap.fetchall()
        new_project_state=project_state
        if rows_dir is not None and len(rows_dir) > 0:
            new_project_state=-1
            for dir in rows_dir:
                new_project_state = max(new_project_state, common.project_status_from_path(dir[0]))

        # find the count from the ap database
        select_accepted=f"""select sum(a.accepted_count)
                        from target t, accepted_data a, filter f, profile p
                        where a.target_id=t.id
                        and a.filter_id=f.id
                        and a.camera_id=p.camera_id
                        and a.optic_id=p.optic_id
                        and p.id='{profile}'
                        and t.name=\"{targetname}\"
                        and a.panel_name=\"{panelname}\"
                        and f.name='{filtername}'
                        and a.shutter_time_seconds='{exposure_duration_s}'
                        ;
                     """
        c_ap.execute(select_accepted)

        rows_ap = c_ap.fetchall()
        # handle if no rows were returned by setting accepted = 0
        new_accepted_count = 0
        for row_ap in rows_ap:
            new_accepted_count = row_ap[0]
        if new_accepted_count is None:
            new_accepted_count = 0

        # set accepted=desired if withing "master ready" percent so that we don't try to collect
        # single subs for channels that have enough data
        # NOTE set to 2x desired so any % over 100% in scheduler does _not_ kick in anymore
        if new_accepted_count < desired_count and new_accepted_count/desired_count > common.MASTER_READY_PERCENT:
            new_accepted_count = desired_count*2

        if user_debug:
            print(f"DEBUG: {profile} | {targetname} | {filtername} | {panelname} --> {new_accepted_count} (was {old_accepted_count})")

        # note new_accepted_count cannot be None since we set to 0 in that case
        if new_accepted_count != old_accepted_count:
            print(f"update accepted count: {targetname}, panel={panelname}, filter={filtername}: {old_accepted_count} --> {new_accepted_count}")
            if not user_dryrun:
                c_ts.execute(f"""update exposureplan
                                set accepted={new_accepted_count},
                                acquired={new_accepted_count}
                                where id={exposureplan_id};
                            """)

        # did the project state change?
        if new_project_state != project_state:
            print(f"update project state: {project_name}/{targetname}: {project_state} --> {new_project_state}")
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
    if conn_ap is not None:
        conn_ap.close()
    print(e)
    traceback.print_exc()

