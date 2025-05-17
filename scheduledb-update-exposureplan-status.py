"""
This script updates the exposure plan status in the NINA Scheduler database.
It uses environment variables to locate the database file.
"""

import sqlite3
import traceback
import os

# originally defined in common, but to reduce dependencies pulling it out.
def replace_env_vars(input:str):
    """
    Replace environment variables in a given string with their actual values.

    Args:
        input (str): The input string containing environment variable placeholders (e.g., %VAR_NAME%).

    Returns:
        str: The input string with environment variables replaced by their corresponding values.
    """
    output = input
    output_uc = input.upper()
    for e in os.environ.items():
        k = f"%{e[0]}%"
        v = e[1]
        while k in output_uc:
            # env vars are uppercase but ignore case when used.  use slices to do replacing.
            k_start = output_uc.find(k)
            output = output[:k_start] + v + output[k_start + len(k):]
            output_uc = output.upper()
    return output

try:
    """
    Main script execution to update the exposure plan status in the NINA Scheduler database.

    This script connects to the SQLite database, updates the `accepted` column in the `exposureplan` table
    to match the `acquired` column, and commits the changes. It uses environment variables to locate the database file.

    Exceptions:
        sqlite3.Error: Handles SQLite errors and ensures the database connection is closed properly.
    """
    conn_ts = sqlite3.connect(replace_env_vars(r"%LocalAppData%\NINA\SchedulerPlugin\schedulerdb.sqlite"))
    c_ts = conn_ts.cursor()

    c_ts.execute("update exposureplan set accepted=acquired;")
    conn_ts.commit()

except sqlite3.Error as e:
    if conn_ts is not None:
        conn_ts.close()
    print(e)
    traceback.print_exc()
