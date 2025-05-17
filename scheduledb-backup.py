"""
This script creates a backup of the NINA Scheduler database.
It uses a function from the `common` module to perform the backup.
"""

import common

common.backup_scheduler_database()