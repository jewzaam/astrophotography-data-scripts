"""
This script launches a new instance of PixInsight with an incremented instance number.
It checks for existing instances and calculates the next available instance number.
"""

import subprocess
import psutil

max_n=0

# find maximum used "n" for PixInsight
for p in psutil.process_iter():
    if p.name() == "PixInsight.exe":
        try:
            for arg in p.cmdline():
                if arg.startswith("-n=") or arg.startswith("--new="):
                    n=int(arg[-arg.index("=")+1:])
                    if n > max_n:
                        max_n = n
        except:
            pass

print(f"INFO: max 'n' found is {max_n}, starting {max_n+1}")
p = subprocess.Popen([r'C:\Program Files\PixInsight\bin\PixInsight.exe', f"-n={max_n+1}"])
