import os
import subprocess
import sys
import time
from datetime import datetime, timedelta

# -------------------------
# Periodically compute average over last 10 minutes
# -------------------------
try:
    while True:
        ten_minutes_ago = datetime.now() - timedelta(minutes=10)
        ## Fetch the data from the choosen source (to be implemented)
        
        avg_temp = 0 ## replace with actual values
        if avg_temp is not None:
            print(f"{datetime.now()} - Average temperature last 10 minutes: {avg_temp:.2f} °C")
        else:
            print(f"{datetime.now()} - No data in last 10 minutes.")
        time.sleep(600)  # every 10 minutes
except KeyboardInterrupt:
    print("Stopped consuming data.")
finally:
    print("Exiting.")
