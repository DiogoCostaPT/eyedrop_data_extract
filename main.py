
"""
EyeDrop
data ingestion, procesing and web extraction
"""

from datetime import datetime, timedelta
import data_webExtract as WextSnirh
import time
import os
import shutil

historic_dir = os.path.join(os.getcwd(), "data/data_autoExtract/historic")
last_week_dir = os.path.join(os.getcwd(), "data/data_autoExtract/last_week")

# Weekly loop automated
week_secs_step = 60 * 60 * 24 * 7

#for it in range(1000):
for it in range(1):

    #############################################
    # COMMENT OUT WHEN CODE IS READY FOR DEPLOYMENT
    # Move last week's data to historic
    #lastweek_dataFiles = os.listdir(last_week_dir)
    #for file in lastweek_dataFiles:
    #    if (file.endswith('.npy') or file.endswith('.log')):
    #        shutil.move(os.path.join(last_week_dir, file), os.path.join(historic_dir, file))
    #############################################

    # Last week
    #lastWeek = datetime.today() - timedelta(days=7)
    lastWeek = datetime(1990, 1, 1)
    lastWeek_str = lastWeek.strftime("%d/%m/%Y")
    # Today
    today_str = datetime.today().strftime("%d/%m/%Y")
    # Next week (subsequent extraction)
    nextWeek = datetime.today() + timedelta(days=7)
    nextWeek_str = nextWeek.strftime("%d/%m/%Y")

    # Set period of extraction for now
    PERIOD = {
        "tmin": lastWeek_str,
        "tmax": today_str
    }

    print("##################################################")
    print(f"Extraction # {str(it)})")
    print(f"-> Week period: {lastWeek_str} to {today_str}")
    print("##################################################")

    DATABASE_list = ["infreq", "auto"]

    # Extract Data
    # 1) auto
    # 2) infreq
    for i in range(2):

        DATABASE = DATABASE_list[i]

        WextSnirh.extract_snirh_main(
            PERIOD,
            DATABASE,
            last_week_dir)

    print("##################################################")
    print(f"Next Extraction on {nextWeek_str})")
    # Wait a week
    time.sleep(week_secs_step)  # Seconds
    # Last week







