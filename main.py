
"""
EyeDrop
data ingestion, procesing and web extraction
"""

from datetime import datetime, timedelta
import data_webExtract as WextSnirh
import time

# Weekly loop automated
week_secs_step = 60 * 60 * 24 * 7

#for it in range(1000):
for it in range(1):

    lastWeek = datetime.today() - timedelta(days=8000)
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

    DATABASE_list = ["auto", "infreq"]

    # Extract Data
    # 1) auto
    # 2) infreq
    for i in range(2):

        DATABASE = DATABASE_list[i]
        WextSnirh.extract_snirh_main(
            PERIOD,
            DATABASE)

    print("##################################################")
    print(f"Next Extraction on {nextWeek_str})")
    # Wait a week
    time.sleep(week_secs_step)  # Seconds
    # Last week







