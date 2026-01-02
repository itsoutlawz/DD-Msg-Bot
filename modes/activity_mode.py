import os

from config import BASE_URL
from modes.inbox_mode import _run_inbox_like


def run_activity_mode(driver):
    _run_inbox_like(
        driver,
        page_url=f"{BASE_URL}/inbox/activity/#section0",
        sheet_name="Activity",
        section="Activity",
        export_filename="activity.csv",
    )
