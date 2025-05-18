# scripts/ga4_page_checks.py

import os
from datetime import timedelta
from scripts.fetch_ga4_page import fetch_g4a_page_data

def check_ga4_has_data(**context):
    execution_date = context["execution_date"]
    date_str = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    rows = fetch_g4a_page_data(start_date=date_str, end_date=date_str)

    if not rows:
        raise ValueError(f"No GA4 data returned for {date_str}")

    # Push rows to XCom for use in schema check
    context["ti"].xcom_push(key="ga4_page_rows", value=rows)

    print(f"GA4 API returned {len(rows)} rows.")


def check_ga4_schema(**context):
    required_keys = {
        "date", "pagePath", "landingPage", "landingPagePlusQueryString", "pageTitle",
        "sessionSource", "sessionMedium", "pageReferrer",
        "screenPageViews", "sessions", "totalUsers", "newUsers", "userEngagementDuration"
    }

    rows = context["ti"].xcom_pull(key="ga4_page_rows", task_ids="check_ga4_has_data")

    for i, row in enumerate(rows):
        missing = required_keys - row.keys()
        if missing:
            raise KeyError(f"🚨 Row {i} is missing keys: {missing}")
    print(f"✅ All rows passed schema validation.")
