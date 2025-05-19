# scripts/ga4_page_checks.py

import os
from datetime import timedelta
from scripts.fetch_ga4_page import fetch_ga4_page_data
from scripts.fetch_ga4_user import fetch_ga4_user_data

def check_ga4_page_data(**context):
    execution_date = context["execution_date"]
    date_str = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    rows = fetch_ga4_page_data(start_date=date_str, end_date=date_str)

    if not rows:
        raise ValueError(f"No GA4 data returned for {date_str}")

    # Push rows to XCom for use in schema check
    context["ti"].xcom_push(key="ga4_page_rows", value=rows)

    print(f"GA4 API returned {len(rows)} rows.")


def check_ga4_page_schema(**context):
    required_keys = {
        "date", "pagePath", "landingPage", "landingPagePlusQueryString", "pageTitle",
        "sessionSource", "sessionMedium", "pageReferrer",
        "screenPageViews", "sessions", "totalUsers", "newUsers", "userEngagementDuration"
    }

    # Must match the task_id from your DAG
    rows = context["ti"].xcom_pull(key="ga4_page_rows", task_ids="check_ga4_has_data")

    if not rows:
        raise ValueError("No page data available in XCom.")

    for i, row in enumerate(rows):
        missing = required_keys - row.keys()
        if missing:
            raise KeyError(f"Row {i} is missing keys: {missing}")

    print(f"All {len(rows)} rows passed schema validation.")


    

def check_ga4_user_data(**context):
    from datetime import timedelta
    from scripts.fetch_ga4_user import fetch_ga4_user_data

    execution_date = context["execution_date"]
    date_str = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    rows = fetch_ga4_user_data(start_date=date_str, end_date=date_str)

    if not rows:
        raise ValueError(f"No GA4 user data returned for {date_str}")

    # Push to XCom for downstream schema check
    context["ti"].xcom_push(key="ga4_user_rows", value=rows)

    print(f"GA4 API returned {len(rows)} user rows.")


def check_ga4_user_schema(**context):
    required_keys = {
        "date", "city", "region", "deviceCategory", "operatingSystem",
        "browser", "firstSessionDate", "searchTerm", "platform", "totalUsers",
        "activeUsers", "engagementRate", "sessionsPerUser", "loaded_date"
    }

    rows = context["ti"].xcom_pull(key="ga4_user_rows", task_ids="check_ga4_user_data")

    if not rows:
        raise ValueError("No user data found in XCom.")

    for i, row in enumerate(rows):
        missing = required_keys - row.keys()
        if missing:
            raise KeyError(f"Row {i} is missing keys: {missing}")

    print(f"All {len(rows)} rows passed user schema validation.")



def check_ga4_engagement_data(**context):
    from datetime import timedelta
    from scripts.fetch_ga4_engagement import fetch_ga4_engagement_data

    execution_date = context["execution_date"]
    date_str = (execution_date - timedelta(days=1)).strftime('%Y-%m-%d')

    rows = fetch_ga4_engagement_data(start_date=date_str, end_date=date_str)

    if not rows:
        raise ValueError(f"No GA4 user data returned for {date_str}")

    # Push to XCom for downstream schema check
    context["ti"].xcom_push(key="ga4_engagement_rows", value=rows)

    print(f"GA4 API returned {len(rows)} user rows.")


def check_ga4_engagement_schema(**context):
    required_keys = {
        "date", "pagePath", "pageTitle", "dateHourMinute", 
        "eventName", "sessionSource", "sessionMedium", "eventCount", 
        "engagedSessions", "bounceRate", "averageSessionDuration", "userEngagementDuration",
        "loaded_date"
    }

    rows = context["ti"].xcom_pull(key="ga4_engagement_rows", task_ids="check_ga4_engagement_data")

    if not rows:
        raise ValueError("No user data found in XCom.")

    for i, row in enumerate(rows):
        missing = required_keys - row.keys()
        if missing:
            raise KeyError(f"Row {i} is missing keys: {missing}")

    print(f"All {len(rows)} rows passed user schema validation.")