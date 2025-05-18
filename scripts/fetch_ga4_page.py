import os
import datetime
import logging
from dotenv import load_dotenv
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.oauth2 import service_account


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def fetch_ga4_page_data(start_date: str, end_date: str):
    """
    Fetches Google Analytics pageview data between start_date and end_date inclusive.
    Adds a 'loaded_date' field with the date it was pulled from GA.
    Args:
        start_date (str): 'YYYY-MM-DD'
        end_date (str): 'YYYY-MM-DD'
    Returns:
        List[dict]: List of row records
    """

    # Override with environment variables if present (e.g., from Airflow)
    start_date = os.getenv("GA4_START_DATE", start_date)
    end_date = os.getenv("GA4_END_DATE", end_date)

    # Configuration
    KEY_PATH = os.path.abspath("project_secrets/g4a_service_key.json")
    PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")

    # Authorization
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = BetaAnalyticsDataClient(credentials=credentials)

    # Dimensions and metrics
    dimensions = [
        Dimension(name="date"),
        Dimension(name="pagePath"),
        Dimension(name="landingPage"),
        Dimension(name="landingPagePlusQueryString"),
        Dimension(name="pageTitle"),
        Dimension(name="sessionSource"),
        Dimension(name="sessionMedium"),
        Dimension(name="pageReferrer")
    ]

    metrics = [
        Metric(name="screenPageViews"),
        Metric(name="sessions"),
        Metric(name="totalUsers"),
        Metric(name="newUsers"),
        Metric(name="userEngagementDuration")
    ]

    request = RunReportRequest(
        property=f"properties/{PROPERTY_ID}",
        dimensions=dimensions,
        metrics=metrics,
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)]
    )

    response = client.run_report(request)
    loaded_date = datetime.date.today() - datetime.timedelta(days=1)

    rows = []
    for row in response.rows:
        record = {dim.name: row.dimension_values[i].value for i, dim in enumerate(dimensions)}
        for j, met in enumerate(metrics):
            val = row.metric_values[j].value
            try:
                if val.isdigit():
                    record[met.name] = int(val)
                else:
                    record[met.name] = float(val)
            except (ValueError, TypeError):
                record[met.name] = None
        record["date"] = datetime.datetime.strptime(record["date"], "%Y%m%d").date()
        record["loaded_date"] = loaded_date
        rows.append(record)

    logger.info(f"Page pulled {len(rows)} rows from GA4 for {start_date} to {end_date}")
    return rows
