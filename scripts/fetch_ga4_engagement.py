import os
import datetime
import logging
import datetime
from pprint import pprint
from dotenv import load_dotenv
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.oauth2 import service_account


load_dotenv()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_g4a_engagement_data(start_date: str, end_date: str):
        """
        Fetches Google Analytics engagement data between start_date and end_date inclusive
        Adds a 'loaded_date' field with the date it was pulled from GA
        Args:
            start_date (str): 'YYYY-MM-DD'
            end_date (str): 'YYYY-MM-DD'
        Returns:
            List[dict]: List of row records
        """

        # Configuration
        KEY_PATH = os.path.abspath("project_secrets/g4a_service_key.json")
        PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")

        # Authorization
        credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
        client = BetaAnalyticsDataClient(credentials=credentials)

        # Defining the required dimensions and metrics
        dimensions=[
            Dimension(name="date"),
            Dimension(name="pagePath"),
            Dimension(name="pageTitle"),
            Dimension(name="dateHourMinute"),
            Dimension(name="eventName"),
            Dimension(name="sessionSource"),
            Dimension(name="sessionMedium")
            ]

        metrics=[
            Metric(name="eventCount"),
            Metric(name="engagedSessions"),
            Metric(name="bounceRate"),
            Metric(name="averageSessionDuration"),
            Metric(name="userEngagementDuration")
            ]
        
        # Gets data from certain date range
        request = RunReportRequest(
              property=f"properties/{PROPERTY_ID}",
              dimensions=dimensions,
              metrics=metrics,
              date_ranges=[DateRange(start_date=start_date, end_date=end_date)]
            )

        # Get the data from GA
        response = client.run_report(request)
        loaded_date = str(datetime.date.today())

        rows=[]
        for row in response.rows:
            record={dim.name:row.dimension_values[i].value for i, dim in enumerate(dimensions)}
            for j,met in enumerate(metrics):
                val=row.metric_values[j].value
                record[met.name]=float(val) if '.' in val else int(val)
            record["date"] = datetime.datetime.strptime(record["date"], "%Y%m%d").date()
            record["loaded_date"] = datetime.date.today() - datetime.timedelta(days=1)
            rows.append(record)
                  

        logger.info(f"User pulled {len(rows)} rows from GA4 for {start_date} to {end_date}")
        return rows
