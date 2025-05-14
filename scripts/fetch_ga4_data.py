import os
from dotenv import load_dotenv
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import RunReportRequest, DateRange, Dimension, Metric
from google.oauth2 import service_account

load_dotenv()

# Configuration
KEY_PATH = os.path.abspath("secrets/g4a_service_key.json")
PROPERTY_ID = os.getenv("GA4_PROPERTY_ID")

# Authorization
credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = BetaAnalyticsDataClient(credentials=credentials)

# Basic Test Request
request = RunReportRequest(
    property=f"properties/{PROPERTY_ID}",
    dimensions=[Dimension(name='date')],
    metrics=[Metric(name="screenPageViews")],
    date_ranges=[DateRange(start_date="3daysAgo", end_date="today")]
)

# Run and Print
try:
    response = client.run_report(request)
    print("Success! Returned rows:\n")
    for row in response.rows:
        date = row.dimension_values[0].value
        views = row.metric_values[0].value
        print(f"{date} - {views} page views")
except Exception as e:
    print("Error accessing GA4 data:", str(e))