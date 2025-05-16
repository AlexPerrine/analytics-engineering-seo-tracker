import os
import datetime
import pyarrow as pa
from dotenv import load_dotenv
from secrets.aws_secret_manager import get_secret
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import *
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from scripts.fetch_ga4_data import fetch_g4a_data

load_dotenv()
print("Starting GA4 ingestion script...")

def ingest_g4a_data():
    # Getting GA4 data for May
    rows = fetch_g4a_data(start_date='2025-05-01', end_date='2025-05-14')

    if not rows:
        print('No Google Analytics data found for selected range. Skipping ingestion')
        return
    
    schema=Schema(
        NestedField.required(1,"date",DateType()),
        NestedField.required(2,"page_path",StringType()),
        NestedField.required(3,"landing_page",StringType()),
        NestedField.required(4,"device_category",StringType()),
        NestedField.required(5,"os_family",StringType()),
        NestedField.required(6,"session_source",StringType()),
        NestedField.required(7,"session_medium",StringType()),
        NestedField.required(8,"channel_grouping",StringType()),
        NestedField.required(9,"city",StringType()),


    )