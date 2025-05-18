import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import datetime
import pyarrow as pa
from dotenv import load_dotenv
from project_secrets.aws_secret_manager import get_secret
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema
from pyiceberg.types import *
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform
from scripts.fetch_ga4_page import fetch_g4a_page_data

load_dotenv()
print("Starting GA4 Pageviews ingestion...")

def ingest_g4a_page_data(start_date, end_date):
    """
    Loads GA4 page-level metrics from the Reporting API and writes them to Iceberg.

    Creates the table if it doesn't exist, appends new data as a PyArrow table,
    and tags each row with source metadata. Used for manual backfills or Airflow DAGs.
    """

    # Getting GA4 data
    rows = fetch_g4a_page_data(start_date=start_date, end_date=end_date)

    if not rows:
        print('No Google Analytics page data found for selected range. Skipping ingestion')
        return
    
    for row in rows:
        row['source_system'] = "api"

    
    
    schema=Schema(
        NestedField(field_id=1, name="date", field_type=DateType(), required=True),
        NestedField(field_id=2, name="pagePath",field_type=StringType(),required=True),
        NestedField(field_id=3, name="landingPage", field_type=StringType(), required=True),
        NestedField(field_id=4, name="landingPagePlusQueryString",field_type=StringType(),required=True),
        NestedField(field_id=5, name="pageTitle",field_type=StringType(),required=True),
        NestedField(field_id=6, name="sessionSource",field_type=StringType(),required=True),
        NestedField(field_id=7, name="sessionMedium", field_type=StringType(),required=True),
        NestedField(field_id=8, name="pageReferrer",field_type=StringType(),required=True),
        NestedField(field_id=9, name="screenPageViews", field_type=IntegerType(),required=True),
        NestedField(field_id=10, name="sessions", field_type=IntegerType(), required=True),
        NestedField(field_id=11, name="totalUsers", field_type=IntegerType(), required=True),
        NestedField(field_id=12, name="newUsers", field_type=IntegerType(), required=True),
        NestedField(field_id=13, name="userEngagementDuration", field_type=FloatType(), required=True),
        NestedField(field_id=14, name="loaded_date", field_type=DateType(), required=True),
        NestedField(field_id=15, name="source_system", field_type=StringType(), required=True),
    )

    spec=PartitionSpec(
        fields=[PartitionField(source_id=1, field_id=1001, name="date_day", transform=DayTransform())]
    )

    catalog=load_catalog(
        name="academy",
        type="rest",
        uri="https://api.tabular.io/ws",
        warehouse=get_secret("CATALOG_NAME"),
        credential=get_secret("TABULAR_CREDENTIAL")
    )

    iceberg_table=catalog.create_table_if_not_exists(
        identifier="alexperrine.raw_ga_pageviews",
        schema=schema,
        partition_spec=spec
    )

    print("Iceberg table created or already exists")

    arrow_schema = iceberg_table.schema().as_arrow()
    pa_table=pa.Table.from_pylist(rows, schema=arrow_schema)
    iceberg_table.append(pa_table)
    print(f"Ingested {len(rows)} rows into raw_ga_pageviews")

    table=catalog.load_table("alexperrine.raw_ga_pageviews")
    snapshot=table.current_snapshot()
    if "audit" not in table.refs() and snapshot:
        table.manage_snapshots().create_branch(snapshot.snapshot_id, "audit")
        print(f"Created 'audit' branch at snapshotID {snapshot.snapshot_id}")
    else:
        print(f"'audit' branch already exists or no snapshot found")

if __name__ == "__main__":
    start = os.getenv("START_DATE")
    end = os.getenv("END_DATE")
    ingest_g4a_page_data(start, end)