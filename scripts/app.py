import os
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create a connection to Snowflake
conn = snowflake.connector.connect(
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
    database=os.getenv("SNOWFLAKE_DATABASE"),
    schema=os.getenv("SNOWFLAKE_SCHEMA"),
    role=os.getenv("SNOWFLAKE_ROLE")  # you can remove this line if you don't want to specify a role
)

# Query your data mart (limit to avoid pulling too much data initially)
query = """
    SELECT
        event_date,
        pagepath,
        pagetitle,
        eventtype,
        session_channel,
        eventtimebucket,
        total_events,
        total_engaged_sessions,
        avg_engagement_duration,
        bounce_rate
    FROM alexperrine.fact_event_engagement
    LIMIT 100
"""


# Load data into a DataFrame
df = pd.read_sql(query, conn)

# Close the Snowflake connection
conn.close()

# Streamlit app layout
st.title("Kristen Elizabeth Photography Engagement Dashboard")
st.write("Here’s a preview of your **fact_event_engagement** data:")

df.columns = df.columns.str.lower()

# Display the data table
st.dataframe(df)

# Example visualization: Events by time bucket
st.subheader("Total Events by Time Bucket")
events_by_time = df.groupby("eventtimebucket")["total_events"].sum().reset_index()
st.bar_chart(events_by_time.set_index("eventtimebucket"))

# Optional: add other visualizations here (e.g., top pages, session channels)
