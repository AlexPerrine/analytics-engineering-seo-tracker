import os
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Create a connection to Snowflake
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE")
    )
    return conn

def fetch_unique_page_types():
    query = "SELECT DISTINCT pagetype FROM fact_blog_engagement"
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    page_types = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return page_types

def fetch_kpis(start_date, end_date, pagetype=None):
    query = f"""
    SELECT
        SUM(total_pageviews) AS total_pageviews,
        SUM(total_engaged_sessions) AS total_engaged_sessions,
        AVG(avg_engagement_duration) AS avg_engagement_duration,
        AVG(bounce_rate) AS avg_bounce_rate
    FROM fact_blog_engagement
    WHERE pageview_date BETWEEN '{start_date}' AND '{end_date}'
    """
    if pagetype and pagetype != "All Page Types":
        query += f" AND pagetype = '{pagetype}'"

    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    cursor.close()
    conn.close()

    if result:
        kpis = {
            "total_pageviews": result[0] or 0,
            "total_engaged_sessions": result[1] or 0,
            "avg_engagement_duration": result[2] or 0.0,
        }
        return kpis
    return {}

def fetch_pageviews_by_page(start_date, end_date, pagetype=None):
    query=f"""
    SELECT
        pageview_date,
        pagetitle,
        SUM(total_pageviews) as total_pageviews
    FROM fact_blog_engagement
    WHERE pageview_date BETWEEN '{start_date}' AND '{end_date}'
        AND pagetype = '{pagetype}'
    GROUP BY pageview_date, pagetitle
    ORDER BY pageview_date
    """
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(data, columns = ['pageview_date','pagetitle','total_pageviews'])
    return df

# Streamlit app layout
st.set_page_config(page_title="Blog & SEO Dashboard", layout="wide")
st.title("Blog & SEO Performance Tracker")

# Sidebar and Filters
st.sidebar.header("Filters")
start_date = st.sidebar.date_input("Start Date", pd.to_datetime("2025-01-01"))
end_date = st.sidebar.date_input("End Date", pd.to_datetime("2025-05-01"))
page_types = fetch_unique_page_types()
page_type_filter = st.sidebar.selectbox("Select Page Type", ["All Page Types"] + page_types)

# Show KPIs
kpis = fetch_kpis(start_date, end_date, page_type_filter)
col1, col2, col3 = st.columns(3)
col1.metric("Total Pageviews", f"{kpis.get('total_pageviews', 0):,}")
col2.metric("Engaged Sessions", f"{kpis.get('total_engaged_sessions', 0):,}")
col3.metric("Avg. Engagement Duration", f"{kpis.get('avg_engagement_duration', 0):.2f} sec")

# Pageviews over time
st.header(f"Pageviews Over Time by {page_type_filter}")
if page_type_filter == "All Page Types":
    st.info("Too many pages to show in a single chart! Please choose a specific page type from the dropdown to see detailed trends.")
else:
    df_linechart = fetch_pageviews_by_page(start_date, end_date, page_type_filter)
    if not df_linechart.empty:
        import plotly.express as px
        fig = px.line(
            df_linechart,
            x="pageview_date",
            y="total_pageviews",
            color="pagetitle",
            title="Pageviews Over Time by Page",
            markers=True
        )
        fig.update_layout(xaxis_title="Date", yaxis_title="Total Pageviews", legend_title="Page Title")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data for the page type in the selected date range.")
