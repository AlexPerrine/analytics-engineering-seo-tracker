import os
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv
import datetime

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
        pagetype,
        SUM(total_pageviews) as total_pageviews
    FROM fact_blog_engagement
    WHERE pageview_date BETWEEN '{start_date}' AND '{end_date}'
        AND pagetype = '{pagetype}'
    GROUP BY pageview_date, pagetype
    ORDER BY pageview_date
    """
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(data, columns = ['pageview_date','pagetype','total_pageviews'])
    return df

def fetch_eventtimebucket_counts(start_date, end_date, pagetype=None):
    # no user id so unique first visit is a proxy for unique session
    query=f"""
    SELECT
        e.eventtimebucket,
        COUNT(DISTINCT e.session_channel || e.event_date || e.pagepath) AS unique_first_visits
    FROM fact_event_engagement as e
    JOIN fact_blog_engagement b
        ON e.pagepath = b.pagepath
    WHERE e.event_date BETWEEN '{start_date}' AND '{end_date}'
    """
    if pagetype and pagetype != "All Page Types":
        query += f" AND b.pagetype = '{pagetype}'"

    query += """
    GROUP BY e.eventtimebucket
    ORDER BY unique_first_visits DESC
    """

    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(data, columns=['eventtimebucket', 'unique_first_visits'])
    return df

def fetch_referrer_platform_counts(start_date, end_date, pagetype=None):
    query=f"""
    SELECT
        referrer_platform,
        SUM(total_pageviews) AS total_pageviews
    FROM fact_blog_engagement
    WHERE pageview_date BETWEEN '{start_date}' AND '{end_date}'
"""
    if pagetype and pagetype != "All Page Types":
        query += f" AND pagetype = '{pagetype}'"
    
    query += """
    GROUP BY referrer_platform
    ORDER BY total_pageviews DESC
    """
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    data = cursor.fetchall()
    cursor.close()
    conn.close()
    df = pd.DataFrame(data, columns=['referrer_platform', 'total_pageviews'])
    return df

# Part of the DE Bootcamp, need to get lat/long for each location and new locations from API
# def fetch_location_data(start_date, end_date):
#     query=f"""
#         SELECT
#         city,
#         region,
#         COUNT(DISTINCT CONCAT(city, region, session_date)) AS total_visits
#     FROM fact_user_sessions
#     WHERE session_date BETWEEN '{start_date}' AND '{end_date}'
#     GROUP BY city, region
#     ORDER BY total_visits DESC
# """

#     conn = get_snowflake_connection()
#     cursor = conn.cursor()
#     cursor.execute(query)
#     data = cursor.fetchall()
#     cursor.close()
#     conn.close()
#     df = pd.DataFrame(data, columns=['city', 'region', 'total_visits'])
#     return df


# Streamlit app layout
st.set_page_config(page_title="Site Performance & User Behavior Insights", layout="wide")
st.title("Site Performance & User Behavior Insights")

# Sidebar and Filters
st.sidebar.header("Filters")
start_date = st.sidebar.date_input("Start Date", pd.to_datetime("2025-01-01"))
end_date = st.sidebar.date_input("End Date", pd.to_datetime(datetime.date.today()))
page_types = fetch_unique_page_types()
page_type_filter = st.sidebar.selectbox("Select Page Type", ["All Page Types"] + page_types)

# Show KPIs
kpis = fetch_kpis(start_date, end_date, page_type_filter)
col1, col2, col3 = st.columns(3)
col1.metric("Total Pageviews", f"{kpis.get('total_pageviews', 0):,}")
col2.metric("Engaged Sessions", f"{kpis.get('total_engaged_sessions', 0):,}")
col3.metric("Avg. Engagement Duration", f"{kpis.get('avg_engagement_duration', 0):.2f} sec")



# Create 2 columns
col1, col2 = st.columns(2)

# Left Column: Event Time Buckets
with col1:
    st.subheader("When Do People Visit? (by Time Bucket)")
    df_eventtimebucket = fetch_eventtimebucket_counts(start_date, end_date, page_type_filter)
    if not df_eventtimebucket.empty:
        import plotly.express as px
        fig1 = px.bar(
            df_eventtimebucket,
            x="eventtimebucket",
            y="unique_first_visits",
            title="Visit Counts by Time Bucket",
            text_auto=True
        )
        fig1.update_layout(
            xaxis_title="Time of Day",
            yaxis_title="Total Visits",
            template="simple_white"
        )
        st.plotly_chart(fig1, use_container_width=True)
        st.caption(
            "ℹ️ *Note: This chart uses a proxy to approximate unique first visits based on "
            "`session_channel + event_date + pagepath`. It may not perfectly match true "
            "session-level data if a more precise `session_id` is unavailable.*"
        )
    else:
        st.warning("No data for the page type in the selected date range.")

# Right Column: Referrer Platform
with col2:
    st.subheader("How Do People Find Us? (by Referrer Platform)")
    df_referrer_platform = fetch_referrer_platform_counts(start_date, end_date, page_type_filter)
    if not df_referrer_platform.empty:
        fig2 = px.bar(
            df_referrer_platform,
            x="referrer_platform",
            y="total_pageviews",
            title="Referrer Platforms",
            text_auto=True
        )
        fig2.update_layout(
            xaxis_title="Referrer Platform",
            yaxis_title="Total Visits",
            template="simple_white"
        )
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.warning("No referrer data for this page type in the selected date range.")


# Pageviews over time
st.header(f"Pageviews Over Time by {page_type_filter}")
if page_type_filter == "All Page Types":
    st.info("Too many pages to show in a single chart! Please choose a specific page type from the dropdown to see detailed trends.")
else:
    df_linechart = fetch_pageviews_by_page(start_date, end_date, page_type_filter)
    if not df_linechart.empty:
        df_linechart = df_linechart.sort_values(by="pageview_date")
        df_linechart["rolling_avg"] = (
            df_linechart
            .groupby("pagetype")["total_pageviews"]
            .transform(lambda x: x.rolling(window=7, min_periods=1).mean())
            )
        import plotly.express as px
        import plotly.graph_objects as go
        fig = go.Figure()

        # Plot original pageviews with light blue lines
        for page in df_linechart["pagetype"].unique():
            df_page = df_linechart[df_linechart["pagetype"] == page]
            fig.add_trace(go.Scatter(
                x=df_page["pageview_date"],
                y=df_page["total_pageviews"],
                mode="lines+markers",
                name=page,
                line=dict(color="lightblue", width=2),
                marker=dict(color="lightblue")
            ))

        for page in df_linechart["pagetype"].unique():
            df_page = df_linechart[df_linechart["pagetype"] == page]
            fig.add_scatter(
                x=df_page["pageview_date"],
                y=df_page["rolling_avg"],
                mode="lines",
                name=f"{page} (7-day avg)",
                line=dict(color="black", dash="dash") 
                )
        fig.update_layout(xaxis_title="Date", 
                          yaxis_title="Total Pageviews", 
                          legend_title="Page Type")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data for the page type in the selected date range.")




# Will be part of the DE capstone as I will need to get the lat/long for each location and update automatically when a new location comes in.
# # Fetch data first
# st.header("Where Are People Visiting From?")
# df_locations = fetch_location_data(start_date, end_date)

# if not df_locations.empty:
#     map_mode = st.radio("Map Mode", ["Bubble Map", "Heatmap"])
#     # Rest of the map visualization code...
#     import plotly.express as px

#     # For demonstration, using dummy lat/lon columns
#     # Ideally, you would geocode city/region to get lat/lon
#     # Here's a placeholder to avoid errors:
#     df_locations["latitude"] = 44.9778  # Minneapolis
#     df_locations["longitude"] = -93.2650

# if map_mode == "Bubble Map":
#     fig = px.scatter_geo(
#         df_locations,
#         lat="latitude",
#         lon="longitude",
#         hover_name="city",
#         size="total_visits",
#         color="total_visits",
#         projection="albers usa",
#         title="Visitor Locations (Bubble Map)"
#     )
#     fig.update_geos(
#         scope="usa",
#         showcountries=True,
#         countrycolor="Black",
#         showland=True,
#         landcolor="LightGray"
#     )
#     fig.update_layout(template="simple_white")
#     st.plotly_chart(fig, use_container_width=True)

# elif map_mode == "Heatmap":
#     fig = px.density_mapbox(
#         df_locations,
#         lat="latitude",
#         lon="longitude",
#         z="total_visits",
#         radius=20,
#         center=dict(lat=38, lon=-94),
#         zoom=3,
#         mapbox_style="carto-positron",
#         title="Visitor Locations (Heatmap)"
#     )
#     fig.update_layout(margin={"r":0,"t":40,"l":0,"b":0})
#     st.plotly_chart(fig, use_container_width=True)
# else:
#     st.warning("No location data for this date range.")
