import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
import streamlit as st

from dotenv import load_dotenv

from queries import (
    get_kpi_data,
    get_daily_trips,
    get_payment_analysis,
    get_vendor_analysis,
    get_location_analysis,
)


# Load environment variables
load_dotenv("../.env")


# Configure Streamlit page
st.set_page_config(
    page_title="NYC Taxi Trip Analysis",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="collapsed",
)


# Custom dashboard styling
st.markdown(
    """
    <style>

    /* Main page */
    .main {
        padding-top: 1rem;
    }

    /* Header */
    .dashboard-header {
        background: linear-gradient(
            135deg,
            #0f172a,
            #172554
        );
        padding: 30px 35px;
        border-radius: 18px;
        margin-bottom: 25px;
        border: 1px solid #263449;
    }

    .dashboard-title {
        font-size: 42px;
        font-weight: 800;
        color: white;
        margin-bottom: 5px;
    }

    .dashboard-subtitle {
        font-size: 17px;
        color: #b8c5d6;
    }

    /* KPI cards */
    .kpi-card {
        background: #111827;
        border: 1px solid #263449;
        border-radius: 15px;
        padding: 20px;
        min-height: 135px;
    }

    .kpi-title {
        color: #aebbd0;
        font-size: 15px;
        font-weight: 600;
        margin-bottom: 10px;
    }

    .kpi-value {
        color: white;
        font-size: 30px;
        font-weight: 750;
    }

    /* Section titles */
    .section-title {
        font-size: 25px;
        font-weight: 750;
        color: white;
        margin-top: 15px;
        margin-bottom: 10px;
    }

    /* Small description */
    .section-description {
        color: #9caabd;
        font-size: 14px;
        margin-bottom: 15px;
    }

    /* Footer */
    .footer {
        text-align: center;
        color: #7f8da3;
        padding: 30px 0 10px 0;
        font-size: 13px;
    }

    </style>
    """,
    unsafe_allow_html=True,
)


# Create Snowflake connection
def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database="TAXI_DATA",
        schema="SILVER",
    )


# Display dashboard header
st.markdown(
    """
    <div class="dashboard-header">
        <div class="dashboard-title">🚕 NYC Taxi Trip Analysis</div>
        <div class="dashboard-subtitle">
            Trip Patterns, Revenue Insights and Operational Analysis
        </div>
    </div>
    """,
    unsafe_allow_html=True
)


# Load data from Snowflake
try:
    conn = get_snowflake_connection()

    kpi_data = get_kpi_data(conn)
    daily_data = get_daily_trips(conn)
    payment_data = get_payment_analysis(conn)
    vendor_data = get_vendor_analysis(conn)
    location_data = get_location_analysis(conn)

    conn.close()

except Exception as e:
    st.error(f"❌ Snowflake connection/data error: {e}")
    st.stop()


# Display KPI section
if not kpi_data.empty:

    data = kpi_data.iloc[0]

    st.markdown(
        '<div class="section-title">📊 Key Performance Indicators</div>',
        unsafe_allow_html=True,
    )

    st.markdown(
        '<div class="section-description">'
        'Overall performance of the taxi trip dataset'
        '</div>',
        unsafe_allow_html=True,
    )

    # Display KPI cards
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-title">🚕 Total Trips</div>
                <div class="kpi-value">
                    {int(data["TOTAL_TRIPS"]):,}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col2:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-title">💰 Total Revenue</div>
                <div class="kpi-value">
                    ${data["TOTAL_REVENUE"]:,.2f}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col3:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-title">💵 Average Fare</div>
                <div class="kpi-value">
                    ${data["AVG_FARE"]:,.2f}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.write("")

    col4, col5, col6 = st.columns(3)

    with col4:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-title">📏 Average Trip Distance</div>
                <div class="kpi-value">
                    {data["AVG_TRIP_DISTANCE"]:,.2f} mi
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col5:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-title">⏱️ Average Trip Duration</div>
                <div class="kpi-value">
                    {data["AVG_TRIP_DURATION"]:,.2f} min
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with col6:
        st.markdown(
            f"""
            <div class="kpi-card">
                <div class="kpi-title">💵 Average Tip</div>
                <div class="kpi-value">
                    ${data["AVG_TIP"]:,.2f}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# Display daily trip and revenue analysis
st.divider()

st.markdown(
    '<div class="section-title">📈 Daily Trip & Revenue Analysis</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="section-description">'
    'Daily trip volume and revenue generated'
    '</div>',
    unsafe_allow_html=True,
)

daily_data["TRIP_DATE"] = pd.to_datetime(
    daily_data["TRIP_DATE"]
)

col1, col2 = st.columns(2)

with col1:
    fig_trips = px.bar(
        daily_data,
        x="TRIP_DATE",
        y="TOTAL_TRIPS",
        title="Daily Trips",
        labels={
            "TRIP_DATE": "Date",
            "TOTAL_TRIPS": "Total Trips",
        },
        text="TOTAL_TRIPS",
    )

    fig_trips.update_traces(
        textposition="outside"
    )

    fig_trips.update_layout(
        template="plotly_dark",
        height=400,
        showlegend=False,
        margin=dict(l=20, r=20, t=60, b=20),
    )

    st.plotly_chart(
        fig_trips,
        use_container_width=True,
    )

with col2:
    fig_revenue = px.line(
        daily_data,
        x="TRIP_DATE",
        y="TOTAL_REVENUE",
        title="Daily Revenue",
        labels={
            "TRIP_DATE": "Date",
            "TOTAL_REVENUE": "Revenue ($)",
        },
        markers=True,
    )

    fig_revenue.update_layout(
        template="plotly_dark",
        height=400,
        margin=dict(l=20, r=20, t=60, b=20),
    )

    st.plotly_chart(
        fig_revenue,
        use_container_width=True,
    )


# Display payment analysis
st.divider()

st.markdown(
    '<div class="section-title">💳 Payment Analysis</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="section-description">'
    'Trip volume, revenue and average fare by payment type'
    '</div>',
    unsafe_allow_html=True,
)

col1, col2 = st.columns(2)

with col1:
    fig_payment_trips = px.bar(
        payment_data,
        x="PAYMENT_TYPE",
        y="TOTAL_TRIPS",
        title="Trips by Payment Type",
        labels={
            "PAYMENT_TYPE": "Payment Type",
            "TOTAL_TRIPS": "Total Trips",
        },
        text="TOTAL_TRIPS",
    )

    fig_payment_trips.update_layout(
        template="plotly_dark",
        height=400,
        margin=dict(l=20, r=20, t=60, b=20),
    )

    st.plotly_chart(
        fig_payment_trips,
        use_container_width=True,
    )

with col2:
    fig_payment_revenue = px.bar(
        payment_data,
        x="PAYMENT_TYPE",
        y="TOTAL_REVENUE",
        title="Revenue by Payment Type",
        labels={
            "PAYMENT_TYPE": "Payment Type",
            "TOTAL_REVENUE": "Revenue ($)",
        },
        text="TOTAL_REVENUE",
    )

    fig_payment_revenue.update_layout(
        template="plotly_dark",
        height=400,
        margin=dict(l=20, r=20, t=60, b=20),
    )

    st.plotly_chart(
        fig_payment_revenue,
        use_container_width=True,
    )


# Display vendor analysis
st.divider()

st.markdown(
    '<div class="section-title">🚖 Vendor Performance</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="section-description">'
    'Comparison of trip volume and revenue across taxi vendors'
    '</div>',
    unsafe_allow_html=True,
)

col1, col2 = st.columns(2)

with col1:
    fig_vendor_trips = px.bar(
        vendor_data,
        x="VENDOR_ID",
        y="TOTAL_TRIPS",
        title="Trips by Vendor",
        labels={
            "VENDOR_ID": "Vendor ID",
            "TOTAL_TRIPS": "Total Trips",
        },
        text="TOTAL_TRIPS",
    )

    fig_vendor_trips.update_layout(
        template="plotly_dark",
        height=400,
        margin=dict(l=20, r=20, t=60, b=20),
    )

    st.plotly_chart(
        fig_vendor_trips,
        use_container_width=True,
    )

with col2:
    fig_vendor_revenue = px.bar(
        vendor_data,
        x="VENDOR_ID",
        y="TOTAL_REVENUE",
        title="Revenue by Vendor",
        labels={
            "VENDOR_ID": "Vendor ID",
            "TOTAL_REVENUE": "Revenue ($)",
        },
        text="TOTAL_REVENUE",
    )

    fig_vendor_revenue.update_layout(
        template="plotly_dark",
        height=400,
        margin=dict(l=20, r=20, t=60, b=20),
    )

    st.plotly_chart(
        fig_vendor_revenue,
        use_container_width=True,
    )


# Display pickup location analysis
st.divider()

st.markdown(
    '<div class="section-title">📍 Pickup Location Analysis</div>',
    unsafe_allow_html=True,
)

st.markdown(
    '<div class="section-description">'
    'Top pickup location IDs based on trip volume'
    '</div>',
    unsafe_allow_html=True,
)

top_locations = location_data.head(10).copy()

top_locations = top_locations.sort_values(
    "TOTAL_TRIPS",
    ascending=True,
)

fig_locations = px.bar(
    top_locations,
    x="TOTAL_TRIPS",
    y="PU_LOCATION_ID",
    orientation="h",
    title="Top 10 Pickup Locations by Trip Volume",
    labels={
        "PU_LOCATION_ID": "Pickup Location ID",
        "TOTAL_TRIPS": "Total Trips",
    },
    text="TOTAL_TRIPS",
)

fig_locations.update_layout(
    template="plotly_dark",
    height=500,
    margin=dict(l=20, r=20, t=60, b=20),
)

st.plotly_chart(
    fig_locations,
    use_container_width=True,
)


# Display location details table
st.markdown(
    '<div class="section-title">📋 Location Performance Details</div>',
    unsafe_allow_html=True,
)

display_locations = location_data.head(10).copy()

display_locations.columns = [
    "Pickup Location",
    "Total Trips",
    "Total Revenue",
    "Average Fare",
    "Avg Distance",
    "Avg Duration",
]

st.dataframe(
    display_locations,
    use_container_width=True,
    hide_index=True,
)


# Display footer
st.divider()

st.markdown(
    """
    <div class="footer">
        NYC Taxi Data Engineering & Analytics Dashboard
        <br>
        Kafka • MinIO • Polars • Airflow • Snowflake • Streamlit
    </div>
    """,
    unsafe_allow_html=True,
)