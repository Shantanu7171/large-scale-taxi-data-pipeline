import pandas as pd


# Get KPI data
def get_kpi_data(conn):
    query = """
        SELECT
            TOTAL_TRIPS,
            TOTAL_REVENUE,
            AVG_FARE,
            AVG_TRIP_DISTANCE,
            AVG_TRIP_DURATION,
            AVG_TIP
        FROM TAXI_DATA.SILVER.VW_TRIP_PERFORMANCE
    """

    return pd.read_sql(query, conn)


# Get daily trip and revenue data
def get_daily_trips(conn):
    query = """
        SELECT
            TRIP_DATE,
            TOTAL_TRIPS,
            TOTAL_REVENUE,
            AVG_TRIP_DISTANCE,
            AVG_TRIP_DURATION
        FROM TAXI_DATA.SILVER.VW_DAILY_TRIPS
        ORDER BY TRIP_DATE
    """

    return pd.read_sql(query, conn)


# Get payment analysis data
def get_payment_analysis(conn):
    query = """
        SELECT
            PAYMENT_TYPE,
            TOTAL_TRIPS,
            TOTAL_REVENUE,
            AVG_FARE,
            AVG_TIP,
            AVG_TRIP_DISTANCE
        FROM TAXI_DATA.SILVER.VW_PAYMENT_ANALYSIS
        ORDER BY TOTAL_TRIPS DESC
    """

    return pd.read_sql(query, conn)


# Get vendor analysis data
def get_vendor_analysis(conn):
    query = """
        SELECT
            VENDOR_ID,
            TOTAL_TRIPS,
            TOTAL_REVENUE,
            AVG_FARE,
            AVG_TRIP_DISTANCE,
            AVG_TRIP_DURATION,
            AVG_TIP
        FROM TAXI_DATA.SILVER.VW_VENDOR_ANALYSIS
        ORDER BY TOTAL_TRIPS DESC
    """

    return pd.read_sql(query, conn)


# Get pickup location analysis data
def get_location_analysis(conn):
    query = """
        SELECT
            PU_LOCATION_ID,
            TOTAL_TRIPS,
            TOTAL_REVENUE,
            AVG_FARE,
            AVG_TRIP_DISTANCE,
            AVG_TRIP_DURATION
        FROM TAXI_DATA.SILVER.VW_LOCATION_ANALYSIS
        ORDER BY TOTAL_TRIPS DESC
    """

    return pd.read_sql(query, conn)