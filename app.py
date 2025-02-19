import streamlit as st
from connection import get_clickhouse_client
from datetime import datetime, timedelta
import calendar
from traffic import fetch_traffic_data, create_traffic_pivot
from od import fetch_od_data, create_od_pivot

def get_date_ranges():
    """Get date ranges including last month and last-last month"""
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    # FTD (yesterday)
    ftd_start = yesterday.strftime('%Y-%m-%d')
    ftd_end = yesterday.strftime('%Y-%m-%d')
    
    # MTD (1st of current month to yesterday)
    mtd_start = today.replace(day=1).strftime('%Y-%m-%d')
    mtd_end = yesterday.strftime('%Y-%m-%d')
    
    # LMTD (1st of prev month to same date last month)
    first_of_month = today.replace(day=1)
    last_month = first_of_month - timedelta(days=1)
    lmtd_start = last_month.replace(day=1).strftime('%Y-%m-%d')
    lmtd_end = last_month.replace(day=min(yesterday.day, calendar.monthrange(last_month.year, last_month.month)[1])).strftime('%Y-%m-%d')
    
    # Last month (full month)
    last_month_start = last_month.replace(day=1)
    last_month_end = last_month
    
    # Last-last month (full month)
    last_last_month = last_month_start - timedelta(days=1)
    last_last_month_start = last_last_month.replace(day=1)
    last_last_month_end = last_last_month
    
    return {
        'FTD': (ftd_start, ftd_end),
        'MTD': (mtd_start, mtd_end),
        'LMTD': (lmtd_start, lmtd_end),
        'last_month': (last_month_start.strftime('%Y-%m-%d'), last_month_end.strftime('%Y-%m-%d')),
        'last_last_month': (last_last_month_start.strftime('%Y-%m-%d'), last_last_month_end.strftime('%Y-%m-%d'))
    }

# Main app section
st.title("RCS Traffic Summary Dashboard")

try:
    client = get_clickhouse_client()
    date_ranges = get_date_ranges()
    
    # Process Traffic data
    traffic_dfs = {}
    for period, (start_date, end_date) in date_ranges.items():
        df = fetch_traffic_data(client, start_date, end_date)
        if not df.empty:
            traffic_dfs[period] = df
    
    if traffic_dfs:
        st.header("Traffic Summary")
        traffic_pivot = create_traffic_pivot(traffic_dfs)
        st.dataframe(traffic_pivot)
    
    # Process OD data
    od_dfs = {}
    for period, (start_date, end_date) in date_ranges.items():
        df = fetch_od_data(client, start_date, end_date)
        if not df.empty:
            od_dfs[period] = df
    
    if od_dfs:
        st.header("OD Traffic Summary")
        od_pivot = create_od_pivot(od_dfs)
        st.dataframe(od_pivot)
    
    if not traffic_dfs and not od_dfs:
        st.write("No data available")
        
except Exception as e:
    st.error(f"An error occurred: {str(e)}")
    print("Full error:", str(e))
