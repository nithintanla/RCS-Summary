import streamlit as st
from connection import get_clickhouse_client
import pandas as pd
from datetime import datetime, timedelta
import calendar
import os

def load_query(query_name):
    """Load SQL query from queries.sql file"""
    query_file = os.path.join(os.path.dirname(__file__), 'queries.sql')
    with open(query_file, 'r') as f:
        content = f.read()
    
    # Split queries by semicolon and strip whitespace
    queries = [q.strip() for q in content.split(';') if q.strip()]
    
    # First query is traffic summary, second is OD traffic
    if query_name == 'traffic':
        return queries[0]
    elif query_name == 'od_traffic':
        return queries[1]
    return ''

def fetch_data(client, start_date, end_date):
    """Fetch data for given date range"""
    query = load_query('traffic').format(
        start_date=start_date,
        end_date=end_date
    )
    try:
        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=[
            'dtDate', 'iHour', 'iORGID', 'vcORGName', 'iAgentID', 'vcAgentID', 
            'vcAgentName', 'iBrandID', 'vcBrandName', 'iTemplateID', 'vcTemplateName',
            'vcTrafficType', 'vcAgentType', 'vcIndustry', 'iTotalSubmitSuccess',
            'iTotalSentSuccess', 'iTotalDelivered', 'iTotalRead', 'iTotalFailed',
            'iTotalExpired', 'vcPlatform', 'vcType'
        ])
        # Verify column names
        print("Available columns:", df.columns.tolist())
        return df
    except Exception as e:
        st.error(f"Query execution failed: {str(e)}")
        print("Full error:", str(e))  # Add detailed error logging
        return pd.DataFrame()

def fetch_od_data(client, start_date, end_date):
    """Fetch OD data for given date range"""
    query = load_query('od_traffic').format(
        start_date=start_date,
        end_date=end_date
    )
    try:
        result = client.query(query)
        df = pd.DataFrame(result.result_rows, columns=[
            'Date', 'Aggregator', 'Brand', 'AgentID', 'Agent', 'TrafficType',
            'ContentType', 'TemplateType', 'Parts', 'Received', 'Sent', 'Delivered'
        ])
        
        # Add PartType column based on ContentType
        df['PartType'] = df['ContentType'].apply(
            lambda x: 'Single Part' if x == 'Basic' else 'Multipart'
        )
        return df
    except Exception as e:
        st.error(f"Query execution failed: {str(e)}")
        return pd.DataFrame()

def get_date_ranges():
    """Calculate date ranges for FTD, MTD, and LMTD"""
    today = datetime.now()
    yesterday = today - timedelta(days=1)
    
    # Format dates as 'YYYY-MM-DD' strings
    def format_date(dt):
        return dt.strftime('%Y-%m-%d')
    
    # FTD range (previous day)
    ftd_start = format_date(yesterday)
    ftd_end = format_date(yesterday)
    
    # MTD range (current month till yesterday)
    mtd_start = format_date(today.replace(day=1))
    mtd_end = format_date(yesterday)
    
    # LMTD range (last month till same day)
    last_month = today.replace(day=1) - timedelta(days=1)
    lmtd_start = format_date(last_month.replace(day=1))
    lmtd_end = format_date(last_month.replace(day=yesterday.day))
    
    return {
        'ftd': (ftd_start, ftd_end),
        'mtd': (mtd_start, mtd_end),
        'lmtd': (lmtd_start, lmtd_end)
    }

def analyze_bot_traffic(df):
    """Create bot-wise summary"""
    return df.groupby('vcAgentName').agg({
        'iTotalSentSuccess': 'sum',
        'iTotalDelivered': 'sum'
    }).round(0).astype(int)

def analyze_aggregator_traffic(df):
    """Create aggregator-wise summary"""
    return df.groupby('vcORGName').agg({
        'iTotalSentSuccess': 'sum',
        'iTotalDelivered': 'sum'
    }).round(0).astype(int)

def analyze_daily_type_traffic(df):
    """Create daily traffic by type summary"""
    daily_by_type = df.groupby(['dtDate', 'vcType']).agg({
        'iTotalSentSuccess': 'sum',
        'iTotalDelivered': 'sum'
    }).round(0).astype(int)
    
    # Pivot the data with vcType at top level
    pivoted = daily_by_type.unstack(level='vcType')
    
    # Rename columns to be more readable
    new_columns = []
    for type_name in pivoted.columns.levels[1]:
        new_columns.extend([
            ('Sent', type_name),
            ('Delivered', type_name)
        ])
    
    # Reorganize columns to put Sent before Delivered for each type
    pivoted = pivoted.reindex(columns=[
        ('iTotalSentSuccess', col) for col in pivoted.columns.levels[1]
    ] + [
        ('iTotalDelivered', col) for col in pivoted.columns.levels[1]
    ])
    
    # Rename columns
    pivoted.columns = pd.MultiIndex.from_tuples([
        ('Sent', col[1]) if col[0] == 'iTotalSentSuccess' 
        else ('Delivered', col[1]) 
        for col in pivoted.columns
    ])
    
    # Add totals
    pivoted[('Sent', 'Total')] = pivoted.xs('Sent', axis=1, level=0).sum(axis=1)
    pivoted[('Delivered', 'Total')] = pivoted.xs('Delivered', axis=1, level=0).sum(axis=1)
    
    return pivoted

def analyze_od_bot_traffic(df):
    """Create OD traffic by bot summary"""
    od_data = df[df['vcType'] == 'OD']
    return od_data.groupby(['dtDate', 'vcAgentName']).agg({
        'iTotalSentSuccess': 'sum',
        'iTotalDelivered': 'sum'
    }).round(0).astype(int)

def analyze_hierarchical_traffic(ftd_df, mtd_df, lmtd_df, dec_df, jan_df):
    """Create hierarchical traffic summary with specific ordering of columns and rows"""
    
    def process_period_data(df):
        """Process single period data into hierarchical structure"""
        # Convert to millions
        df['traffic'] = df['iTotalSentSuccess'] / 1000000
        
        # Get unique aggregators and types
        aggregators = df['vcORGName'].unique()
        traffic_types = df['vcType'].dropna().unique()
        
        # Create result series with hierarchical index
        result = pd.Series(dtype='float64')
        
        for agg in aggregators:
            # Get aggregator total
            agg_data = df[df['vcORGName'] == agg]
            agg_total = agg_data['traffic'].sum()
            result[agg] = agg_total
            
            # Get type breakdown for this aggregator
            for vtype in traffic_types:
                type_total = agg_data[agg_data['vcType'] == vtype]['traffic'].sum()
                if type_total > 0:  # Only add if there's traffic
                    result[f"{agg} - {vtype}"] = type_total
        
        return result
    
    # Process each period
    periods = {
        'FTD': ftd_df,
        'MTD': mtd_df,
        'LMTD': lmtd_df,
        'Jan25': jan_df,
        'Dec24': dec_df
    }
    
    # Create initial DataFrame
    summary_data = {period: process_period_data(df) for period, df in periods.items()}
    summary = pd.DataFrame(summary_data).fillna(0)
    
    # Calculate Growth and Projection
    summary['Growth'] = summary['MTD'] - summary['LMTD']
    current_day = datetime.now().day
    days_in_month = calendar.monthrange(datetime.now().year, datetime.now().month)[1]
    summary['Proj'] = summary['MTD'] * (days_in_month / current_day)
    
    # Calculate type totals
    all_types = set()
    for df in [ftd_df, mtd_df, lmtd_df, dec_df, jan_df]:
        all_types.update(df['vcType'].dropna().unique())
    
    # Add type totals
    for vtype in all_types:
        type_rows = [idx for idx in summary.index if idx.endswith(f"- {vtype}")]
        if type_rows:
            type_total = summary.loc[type_rows].sum()
            summary.loc[f"Total - {vtype}"] = type_total
    
    # Add grand total
    grand_total = pd.Series({
        col: summary[~summary.index.str.contains('-', na=False)][col].sum()
        for col in summary.columns
    }, name='Grand Total')
    
    summary.loc['Grand Total'] = grand_total
    
    # Reorder columns
    column_order = ['FTD', 'MTD', 'LMTD', 'Jan25', 'Dec24', 'Growth', 'Proj']
    summary = summary[column_order]
    
    return summary.round(2)

def create_collapsible_view(df):
    """Create collapsible hierarchical view of traffic data"""
    # Group by aggregator and type
    grouped = df.groupby(['vcORGName', 'vcType']).agg({
        'iTotalSentSuccess': 'sum',
        'iTotalDelivered': 'sum'
    }).round(0).astype(int)
    
    # Get unique aggregators
    aggregators = df['vcORGName'].unique()
    
    # Display each aggregator in an expander
    for agg in aggregators:
        # Calculate aggregator total
        agg_total = grouped.loc[agg].sum()
        
        # Create expander with aggregator name and total
        with st.expander(f"{agg} (Total Sent: {agg_total['iTotalSentSuccess']:,}, Delivered: {agg_total['iTotalDelivered']:,})"):
            # Get type breakdown for this aggregator
            type_data = grouped.loc[agg]
            
            # Create and display DataFrame for this aggregator's types
            st.dataframe(
                type_data.style.format('{:,}'),
                use_container_width=True
            )

def create_od_summary(dfs):
    """Create OD traffic summary with aggregator and part type breakdown"""
    def process_df(df):
        # Group by Aggregator and PartType
        return df.groupby(['Aggregator', 'PartType'])['Sent'].sum().round(0)
    
    summary_data = {period: process_df(df) for period, df in dfs.items()}
    summary = pd.DataFrame(summary_data).fillna(0)
    
    # Calculate Growth and Projection
    summary['Growth'] = summary['mtd'] - summary['lmtd']
    current_day = datetime.now().day
    days_in_month = calendar.monthrange(datetime.now().year, datetime.now().month)[1]
    summary['Proj'] = summary['mtd'] * (days_in_month / current_day)
    
    # Reorder columns
    column_order = ['ftd', 'mtd', 'lmtd', 'Growth', 'Proj', 'prev', 'prev_prev']
    summary = summary[column_order]
    
    # Process aggregators in specific order
    final_rows = []
    for agg in ['Karix', 'ValueFirst']:
        if agg in summary.index.get_level_values(0):
            # Add aggregator total
            agg_data = summary.loc[agg]
            total_row = agg_data.sum()
            final_rows.append((agg, 'Total', total_row))
            
            # Add part type rows
            for part in ['Single Part', 'Multipart']:
                if part in agg_data.index:
                    final_rows.append((agg, part, agg_data.loc[part]))
    
    # Add grand totals
    grand_total = summary.groupby(level=1).sum()
    for part in ['Single Part', 'Multipart']:
        if part in grand_total.index:
            final_rows.append(('Grand Total', part, grand_total.loc[part]))
    
    # Convert to DataFrame
    result = pd.DataFrame([
        {'Aggregator': agg, 'PartType': pt, **values.to_dict()}
        for agg, pt, values in final_rows
    ])
    result.set_index(['Aggregator', 'PartType'], inplace=True)
    
    return result.round(2)

def load_type_mapping():
    """Load vcType mapping from CSV file"""
    mapping_file = os.path.join(os.path.dirname(__file__), 'mapping.csv')
    return pd.read_csv(mapping_file).set_index('vcAgentID')['vcType'].to_dict()

def apply_type_mapping(df, mapping):
    """Apply vcType mapping to DataFrame"""
    df['vcType'] = df['vcAgentID'].map(mapping)
    # Set default value 'Enterprise' for null or empty vcType
    df['vcType'] = df['vcType'].fillna('Enterprise')
    return df

def fetch_all_period_data(client):
    """Fetch data for all required date ranges"""
    dates = get_date_ranges()
    
    # Add ranges for previous months
    today = datetime.now()
    prev_month = (today.replace(day=1) - timedelta(days=1)).replace(day=1)
    prev_prev_month = (prev_month - timedelta(days=1)).replace(day=1)
    
    dates['prev'] = (prev_month, prev_month.replace(day=calendar.monthrange(prev_month.year, prev_month.month)[1]))
    dates['prev_prev'] = (prev_prev_month, prev_prev_month.replace(day=calendar.monthrange(prev_prev_month.year, prev_prev_month.month)[1]))
    
    # Load type mapping
    type_mapping = load_type_mapping()
    
    # Fetch and process data for all periods
    dfs = {}
    for period, (start, end) in dates.items():
        df = fetch_data(client, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d'))
        df = apply_type_mapping(df, type_mapping)
        dfs[period] = df
    
    return dfs

def create_traffic_summary_pivot(dfs):
    """Create traffic summary pivot table"""
    def process_df(df):
        return df.groupby(['vcORGName', 'vcType'])['iTotalSentSuccess'].sum() / 1_000_000  # Convert to millions
    
    # Process each period
    summary = {}
    for period, df in dfs.items():
        summary[period.upper()] = process_df(df)
    
    # Create DataFrame with all periods
    pivot_df = pd.DataFrame(summary)
    
    # Calculate Growth (MTD vs LMTD)
    pivot_df['Growth'] = ((pivot_df['MTD'] - pivot_df['LMTD']) / pivot_df['LMTD'] * 100).round(2)
    
    # Calculate Projection
    current_day = datetime.now().day
    days_in_month = calendar.monthrange(datetime.now().year, datetime.now().month)[1]
    pivot_df['Proj'] = (pivot_df['MTD'] * (days_in_month / current_day)).round(2)
    
    # Rename prev and prev_prev columns
    pivot_df = pivot_df.rename(columns={'PREV': 'Prev', 'PREV_PREV': 'prev_prev'})
    
    # Reorder columns
    column_order = ['FTD', 'MTD', 'LMTD', 'Growth', 'Proj', 'Prev', 'prev_prev']
    pivot_df = pivot_df[column_order]
    
    return pivot_df

def load_mapping():
    """Load vcType mapping from CSV file"""
    mapping_df = pd.read_csv('mapping.csv')
    return dict(zip(mapping_df['vcAgentID'], mapping_df['vcType']))

def apply_mapping(df, mapping):
    """Apply vcType mapping based on vcAgentID"""
    df['vcType'] = df['vcAgentID'].map(mapping)
    df['vcType'] = df['vcType'].fillna('Enterprise')  # Default value
    return df

def create_traffic_pivot(dfs):
    """Create traffic summary pivot table with formatting"""
    def process_df(df):
        return df.groupby(['vcORGName', 'vcType'])['iTotalSentSuccess'].sum() / 1_000_000

    # Process each period's data
    summary = {}
    for period, df in dfs.items():
        summary[period.upper()] = process_df(df)
    
    # Create pivot DataFrame
    pivot_df = pd.DataFrame(summary)
    
    # Calculate metrics
    pivot_df['Growth'] = ((pivot_df['MTD'] - pivot_df['LMTD']) / pivot_df['LMTD'] * 100).round(2)
    
    # Calculate projection
    current_day = datetime.now().day
    days_in_month = calendar.monthrange(datetime.now().year, datetime.now().month)[1]
    pivot_df['Proj'] = (pivot_df['MTD'] * (days_in_month / current_day)).round(2)
    
    # Add vcType totals
    type_totals = {}
    for vctype in pivot_df.index.get_level_values('vcType').unique():
        type_mask = pivot_df.index.get_level_values('vcType') == vctype
        type_total = pivot_df[type_mask].sum()
        type_totals[f"Total - {vctype}"] = type_total
    
    # Add grand total
    grand_total = pivot_df.groupby(level=0).sum().sum()
    type_totals['Grand Total'] = grand_total
    
    # Add totals to DataFrame
    totals_df = pd.DataFrame(type_totals)
    pivot_df = pd.concat([pivot_df, totals_df.T])
    
    # Reorder columns
    column_order = ['FTD', 'MTD', 'LMTD', 'Growth', 'Proj', 'Prev', 'prev_prev']
    pivot_df = pivot_df[column_order]
    
    return pivot_df

def create_rcs_pivot(dfs):
    """Create RCS traffic summary pivot table"""
    def process_df(df):
        return df.groupby(['vcORGName', 'vcType'])['iTotalSentSuccess'].sum()  # Removed division by 1,000,000

    # Process each period's data
    summary = {}
    for period, df in dfs.items():
        summary[period.upper()] = process_df(df)
    
    # Create pivot DataFrame
    pivot_df = pd.DataFrame(summary)
    
    # Calculate metrics
    pivot_df['Growth'] = ((pivot_df['MTD'] - pivot_df['LMTD']) / pivot_df['LMTD'] * 100).round(2)
    
    current_day = datetime.now().day
    days_in_month = calendar.monthrange(datetime.now().year, datetime.now().month)[1]
    pivot_df['Proj'] = (pivot_df['MTD'] * (days_in_month / current_day)).round(2)
    
    # Add vcType totals
    type_totals = {}
    for vctype in pivot_df.index.get_level_values('vcType').unique():
        type_mask = pivot_df.index.get_level_values('vcType') == vctype
        type_total = pivot_df[type_mask].sum()
        type_totals[f"Total - {vctype}"] = type_total
    
    # Add grand total
    grand_total = pivot_df.groupby(level=0).sum().sum()
    type_totals['Grand Total'] = grand_total
    
    # Add totals to DataFrame
    totals_df = pd.DataFrame(type_totals)
    pivot_df = pd.concat([pivot_df, totals_df.T])
    
    # Reorder columns
    column_order = ['FTD', 'MTD', 'LMTD', 'Growth', 'Proj']
    pivot_df = pivot_df[column_order]
    
    return pivot_df

def create_od_pivot(dfs):
    """Create OD traffic summary pivot table"""
    def process_df(df):
        return df.groupby(['Aggregator', 'ContentType'])['Sent'].sum()  # Removed division by 1,000,000

    # Process each period's data
    summary = {}
    for period, df in dfs.items():
        summary[period.upper()] = process_df(df)
    
    # Create pivot DataFrame
    pivot_df = pd.DataFrame(summary)
    
    # Calculate metrics
    pivot_df['Growth'] = ((pivot_df['MTD'] - pivot_df['LMTD']) / pivot_df['LMTD'] * 100).round(2)
    
    current_day = datetime.now().day
    days_in_month = calendar.monthrange(datetime.now().year, datetime.now().month)[1]
    pivot_df['Proj'] = (pivot_df['MTD'] * (days_in_month / current_day)).round(2)
    
    # Add ContentType totals
    type_totals = {}
    for ctype in pivot_df.index.get_level_values('ContentType').unique():
        type_mask = pivot_df.index.get_level_values('ContentType') == ctype
        type_total = pivot_df[type_mask].sum()
        type_totals[f"Total - {ctype}"] = type_total
    
    # Add grand total
    grand_total = pivot_df.groupby(level=0).sum().sum()
    type_totals['Grand Total'] = grand_total
    
    # Add totals to DataFrame
    totals_df = pd.DataFrame(type_totals)
    pivot_df = pd.concat([pivot_df, totals_df.T])
    
    # Reorder columns
    column_order = ['FTD', 'MTD', 'LMTD', 'Growth', 'Proj']
    pivot_df = pivot_df[column_order]
    
    return pivot_df

def style_pivot_table(pivot_df):
    """Apply styling to pivot table"""
    return pivot_df.style\
        .format({
            'FTD': '{:,.0f}',  # Changed to show full numbers with commas
            'MTD': '{:,.0f}',
            'LMTD': '{:,.0f}',
            'Growth': '{:.2f}%',
            'Proj': '{:,.0f}'
        })\
        .apply(lambda x: ['color: green' if not isinstance(x.name, tuple) 
                         else 'color: white' for _ in x], axis=1)\
        .set_properties(**{
            'background-color': 'black',
            'padding': '10px'
        })\
        .set_table_styles([
            {'selector': 'th', 'props': [
                ('background-color', 'black'),
                ('color', 'white'),
                ('font-weight', 'bold')
            ]}
        ])

def main():
    st.set_page_config(page_title="RCS Traffic Summary", layout="wide")
    
    client = get_clickhouse_client()
    
    try:
        # Load mapping
        mapping = load_mapping()
        
        # Fetch RCS data
        rcs_dfs = {}
        od_dfs = {}
        dates = get_date_ranges()
        
        for period, (start, end) in dates.items():
            # Fetch and process RCS data
            rcs_df = fetch_data(client, start, end)
            if not rcs_df.empty:
                rcs_df = apply_mapping(rcs_df, mapping)
                rcs_dfs[period] = rcs_df
            
            # Fetch and process OD data
            od_df = fetch_od_data(client, start, end)
            if not od_df.empty:
                od_dfs[period] = od_df
        
        # Create and display RCS pivot table
        st.title("RCS Traffic Summary")
        if rcs_dfs:
            rcs_pivot = create_rcs_pivot(rcs_dfs)
            st.dataframe(style_pivot_table(rcs_pivot), use_container_width=True)
        
        # Create and display OD pivot table
        st.title("OD Traffic Summary")
        if od_dfs:
            od_pivot = create_od_pivot(od_dfs)
            st.dataframe(style_pivot_table(od_pivot), use_container_width=True)
            
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        print("Full error:", str(e))

if __name__ == "__main__":
    main()