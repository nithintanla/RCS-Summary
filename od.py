import pandas as pd
from connection import get_clickhouse_client
import os
import calendar
from datetime import datetime, timedelta

def load_query(query_name):
    """Load SQL query from queries.sql file"""
    query_file = os.path.join(os.path.dirname(__file__), 'queries.sql')
    with open(query_file, 'r') as f:
        content = f.read()
    
    # Split queries by semicolon and strip whitespace
    queries = [q.strip() for q in content.split(';') if q.strip()]
    
    if query_name == 'od_traffic':
        return queries[1]
    return ''

def fetch_od_data(client, start_date, end_date):  # Changed from fetch_od_traffic_data
    """Fetch OD traffic data"""
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
        
        # Convert metrics to millions
        for col in ['Received', 'Sent', 'Delivered']:
            df[col] = df[col] / 1_000_000
            
        df['PartType'] = df['ContentType'].apply(
            lambda x: 'Single Part' if x == 'Basic' else 'Multipart'
        )
        return df
    except Exception as e:
        print(f"OD query failed: {str(e)}")
        return pd.DataFrame()

def create_od_pivot(dfs):
    """Create OD pivot table with all columns including last two months"""
    if not dfs:
        return pd.DataFrame(columns=['FTD', 'MTD', 'LMTD'])

    # Get month names for column headers
    today = datetime.now()
    last_month = (today.replace(day=1) - timedelta(days=1))
    last_last_month = (last_month.replace(day=1) - timedelta(days=1))
    
    last_month_name = last_month.strftime('%b%y')
    last_last_month_name = last_last_month.strftime('%b%y')
    
    # Create and process main data
    all_data = pd.DataFrame(columns=['Index', 'FTD', 'MTD', 'LMTD'])
    
    # Process each aggregator's data
    for agg in sorted(dfs['FTD']['Aggregator'].unique()):
        # Add aggregator data
        agg_data = {'Index': agg}
        for period in ['FTD', 'MTD', 'LMTD']:
            if period in dfs:
                agg_df = dfs[period]
                agg_data[period] = agg_df[agg_df['Aggregator'] == agg]['Sent'].sum()
        
        # Add last month and last-last month data
        for period, col_name in [('last_month', last_month_name), ('last_last_month', last_last_month_name)]:
            if period in dfs:
                period_df = dfs[period]
                agg_data[col_name] = period_df[period_df['Aggregator'] == agg]['Sent'].sum()
        
        all_data = pd.concat([all_data, pd.DataFrame([agg_data])], ignore_index=True)
        
        # Add type breakdowns
        for ptype in ['Single Part', 'Multipart']:
            type_data = {'Index': ptype}
            for period in ['FTD', 'MTD', 'LMTD']:
                if period in dfs:
                    period_df = dfs[period]
                    mask = (period_df['Aggregator'] == agg) & (period_df['PartType'] == ptype)
                    type_data[period] = period_df[mask]['Sent'].sum()
            
            # Add last month and last-last month data for types
            for period, col_name in [('last_month', last_month_name), ('last_last_month', last_last_month_name)]:
                if period in dfs:
                    period_df = dfs[period]
                    mask = (period_df['Aggregator'] == agg) & (period_df['PartType'] == ptype)
                    type_data[col_name] = period_df[mask]['Sent'].sum()
            
            all_data = pd.concat([all_data, pd.DataFrame([type_data])], ignore_index=True)
    
    # Calculate totals
    result = all_data.set_index('Index')
    
    # Add type totals
    for ptype in ['Single Part', 'Multipart']:
        type_total = {'Index': f'Total {ptype}'}
        for col in result.columns:
            type_total[col] = result[result.index == ptype][col].sum()
        result.loc[f'Total {ptype}'] = type_total
    
    # Add grand total
    grand_total = result[~result.index.str.contains('Total')].sum()
    result.loc['G. Total'] = grand_total
    
    # Add Growth column
    result['Growth'] = result['MTD'] - result['LMTD']
    
    # Calculate projection
    today = datetime.now()
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    days_completed = today.day
    result['Projection'] = result['MTD'] * (days_in_month / days_completed)
    
    # Round all numeric columns
    result = result.round(0).astype(int)
    
    # Rearrange columns in desired order
    last_month_name = last_month.strftime('%b%y')
    last_last_month_name = last_last_month.strftime('%b%y')
    
    result = result.reindex(columns=[
        'FTD',
        'MTD',
        'LMTD',
        'Growth',
        'Projection',
        last_month_name,
        last_last_month_name
    ])
    
    return result
