import pandas as pd
from connection import get_clickhouse_client
import os

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
        
        df['PartType'] = df['ContentType'].apply(
            lambda x: 'Single Part' if x == 'Basic' else 'Multipart'
        )
        return df
    except Exception as e:
        print(f"OD query failed: {str(e)}")
        return pd.DataFrame()

def create_od_pivot(dfs):
    """Create OD pivot table with FTD, MTD, LMTD columns"""
    if not dfs:
        return pd.DataFrame(columns=['FTD', 'MTD', 'LMTD'])

    # Initialize with all required columns
    columns = ['Index', 'FTD', 'MTD', 'LMTD']
    all_data = pd.DataFrame(columns=columns)
    
    # Process each aggregator's data
    for agg in sorted(dfs['FTD']['Aggregator'].unique()):
        # Add aggregator total row
        agg_row = {'Index': agg}
        for period, df in dfs.items():
            agg_data = df[df['Aggregator'] == agg]
            agg_row[period] = agg_data['Sent'].sum()
        all_data = pd.concat([all_data, pd.DataFrame([agg_row])], ignore_index=True)
        
        # Add part type breakdowns
        for ptype in ['Single Part', 'Multipart']:
            type_row = {'Index': ptype}
            for period, df in dfs.items():
                type_data = df[(df['Aggregator'] == agg) & (df['PartType'] == ptype)]
                type_row[period] = type_data['Sent'].sum() if not type_data.empty else 0
            all_data = pd.concat([all_data, pd.DataFrame([type_row])], ignore_index=True)
    
    # Calculate type totals
    for ptype in ['Single Part', 'Multipart']:
        total_row = {'Index': f'Total {ptype}'}
        for period in ['FTD', 'MTD', 'LMTD']:
            if period in dfs:
                total_row[period] = dfs[period][dfs[period]['PartType'] == ptype]['Sent'].sum()
        all_data = pd.concat([all_data, pd.DataFrame([total_row])], ignore_index=True)
    
    # Add grand total
    grand_total = {'Index': 'G. Total'}
    for period in ['FTD', 'MTD', 'LMTD']:
        if period in dfs:
            grand_total[period] = dfs[period]['Sent'].sum()
    all_data = pd.concat([all_data, pd.DataFrame([grand_total])], ignore_index=True)
    
    # Set index and fill NaN values
    return all_data.set_index('Index').fillna(0)
