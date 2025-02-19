import pandas as pd
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

def load_mappings():
    """Load agent type mappings from CSV file"""
    mapping_file = os.path.join(os.path.dirname(__file__), 'mapping.csv')
    mappings_df = pd.read_csv(mapping_file)
    return dict(zip(mappings_df['vcAgentID'], mappings_df['vcType']))

def fetch_traffic_data(client, start_date, end_date):
    """Fetch traffic summary data"""
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
        
        # Update vcType using mappings
        agent_type_mappings = load_mappings()
        mask = df['vcType'].isna() | (df['vcType'] == '')
        df.loc[mask, 'vcType'] = df.loc[mask, 'vcAgentID'].map(agent_type_mappings)
        df['vcType'] = df['vcType'].fillna('Enterprise')
        
        return df
    except Exception as e:
        print(f"Traffic query failed: {str(e)}")
        return pd.DataFrame()

def create_traffic_pivot(dfs):
    """Create traffic pivot table with FTD, MTD, LMTD columns"""
    if not dfs:
        return pd.DataFrame(columns=['FTD', 'MTD', 'LMTD'])

    # Initialize with all required columns
    columns = ['Index', 'FTD', 'MTD', 'LMTD']
    all_data = pd.DataFrame(columns=columns)
    
    # Process each organization's data
    for org in sorted(dfs['FTD']['vcORGName'].unique()):
        # Add org total row
        org_row = {'Index': org}
        for period, df in dfs.items():
            org_data = df[df['vcORGName'] == org]
            org_row[period] = org_data['iTotalSentSuccess'].sum()
        all_data = pd.concat([all_data, pd.DataFrame([org_row])], ignore_index=True)
        
        # Add type breakdowns
        for vtype in ['CPL', 'Enterprise', 'OD']:
            type_row = {'Index': vtype}
            for period, df in dfs.items():
                type_data = df[(df['vcORGName'] == org) & (df['vcType'] == vtype)]
                type_row[period] = type_data['iTotalSentSuccess'].sum() if not type_data.empty else 0
            all_data = pd.concat([all_data, pd.DataFrame([type_row])], ignore_index=True)
    
    # Calculate type totals
    for vtype in ['CPL', 'Enterprise', 'OD']:
        total_row = {'Index': f'Total {vtype}'}
        for period in ['FTD', 'MTD', 'LMTD']:
            if period in dfs:
                total_row[period] = dfs[period][dfs[period]['vcType'] == vtype]['iTotalSentSuccess'].sum()
        all_data = pd.concat([all_data, pd.DataFrame([total_row])], ignore_index=True)
    
    # Add grand total
    grand_total = {'Index': 'G. Total'}
    for period in ['FTD', 'MTD', 'LMTD']:
        if period in dfs:
            grand_total[period] = dfs[period]['iTotalSentSuccess'].sum()
    all_data = pd.concat([all_data, pd.DataFrame([grand_total])], ignore_index=True)
    
    # Set index and fill NaN values
    return all_data.set_index('Index').fillna(0)
