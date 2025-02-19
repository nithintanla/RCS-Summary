import pandas as pd
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
    """Create traffic pivot table with all columns including last two months"""
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
    
    # Process each organization's data
    for org in sorted(dfs['FTD']['vcORGName'].unique()):
        # Add organization data
        org_data = {'Index': org}
        for period in ['FTD', 'MTD', 'LMTD']:
            if period in dfs:
                org_df = dfs[period]
                org_data[period] = org_df[org_df['vcORGName'] == org]['iTotalSentSuccess'].sum()
        
        # Add last month and last-last month data
        for period, col_name in [('last_month', last_month_name), ('last_last_month', last_last_month_name)]:
            if period in dfs:
                period_df = dfs[period]
                org_data[col_name] = period_df[period_df['vcORGName'] == org]['iTotalSentSuccess'].sum()
        
        all_data = pd.concat([all_data, pd.DataFrame([org_data])], ignore_index=True)
        
        # Add type breakdowns
        for vtype in ['CPL', 'Enterprise', 'OD']:
            type_data = {'Index': vtype}
            for period in ['FTD', 'MTD', 'LMTD']:
                if period in dfs:
                    period_df = dfs[period]
                    mask = (period_df['vcORGName'] == org) & (period_df['vcType'] == vtype)
                    type_data[period] = period_df[mask]['iTotalSentSuccess'].sum()
            
            # Add last month and last-last month data for types
            for period, col_name in [('last_month', last_month_name), ('last_last_month', last_last_month_name)]:
                if period in dfs:
                    period_df = dfs[period]
                    mask = (period_df['vcORGName'] == org) & (period_df['vcType'] == vtype)
                    type_data[col_name] = period_df[mask]['iTotalSentSuccess'].sum()
            
            all_data = pd.concat([all_data, pd.DataFrame([type_data])], ignore_index=True)
    
    # Calculate totals
    result = all_data.set_index('Index')
    
    # Add type totals
    for vtype in ['CPL', 'Enterprise', 'OD']:
        type_total = {'Index': f'Total {vtype}'}
        for col in result.columns:
            type_total[col] = result[result.index == vtype][col].sum()
        result.loc[f'Total {vtype}'] = type_total
    
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
