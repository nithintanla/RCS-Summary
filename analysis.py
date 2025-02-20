import pandas as pd
from datetime import datetime
import os
import calendar

def create_vctype_daily_pivot(df):
    """Table 1: Date-wise breakdown by vcType with vcType headers"""
    if df.empty:
        return pd.DataFrame()
    
    # Create initial pivot
    pivot = pd.pivot_table(
        df,
        index='dtDate',
        columns=['vcType'],
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum',  # Removed millions division
        fill_value=0
    ).round(2)
    
    # Restructure with proper headers
    header_1 = pd.DataFrame(columns=pivot.columns)
    header_2 = pd.DataFrame(columns=pivot.columns)
    
    # Fill headers
    for vtype in ['CPL', 'Enterprise', 'OD']:
        if ('iTotalSentSuccess', vtype) in pivot.columns:
            header_1.loc[0, [('iTotalSentSuccess', vtype), ('iTotalDelivered', vtype)]] = vtype
            header_2.loc[0, ('iTotalSentSuccess', vtype)] = 'Sent'
            header_2.loc[0, ('iTotalDelivered', vtype)] = 'Delivered'
    
    # Combine headers with data
    result = pd.concat([header_1, header_2, pivot])
    
    # Add totals
    result.loc['Total'] = result.iloc[2:].sum()
    
    return result

def create_od_agent_pivot(df):
    """Table 2: OD traffic by agent with aggregator grouping"""
    if df.empty:
        return pd.DataFrame()
    
    # Filter OD traffic
    od_data = df[df['vcType'] == 'OD']
    
    # Create pivot with aggregator grouping
    pivot = pd.pivot_table(
        od_data,
        index=['vcORGName', 'vcAgentName'],
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum',  # Removed millions division
        fill_value=0
    ).round(2)
    
    # Rename columns
    pivot.columns = ['Sent', 'Delivered']
    
    # Add subtotals for each aggregator
    result = pivot.copy()
    for org in od_data['vcORGName'].unique():
        org_data = pivot.loc[org]
        result.loc[(org, 'Total')] = org_data.sum()
    
    # Add grand total
    grand_total = pd.Series({
        'Sent': pivot['Sent'].sum(),
        'Delivered': pivot['Delivered'].sum()
    })
    result.loc[('Grand Total', '')] = grand_total
    
    return result

def create_content_type_pivot(od_df):
    """Table 3: Content type breakdown by agent and date"""
    if od_df.empty:
        return pd.DataFrame()
    
    # Create pivot with agent and content type indices
    pivot = pd.pivot_table(
        od_df,
        index=['Agent', 'ContentType'],
        columns='Date',
        values='Sent',
        aggfunc='sum',  # Removed millions division
        fill_value=0
    ).round(2)
    
    # Add total column
    pivot['Total'] = pivot.sum(axis=1)
    
    # Add grand total
    pivot.loc[('Grand Total', '')] = pivot.sum()
    
    return pivot

def create_detailed_hierarchy(df):
    """Table 4: Detailed hierarchy with all dimensions"""
    if df.empty:
        return pd.DataFrame()
    
    records = []
    
    for date in sorted(df['dtDate'].unique()):
        date_df = df[df['dtDate'] == date]
        for org in sorted(date_df['vcORGName'].unique()):
            org_df = date_df[date_df['vcORGName'] == org]
            for vtype in sorted(org_df['vcType'].unique()):
                type_df = org_df[org_df['vcType'] == vtype]
                for agent in sorted(type_df['vcAgentName'].unique()):
                    agent_df = type_df[type_df['vcAgentName'] == agent]
                    records.append({
                        'Date': date.strftime('%Y-%m-%d'),
                        'Organization': org,
                        'Type': vtype,
                        'Agent': agent,
                        'Sent': agent_df['iTotalSentSuccess'].sum(),
                        'Delivered': agent_df['iTotalDelivered'].sum()
                    })
    
    result = pd.DataFrame(records)
    if result.empty:
        return pd.DataFrame(columns=['Date', 'Organization', 'Type', 'Agent', 'Sent', 'Delivered'])
    
    # Calculate totals first
    total_sent = result['Sent'].sum()
    total_delivered = result['Delivered'].sum()
    
    # Add totals row using a new dictionary
    result = pd.concat([
        result,
        pd.DataFrame([{
            'Date': 'Total',
            'Organization': 'Total',
            'Type': 'Total',
            'Agent': 'Total',
            'Sent': total_sent,
            'Delivered': total_delivered
        }])
    ], ignore_index=True)
    
    return result.fillna(0).round(0).astype({'Sent': int, 'Delivered': int})

def create_agg_agent_pivot(df):
    """Table 5: Aggregator-Agent breakdown"""
    if df.empty:
        return pd.DataFrame(columns=['Organization', 'Agent', 'Sent', 'Delivered'])
    
    records = []
    for org in sorted(df['vcORGName'].unique()):
        org_df = df[df['vcORGName'] == org]
        for agent in sorted(org_df['vcAgentName'].unique()):
            agent_df = org_df[org_df['vcAgentName'] == agent]
            records.append({
                'Organization': org,
                'Agent': agent,
                'Sent': agent_df['iTotalSentSuccess'].sum(),
                'Delivered': agent_df['iTotalDelivered'].sum()
            })
    
    result = pd.DataFrame(records)
    if not result.empty:
        # Calculate totals
        total_sent = result['Sent'].sum()
        total_delivered = result['Delivered'].sum()
        
        # Add totals row
        result = pd.concat([
            result,
            pd.DataFrame([{
                'Organization': 'Total',
                'Agent': 'Total',
                'Sent': total_sent,
                'Delivered': total_delivered
            }])
        ], ignore_index=True)
    
    return result.fillna(0).round(0).astype({'Sent': int, 'Delivered': int})

def create_agent_agg_pivot(df):
    """Table 6: Agent-Aggregator breakdown"""
    if df.empty:
        return pd.DataFrame(columns=['Agent', 'Organization', 'Sent', 'Delivered'])
    
    records = []
    for agent in sorted(df['vcAgentName'].unique()):
        agent_df = df[df['vcAgentName'] == agent]
        for org in sorted(agent_df['vcORGName'].unique()):
            org_df = agent_df[agent_df['vcORGName'] == org]
            records.append({
                'Agent': agent,
                'Organization': org,
                'Sent': org_df['iTotalSentSuccess'].sum(),
                'Delivered': org_df['iTotalDelivered'].sum()
            })
    
    result = pd.DataFrame(records)
    if not result.empty:
        # Calculate totals
        total_sent = result['Sent'].sum()
        total_delivered = result['Delivered'].sum()
        
        # Add totals row
        result = pd.concat([
            result,
            pd.DataFrame([{
                'Agent': 'Total',
                'Organization': 'Total',
                'Sent': total_sent,
                'Delivered': total_delivered
            }])
        ], ignore_index=True)
    
    return result.fillna(0).round(0).astype({'Sent': int, 'Delivered': int})

def create_volume_analysis(od_df):
    """Table 7: Volume analysis with totals"""
    if od_df.empty:
        return pd.DataFrame()
    
    # Add RCS and SMS volume columns
    working_df = od_df.copy()
    working_df['RCS_Vol'] = working_df['Delivered']
    working_df['SMS_Vol'] = working_df['Parts'] * working_df['Delivered']
    
    # Initialize DataFrame for results
    records = []
    
    # Process each date
    for date in sorted(working_df['Date'].unique()):
        date_df = working_df[working_df['Date'] == date]
        
        # RCS Volume calculations
        basic_df = date_df[date_df['ContentType'] == 'Basic']
        multi_df = date_df[date_df['ContentType'] != 'Basic']
        
        # RCS Volumes
        records.append({
            'Category': 'RCS Vol',
            'PartType': 'Single Part',
            'Date': date,
            'Volume': basic_df['RCS_Vol'].sum()
        })
        records.append({
            'Category': 'RCS Vol',
            'PartType': 'Multipart',
            'Date': date,
            'Volume': multi_df['RCS_Vol'].sum()
        })
        
        # SMS Volumes
        records.append({
            'Category': 'SMS Vol',
            'PartType': 'Single Part',
            'Date': date,
            'Volume': basic_df['SMS_Vol'].sum()
        })
        records.append({
            'Category': 'SMS Vol',
            'PartType': 'Multipart',
            'Date': date,
            'Volume': multi_df['SMS_Vol'].sum()
        })
    
    # Create pivot table with multi-index
    result = pd.pivot_table(
        pd.DataFrame(records),
        index=['Category', 'PartType'],
        columns=['Date'],
        values='Volume',
        aggfunc='sum',
        fill_value=0
    )
    
    # Add Total column
    result['Total'] = result.sum(axis=1)
    
    # Calculate Projection
    today = datetime.now()
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    days_completed = today.day
    result['Projection'] = result['Total'] * (days_in_month / days_completed)
    
    # Add RCS and SMS volume totals
    rcs_total = result.loc['RCS Vol'].sum()
    sms_total = result.loc['SMS Vol'].sum()
    
    # Add new rows for totals
    result.loc[('RCS Vol', 'Total')] = result.loc['RCS Vol'].sum()
    result.loc[('SMS Vol', 'Total')] = result.loc['SMS Vol'].sum()
    
    return result.round(2)

def create_traffic_pivot(dfs):
    """Create traffic pivot table and remove empty rows"""
    if not dfs:
        return pd.DataFrame(columns=['FTD', 'MTD', 'LMTD'])

    # ...existing pivot creation code...
    
    # Remove rows where all numeric columns are 0
    numeric_cols = result.select_dtypes(include=['float64', 'int64']).columns
    result = result[~(result[numeric_cols] == 0).all(axis=1)]
    
    return result

def create_od_pivot(dfs):
    """Create OD pivot table and remove empty rows"""
    if not dfs:
        return pd.DataFrame(columns=['FTD', 'MTD', 'LMTD'])

    # ...existing pivot creation code...
    
    # Remove rows where all numeric columns are 0
    numeric_cols = result.select_dtypes(include=['float64', 'int64']).columns
    result = result[~(result[numeric_cols] == 0).all(axis=1)]
    
    return result

def export_mtd_analysis(traffic_df, od_df):
    """Export all analyses to Excel with consistent styling"""
    if traffic_df.empty and od_df.empty:
        return
        
    output_path = os.path.join(os.path.dirname(__file__), 'RCS_Analysis.xlsx')
    
    with pd.ExcelWriter(output_path, engine='xlsxwriter', mode='w') as writer:
        workbook = writer.book
        
        # Define left-aligned format
        left_format = workbook.add_format({'align': 'left'})
        
        # Export each table with consistent formatting
        for name, df in [
            ('OD Summary', create_od_agent_pivot(traffic_df)),
            ('RCSVol&SMSVol', create_volume_analysis(od_df)),
            ('Summary', create_vctype_daily_pivot(traffic_df)),
            ('Daywise OD summary', create_content_type_pivot(od_df)),
            ('Daywise', create_detailed_hierarchy(traffic_df)),
            ('Aggregator', create_agg_agent_pivot(traffic_df)),
            ('Botwise', create_agent_agg_pivot(traffic_df))
        ]:
            # Export DataFrame
            df.to_excel(writer, sheet_name=name)
            
            # Get the worksheet
            worksheet = writer.sheets[name]
            
            # Set left alignment for all columns
            for col in range(len(df.columns) + 1):  # +1 for index
                worksheet.set_column(col, col, None, left_format)
        
        print(f"Analysis exported to: {output_path}")

def analyze_mtd_data(traffic_df, od_df):
    """Process MTD data and create analysis"""
    if traffic_df.empty and od_df.empty:
        return
        
    # Convert dates
    traffic_df['dtDate'] = pd.to_datetime(traffic_df['dtDate'])
    od_df['Date'] = pd.to_datetime(od_df['Date'])
    
    # Filter for MTD
    today = datetime.now()
    start_date = today.replace(day=1).date()
    mtd_traffic = traffic_df[traffic_df['dtDate'].dt.date >= start_date]
    mtd_od = od_df[od_df['Date'].dt.date >= start_date]
    
    # Export analyses
    export_mtd_analysis(mtd_traffic, mtd_od)
