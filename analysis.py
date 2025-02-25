import pandas as pd
from datetime import datetime
import os
import calendar

def create_vctype_daily_pivot(df):
    """Updated Summary sheet with specified columns"""
    if df.empty:
        return pd.DataFrame()
    
    pivot = pd.pivot_table(
        df,
        index='dtDate',
        columns='vcType',
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum'
    ).round(4)
    
    # Restructure columns for OD, Enterprise, CPL
    new_cols = []
    for vtype in ['OD', 'Enterprise', 'CPL']:
        if ('iTotalSentSuccess', vtype) in pivot.columns:
            new_cols.extend([('iTotalSentSuccess', vtype), ('iTotalDelivered', vtype)])
    
    pivot = pivot[new_cols]
    
    # Add total columns
    pivot['Total_Sent'] = pivot.xs('iTotalSentSuccess', axis=1, level=0).sum(axis=1)
    pivot['Total_Delivered'] = pivot.xs('iTotalDelivered', axis=1, level=0).sum(axis=1)
    
    # Flatten MultiIndex columns
    pivot.columns = [f"{vtype} {metric}" for metric, vtype in pivot.columns]
    
    return pivot

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
        return pd.DataFrame(columns=['Date', 'Organization', 'Type', 'Agent', 'Sent', 'Delivered'])
    
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
    if not result.empty:
        # Calculate totals
        total_sent = result['Sent'].sum()
        total_delivered = result['Delivered'].sum()
        
        # Add totals row
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
        ])
    
    return result.fillna(0).round(4)  # Changed to 4 decimal places

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
        ])
    
    return result.fillna(0).round(4)  # Changed to 4 decimal places

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
        ])
    
    return result.fillna(0).round(4)  # Changed to 4 decimal places

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
    output_path = os.path.join(os.path.dirname(__file__), 'RCS_Analysis.xlsx')
    
    with pd.ExcelWriter(output_path, engine='xlsxwriter', mode='w') as writer:
        workbook = writer.book
        
        # Define formats
        left_format = workbook.add_format({
            'align': 'left',
            'num_format': '0.0000'  # 4 decimal places
        })
        date_format = workbook.add_format({
            'align': 'left',
            'num_format': 'yyyy-mm-dd'  # Date without time
        })

        # Export each table with formatting
        sheets = {
            'OD Summary': create_od_agent_pivot(traffic_df),
            'RCSVol&SMSVol': create_volume_analysis(od_df),
            'Summary': create_vctype_daily_pivot(traffic_df),
            'Daywise OD summary': create_content_type_pivot(od_df),
            'Daywise': create_detailed_hierarchy(traffic_df),
            'Aggregator': create_agg_agent_pivot(traffic_df),
            'Botwise': create_agent_agg_pivot(traffic_df)
        }
        
        for name, df in sheets.items():
            if df.empty:
                continue
                
            # Reset index to remove numbering
            if isinstance(df.index, pd.MultiIndex):
                # For MultiIndex, split into separate columns
                df = df.reset_index()
                
                # Split any combined columns (like Agent - ContentType)
                for col in df.columns:
                    if isinstance(col, str) and ' - ' in col:
                        parts = col.split(' - ')
                        for i, part in enumerate(parts):
                            df[part] = df[col].apply(lambda x: str(x).split(' - ')[i] if ' - ' in str(x) else x)
                        df = df.drop(columns=[col])
            else:
                df = df.reset_index()
            
            # Flatten MultiIndex columns if they exist
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [f"{col[0]}_{col[1]}" if isinstance(col, tuple) else col for col in df.columns]
            
            # Format dates if present
            date_cols = [col for col in df.columns if 'date' in str(col).lower() or 'dt' in str(col).lower()]
            for col in date_cols:
                if pd.api.types.is_datetime64_any_dtype(df[col]):
                    df[col] = df[col].dt.strftime('%Y-%m-%d')
            
            # Organize columns - move numeric columns to the end
            numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns
            non_numeric_cols = df.select_dtypes(exclude=['float64', 'int64']).columns
            df = df[list(non_numeric_cols) + list(numeric_cols)]
            
            # Export DataFrame
            df.to_excel(writer, sheet_name=name, index=False)
            
            # Get worksheet
            worksheet = writer.sheets[name]
            
            # Apply formats to all columns
            for col_num, col in enumerate(df.columns):
                if col in date_cols:
                    worksheet.set_column(col_num, col_num, None, date_format)
                else:
                    worksheet.set_column(col_num, col_num, None, left_format)

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
