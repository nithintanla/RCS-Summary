import pandas as pd
from datetime import datetime
import os

def create_vctype_daily_pivot(df):
    """Table 1: Date-wise breakdown by vcType"""
    if df.empty:
        return pd.DataFrame()
        
    # Create pivot and calculate values
    pivot = pd.pivot_table(
        df,
        index='dtDate',
        columns=['vcType'],
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum',
        fill_value=0
    )
    
    # Convert to regular DataFrame and rename columns
    result = pd.DataFrame(index=pivot.index)
    
    # Process each type
    for vtype in ['CPL', 'Enterprise', 'OD']:
        if ('iTotalSentSuccess', vtype) in pivot.columns:
            result[f"{vtype}_Sent"] = pivot[('iTotalSentSuccess', vtype)]
            result[f"{vtype}_Delivered"] = pivot[('iTotalDelivered', vtype)]
    
    # Add total row
    result.loc['Total'] = result.sum()
    
    return result.fillna(0).round(0).astype(int)

def create_od_agent_pivot(df):
    """Table 2: OD traffic by agent and aggregator"""
    if df.empty:
        return pd.DataFrame()
    
    # Filter OD traffic
    od_data = df[df['vcType'] == 'OD']
    
    # Create pivot
    pivot = pd.pivot_table(
        od_data,
        index='vcAgentName',
        values=['iTotalSentSuccess', 'iTotalDelivered'],
        aggfunc='sum',
        fill_value=0
    )
    
    # Rename columns
    pivot.columns = ['Sent', 'Delivered']
    
    # Add total row
    pivot.loc['Total'] = pivot.sum()
    
    return pivot.fillna(0).round(0).astype(int)

def create_content_type_pivot(od_df):
    """Table 3: Content type breakdown by agent"""
    if od_df.empty:
        return pd.DataFrame()
    
    all_data = pd.DataFrame(columns=['Index', 'Basic', 'Single'])
    
    # Process each agent
    for agent in sorted(od_df['Agent'].unique()):
        agent_df = od_df[od_df['Agent'] == agent]
        row = {
            'Index': agent,
            'Basic': agent_df[agent_df['ContentType'] == 'Basic']['Sent'].sum(),
            'Single': agent_df[agent_df['ContentType'] == 'Single']['Sent'].sum()
        }
        all_data = pd.concat([all_data, pd.DataFrame([row])], ignore_index=True)
    
    # Set index and add totals
    result = all_data.set_index('Index')
    result.loc['Grand Total'] = result.sum()
    
    return result.fillna(0).round(0).astype(int)

def create_detailed_hierarchy(df):
    """Table 4: Detailed hierarchy with all dimensions"""
    if df.empty:
        return pd.DataFrame()
    
    # Create DataFrame with separate columns
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

def export_mtd_analysis(traffic_df, od_df):
    """Export all analyses to Excel"""
    if traffic_df.empty and od_df.empty:
        return
        
    output_path = os.path.join(os.path.dirname(__file__), 'RCS_Analysis.xlsx')
    
    with pd.ExcelWriter(output_path, engine='xlsxwriter', mode='w') as writer:
        # Export each pivot table
        create_vctype_daily_pivot(traffic_df).to_excel(
            writer, sheet_name='1. Daily Traffic'
        )
        create_od_agent_pivot(traffic_df).to_excel(
            writer, sheet_name='2. OD Agent Traffic'
        )
        create_content_type_pivot(od_df).to_excel(
            writer, sheet_name='3. Content Types'
        )
        create_detailed_hierarchy(traffic_df).to_excel(
            writer, sheet_name='4. Detailed Hierarchy'
        )
        create_agg_agent_pivot(traffic_df).to_excel(
            writer, sheet_name='5. Agg-Agent'
        )
        create_agent_agg_pivot(traffic_df).to_excel(
            writer, sheet_name='6. Agent-Agg'
        )
        
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
