import smtplib
from email.message import EmailMessage
import pandas as pd
import os

def create_clean_table(df):
    """Convert DataFrame to HTML table with styling"""
    # Convert DataFrame to HTML with styling
    html = df.to_html(
        classes='clean-table',
        float_format=lambda x: '{:.2f}'.format(x),  # Keep 2 decimal places
        border=1,
        justify='left'
    )
    
    # Add clean, minimal CSS with row coloring
    styled_html = f"""
    <style>
        .clean-table {{
            border-collapse: collapse;
            font-family: 'Calibri', sans-serif;
            font-size: 11pt;
            width: 100%;
            margin: 10px 0;
        }}
        .clean-table th, .clean-table td {{
            padding: 4px 8px;
            border: 1px solid #dddddd;
            text-align: left;
        }}
        .clean-table th {{
            background-color: #f8f9fa;
            color: #333333;
            font-weight: normal;
        }}
        /* Color for aggregator rows (those without 'Part' in index) */
        .clean-table tr td:first-child {{
            background-color: #f5f5f5;
        }}
        /* Color for total rows */
        .clean-table tr:last-child td,
        .clean-table tr td:first-child[data-value*="Total"] {{
            background-color: #f0f0f0;
            font-weight: bold;
        }}
    </style>
    {html}
    """
    return styled_html

def send_summary_email(traffic_pivot, od_pivot, excel_path, recipients):
    """Send email using existing working configuration"""
    try:
        # Create email message
        email = EmailMessage()
        email['Subject'] = f"RCS Traffic Summary Report | {pd.Timestamp.now().strftime('%Y-%m-%d')}"
        email['From'] = 'donotreply<donotreply@tanla.com>'
        email['To'] = ', '.join(recipients)
        
        # Create clean, professional email body
        body = f"""
        <html>
        <body style="font-family: 'Calibri', sans-serif; font-size: 11pt; color: #333333; line-height: 1.4;">
            <p>Dear Sir,</p>
            
            <p>Please find the RCS vol for {pd.Timestamp.now().strftime('%b\'%y')}.</p>
            
            <div style="margin: 20px 0;">
                <p style="font-weight: bold; margin-bottom: 8px;">Overall Traffic Summary:</p>
                <p style="color: #666666; font-size: 10pt; margin-bottom: 8px;">*vol in Millions</p>
                {create_clean_table(traffic_pivot)}
            </div>
            
            <div style="margin: 20px 0;">
                <p style="font-weight: bold; margin-bottom: 8px;">OD Traffic Summary:</p>
                <p style="color: #666666; font-size: 10pt; margin-bottom: 8px;">*vol in Millions</p>
                {create_clean_table(od_pivot)}
            </div>
            
            <p style="color: #666666; margin-top: 20px;">
                Please find the detailed analysis in the attached Excel file.
            </p>
        </body>
        </html>
        """
        
        email.set_content(body, subtype='html')
        
        # Attach Excel file
        if os.path.exists(excel_path):
            with open(excel_path, 'rb') as f:
                email.add_attachment(
                    f.read(),
                    maintype='application',
                    subtype='xlsx',
                    filename='RCS_Analysis.xlsx'
                )
        
        # Use the working SMTP configuration
        with smtplib.SMTP('smtp.office365.com', 25) as server:
            server.connect('smtp.office365.com', 25)
            server.ehlo()
            server.starttls()
            server.login('donotreply-ildhub@tanla.com', 'Jar45492')
            server.send_message(email)
            
        print("Email sent successfully!")
        return True
        
    except Exception as e:
        print(f"Failed to send email: {str(e)}")
        return False
