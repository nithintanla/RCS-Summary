import smtplib
from email.message import EmailMessage
import pandas as pd
import os

def create_clean_table(df):
    """Convert DataFrame to clean HTML table with Outlook-style formatting"""
    # Convert DataFrame to HTML with minimal styling
    html = df.to_html(
        classes='clean-table',
        float_format=lambda x: '{:,.0f}'.format(x),
        border=0
    )
    
    # Add clean, minimal CSS
    styled_html = f"""
    <style>
        .clean-table {{
            border-collapse: collapse;
            font-family: 'Calibri', sans-serif;
            font-size: 11pt;
            width: 100%;
            margin: 10px 0;
        }}
        .clean-table th {{
            background-color: #f8f9fa;
            color: #333333;
            font-weight: normal;
            padding: 4px 8px;
            text-align: left;
            border-bottom: 1px solid #dddddd;
        }}
        .clean-table td {{
            padding: 4px 8px;
            border-bottom: 1px solid #dddddd;
        }}
        .clean-table tr:last-child td {{
            border-bottom: none;
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
            <p>Dear All,</p>
            
            <p>Please find the RCS Traffic Summary Report as of {pd.Timestamp.now().strftime('%Y-%m-%d')}</p>
            
            <div style="margin: 20px 0;">
                <p style="font-weight: bold; margin-bottom: 8px;">Traffic Summary:</p>
                {create_clean_table(traffic_pivot)}
            </div>
            
            <div style="margin: 20px 0;">
                <p style="font-weight: bold; margin-bottom: 8px;">OD Traffic Summary:</p>
                {create_clean_table(od_pivot)}
            </div>
            
            <p style="color: #666666; margin-top: 20px;">
                Please find the detailed analysis in the attached Excel file.
            </p>
            
            <p style="color: #666666; font-size: 10pt; margin-top: 30px;">
                This is an auto-generated email. Please do not reply.
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
