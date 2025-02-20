import smtplib
from email.message import EmailMessage
import pandas as pd
import os

def send_summary_email(traffic_pivot, od_pivot, excel_path, recipients):
    """Send email using existing working configuration"""
    try:
        # Create email message
        email = EmailMessage()
        email['Subject'] = f"RCS Traffic Summary Report | {pd.Timestamp.now().strftime('%Y-%m-%d')}"
        email['From'] = 'donotreply<donotreply@tanla.com>'
        email['To'] = ', '.join(recipients)
        
        # Create simple HTML body
        body = f"""
        <p>Dear All,</p>
        <p>Please find the RCS Traffic Summary Report as of {pd.Timestamp.now().strftime('%Y-%m-%d')}</p>
        
        <p>Traffic Summary:</p>
        {traffic_pivot.to_html()}
        
        <p>OD Traffic Summary:</p>
        {od_pivot.to_html()}
        
        <p>Please find the detailed analysis in the attached Excel file.</p>
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
