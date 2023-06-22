import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pymysql

# MySQL connection
db = pymysql.connect(
    host="localhost",
    port=3306,
    user="",
    password="",
    db=""
)

cursor = db.cursor()

# Query to get 500 users who haven't been sent an email yet
sql_query = "SELECT * FROM user_info WHERE batch_mail = 0 LIMIT 500"
cursor.execute(sql_query)

rows = cursor.fetchall()

# Gmail credentials
username = '' # Replace with your Gmail address
password = '' # Replace with your Gmail password

# Create a secure SSL context
context = ssl.create_default_context()

for row in rows:
    try:
        receiver_email = row[3] # Assuming the email is in the 4th column
        user_name = row[1] # Assuming the username is in the 2nd column
        phone = row[4] # Assuming the phone number is in the 6th column

        # Email subject
        subject = f"{user_name} by abc.com"

        # HTML Email content
        html = f"""\
        <html>
        <body>
            <p style="color:red; font-size:20px;">test</p>
        </body>
        </html>
        """

        message = MIMEMultipart("alternative")
        message["Subject"] = subject
        message["From"] = username
        message["To"] = receiver_email

        # Attach the HTML to the message
        message.attach(MIMEText(html, "html"))

        # Send email
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(username, password)
            server.sendmail(username, receiver_email, message.as_string())

        print(f"Email sent to {receiver_email}")

        # Update the batch_mail field in the database
        sql_update = "UPDATE user_info SET batch_mail = 1 WHERE email = %s"
        cursor.execute(sql_update, (receiver_email,))
        db.commit()

    except Exception as e:
        print(f"Failed to send email to {receiver_email} due to {str(e)}")
        continue

# Close the database connection
db.close()
