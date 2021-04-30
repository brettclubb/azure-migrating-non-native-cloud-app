import logging
import azure.functions as func
import psycopg2
import os
from datetime import datetime
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Content

def main(msg: func.ServiceBusMessage):

    notification_id = int(msg.get_body().decode('utf-8'))
    logging.info('Python ServiceBus queue trigger processed message: %s',notification_id)

    # Get connection to database
    conn = psycopg2.connect("dbname=techconfdb user=webapp")

    try:
        cur = conn.cursor()

        # Get notification message and subject from database using the notification_id
        notification = cur.Execute("SELECT message, subject FROM notification WHERE id = %s;", notification_id)

        # Get attendees email and name
        attendees = cur.Execute("SELECT email, name FROM attendee;")

        # Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            subject = ("%s - %s", (attendee.name, notification.subject))
            content = Content("text/plain", notification.message)
            Mail("from@email.com", attendee.email, subject, content)

        # Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        cur.Execute("UPDATE notification SET completed_date = %s, status = %s ;", (datetime.now, len(attendees)))

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # Close connection
        cur.close()
        conn.close()