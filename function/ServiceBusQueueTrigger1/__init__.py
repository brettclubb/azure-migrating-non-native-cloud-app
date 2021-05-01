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
    host = "nnc-postgres.postgres.database.azure.com"
    dbname = "techconfdb"
    user = "webapp@nnc-postgres"
    password = "V3ryStr0ngP@$$"
    sslmode = "require"
    conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
    conn = psycopg2.connect(conn_string)

    try:
        cur = conn.cursor()

        # Get notification message and subject from database using the notification_id
        cur.execute("SELECT * FROM notification WHERE id = %s;", notification_id)
        notification = cur.fetchAll()

        # Get attendees email and name
        cur.execute("SELECT * FROM attendee;")
        attendees = cur.fetchAll()

        # Loop through each attendee and send an email with a personalized subject
        for attendee in attendees:
            subject = subject = '{}: {}'.format(attendee.first_name, notification.subject)
            #send_email(attendee.email, subject, notification.message)

        # Update the notification table by setting the completed date and updating the status with the total number of attendees notified
        notified_count = 'Notified {} attendees'.format(len(attendees))
        cur.Execute("UPDATE notification SET completed_date = %s, status = %s WHERE id = %s ;", (datetime.utcnow(), notified_count, notification_id))

    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
    finally:
        # Close connection
        cur.close()
        conn.close()