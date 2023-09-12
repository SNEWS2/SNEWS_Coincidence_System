import os, json
from datetime import datetime
from .core.logging import getLogger
from .snews_hb import beats_path

### smtplib bits
# Import smtplib for the actual sending function
import smtplib

# For guessing MIME type
import mimetypes

# Import the email modules we'll need
import email
import email.mime.text
import email.mime.application
from email.mime.multipart import MIMEMultipart
###

log = getLogger(__name__)

### Pull in environment variables for use with _smtp_sender service function
smtpserver = os.getenv("smtp_server_addr")
sender = os.getenv("snews_sender_email")
password = os.getenv("snews_sender_pass")

contact_list_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'auxiliary/contact_list.json'))
with open(contact_list_file) as file:
    contact_list = json.load(file)

### SNEWS Alert
def send_email(alert_content):
    """ Send the SNEWS alert via e-mail
    """
    # echo "This is not the same message as before" | mail -s "Echo test email" someone@example.com
    pretty_alert = ''
    for k, v in alert_content.items():
        space = 40 - len(k)
        pretty_alert += f'{k} :{r" "*space}{v}\n'
    subject = "SNEWS COINCIDENCE" + datetime.utcnow().isoformat()
    emails = 'snews2-test-ahabig@d.umn.edu'

    _smtp_sender(pretty_alert, subject, emails)
    log.info(f"\t\t> SNEWS Alert mail was sent at {datetime.utcnow().isoformat()} to {emails}")

### _smtp_sender service function.  All other functions in this file 
### call this for mail handling.
def _smtp_sender(body, subject, addr, attachment=None):
    # Create a text/plain message
    msg = email.mime.multipart.MIMEMultipart()
    msg['Subject'] = subject

    msg['From'] = 'SNEWS USER <' + sender + '>'
    msg['To'] = addr

    # The main body is just another attachment
    emailbody = email.mime.text.MIMEText(body)
    msg.attach(emailbody)

    if attachment is not None:
         fp=open(attachment,'rb')
         att = email.mime.application.MIMEApplication(fp.read(),_subtype="octet-stream")
         fp.close()
         att.add_header('Content-Disposition','attachment',filename=attachment)
         msg.attach(att)

    # TODO: This should be defined as a os env
    with smtplib.SMTP(smtpserver) as smtp:
         smtp.connect(smtpserver)
         # TODO: We may need this.  Leaving for now.
         ## Bits and pieces for authenticated smtp.
         #smtp.starttls()
         #smtp.login(sender,password)

         #SMTP.sendmail(from_addr, to_addrs, msg, mail_options=(), rcpt_options=())
         smtp.sendmail(sender,['cjorr@purdue.edu'], msg.as_string())
         smtp.sendmail(sender, addr, msg.as_string())

    log.info(f"\t\t> An e-mail was sent at {datetime.utcnow().isoformat()} to {addr} via _smtp_sender")

### FEEDBACK EMAIL
def send_feedback_mail(detector, attachment=None, message_content=None, given_contact=None):
    """ Send feedback email to authorized, requested users
    """
    # Accept a contact list (e-mail(s)) # mail addresses already checked
    if type(given_contact) != list:
        contacts = list(given_contact)
    else:
        contacts = given_contact

    message_content = message_content or ""

    if len(contacts) > 0:
        time = datetime.utcnow().isoformat()
        subject = "SNEWS FEEDBACK " + time
        base_msg = f"echo {message_content} | s-nail -s 'SNEWS FEEDBACK {time}' "
        if attachment is not None:
            attached_file = os.path.join(beats_path, attachment)
            base_msg += f" -a {attached_file}"

        for contact in contacts:
            base_msg += f" {contact}"
            log.info(f"\t\t> Trying to send feedback to {contact} for {detector}")
            _smtp_sender(message_content, subject, contact, attachment)
    else:
        log.info(f"\t\t> Feedback mail is requested for {detector}. However, there are no contacts added.")

### Send WARNING message
def send_warning_mail(detector, message_content=None):
    """ Send warning mail when a heartbeat is skipped
        This function is invoked within the feedback script
    """
    contacts = contact_list[detector]["emails"]
    message_content = message_content or ""
    subject = "SNEWS Server Heartbeat for " + detector + " is skipped!"
    if len(contacts) > 0:
        for contact in contacts:
            log.info(f"\t\t> Trying to send warning to {contact} for {detector}\n")
            _smtp_sender(message_content, subject, contact)
    else:
        log.info(f"\t\t> Warning is triggered for {detector}. However, there are no contacts added.")
