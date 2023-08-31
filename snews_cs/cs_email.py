
# https://unix.stackexchange.com/questions/381131/simplest-way-to-send-mail-with-image-attachment-from-command-line-using-gmail

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
    emails = 'snews2-test-ahabig@d.umn.edu'
    os.system( f'echo "{pretty_alert}"| mail -s "SNEWS COINCIDENCE {datetime.utcnow().isoformat()}" {emails}')
    log.info(f"\t\t> SNEWS Alert mail was sent at {datetime.utcnow().isoformat()} to {emails}")

###  First pass at using smtplib.
###  Note that this is sample code.
def _smtp_sender(body, subject, addr, attachment=None):
    # Create a text/plain message
    msg = email.mime.multipart.MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = 'SNEWS TEST USER <cjorr@purdue.edu>'
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

    s = smtplib.SMTP('smtp.purdue.edu')
    ## Bits and pieces for authenticated smtp.
    #s.starttls()
    #s.login('xyz@gmail.com','xyzpassword')
    s.sendmail('cjorr@purdue.edu',['cjorr@purdue.edu'], msg.as_string())
    s.quit()

def _mail_sender(mails):
    """ Send the generated emails via s-nail
    """
    success = False
    for i, mail in enumerate(mails[::-1]):
        # try sending either of the mails
        stdout = os.system(mail)
        if stdout == 0:
            success = True
    if success:
        log.info(f"\t> Mail was successfully sent!\n")
        return True
    else:
        log.error(f"\t> Mail could not be sent.\n")
        return False

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
        base_msg = f"echo {message_content} | s-nail -s 'SNEWS FEEDBACK {time}' "
        if attachment is not None:
            attached_file = os.path.join(beats_path, attachment)
            base_msg += f" -a {attached_file}"

        for contact in contacts:
            base_msg += f" {contact}"
            log.info(f"\t\t> Trying to send feedback to {contact} for {detector}")
            out = _mail_sender([base_msg])
            return out
    else:
        log.info(f"\t\t> Feedback mail is requested for {detector}. However, there are no contacts added.")

### Send WARNING message

base_warning = "echo {message_content} | " \
               "s-nail " \
               "-s 'SNEWS Server Heartbeat for {detector} is skipped!' " \
               "{contact}"

def send_warning_mail(detector, message_content=None):
    """ Send warning mail when a heartbeat is skipped
        This function is invoked within the feedback script
    """
    contacts = contact_list[detector]["emails"]
    message_content = message_content or ""
    if len(contacts) > 0:
        for contact in contacts:
            mail_regular = base_warning.format(message_content=message_content,
                                               detector=detector,
                                               contact=contact)
            log.info(f"\t\t> Trying to send warning to {contact} for {detector}\n")
            out = _mail_sender([mail_regular])
    else:
        log.info(f"\t\t> Warning is triggered for {detector}. However, there are no contacts added.")
