
# https://unix.stackexchange.com/questions/381131/simplest-way-to-send-mail-with-image-attachment-from-command-line-using-gmail

import os, json
from datetime import datetime
from .core.logging import getLogger

log = getLogger(__name__)

sender = os.getenv("snews_sender_email")
password = os.getenv("snews_sender_pass")
beats_path = os.path.join(os.path.dirname(__file__), "../beats")

contact_list_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'auxiliary/contact_list.json'))
with open(contact_list_file) as file:
    contact_list = json.load(file)


def send_email(alert_content):
    """ Send the SNEWS alert via e-mail
    """
    # echo "This is not the same message as before" | mail -s "Echo test email" someone@example.com
    pretty_alert = ''
    for k, v in alert_content.items():
        space = 40 - len(k)
        pretty_alert += f'{k} :{r" "*space}{v}\n'
    # emails = 'sebastiantorreslara17@gmail.com'
    emails = 'snews2-test-ahabig@d.umn.edu'
    os.system( f'echo "{pretty_alert}"| mail -s "SNEWS COINCIDENCE {datetime.utcnow().isoformat()}" {emails}')
    log.info(f"\t\t> SNEWS Alert mail was sent at {datetime.utcnow().isoformat()} to {emails}")


base_msg="'smtps://$USER:$PASSWORD@smtp.gmail.com' mutt " \
         "  -F /dev/null " \
         "  -e 'set from={sender}' " \
         "  -e 'set smtp_url=$SMTP_URL' " \
         "  -s 'SNEWS Server Feedback for %detector' " \
         "  -a {attachment} --  " \
         "  {contact} << EOM " \
         " Dear {detector}, Attached {attachment} please find the feedback information," \
         " {message_content}" \
         " Provided by the SNEWS server. --Cheers EOM"

def send_feedback_mail(detector, attachment, message_content=None, given_contact=None):
    """ Send feedback email to authorized, requested users
    """
    # Accept a contact list (e-mail(s)) # mail addresses already checked
    if type(given_contact) != list:
        contacts = list(given_contact)
    else:
        contacts = given_contact

    message_content = message_content or ""
    if len(contacts) > 0:
        for contact in contacts:
            mail = base_msg.format(sender=sender,
                                   detector=detector,
                                   attachment=os.path.join(beats_path, attachment),
                                   contact=contact,
                                   message_content=message_content)
            # os.system(mail)
            print(mail)
        log.info(f"\t\t> Feedback Sent to {contacts} for {detector}")
    else:
        log.info(f"\t\t> Feedback mail is requested for {detector}. However, there are no contacts added.")


base_warning="'smtps://user:password@smtp.gmail.com' mutt " \
             "  -F /dev/null " \
             "  -e 'set from={sender}' " \
             "  -e 'set smtp_url=$SMTP_URL' " \
             "  -s 'SNEWS Server Heartbeat for {detector} is skipped!' " \
             "  {contact} << EOM " \
             " Dear {detector}, " \
             " {message_content}" \
             " Provided by the SNEWS server. --Cheers EOM"

def send_warning_mail(detector, message_content=None):
    contacts = contact_list[detector]["emails"]
    message_content = message_content or ""
    if len(contacts) > 0:
        for contact in contacts:
            mail = base_warning.format(sender=sender,
                                   detector=detector,
                                   contact=contact,
                                   message_content=message_content)
            # os.system(mail)
            print(mail)
        log.info(f"\t\t> Warning Sent to {contacts}\n")



#### sudo apt-get install sendmail
#################### TEST
test_warning="'smtps://{user}:{password}@smtp.gmail.com' mutt " \
             "  -F /dev/null " \
             "  -e 'set from={sender}' " \
             "  -e 'set smtp_url=$SMTP_URL' " \
             "  -s 'SNEWS Server Heartbeat for %detector is skipped!' " \
             "  Melih << EOM " \
             " Dear {detector}, " \
             " {message_content}" \
             " Provided by the SNEWS server. --Cheers EOM"

def send_test_mail(user, password, sender='Melih'):
    mail = base_msg.format(user=user,
                           password=password,
                           sender=sender,
                           detector="XENONnT",
                           message_content="Random message_content")
    # os.system(mail)

    # Import smtplib for the actual sending function
    import smtplib
    # Import the email modules we'll need
    from email.mime.text import MIMEText

    # Open a plain text file for reading.  For this example, assume that
    # the text file contains only ASCII characters.
    # with open(textfile, 'rb') as fp:
        # Create a text/plain message
    msg = MIMEText("Some random text") #MIMEText(fp.read())

    # me == the sender's email address
    # you == the recipient's email address
    msg['Subject'] = 'The contents of some file' #% textfile
    msg['From'] = "kara@kit.edu"
    msg['To'] = "mlh-kara@hotmail.com"

    # Send the message via our own SMTP server, but don't include the
    # envelope header.
    s = smtplib.SMTP('localhost')
    s.sendmail("kara@kit.edu", ["mlh-kara@hotmail.com"], msg.as_string())
    s.quit()
