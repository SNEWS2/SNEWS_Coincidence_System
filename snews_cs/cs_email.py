
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

### SNEWS Alert
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


def _mail_sender(mails):
    """ Send the emails generated
        Try sending via mutt, if doesn't work try regular `mail` app
    """
    for i, mail in enumerate(mails):
        # try sending either of the mails
        try:
            os.system(mail)
            # break if successful
            log.info(f"\t\t\t> mail successfully sent.")
            break
        except Exception as e:
            log.error(f"\t> {i} attempt didn't work {e}\n")

### FEEDBACK EMAIL
base_msg_mutt = "'smtps://$USER:$PASSWORD@smtp.gmail.com' mutt " \
                 "  -F /dev/null " \
                 "  -e 'set from={sender}' " \
                 "  -e 'set smtp_url=$SMTP_URL' " \
                 "  -s 'SNEWS Server Feedback for %detector' " \
                 "  -a {attachment} --  " \
                 "  {contact} << EOM " \
                 " Dear {detector}, Attached {attachment} please find the feedback information," \
                 " {message_content}" \
                 " Provided by the SNEWS server. --Cheers EOM"

base_msg = "echo {message_content} | " \
           "mail " \
           "-s 'SNEWS COINCIDENCE {timenow}' " \
           "-A {attachment}" \
           "{contact}"

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
            # format messages
            mail_mutt = base_msg_mutt.format(sender=sender,
                                            detector=detector,
                                            attachment=os.path.join(beats_path, attachment),
                                            contact=contact,
                                            message_content=message_content)
            time = datetime.utcnow().isoformat()
            mail_regular = base_msg.format(message_content=message_content,
                                           timenow=time,
                                           attachment=attachment,
                                           contact=contact)

            log.info(f"\t\t> Trying to send feedback to {contact} for {detector}")
            _mail_sender([mail_mutt, mail_regular])
    else:
        log.info(f"\t\t> Feedback mail is requested for {detector}. However, there are no contacts added.")

### Send WARNING message
base_warning_mutt = "'smtps://user:password@smtp.gmail.com' mutt " \
                     "  -F /dev/null " \
                     "  -e 'set from={sender}' " \
                     "  -e 'set smtp_url=$SMTP_URL' " \
                     "  -s 'SNEWS Server Heartbeat for {detector} is skipped!' " \
                     "  {contact} << EOM " \
                     " Dear {detector}, " \
                     " {message_content}" \
                     " Provided by the SNEWS server. --Cheers EOM"

base_warning = "echo {message_content} | " \
               "mail " \
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
            mail_mutt = base_warning_mutt.format(sender=sender,
                                                 detector=detector,
                                                 contact=contact,
                                                 message_content=message_content)
            mail_regular = base_warning.format(message_content=message_content,
                                               detector=detector,
                                               contact=contact)
            log.info(f"\t\t> Trying to send warning to {contact} for {detector}\n")
            _mail_sender([mail_mutt, mail_regular])
    else:
        log.info(f"\t\t> Warning is triggered for {detector}. However, there are no contacts added.")

#### sudo apt-get install sendmail

