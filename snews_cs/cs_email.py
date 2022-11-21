
# https://unix.stackexchange.com/questions/381131/simplest-way-to-send-mail-with-image-attachment-from-command-line-using-gmail

import os
from datetime import datetime
import json
from .core.logging import getLogger

log = getLogger(__name__)

sender = os.getenv("snews_sender_email")
password = os.getenv("snews_sender_pass")
beats_path = os.path.join(os.path.dirname(__file__), "../beats")

contact_list_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'auxiliary/contact_list.json'))
with open(contact_list_file) as file:
    contact_list = json.load(file)


def send_email(alert_content):
    # echo "This is not the same message as before" | mail -s "Echo test email" someone@example.com
    pretty_alert = ''
    for k, v in alert_content.items():
        space = 40 - len(k)
        pretty_alert += f'{k} :{r" "*space}{v}\n'
    # emails = 'sebastiantorreslara17@gmail.com'
    emails = 'snews2-test-ahabig@d.umn.edu'
    os.system( f'echo "{pretty_alert}"| mail -s "SNEWS COINCIDENCE {datetime.utcnow().isoformat()}" {emails}')
    log.info(f"\t\t> SNEWS Alert mail was sent at {datetime.utcnow().isoformat()} to {emails}")


# def send_mailto_contact(detector_name, content):
#     contacts = contact_list["emails"][detector_name]
#     if len(contacts)>0:
#         for contact in contacts:
#             os.system(f'echo "{content}"| mail -s "SNEWS COINCIDENCE {datetime.utcnow().isoformat()}" {contact}')
#         log.info(f"\t\t> SNEWS Feedback mail was sent at {datetime.utcnow().isoformat()} to {', '.join(contacts)}")


base_msg="'smtps://$USER:$PASSWORD@smtp.gmail.com' mutt " \
         "  -F /dev/null " \
         "  -e 'set from=%sender' " \
         "  -e 'set smtp_url=$SMTP_URL' " \
         "  -s 'SNEWS Server Feedback for %detector' " \
         "  -a %attachment --  " \
         "  %contact << EOM " \
         " Dear %detector, Attached %attachment please find the feedback information," \
         " %message_content" \
         " Provided by the SNEWS server. --Cheers EOM"

def send_feedback_mail(detector, attachment, message_content=None):
    contacts = contact_list["emails"][detector]
    mail = ""
    message_content = message_content or ""
    if len(contacts) > 0:
        for contact in contacts:
            mail = base_msg.format(sender=sender,
                                   detector=detector,
                                   attachment=os.path.join(beats_path, attachment),
                                   contact=contact,
                                   message_content=message_content)
            os.system(mail)
        log.info(f"\t\t> Feedback Sent to {contacts}\n"
                 f"\n{mail}\n")


base_warning="'smtps://user:password@smtp.gmail.com' mutt " \
             "  -F /dev/null " \
             "  -e 'set from=%sender' " \
             "  -e 'set smtp_url=$SMTP_URL' " \
             "  -s 'SNEWS Server Heartbeat for %detector is skipped!' " \
             "  %contact << EOM " \
             " Dear %detector, " \
             " %message_content" \
             " Provided by the SNEWS server. --Cheers EOM"

def send_warning_mail(detector, message_content=None):
    contacts = contact_list["emails"][detector]
    mail = ""
    message_content = message_content or ""
    if len(contacts) > 0:
        for contact in contacts:
            mail = base_msg.format(sender=sender,
                                   detector=detector,
                                   contact=contact,
                                   message_content=message_content)
            os.system(mail)
        log.info(f"\t\t> Warning Sent to {contacts}\n"
                 f"\n{mail}\n")

