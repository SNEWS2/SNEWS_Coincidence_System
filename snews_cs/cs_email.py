import os
from datetime import datetime


def current_time():
    return datetime.utcnow().strftime("%y/%m/%d %H:%M:%S:%f")


def send_email(alert_content):
    # echo "This is not the same message as before" | mail -s "Echo test email" someone@example.com
    pretty_alert = ''
    for k, v in alert_content.items():
        space = 40 - len(k)
        pretty_alert += f'{k} :{" "*space}{v}\n'
    # emails = 'sebastiantorreslara17@gmail.com'
    emails = 'snews2-test-ahabig@d.umn.edu'
    os.system(
        f'echo "{pretty_alert}"| mail -s "SNEWS COINCIDENCE {current_time()}" {emails}')
