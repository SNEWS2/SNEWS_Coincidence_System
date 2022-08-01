import os
from datetime import datetime


def current_time():
    return datetime.utcnow().strftime("%y/%m/%d %H:%M:%S:%f")


def send_email(alert_content):
    # echo "This is not the same message as before" | mail -s "Echo test email" someone@example.com
    pretty_alert = ''
    for key, val in alert_content.items():
        pretty_alert += f'"{key}" :\t{val}\n'
    # emails = 'ahabig@d.umn.edu, sebastiantorreslara17@gmail.com, joesmolsky@gmail.com, mlh-kara@hotmail.com'
    emails = 'snews2-test-ahabig@d.umn.edu'
    os.system(
        f'echo "{pretty_alert}"| mail -s "SNEWS COINCIDENCE {current_time()}" {emails}')
