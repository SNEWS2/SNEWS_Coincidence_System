# Authors:
# Melih Kara, Karlsruhe Institute of Technology
# make sure you have the slackAPI installed pip install slack_sdk
# https://api.slack.com/reference/surfaces/formatting
# TODO: Later, use threaded options to add new incoming messages to same alert

from slack_sdk import WebClient
import os
from . import cs_utils
from .cs_alert_schema import CoincidenceTierAlert
from .core.logging import getLogger
import warnings
import pandas as pd

log = getLogger(__name__)

cs_utils.set_env()
slack_token = os.getenv('SLACK_TOKEN')
client = WebClient(slack_token)
broker = os.getenv("HOP_BROKER")
alert_topic = os.getenv("ALERT_TOPIC")
slack_channel_id = os.getenv("slack_channel_id")
alert_schema = CoincidenceTierAlert()

def get_image(is_test, alert_data, topic):
    ## parse input
    tag = '<!here>\n' if not is_test else '\n'
    test = "TEST" if is_test else ""
    topic_str = f"\n> Broker: {topic.center(50,'-')}"

    alert_data = alert_data or dict(server_tag="Unknown Server",
                                    alert_type="Unkown",
                                    _id="Unknown ID",
                                    false_alarm_prob="Unknown")
    # update = True if "UPDATE" in alert_data['_id'] else False
    # update = "UPDATE" if update else ""
    alert_type = alert_data['alert_type']
    server = alert_data['server_tag']
    falseprob = alert_data['False Alarm Prob']

    header = f"{test} *SUPERNOVA ALERT* {alert_type}".center(60, '=')+topic_str+f"{tag}" + \
             f"> False Alarm Probability= *{falseprob}*\n> Issued from {server}"
    giflink = "https://raw.githubusercontent.com/SNEWS2/SNEWS_Coincidence_System/main/snews_cs/etc/snalert.gif"
    retractlink = "https://www.shutterstock.com/image-vector/ooops-word-bubble-pop-art-600w-408777070.jpg"
    updatelink = "https://www.shutterstock.com/image-vector/vector-illustration-modern-label-new-600w-1520423249.jpg"
    # updatelink = "https://raw.githubusercontent.com/SNEWS2/SNEWS_Coincidence_System/main/snews_cs/etc/update_image.png"
    #"https://www.ris.world/wp-content/uploads/2018/09/update.jpg"
    sendlink = giflink if alert_type=="NEW_MESSAGE" else (updatelink if alert_type=="UPDATE" else retractlink)

    im = \
        [{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": header
            }
        },
        {
            "type": "image",
            "image_url": sendlink,
            "alt_text": "snews-alert"
        },
        {
            "type": "actions",
            "block_id": "actionblock789",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Checkout the SNEWS webpage!"
                            },
                    "url": "http://snews2.org"
                }
                        ]
        }
    ]
    return im


# it is going to be two subsequent messages
# Later we can brush this up
# TODO: for tables it complains
# UserWarning: The `text` argument is missing in the request payload for a chat.postMessage call -
# It's a best practice to always provide a `text` argument when posting a message.
# The `text` argument is used in places where content cannot be rendered such as: system push notifications,
# assistive technology such as screen readers, etc.
def send_table(alert_data, alert, is_test, topic):
    """ send warning on slack.
        Both alert_data (dictionary with info from each detector)
        and the alert (single dict with collected info) are required
    """
    try:
        df = pd.DataFrame.from_dict(alert_data)
        df_simplified = df[["detector_names", "neutrino_times", "p_vals"]]
        df_simplified.sort_values("neutrino_times", inplace=True)
        table = df_simplified.to_markdown() # tablefmt="grid"
        image_block = get_image(is_test, alert, topic)
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore")
            client.chat_postMessage(channel=slack_channel_id, blocks=image_block)
            client.chat_postMessage(channel=slack_channel_id, text=f'```{table}```')
    except Exception as e:
        log.info(f"We ran into slack connection problems. slack message DID NOT go out.")
        log.info(f"Slack Exception:\n {e}")
