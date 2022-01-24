# Authors:
# Melih Kara, Karlsruhe Institute of Technology
# make sure you have the slackAPI installed pip install slack_sdk
# https://api.slack.com/reference/surfaces/formatting
# TODO: Later, use threaded options to add new incoming messages to same alert

from slack_sdk import WebClient
import os
from . import snews_utils

snews_utils.set_env()
slack_token = os.getenv('SLACK_TOKEN')
client = WebClient(slack_token)
broker = os.getenv("HOP_BROKER")
alert_topic = os.getenv("ALERT_TOPIC")
slack_channel_id = os.getenv("slack_channel_id")

def get_image(is_test=True):
    tag = '<!here>' if not is_test else ' '
    im = \
        [{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*SUPERNOVA ALERT*".center(50, '=')+f"\n{tag}"
            }
        },
            {
                "type": "image",
                "image_url": "https://raw.githubusercontent.com/SNEWS2/hop-SNalert-app/snews2_dev/hop_comms/auxiliary/snalert.gif",
                "alt_text": "snews-alert"
            },
        ]
    return im


# it is going to be two subsequent messages
# Later we can brush this up
# TODO: for tables it complains
# UserWarning: The `text` argument is missing in the request payload for a chat.postMessage call -
# It's a best practice to always provide a `text` argument when posting a message.
# The `text` argument is used in places where content cannot be rendered such as: system push notifications,
# assistive technology such as screen readers, etc.
def send_table(df, is_test=True):
    table = df.to_markdown(tablefmt="grid")
    image_block = get_image(is_test)
    client.chat_postMessage(channel=slack_channel_id, blocks=image_block)
    client.chat_postMessage(channel=slack_channel_id, text=f'```{table}```')
