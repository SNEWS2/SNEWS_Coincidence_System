# Authors:
# Melih Kara, Karlsruhe Institute of Technology
# make sure you have the slackAPI installed pip install slack_sdk
# https://api.slack.com/reference/surfaces/formatting

from slack_sdk import WebClient
import os
import snews_utils
from hop import Stream
# import hop_sub
snews_utils.set_env()

slack_token = os.getenv('SLACK_TOKEN')
client = WebClient(slack_token)
broker = os.getenv("HOP_BROKER")
alert_topic = os.getenv("ALERT_TOPIC")
slack_channel_id = os.getenv("slack_channel_id")

pl = \
    [{
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*SUPERNOVA ALERT*".center(50,'=')
            }
        },
		{
			"type": "image",
			"image_url": "https://raw.githubusercontent.com/SNEWS2/hop-SNalert-app/snews2_dev/hop_comms/auxiliary/snalert.gif",
			"alt_text": "snews-alert"
		},
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "test"
            }
        }
	]

def format_messages(message):
    msg = f"\n\t\t *SUPERNOVA ALERT* <!here>\n" \
    f">- The Alert ID: {message['_id']}\n" \
    f">- :satellite_antenna: Detector Events {', '.join([i for i in message['detector_events']])}\n" \
    f">- :clock8: Sent Time `{message['sent_time']}`\n"\
    f">- :boom: Neutrino times `{'`, `'.join([i for i in message['neutrino_times']])}`"
    return msg


def call_slack(p):
    pl[2]['text']['text'] = p
    client.chat_postMessage(channel=slack_channel_id, blocks=pl)

stream = Stream(persist=True)
with stream.open(alert_topic, "r") as s:
    for message in s:
        fmt_msg = format_messages(message)
        print(fmt_msg)
        call_slack(fmt_msg)
