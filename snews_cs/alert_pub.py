"""
An interface for SNEWS alert publisher

Created:
August 2021
Authors:
Melih Kara
Sebastian Torres-Lara
"""

import os
import click
from hop import Stream
from . import cs_utils


class AlertPublisher:
    """Class to publish SNEWS SuperNova Alerts based on coincidence"""

    def __init__(
        self, env_path=None, verbose=True, auth=True, firedrill_mode=True, is_test=False
    ):
        """
        Alert publisher constructor
        Parameters
        ----------
        env_path: str
            path to env file, defaults to
        verbose: bool
            Show alert, defaults to True
        auth: bool
            Use hop-auth credentials, defaults to True
        """
        cs_utils.set_env(env_path)
        self.auth = auth
        self.broker = os.getenv("HOP_BROKER")
        if firedrill_mode:
            self.alert_topic = os.getenv("FIREDRILL_ALERT_TOPIC")
        else:
            self.alert_topic = os.getenv("ALERT_TOPIC")
        self.verbose = verbose

        if is_test:
            # use a test topic
            self.alert_topic = os.getenv("CONNECTION_TEST_TOPIC")

    def __enter__(self):
        self.stream = Stream(until_eos=True, auth=self.auth).open(self.alert_topic, "w")
        return self

    def __exit__(self, *args):
        self.stream.close()

    def send(self, message):
        """This method will set the sent_time and send the message to the hop broker.

        Parameters
        ----------
        message: dict
            dict containing observation message.

        """
        self.stream.write(message)
        self.display_message(message)

    def display_message(self, message):
        if self.verbose:
            tier = "TEST ALERT"
            click.secho(f'{"-" * 64}', fg="bright_blue")
            click.secho(f"Sending {tier}", fg="bright_red")
            for k, v in message.items():
                print(f"{k:<35s}:{v}")


# Display message prints out the following on the server logs
#
# SNEWS_Coincidence_ALERT 2022-09-28T08:21:21.954651
# ----------------------------------------------------------------
# Sending TEST ALERT
#
# maybe we can get rid of "Sending TEST ALERT" message
