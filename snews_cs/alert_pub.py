"""
An interface for SNEWS member experiment 
to publish their observation and heartbeat messages.

Created: 
August 2021
Authors: 
Melih Kara
Sebastian Torres-Lara
"""
import hop, sys, time, os, json, click
from hop import Stream
from datetime import datetime
from collections import namedtuple
from dotenv import load_dotenv
from . import cs_utils
from .snews_db import Storage


class AlertPublisher:
    """ Class to publish SNEWS SuperNova Alerts based on coincidence
    
    Notes
    -----
    Only relevant for the server
    """
    def __init__(self, env_path=None, verbose=True, auth=True, use_local=False):
        cs_utils.set_env(env_path)
        self.auth = auth
        self.broker = os.getenv("HOP_BROKER")
        self.alert_topic = os.getenv("ALERT_TOPIC")
        self.times = cs_utils.TimeStuff()
        self.verbose = verbose
        self.time_str = lambda: self.times.get_snews_time()
        self.storage = Storage(drop_db=False, use_local_db=use_local)

    def __enter__(self):
        self.stream = Stream(until_eos=True, auth=self.auth).open(self.alert_topic, 'w')
        return self

    def __exit__(self, *args):
        self.stream.close()

    def send(self, messages):
        """This method will set the sent_time and send the message to the hop broker.

        Parameters
        ----------
        messages: list
            list containing observation message.

        """
    
        self.stream.write(message)
        self.display_message(message)

    def display_message(self, message):
        if self.verbose:
            tier = message['_id'].split('_')[1]
            click.secho(f'{"-" * 64}', fg='bright_blue')
            click.secho(f'Sending {tier}', fg='bright_red')
            for k, v in message.items():
                print(f'{k:<20s}:{v}')
                
                
                
    # def publish_retraction(self, retracted_mgs):
    #     """
    #     Takes retracted alert and publishes it.
    #
    #     Parameters
    #     ----------
    #     retracted_mgs: 'dict'
    #         Retracted alert message
    #     """
    #     stream = Stream(until_eos=True)
    #     with stream.open(self.alert_topic, "w") as s:
    #         s.write(retracted_mgs)
