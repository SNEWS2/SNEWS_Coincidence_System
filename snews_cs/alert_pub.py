"""
An interface for SNEWS alert publisher

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

    """
    def __init__(self, env_path=None, verbose=True, auth=True, use_local=False, firedrill_mode=True):
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
        use_local: bool
            Use local MongoClient, defaults to True
        """
        cs_utils.set_env(env_path)
        self.auth = auth
        self.broker = os.getenv("HOP_BROKER")
        if firedrill_mode:
            self.alert_topic = os.getenv("FIREDRILL_ALERT_TOPIC")
        else:
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
            print(message['_id'])
            tier = 'TEST ALERT'
            click.secho(f'{"-" * 64}', fg='bright_blue')
            click.secho(f'Sending {tier}', fg='bright_red')
            for k, v in message.items():
                print(f'{k:<20s}:{v}')
                
