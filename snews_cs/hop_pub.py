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
from . import snews_utils
from .cs_alert_schema import Alert_Schema
from .snews_db import Storage


class Publish_Alert:
    """ Class to publish SNEWS SuperNova Alerts based on coincidence
    
    Notes
    -----
    Only relevant for the server
    """

    def __init__(self, env_path=None, use_local=False):
        self.env = env_path
        snews_utils.set_env(env_path)
        self.broker = os.getenv("HOP_BROKER")
        self.alert_topic = os.getenv("ALERT_TOPIC")
        self.times = snews_utils.TimeStuff(env_path)
        self.time_str = lambda: self.times.get_snews_time()
        self.storage = Storage(drop_db=False, use_local_db=use_local)

    # decider should call this
    def publish(self, msg_type, data):
        """ Publish alert message
            This function should only be called by the
            CoincDecider class when a coincidence between
            different observations trigger an alert

        Parameters
        ----------
        msg_type : `str`
            Type (Tier) of the message. Has to be one of 
            the 'CoincidenceTierAlert', 'SigTierAlert', 'TimeTierAlert'
        data : `dict`
            Data dictionary received from snews_utils.data_alert()

        """
        schema = Alert_Schema(self.env)
        sent_time = self.times.get_snews_time()
        alert_schema = schema.get_cs_alert_schema(msg_type=msg_type, sent_time=sent_time, data=data)

        stream = Stream(persist=False)
        with stream.open(self.alert_topic, "w") as s:
            s.write(alert_schema)
            self.storage.insert_mgs(alert_schema)
        # for k, v in alert_schema.items():
        #     print(f'{k:<20s}:{v}')

    def publish_retraction(self, retracted_mgs):
        """
        Takes retracted alert and publishes it.

        Parameters
        ----------
        retracted_mgs: 'dict'
            Retracted alert message
        """
        stream = Stream(persist=False)
        with stream.open(self.alert_topic, "w") as s:
            s.write(retracted_mgs)
