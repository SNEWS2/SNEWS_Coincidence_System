"""
This a module to handle all heartbeat related work

"""

import os, json
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

from .cs_utils import set_env, make_beat_directory
from .core.logging import getLogger

log = getLogger(__name__)

# Check if detector name is in registered list.
detector_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'etc/detector_properties.json'))

with open(detector_file) as file:
    snews_detectors = json.load(file)

snews_detectors = list(snews_detectors.keys())

#- Cache of old heartbeats
beats_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../beats"))
cache_db = os.path.abspath(os.path.join(beats_path, 'cached_heartbeats_mirror.db'))

def get_data_strings(df_input):
    """ Convert datetime objects to strings

    """
    df = df_input.copy()
    type_dtime = type(datetime.utcnow())
    for col in df.columns:
        for i in range(len(df)):
            if type(df[col].iloc[i]) == type_dtime:
                # print(col, i, df[col].iloc[i])
                df.at[i, col] = df.at[i, col].isotime()
    return df

def to_numpy_datetime(input_datetime):
    if isinstance(input_datetime, (str, np.datetime64, pd.Timestamp, datetime)):
        # If the input is already a string or NumPy datetime64, return it as is
        return np.datetime64(input_datetime)
    else:
        raise ValueError("Unsupported datetime type. Supported types: str, np.datetime64, pd.Timestamp, datetime.datetime")



## TODO: make a list of internal heartbeats, and send us = SERVER heartbeats.
# How many times a day can server log these heartbeats, (what is the livetime of server)

def sanity_checks(message):
    """ check if the message will crash the server
        Check  the following
             - detector frequencies are reasonable
             - latencies are reasonable
             - At least one detector is operational
    """
    issue = "no issue"
    for key in ['detector_name', 'detector_status']:
        if key not in message.keys():
           issue = f" {key} is not in message keys"
    if issue == "no issue":
        if not isinstance(message['Received Times'], np.datetime64):
            issue = f" {message['Received Times']} is not a np.datetime64 object"
        if not message['detector_status'].lower() in ['on','off']:
            issue = f" {message['detector_status']} is neither ON nor OFF"
        if not message['detector_name'] in snews_detectors:
            issue = f" {message['detector_name']} is not a valid detector"

    if issue == "no issue":
        # do not log each time
        return True
    else:
        log.error(f"\t> {message} is received at snews_hb.py but not valid.\n"
                  f"issue is: {issue}")
        return False


class HeartBeat:
    """ Class to handle heartbeat message stream
    """

    def __init__(self, env_path=None, store=True, firedrill_mode=True):
        """
        :param store: `bool`
        """
        log.info("\t> Heartbeat Instance is created.")
        set_env(env_path)
        self.store = store
        log.info(f"\t> Heartbeats are stored in {beats_path}.")
        make_beat_directory(beats_path)
        self.delete_after = float(os.getenv("HB_DELETE_AFTER", "7"))  # days
        if firedrill_mode:
            self.heartbeat_topic = os.getenv("FIREDRILL_OBSERVATION_TOPIC")
        else:
            self.heartbeat_topic = os.getenv("OBSERVATION_TOPIC")

        self.column_names = ["Received Times", "Detector", "Stamped Times", "Latency", "Time After Last", "Status"]

        #- Update the alert cache
        self.cache_engine = create_engine(f'sqlite:///{cache_db}')
        self.cache_df = None
        try:
            #- Try reading cached data from SQL DB
            self.cache_df = pd.read_sql_table('cached_heartbeats', self.cache_engine, parse_dates=['Received Times', 'Stamped Times'])
        except:
            #- Fall-through if cache does not exist; create it
            self.cache_df = pd.DataFrame(columns=self.column_names)

        self._last_row = pd.DataFrame(columns=self.column_names) #pd.Series(index=self.column_names)

    def make_entry(self, message):
        """ Make an entry in the cache df using new message
            # NOTE:
            since we create last row separately, the sequence matters
        """
        msg = {"Received Times": message["Received Times"],
               "Detector": message["detector_name"]}

        # "sent_time" in the message has the following format;
        #  message["sent_time"] = np.datetime_as_string(np.datetime64(datetime.utcnow().isoformat()), unit='ns')
        stamped_time_obj = np.datetime64(message["sent_time"])
        msg["Stamped Times"] = stamped_time_obj
        msg["Latency"] = msg["Received Times"] - msg["Stamped Times"]
        # keep latency as integer seconds
        msg["Latency"] = msg["Latency"].astype('timedelta64[s]').astype(int)

        # check the last message of given detector
        detector_df = self.cache_df[self.cache_df["Detector"] == msg['Detector']]
        if len(detector_df):
            msg["Time After Last"] = (msg["Received Times"] - detector_df["Received Times"].max()).total_seconds()
        else:
            msg["Time After Last"] = 0 #timedelta(0)

        msg["Status"] = message["detector_status"]
        self._last_row = pd.DataFrame([msg])
        # add this new entry to cache
        if len(self.cache_df) == 0:
            self.cache_df = self._last_row
        else:
            self.cache_df = pd.concat([self.cache_df, self._last_row], ignore_index=True)

    def drop_old_messages(self):
        """ Keep the heartbeats for a time period delta.
            Drop the earlier messages from cache
        """
        delta = f'{self.delete_after} day'

        curr_time = np.datetime64(datetime.utcnow().isoformat()) #pd.to_datetime('now', utc=True)
        delta_t = curr_time - pd.to_datetime(self.cache_df['Received Times'])
        select = delta_t < pd.Timedelta(delta)

        self.cache_df = self.cache_df[select]
        self.cache_df.sort_values(by=['Received Times'], inplace=True)

    def update_cache(self):
        """Update the alert cache with whatever we decided to keep.
        """
        self.cache_df.to_sql('cached_heartbeats', self.cache_engine, if_exists='replace', index=False)

    def display_table(self):
        """ When printed out, these table can be read from the purdue servers
            -only- by admins.

        """
        print(f"\nCurrent cache \n{'=' * 133}\n{self.cache_df.to_markdown()}\n{'=' * 133}\n")

    def electrocardiogram(self, message):
        try:
            message["Received Times"] = np.datetime64(datetime.utcnow().isoformat()) #pd.to_datetime('now', utc=True)
            if sanity_checks(message):
                self.make_entry(message)
                self.update_cache()
                self.drop_old_messages()
                # if all successful, return True. Not logging each time, not to overcrowd
                return True
            else:
                return False
        except Exception as e:
            log.error(f"\t Some heartbeats didn't make it\n{e}\n")
            print(f"Something went terribly wrong \n {e}")
            return False
