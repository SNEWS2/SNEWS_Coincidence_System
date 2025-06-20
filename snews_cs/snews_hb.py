"""
This a module to handle all heartbeat related work

"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

from .core.logging import getLogger
from .cs_utils import set_env
from .database import Database

log = getLogger(__name__)

# Check if detector name is in registered list.
detector_file = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "etc/detector_properties.json")
)

with open(detector_file) as file:
    snews_detectors = json.load(file)

snews_detectors = list(snews_detectors.keys())
beats_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../beats"))


# TODO: make a list of internal heartbeats, and send us = SERVER heartbeats.
# How many times a day can server log these heartbeats, (what is the livetime of server)


def sanity_checks(message):
    """check if the message will crash the server
    Check  the following
         - detector frequencies are reasonable
         - latencies are reasonable
         - At least one detector is operational
    """
    for key in ["detector_name", "detector_status"]:
        if key not in message.keys():
            log.error(
                f"\t> {message} is received at snews_hb.py but not valid.\n"
                f"{key} is not in message keys"
            )
            return False

    if not isinstance(message["received_time_utc"], np.datetime64):
        log.error(
            f"\t> {message} is received at snews_hb.py but not valid.\n"
            f"{message['received_time']} is not a np.datetime64 object"
        )
        return False

    if not message["detector_status"].lower() in ["on", "off"]:
        log.error(
            f"\t> {message} is received at snews_hb.py but not valid.\n"
            f"{message['detector_status']} is neither ON nor OFF"
        )
        return False

    if not message["detector_name"] in snews_detectors:
        log.error(
            f"\t> {message} is received at snews_hb.py but not valid.\n"
            f"{message['detector_name']} is not a valid detector"
        )
        return False

    return True


class HeartBeat:
    """Class to handle heartbeat message stream"""

    def __init__(self, env_path=None, store=True, firedrill_mode=True):
        """
        :param store: `bool`
        """
        log.info("\t> Heartbeat Instance is created.")
        set_env(env_path)
        self.store = store

        self.delete_after = float(os.getenv("HB_DELETE_AFTER", "7"))  # days

        if firedrill_mode:
            self.heartbeat_topic = os.getenv("FIREDRILL_OBSERVATION_TOPIC")
        else:
            self.heartbeat_topic = os.getenv("OBSERVATION_TOPIC")

        self.column_names = [
            "received_time_utc",
            "detector",
            "stamped_time_utc",
            "latency",
            "time_after_last",
            "status",
        ]

        # Update the alert cache
        self.cache_engine = Database(
            db_file_path=Path(__file__).parent.parent / "snews_cs.db"
        ).engine
        self.cache_df = None

        try:
            # Try reading cached data from SQL DB
            self.cache_df = pd.read_sql_table(
                "cached_heartbeats",
                self.cache_engine,
                parse_dates=["received_time_utc", "stamped_time_utc"],
            )
        except Exception:
            # Fall-through if cache does not exist; create it
            self.cache_df = pd.DataFrame(columns=self.column_names)

        self._last_row = pd.DataFrame(columns=self.column_names)

    def make_entry(self, message):
        """Make an entry in the cache df using new message
        # NOTE:
        since we create last row separately, the sequence matters
        """
        msg = {
            "received_time_utc": message["received_time_utc"],
            "detector": message["detector_name"],
        }

        msg["stamped_time_utc"] = np.datetime64(message["sent_time_utc"])
        msg["latency"] = (
            (msg["received_time_utc"] - msg["stamped_time_utc"])
            .astype("timedelta64[s]")
            .astype(int)
        )

        # check the last message of given detector
        detector_df = self.cache_df[self.cache_df["detector"] == msg["detector"]]
        if len(detector_df):
            msg["time_after_last"] = (
                msg["received_time_utc"] - detector_df["received_time_utc"].max()
            ).total_seconds()
        else:
            msg["time_after_last"] = 0  # timedelta(0)

        msg["status"] = message["detector_status"]
        self._last_row = pd.DataFrame([msg])
        # add this new entry to cache
        if len(self.cache_df) == 0:
            self.cache_df = self._last_row
        else:
            self.cache_df = pd.concat([self.cache_df, self._last_row], ignore_index=True)

    def drop_old_messages(self):
        """Keep the heartbeats for a time period delta.
        Drop the earlier messages from cache
        """
        delta = f"{self.delete_after} day"

        curr_time = np.datetime64(datetime.utcnow().isoformat())  # pd.to_datetime('now', utc=True)
        delta_t = curr_time - pd.to_datetime(self.cache_df["received_time_utc"])
        select = delta_t < pd.Timedelta(delta)

        self.cache_df = self.cache_df[select]
        self.cache_df.sort_values(by=["received_time_utc"], inplace=True)

    def update_cache(self):
        """Update the alert cache with whatever we decided to keep."""
        self.cache_df.to_sql(
            "cached_heartbeats", self.cache_engine, if_exists="replace", index=False
        )

    def display_table(self):
        """When printed out, these table can be read from the purdue servers
        -only- by admins.

        """
        print(f"\nCurrent cache \n{'=' * 133}\n{self.cache_df.to_markdown()}\n{'=' * 133}\n")

    def electrocardiogram(self, message):
        try:
            now_utc = datetime.now(UTC)
            # Convert to timezone-naive
            now_naive = now_utc.replace(tzinfo=None)
            message["received_time_utc"] = np.datetime64(now_naive)
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
