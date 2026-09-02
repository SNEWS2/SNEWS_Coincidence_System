import json
import os
import pickle
import random
import sys
import time
from datetime import datetime

import adc.errors
import click
import numpy as np
import pandas as pd
from hop import Stream

from . import cs_utils, snews_bot
from .alert_pub import AlertPublisher
from .core.logging import getLogger
from .cs_alert_schema import CoincidenceTierAlert
from .cs_email import send_email
from .cs_remote_commands import CommandHandler
from .cs_stats import cache_false_alarm_rate
from .snews_hb import HeartBeat
from .snews_sql import Storage

log = getLogger(__name__)

# needs more work. Vectorization converts the datatype to object in the dataframe and crashes
# to_numpy_datetime = lambda x: np.datetime64(x) if not isinstance(x, np.datetime64) else x
# check if they are already numpy datetime64 objects (failsafe)
# t_1, t_2 = to_numpy_datetime(t_1), to_numpy_datetime(t_2)
# @np.vectorize


def np_datetime_delta_sec(t_1, t_2):
    """Return the time difference between two numpy datetime64 objects in seconds
    Returns: float (seconds)
    Notes
    -----
    t_1 is expected to be the earlier time (no absolute value is taken)
    """
    total_seconds = (t_2 - t_1) / np.timedelta64(1, "s")  # Convert to seconds
    return total_seconds


# TODO: duplicate for a test-cache. Do not drop actual cache each time there are tests
class CacheManager:
    """
    This class handles all the incoming data to the SNEWS CS Cache,
    adding messages, organizing sub-groups, retractions and updating cache entries
    """

    def __init__(self):
        # define the col names of the cache df
        self.cache = pd.DataFrame(
            columns=[
                "id",
                "detector_name",
                "received_time",
                "machine_time_utc",
                "neutrino_time_utc",
                "neutrino_time_as_datetime",
                "p_val",
                "meta",
                "sub_group",
                "neutrino_time_delta",
            ]
        )
        # keep track of updated sub groups
        self.updated = []
        self.msg_state = None
        # this dict is used to store the current state of each sub group in the cache,
        # UPDATE, COINCIDENT, None, RETRACTION.
        self.sub_group_state = {}

    def add_to_cache(self, message):
        """
        Takes in SNEWS message and checks if it is a retraction, update or new addition to cache.
        This is the 'core' function of CoincidenceDataHandler
        Parameters
        ----------
        message : dict
            SNEWS Message, must be PT valid

        """
        # retraction
        if "retract_latest" in message.keys():
            print("RETRACTING MESSAGE FROM")
            self.cache_retraction(retraction_message=message)
            return None  # break if message is meant for retraction
        message["neutrino_time_as_datetime"] = np.datetime64(
            message["neutrino_time_utc"]
        )
        # update
        if message["detector_name"] in self.cache["detector_name"].to_list():
            self._update_message(message)
        # regular add
        else:
            self._manage_cache(message)
            self.cache = self.cache.sort_values(
                by=["sub_group", "neutrino_time_delta"], ignore_index=True
            )
            self.cache = self.cache.reset_index(drop=True)

    def _manage_cache(self, message):
        """
        This method will add a new message to cache, checks if:

            A)It is an initial message (to the entire cache) or if it:
            B)Forms a new sub-group (sends message to _check_coinc_in_subgroups)
            C)Is confident to a sub-group (sends message to _check_coinc_in_subgroups)

        Parameters
        ----------
        message

        """

        # if the cache is empty add the message to cache, declare state of sub group 0 as INITIAL
        if len(self.cache) == 0:
            print("Initial Message!!")
            message["neutrino_time_delta"] = 0
            message["sub_group"] = 0
            self.sub_group_state[0] = "INITIAL"
            self.cache = pd.DataFrame([message])
        # if the cache is not empty, check if the message is coincident with other sub groups
        else:
            self._check_coinc_in_subgroups(message)

    def _check_coinc_in_subgroups(self, message):
        """This method either:

            A)Adds Message to an existing sub-group, if coincident with the initial signal


            B) If NOT coincident with any sub groups it creates two new sub groups,
            setting the message as their initial time.
            The new groups consist of coincident signals with earlier arrival time and
            later arrival times, respectively.
            Once created the new groups are checked to see if they are redundant,
            and if so then they are not added to the main cache.

        Parameters
        ----------
        message : dict
            SNEWS message

        """
        # grab the current sub group tags
        sub_group_tags = self.cache["sub_group"].unique()
        #  this boolean declares whether if the message is not coincident
        is_coinc = False
        for tag in sub_group_tags:

            # query the cache, select the current sub group
            sub_cache = self.cache.query("sub_group==@tag")
            #  reset the index, for the sake of keeping things organized
            sub_cache = sub_cache.reset_index(drop=True)
            # select the initial nu time of the sub group
            sub_ini_t = sub_cache["neutrino_time_as_datetime"].min()
            #  make the nu time delta series
            delta = np_datetime_delta_sec(
                t_2=message["neutrino_time_as_datetime"], t_1=sub_ini_t
            )
            #  if the message's nu time is within the coincidence window
            if 0 < delta <= 10.0:
                # to the message add the corresponding sub group and nu time delta
                message["sub_group"] = tag
                message["neutrino_time_delta"] = delta
                # turn the message into a pd df, this is for concatenating it to the cache
                temp = pd.DataFrame([message])
                # concat the message df to the cahce
                self.cache = pd.concat([self.cache, temp], ignore_index=True)
                #  set the message as coinc
                is_coinc = True
                #  declare the state the sub group to COINC_MSG
                self.sub_group_state[tag] = "COINC_MSG"

        # if the message is not coincident with any of the sub groups create a new sub group
        if not is_coinc:
            # set the message's nu time, as the initial nu time
            new_ini_t = message["neutrino_time_as_datetime"]
            # create the sub group tag
            new_sub_tag = len(sub_group_tags)
            #  turn the message into a df
            message_as_cache = pd.DataFrame([message])
            #  create a temp cache concat the message
            temp_cache = pd.concat([self.cache, message_as_cache], ignore_index=True)
            #  drop dublicates of detector name and nu time
            temp_cache = temp_cache.drop_duplicates(
                subset=["detector_name", "neutrino_time_utc"]
            )
            # create  a new time delta
            temp_cache["neutrino_time_delta"] = np_datetime_delta_sec(
                t_1=new_ini_t, t_2=temp_cache["neutrino_time_as_datetime"]
            )
            # Make two subgroup one for early signal and post
            new_sub_group_early = temp_cache.query("-10 <= neutrino_time_delta <= 0")
            new_sub_group_post = temp_cache.query("0 <= neutrino_time_delta <= 10.0")
            # drop old sub-group col or pandas will scream at you
            new_sub_group_post = new_sub_group_post.drop(columns="sub_group", axis=0)
            new_sub_group_early = new_sub_group_early.drop(columns="sub_group", axis=0)
            # make new sub-group tag
            new_sub_group_early["sub_group"] = new_sub_tag
            new_sub_group_post["sub_group"] = int(new_sub_tag + 1)
            # sort sub-group by nu time
            new_sub_group_early = new_sub_group_early.sort_values(
                by="neutrino_time_as_datetime"
            )
            new_sub_group_post = new_sub_group_post.sort_values(
                by="neutrino_time_as_datetime"
            )
            # check if new sub groups are the same:
            # if so, drop the later one
            if (
                new_sub_group_early["id"].to_list()
                == new_sub_group_post["id"].to_list()
            ):
                new_sub_group_post = new_sub_group_post.drop(
                    columns="sub_group", axis=0
                )
                new_sub_group_post["sub_group"] = new_sub_tag
                self._organize_cache(sub_cache=new_sub_group_post)
            #  organize the cache
            else:
                self._organize_cache(sub_cache=new_sub_group_post)
                self._organize_cache(sub_cache=new_sub_group_early)

    def _check_for_redundancies(self, sub_cache):
        """Checks if sub cache is redundant
        Parameters
        ----------
        sub_cache : dataframe
            New sub group

        Returns
        -------
        bool
            True if sub group is redundant
            False if sub cache is unique

        """
        # create a series of the ids in the sub group
        ids = sub_cache["id"]

        # if this sub group only contains a single message and the detector name is already
        # present in the cache return True
        if (
            len(sub_cache) == 1
            and sub_cache["id"].to_list()[0] in self.cache["id"].to_list()
        ):
            return True
        #  loop through the other sub group tags
        for sub_tag in self.cache["sub_group"].unique():
            # save the other sub groups as a df
            other_sub = self.cache.query("sub_group == @sub_tag")
            # check if the current sub group's ids are in the other sub group
            check_ids = ids.isin(other_sub["id"])
            # if the ids are in the other sub group, return True
            if check_ids.eq(True).all():
                return True
        return False

    def _organize_cache(self, sub_cache):
        """
        This method makes sure that the nu_delta_times are not negative,
        recalculates new deltas using the proper initial time

        Parameters
        ----------
        sub_cache : dataframe
            Sub group

        """
        #  if the sub is redundant then return out of the
        if self._check_for_redundancies(sub_cache):
            return
        # for the sake of keeping things organized reset the index of the sub group
        sub_cache = sub_cache.reset_index(drop=True)
        # if the initial nu time is negative then fix it by passing the sub group to fix_deltas
        if sub_cache["neutrino_time_delta"][0] < 0:
            sub_cache = self._fix_deltas(sub_df=sub_cache)
        if len(sub_cache) > 1:
            #  set the state of the sub group to COINC_MSG_STAGGERED
            self.sub_group_state[sub_cache["sub_group"][0]] = "COINC_MSG_STAGGERED"
        else:
            self.sub_group_state[sub_cache["sub_group"][0]] = None
        # concat to the cache
        self.cache = pd.concat([self.cache, sub_cache], ignore_index=True)
        #  sort the values of the cache by their sub group and nu time ( ascending order)
        self.cache = self.cache.sort_values(
            by=["sub_group", "neutrino_time_as_datetime"]
        ).reset_index(drop=True)

    def _fix_deltas(self, sub_df):
        """
        This method fixes the deltas of the sub group by resetting the initial nu time
        Parameters
        ----------
        sub_df : Dataframe
            Sub cache

        Returns
        -------
        sub_df : Dataframe
            Sub cache with fixed nu time deltas

        """
        #  find the new initial nu time
        initial_time = sub_df["neutrino_time_as_datetime"].min()
        #  drop the old delta col
        sub_df = sub_df.drop(columns="neutrino_time_delta", axis=0)
        #  make the new delta col
        sub_df["neutrino_time_delta"] = np_datetime_delta_sec(
            t_1=initial_time, t_2=sub_df["neutrino_time_as_datetime"]
        )
        #  sort the nu times by ascending order
        sub_df = sub_df.sort_values(by=["neutrino_time_as_datetime"])
        return sub_df

    def _update_message(self, message):
        """If triggered this method updates the p_val and neutrino time of a detector in cache.

        Parameters
        ----------
        message : dict
            SNEWS message

        Returns
        -------

        """

        # declare the name of the detector that will be updated
        update_detector = message["detector_name"]
        # announce that an update is happening
        update_message = f"\t> UPDATING MESSAGE FROM: {update_detector}"
        log.info(update_message)
        # get indices of where the detector name is present
        detector_ind = self.cache.query(
            "detector_name==@update_detector"
        ).index.to_list()
        #  loop through the indices
        for ind in detector_ind:
            # get the sub tag
            sub_tag = self.cache["sub_group"][ind]
            #  declare the state of the sub group as UPDATE
            self.sub_group_state[sub_tag] = "UPDATE"
            #  get the initial nu time of the sub group
            initial_time = self.cache.query("sub_group==@sub_tag")[
                "neutrino_time_as_datetime"
            ].min()
            # ignore update if the updated message is outside the coincident window
            if (
                abs(
                    np_datetime_delta_sec(
                        t_2=message["neutrino_time_as_datetime"], t_1=initial_time
                    )
                )
                > 10.0
            ):
                continue
            # update the message if it is coincident with the current sub group
            else:
                #  find the ind to be updated and replace its contents with
                for key in message.keys():
                    self.cache.at[ind, key] = message[key]
                self.cache.at[ind, "neutrino_time_delta"] = np_datetime_delta_sec(
                    t_2=message["neutrino_time_as_datetime"], t_1=initial_time
                )
                # append the updated list
                self.updated.append(self.cache["sub_group"][ind])

        # if there are any updated sub groups reorganize them
        if len(self.updated) != 0:
            # loop through updated sub group list
            for sub_tag in self.updated:
                #  make a sub group df
                sub_df = self.cache.query("sub_group == @sub_tag")
                # dump the unorganized subgroup
                self.cache = self.cache.query("sub_group != @sub_tag")
                # fix deltas of updated sub group
                sub_df = self._fix_deltas(sub_df)
                # concat the organized sub group with the rest of the cache
                self.cache = pd.concat([self.cache, sub_df], ignore_index=True)
                #  sort the values of the cache by sub group nu time
                self.cache = self.cache.sort_values(
                    by=["sub_group", "neutrino_time_as_datetime"]
                ).reset_index(drop=True)

    def cache_retraction(self, retraction_message):
        """
        This method handles message retraction by parsing the cache and dumping any instance
        of the target detector


        Parameters
        ----------
        retraction_message : dict
            SNEWS retraction message

        """

        retracted_name = retraction_message["detector_name"]
        self.cache = self.cache.query("detector_name!=@retracted_name")
        logstr = retracted_name
        # in case retracted message was an initial
        if len(self.cache) == 0:
            return 0
        for sub_tag in self.cache["sub_group"].unique():
            self.sub_group_state[sub_tag] = "RETRACTION"
            other_sub = self.cache.query("sub_group == @sub_tag")
            if other_sub["neutrino_time_delta"].min() != 0.0:
                if len(other_sub) == 1:
                    other_sub = other_sub.drop(columns=["neutrino_time_delta"])
                    other_sub["neutrino_time_delta"] = [0]

                else:
                    # set new initial nu time
                    new_initial_time = other_sub["neutrino_time_as_datetime"].min()
                    # drop the old delta
                    other_sub = other_sub.drop(columns=["neutrino_time_delta"])
                    #  make new delta
                    other_sub["neutrino_time_delta"] = np_datetime_delta_sec(
                        t_2=other_sub["neutrino_time_as_datetime"], t_1=new_initial_time
                    )
                # concat retracted sub group to the cache
                self.cache = self.cache.query("sub_group!=@sub_tag")
                self.cache = pd.concat([self.cache, other_sub], ignore_index=True)
                self.cache = self.cache.sort_values(
                    by="neutrino_time_utc"
                ).reset_index()
            # log retraction to log file
            log.info(f"\t> Retracted {logstr} from sub-group {sub_tag}")


class CoincidenceDistributor:

    def __init__(
        self,
        env_path=None,
        drop_db=False,
        firedrill_mode=True,
        hb_path=None,
        server_tag=None,
        send_email=False,
        send_slack=True,
        show_table=False,
    ):
        """This class is in charge of sending alerts to SNEWS when CS is triggered

        Parameters
        ----------
        env_path : `str`
            path to env file, defaults to '/etc/test-config.env'
        send_slack: `bool`
            Whether to send alerts on slack

        """
        log.debug("Initializing CoincDecider\n")
        cs_utils.set_env(env_path)
        self.show_table = show_table
        self.send_email = send_email
        self.send_slack = send_slack
        self.hb_path = hb_path
        # name of your sever, used for alerts
        self.server_tag = server_tag
        # initialize local MongoDB
        self.storage = Storage(drop_db=drop_db)
        # declare topic type, used for alerts
        self.topic_type = "CoincidenceTier"
        #  from the env var get the coinc thresh, 10sec
        self.coinc_threshold = float(os.getenv("COINCIDENCE_THRESHOLD"))
        # lifetime of case (sec) = 24hr
        self.cache_expiration = 86400
        # Some Kafka errors are retryable.
        self.retriable_error_count = 0
        self.max_retriable_errors = 20
        self.exit_on_error = False  # True
        self.initial_set = False
        self.alert = AlertPublisher(env_path=env_path, firedrill_mode=firedrill_mode)
        self.test_alert = AlertPublisher(
            env_path=env_path, is_test=True
        )  # overwrites with connection test topic
        if firedrill_mode:
            self.observation_topic = os.getenv("FIREDRILL_OBSERVATION_TOPIC")
        else:
            self.observation_topic = os.getenv("OBSERVATION_TOPIC")
        # for testing, the alerts will be sent to this topic
        self.test_topic = os.getenv("CONNECTION_TEST_TOPIC")
        self.alert_schema = CoincidenceTierAlert(env_path)
        # handle heartbeat
        self.store_heartbeat = bool(os.getenv("STORE_HEARTBEAT", "True"))
        self.heartbeat = HeartBeat(env_path=env_path, firedrill_mode=firedrill_mode)

        self.stash_time = 86400
        self.coinc_data = CacheManager()
        self.test_coinc_data = CacheManager()  # a separate cache for testing
        self.message_count = {}
        self.test_message_count = {}
        # don't use a storage for the test cache

    def clear_cache(self, is_test=False):
        """When a reset cache is passed, recreate the
        CoincidenceDataHandler instance

        """
        if not is_test:
            log.info("\t > [RESET] Resetting the cache.")
            del self.coinc_data
            self.coinc_data = CacheManager()
        else:
            del self.test_coinc_data
            self.test_coinc_data = CacheManager()

    # ----------------------------------------------------------------------------------------------------------------
    def display_table(self, is_test=False):
        """
        Display each sub list individually using a markdown table.

        """
        click.secho(
            "Here is the current coincident table\n",
            fg="magenta",
            bold=True,
        )
        if not is_test:
            cache_data = self.coinc_data
        else:
            cache_data = self.test_coinc_data
        for sub_list in cache_data.cache["sub_group"].unique():
            sub_df = cache_data.cache.query(f"sub_group=={sub_list}")
            sub_df = sub_df.drop(
                columns=[
                    "meta",
                    "machine_time_utc",
                    "schema_version",
                    "neutrino_time_as_datetime",
                ]
            )
            sub_df = sub_df.sort_values(by=["neutrino_time_utc"])
            # snews_bot.send_table(sub_df) # no need to print the table on the server.
            # Logs have the full content
            print(sub_df.to_markdown())
            print("=" * 168)

    def send_alert(self, sub_group_tag, alert_type, is_test=False):
        if not is_test:
            sub_df = self.coinc_data.cache.query("sub_group==@sub_group_tag")
            try:
                false_alarm_prob = cache_false_alarm_rate(
                    cache_sub_list=sub_df, hb_cache=self.heartbeat.cache_df
                )
            except Exception:
                false_alarm_prob = "(couldn't compute)"
            alert_publisher = self.alert
        else:
            sub_df = self.test_coinc_data.cache.query("sub_group==@sub_group_tag")
            false_alarm_prob = "N/A"
            alert_publisher = self.test_alert

        p_vals = sub_df["p_val"].to_list()
        p_vals_avg = np.round(sub_df["p_val"].mean(), decimals=5)
        nu_times = sub_df["neutrino_time_utc"].to_list()
        detector_names = sub_df["detector_name"].to_list()
        alert_data = dict(
            p_vals=p_vals,
            p_val_avg=p_vals_avg,
            sub_list_num=int(sub_group_tag),
            neutrino_times=nu_times,
            detector_names=detector_names,
            false_alarm_prob=false_alarm_prob,
            server_tag=self.server_tag,
            alert_type=alert_type,
        )

        with alert_publisher as pub:
            alert = self.alert_schema.get_cs_alert_schema(
                data=alert_data, is_test=is_test
            )
            pub.send(alert)
            # only check to see if email or slack should be sent if the alert is not a test alert
            if not is_test:
                if self.send_email:
                    send_email(alert)
                if self.send_slack:
                    snews_bot.send_table(
                        alert_data, alert, is_test=True, topic=self.observation_topic
                    )

    # ------------------------------------------------------------------------------------------------------------------
    def alert_decider(self, is_test=False):
        """
        This method will publish an alert every time a new detector submits an observation message.
        """
        click.secho(f'{"=" * 100}', fg="bright_red")

        # Determine which cache to use
        cache_data = self.test_coinc_data if is_test else self.coinc_data
        _message_count = self.test_message_count if is_test else self.message_count
        log_info = " [TEST] " if is_test else " "

        def publish_alert(sub_group_tag, state, message):
            click.secho(
                f"SUB GROUP {sub_group_tag}: {message:^100}".upper(),
                bg="bright_green",
                fg="red",
            )
            click.secho(
                f'{"Publishing an Alert!!!".upper():^100}', bg="bright_green", fg="red"
            )
            click.secho(f'{"=" * 100}', fg="bright_red")
            log.info(f"\t> {log_info} An alert was published: {state} !")
            self.send_alert(
                sub_group_tag=sub_group_tag, alert_type=state, is_test=is_test
            )

        for sub_group_tag, state in cache_data.sub_group_state.items():
            print(f"CHECKING FOR ALERTS IN SUB GROUP: {sub_group_tag}")

            if state is None:
                print(f"NO ALERTS IN SUB GROUP: {sub_group_tag}")
                continue

            message_count = len(cache_data.cache.query("sub_group==@sub_group_tag"))

            if state == "COINC_MSG_STAGGERED":
                publish_alert(sub_group_tag, state, "COINCIDENT DETECTOR..")

            elif (
                state == "RETRACTION" and message_count < _message_count[sub_group_tag]
            ):
                publish_alert(sub_group_tag, state, "RETRACTION HAS BEEN MADE")

            elif state == "INITIAL":
                log.debug(
                    f"\t> {log_info} Initial message in sub group:{sub_group_tag}"
                )
                click.secho(
                    f'SUB GROUP {sub_group_tag}: {"Initial message received".upper():^100}',
                    bg="bright_green",
                    fg="red",
                )
                click.secho(f'{"=" * 100}', fg="bright_red")

            elif state == "UPDATE" and message_count == _message_count[sub_group_tag]:
                click.secho(
                    f'SUB GROUP {sub_group_tag}: {"A MESSAGE HAS BEEN UPDATED".upper():^100}',
                    bg="bright_green",
                    fg="red",
                )
                log.debug(f"\t> {log_info} An UPDATE message is received")
                if message_count > 1:
                    publish_alert(
                        sub_group_tag, state, "Publishing an updated Alert!!!"
                    )
                    log.debug(f"\t> {log_info} An alert is updated!")

            elif state == "COINC_MSG" and message_count > _message_count[sub_group_tag]:
                publish_alert(sub_group_tag, state, "NEW COINCIDENT DETECTOR..")

    # ------------------------------------------------------------------------------
    def deal_with_the_cache(self, snews_message):
        """
        Check if the message is a test or not,
        then add it to the cache and run the alert decider

        Parameters
        ----------
        snews_message: dict read from the Kafka stream.

        Returns
        -------
            adds messages to cache and runs the coincidence decider
        """
        if "is_test" in snews_message.keys():
            is_test = snews_message["is_test"]
        elif (
            "meta" in snews_message.keys() and "is_test" in snews_message["meta"].keys()
        ):
            is_test = snews_message["meta"]["is_test"]
        else:
            is_test = False

        if not is_test:
            self.coinc_data.add_to_cache(message=snews_message)
            # run the search
            self.alert_decider(is_test=is_test)
            # update message count
            for sub_group_tag in self.coinc_data.cache["sub_group"].unique():
                self.message_count[sub_group_tag] = len(
                    self.coinc_data.cache.query("sub_group==@sub_group_tag")
                )
                self.coinc_data.sub_group_state[sub_group_tag] = None

            self.coinc_data.updated = []
            # do not have a storage for the tests
            if not is_test:
                self.storage.insert_coinc_cache(self.coinc_data.cache)
            sys.stdout.flush()
            self.coinc_data.updated = []
            if self.show_table:
                self.display_table("main")  # don't display on the server
        else:
            self.test_coinc_data.add_to_cache(message=snews_message)
            # run the search
            self.alert_decider(is_test=is_test)
            # update message count
            for sub_group_tag in self.test_coinc_data.cache["sub_group"].unique():
                self.test_message_count[sub_group_tag] = len(
                    self.test_coinc_data.cache.query("sub_group==@sub_group_tag")
                )
                self.test_coinc_data.sub_group_state[sub_group_tag] = None
            self.test_coinc_data.updated = []
            # do not have a storage for the tests
            sys.stdout.flush()
            self.test_coinc_data.updated = []
            if self.show_table:
                self.display_table("test")  # don't display on the server

    # -------------------------------------------------------------------------------------------------------------------
    def run_coincidence(self):
        """
        As the name states this method runs the coincidence system.
        Starts by subscribing to the hop observation_topic.

        * If a CoincidenceTier message is received then it is passed to _check_coincidence.
        * other commands include "test-connection", "test-scenarios",
                "hard-reset", "Retraction",

        ****
        Reconnect logic and retryable errors thanks to Spencer Nelson (https://github.com/spenczar)
        https://github.com/scimma/hop-client/issues/140

        """
        fatal_error = True

        while True:
            try:
                stream = Stream(until_eos=False)
                with stream.open(self.observation_topic, "r") as s:
                    click.secho(
                        f"{datetime.utcnow().isoformat()} (re)Initializing Coincidence System for "
                        f"{self.observation_topic}\n"
                    )
                    for snews_message in s:
                        # check for the hop version
                        try:
                            snews_message = snews_message.content
                        except Exception as e:
                            log.error(
                                f"A message with older hop version is found. {e}\n{snews_message}"
                            )
                            snews_message = snews_message

                        # Unpack the message
                        if type(snews_message) is bytes:
                            snews_message = pickle.loads(snews_message)
                            snews_message = snews_message.model_dump()
                        elif type(snews_message) is str:
                            snews_message = json.loads(snews_message)
                        else:
                            log.error("Message is not parsable:")
                            log.error(snews_message)
                            continue

                        # handle the input message
                        handler = CommandHandler(snews_message)
                        # if a coincidence tier message (or retraction) run through the logic
                        if handler.handle(self):
                            snews_message["received_time"] = np.datetime_as_string(
                                np.datetime64(datetime.utcnow().isoformat()), unit="ns"
                            )
                            # print info on the servers terminal, (important info is logged)
                            terminal_output = click.style(
                                f'{"-" * 57}\n', fg="bright_blue"
                            )
                            terminal_output += click.style(
                                f'{"Coincidence Tier Message Received":^57}\n',
                                fg="bright_blue",
                            )
                            terminal_output += click.style(
                                "\t>"
                                f"{snews_message['detector_name']}"
                                f"{snews_message['received_time']}",
                                fg="bright_blue",
                            )
                            click.secho(terminal_output)
                            # add to cache
                            # if actual observation, use coincidence cache, else if testing use
                            # test cache
                            self.deal_with_the_cache(snews_message)

                        # for each read message reduce the retriable err count
                        if self.retriable_error_count > 1:
                            self.retriable_error_count -= 1

            # handle a keyboard interrupt (ctrl+c)
            except KeyboardInterrupt:
                print("Caught a keyboard interrupt.  Goodbye world!")
                log.error("(2) Caught a keyboard interrupt. Exiting.\n")
                fatal_error = True
                self.exit_on_error = True
                sys.exit(0)

            # if there is a KafkaException, check if retriable
            except adc.errors.KafkaException as e:
                if e.retriable:
                    self.retriable_error_count += 1
                    if self.retriable_error_count >= self.max_retriable_errors:
                        log.error(
                            "Max retryable errors exceeded. "
                            f"Here is the most recent exception:\n{e}\n"
                        )
                        fatal_error = True
                    else:
                        log.error(f"Retryable error! \n{e}\n")
                        # sleep with exponential backoff and a bit of jitter.
                        time.sleep(
                            (1.5**self.retriable_error_count)
                            * (1 + random.random())
                            / 2
                        )
                else:
                    log.error(
                        "(1) Something crashed the server, not a retriable error, "
                        f"here is the Exception raised\n{e}\n"
                    )
                    fatal_error = True

            # any other exception is logged, but not fatal (?)
            except Exception as e:
                log.error(
                    f"(2) Something crashed the server, here is the Exception raised\n{e}\n"
                )
                fatal_error = False  # True # maybe not a fatal error?

            finally:
                # if we are breaking on errors and there is a fatal error, break
                if self.exit_on_error and fatal_error:
                    break
                # otherwise continue by re-initiating
                continue
