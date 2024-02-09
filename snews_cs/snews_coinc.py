from . import cs_utils
from .snews_db import Storage
import os, click
from datetime import datetime
from .alert_pub import AlertPublisher, AlertListener
import numpy as np
import pandas as pd
from multiprocessing import Value
from hop import Stream
from . import snews_bot
from .cs_alert_schema import CoincidenceTierAlert
from .cs_remote_commands import CommandHandler
from .core.logging import getLogger
from .cs_email import send_email
from .snews_hb import HeartBeat
from .cs_stats import cache_false_alarm_rate
import sys
import random
import time
import adc.errors

log = getLogger(__name__)


# TODO: duplicate for a test-cache. Do not drop actual cache each time there are tests
class CoincidenceDataHandler:
    """
    This class handles all the incoming data to the SNEWS CS Cache,
    adding messages, organizing sub-groups, retractions and updating cache entries
    """

    def __init__(self):
        self.cache = pd.DataFrame(columns=[
            "_id", "detector_name", "received_time", "machine_time", "neutrino_time",
            'neutrino_time_as_datetime',
            "p_val", "meta", "sub_group", "neutrino_time_delta"])
        self.old_count = self.cache.groupby(by='sub_group').size()
        self.updated = []
        self.msg_state = None

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
        if 'retract_latest' in message.keys():
            print('RETRACTING MESSAGE FROM')
            self.cache_retraction(retraction_message=message)
            return None # break if message is meant for retraction
        message['neutrino_time_as_datetime'] = datetime.fromisoformat(message['neutrino_time'])
        # update
        if message['detector_name'] in self.cache['detector_name'].to_list():
            self._update_message(message)
        # regular add
        else:
            self._manage_cache(message)
            self.cache = self.cache.sort_values(by=['sub_group', 'neutrino_time_delta'], ignore_index=True)
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
        if len(self.cache) == 0:
            print('Initial Message!!')
            message['neutrino_time_delta'] = 0
            message['sub_group'] = 0
            temp = pd.DataFrame([message])
            self.cache = pd.concat([self.cache, temp], ignore_index=True)
        else:
            self._check_coinc_in_subgroups(message)

    def _check_coinc_in_subgroups(self, message):
        """ This method either:

            A)Adds Message to an existing sub-group, if coincident with the initial signal


            B) If it is not coincident with any sub groups it creates two new sub groups,
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
        sub_group_tags = self.cache['sub_group'].unique()
        is_coinc = False
        for tag in sub_group_tags:
            sub_cache = self.cache.query('sub_group==@tag')
            sub_cache = sub_cache.reset_index(drop=True)
            sub_ini_t = sub_cache['neutrino_time_as_datetime'][0]
            delta = (message['neutrino_time_as_datetime'] - sub_ini_t).total_seconds()
            if 0 < delta <= 10.0:
                message['sub_group'] = tag
                message['neutrino_time_delta'] = delta
                temp = pd.DataFrame([message])
                self.cache = pd.concat([self.cache, temp], ignore_index=True)
                is_coinc = True
                self.msg_state = 'COINC_MSG'
        if not is_coinc:
            new_ini_t = message['neutrino_time_as_datetime']
            new_sub_tag = len(sub_group_tags)
            message_as_cache = pd.DataFrame([message])
            temp_cache = pd.concat([self.cache, message_as_cache], ignore_index=True)
            temp_cache = temp_cache.drop_duplicates(subset=['detector_name', 'neutrino_time'])
            temp_cache['neutrino_time_delta'] = (
                    pd.to_datetime(temp_cache['neutrino_time_as_datetime']) - new_ini_t).dt.total_seconds()
            # Make two subgroup one for early signal and post
            new_sub_group_early = temp_cache.query('-10 <= neutrino_time_delta <= 0')
            new_sub_group_post = temp_cache.query('0 <= neutrino_time_delta <= 10.0')
            # drop old sub-group col or pandas will scream at you
            new_sub_group_post = new_sub_group_post.drop(columns='sub_group', axis=0)
            new_sub_group_early = new_sub_group_early.drop(columns='sub_group', axis=0)
            # make new sub-group tag
            new_sub_group_early['sub_group'] = new_sub_tag
            new_sub_group_post['sub_group'] = new_sub_tag + 1
            # sort sub-group by nu time
            new_sub_group_early = new_sub_group_early.sort_values(by='neutrino_time_as_datetime')
            new_sub_group_post = new_sub_group_post.sort_values(by='neutrino_time_as_datetime')

            self._organize_cache(sub_group=new_sub_group_post)
            self._organize_cache(sub_group=new_sub_group_early)

    def _check_for_redundancies(self, sub_group):
        """Checks if sub group is redundant
        Parameters
        ----------
        sub_group : dataframe
            New sub group

        Returns
        -------
        bool
            True if sub group is redundant
            False if sub group is unique

        """
        ids = sub_group['_id']

        if len(sub_group) == 1:
            return True
        for sub_tag in self.cache['sub_group'].unique():
            other_sub = self.cache.query('sub_group == @sub_tag')
            check_ids = ids.isin(other_sub['_id'])
            if check_ids.eq(True).all():
                return True
        return False

    def _organize_cache(self, sub_group):
        """
        This method makes sure that the nu_delta_times are not negative,
        recalculates new deltas using the proper initial time

        Parameters
        ----------
        sub_group : dataframe
            Sub group

        """
        if self._check_for_redundancies(sub_group):
            return
        # Checks if initial nu time of new sub cache is < 0
        sub_group = sub_group.reset_index(drop=True)
        if sub_group['neutrino_time_delta'][0] < 0:
            sub_group = self._fix_deltas(sub_df=sub_group)

        self.cache = pd.concat([self.cache, sub_group], ignore_index=True)
        self.cache = self.cache.sort_values(by=['sub_group', 'neutrino_time_as_datetime']).reset_index(drop=True)

    def _fix_deltas(self, sub_df):
        """
        Parameters
        ----------
        sub_df : Dataframe
            Sub cache

        Returns
        -------
        sub_df : Dataframe
            Sub cache with fixed nu time deltas

        """
        initial_time = sub_df['neutrino_time_as_datetime'].min()
        sub_df = sub_df.drop(columns='neutrino_time_delta', axis=0)
        sub_df['neutrino_time_delta'] = (
                pd.to_datetime(sub_df['neutrino_time_as_datetime']) - initial_time).dt.total_seconds()
        sub_df = sub_df.sort_values(by=['neutrino_time_as_datetime'])
        return sub_df

    def _update_message(self, message):
        """ This method updates the p_val and neutrino of a detector in cache.

        Parameters
        ----------
        message : dict
            SNEWS message

        Returns
        -------

        """
        self.msg_state = 'UPDATE'
        update_detector = message["detector_name"]
        update_message = f'\t> UPDATING MESSAGE FROM: {update_detector}'
        log.info(update_message)
        detector_ind = self.cache.query(f'detector_name==@update_detector').index.to_list()
        # old_nu_times = self.cache['neutrino_time_as_datetime'][detector_ind]
        for ind in detector_ind:
            sub_tag = self.cache['sub_group'][ind]
            initial_time = self.cache.query('sub_group==@sub_tag')['neutrino_time_as_datetime'].min()
            if abs((message['neutrino_time_as_datetime'] - initial_time).total_seconds()) > 10.0:
                continue
            else:
                for key in message.keys():
                    self.cache.at[ind, key] = message[key]
                self.cache.at[ind, 'neutrino_time_delta'] = (
                        message['neutrino_time_as_datetime'] - initial_time).total_seconds()
                self.updated.append(self.cache['sub_group'][ind])

        if len(self.updated) != 0:
            sub_tags = self.cache['sub_group'].unique()
            sub_tags = [i for i in sub_tags if i in self.updated]
            print(sub_tags)
            for sub_tag in sub_tags:
                sub_df = self.cache.query('sub_group == @sub_tag')
                self.cache = self.cache.query('sub_group != @sub_tag')
                sub_df = self._fix_deltas(sub_df)
                self.cache = pd.concat([self.cache, sub_df], ignore_index=True)
                self.cache = self.cache.sort_values(
                    by=['sub_group', 'neutrino_time_as_datetime']).reset_index(drop=True)

    def cache_retraction(self, retraction_message):
        """
        Parameters
        ----------
        retraction_message : dict
            SNEWS retraction message

        """
        self.msg_state = 'RETRACTION'
        retracted_name = retraction_message['detector_name']
        self.cache = self.cache.query('detector_name!=@retracted_name')
        logstr = retracted_name
        # in case retracted message was an initial
        if len(self.cache) == 0:
            return 0
        for sub_tag in self.cache['sub_group'].unique():
            other_sub = self.cache.query('sub_group == @sub_tag')
            if other_sub['neutrino_time_delta'].min() != 0.0:
                if len(other_sub) == 1:
                    other_sub = other_sub.drop(columns=['neutrino_time_delta'])
                    other_sub['neutrino_time_delta'] = [0]

                else:
                    new_initial_time = pd.to_datetime(other_sub['neutrino_time_as_datetime'].min())
                    other_sub = other_sub.drop(columns=['neutrino_time_delta'])
                    other_sub['neutrino_time_delta'] = (pd.to_datetime(
                        other_sub['neutrino_time_as_datetime']) - new_initial_time).dt.total_seconds()
                self.cache = self.cache.query('sub_group!=@sub_tag')
                self.cache = pd.concat([self.cache, other_sub], ignore_index=True)
                self.cache = self.cache.sort_values(by='neutrino_time').reset_index()
        log.info(f"\t> Retracted {logstr}")


class CoincidenceDistributor:

    def __init__(self, replicationleader:Value, env_path=None, use_local_db=True, drop_db=False, firedrill_mode=True,
                 hb_path=None, server_tag=None, send_email=False, send_slack=True, remotecomm=False):
        """This class is in charge of sending alerts to SNEWS when CS is triggered

        Parameters
        ----------
        env_path : `str`
            path to env file, defaults to '/auxiliary/test-config.env'
        use_local_db: `bool`
            tells CoincDecider to use local MongoClient, defaults to True
        send_slack: `bool`
            Whether to send alerts on slack

        """
        log.debug("Initializing CoincDecider\n")
        cs_utils.set_env(env_path)
# mwl
#        self.send_email = send_email
        self.send_email = False
#        self.send_slack = send_slack
        self.send_slack = False
        self.hype_mode_ON = True
        self.hb_path = hb_path
        self.server_tag = server_tag
        #
        # ['leader', 'follower']
        # mp threadlock object, use .value() to get value
        self.replicationleader = replicationleader

        self.storage = Storage(drop_db=drop_db, use_local_db=use_local_db)
        self.topic_type = "CoincidenceTier"
        self.coinc_threshold = float(os.getenv('COINCIDENCE_THRESHOLD'))
        self.cache_expiration = 86400
        self.pub_state = None
        # Some Kafka errors are retryable.
        self.retriable_error_count = 0
        self.max_retriable_errors = 20
        self.exit_on_error = True
        self.initial_set = False
        self.alert = AlertPublisher(env_path=env_path, use_local=use_local_db, firedrill_mode=firedrill_mode)
#        self.alert_listener = AlertListener(env_path=env_path, use_local=use_local_db, firedrill_mode=firedrill_mode)
        if firedrill_mode:
            self.observation_topic = os.getenv("FIREDRILL_OBSERVATION_TOPIC")
            self.alert_topic = os.getenv("FIREDRILL_ALERT_TOPIC")
        else:
            self.observation_topic = os.getenv("OBSERVATION_TOPIC")
            self.alert_topic = os.getenv("ALERT_TOPIC")
        self.alert_schema = CoincidenceTierAlert(env_path)
        # handle heartbeat
        self.store_heartbeat = bool(os.getenv("STORE_HEARTBEAT", "True"))
        self.heartbeat = HeartBeat(env_path=env_path, firedrill_mode=firedrill_mode)

        self.stash_time = 86400
        self.coinc_data = CoincidenceDataHandler()

    def clear_cache(self):
        """ When a reset cache is passed, recreate the
            CoincidenceDataHandler instance

        """
        log.info("\t > [RESET] Resetting the cache.")
        del self.coinc_data
        self.coinc_data = CoincidenceDataHandler()

    # ----------------------------------------------------------------------------------------------------------------
    def display_table(self):
        """
        Display each sub list individually using a markdown table.

        """
        click.secho(
            f'Here is the current coincident table\n',
            fg='magenta', bold=True, )
        for sub_list in self.coinc_data.cache['sub_group'].unique():
            sub_df = self.coinc_data.cache.query(f'sub_group=={sub_list}')
            sub_df = sub_df.drop(columns=['meta', 'machine_time', 'schema_version', 'neutrino_time_as_datetime'])
            sub_df = sub_df.sort_values(by=['neutrino_time'])
            # snews_bot.send_table(sub_df) # no need to print the table on the server. Logs have the full content
            print(sub_df.to_markdown())
            print('=' * 168)

    def update_message_alert(self):
        """
        This method will send out an alert if the CoincidenceData

        """
        if len(self.coinc_data.updated) == 0:
            pass
        else:
            alert_type = 'UPDATE'

            log.debug('\t> An UPDATE message is received')
            for updated_sub in self.coinc_data.updated:
                _sub_df = self.coinc_data.cache.query('sub_group==@updated_sub')
                p_vals = _sub_df['p_val'].to_list()
                p_vals_avg = np.round(_sub_df['p_val'].mean(), decimals=5)
                nu_times = _sub_df['neutrino_time'].to_list()
                detector_names = _sub_df['detector_name'].to_list()
                false_alarm_prob = cache_false_alarm_rate(cache_sub_list=_sub_df, hb_cache=self.heartbeat.cache_df)

                alert_data = dict(p_vals=p_vals,
                                  p_val_avg=p_vals_avg,
                                  sub_list_num=updated_sub,
                                  neutrino_times=nu_times,
                                  detector_names=detector_names,
                                  false_alarm_prob=false_alarm_prob,
                                  server_tag=self.server_tag,
                                  alert_type=alert_type)
                if False and self.announcealert(alert_data):
                    with self.alert as pub:
                        alert = self.alert_schema.get_cs_alert_schema(data=alert_data)
                        pub.send(alert)
                        if self.send_email:
                            send_email(alert)
                        if self.send_slack:
                            snews_bot.send_table(alert_data,
                                                 alert,
                                                 is_test=True,
                                                 topic=self.observation_topic)

                    log.debug('\t> An alert is updated!')
                else:
                    log.debug('\t> An alert is updated, but not sent since I am not the replication leader!')

            self.coinc_data.updated = []


    def announcealert(self, live_alert: dict) -> bool:
        """
            Decide if this server instance should announce the coincidence alert.

            Perhaps dropping time resolution to seconds (or tenths) and hashing the contents of the message would
            be a better way of comparing?
        """
        last_announced_alert = self.storage.get_coincidence_tier_archive()[-1]

        # Should we also calculate how long since the last alert?
        #
        return ( set(last_announced_alert.detector_names) != set(live_alert.detector_names)
                 and set(last_announced_alert.neutrino_times) != set(live_alert.neutrino_times)
                 and set(last_announced_alert.p_vals) != set(live_alert.p_vals)
                 ) or self.replicationleader.value


    # ------------------------------------------------------------------------------------------------------------------
    def hype_mode_publish(self):
        """
        This method will publish an alert every time a new detector
        submits an observation message

        """
        click.secho(f'{"=" * 100}', fg='bright_red')
        new_count = self.coinc_data.cache.groupby(by='sub_group').size()
        for _sub_group, new_message_count in (new_count - self.coinc_data.old_count).iteritems():
            if new_message_count == 0:
                continue
            if self.coinc_data.msg_state == 'RETRACTION':
                continue
            _sub_df = self.coinc_data.cache.query('sub_group==@_sub_group')
            # if empty, new_message_count returns a NaN
            if len(_sub_df['detector_name']) == 1 and (new_message_count == 1 or pd.isna(new_message_count)):
                alert_type = 'INITIAL MESSAGE'
                log.debug(f'\t> Initial message in sub group:{_sub_group}')
            else:
                alert_type = 'NEW_MESSAGE'
                click.secho(f'{"NEW COINCIDENT DETECTOR.. ".upper():^100}', bg='bright_green', fg='red')
                click.secho(f'{"Published an Alert!!!".upper():^100}', bg='bright_green', fg='red')
                click.secho(f'{"=" * 100}', fg='bright_red')

            if alert_type != 'INITIAL MESSAGE':
                p_vals = _sub_df['p_val'].to_list()
                p_vals_avg = np.round(_sub_df['p_val'].mean(), decimals=5)
                nu_times = _sub_df['neutrino_time'].to_list()
                detector_names = _sub_df['detector_name'].to_list()
                false_alarm_prob = cache_false_alarm_rate(cache_sub_list=_sub_df, hb_cache=self.heartbeat.cache_df)

                alert_data = dict(p_vals=p_vals,
                                  p_val_avg=p_vals_avg,
                                  sub_list_num=_sub_group,
                                  neutrino_times=nu_times,
                                  detector_names=detector_names,
                                  false_alarm_prob=false_alarm_prob,
                                  server_tag=self.server_tag,
                                  alert_type=alert_type)
                if False and self.announcealert(alert_data):
                    with self.alert as pub:
                        alert = self.alert_schema.get_cs_alert_schema(data=alert_data)
                        pub.send(alert)
                        if self.send_email:
                            send_email(alert)
                        if self.send_slack:
                            snews_bot.send_table(alert_data,
                                                 alert,
                                                 is_test=True,
                                                 topic=self.observation_topic)

                    log.info(f"\t> An alert was published: {alert_type} !")
                else:
                    log.info(f"\t> An alert would have been published, but I am not the replication leader: {alert_type} !")

        self.coinc_data.old_count = new_count

    # ------------------------------------------------------------------------------------------------------------------
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
                    click.secho(f'{datetime.utcnow().isoformat()} (re)Initializing Coincidence System for '
                                f'{self.observation_topic}\n')
                    for snews_message in s:
                        # check for the hop version
                        try:
                            snews_message = snews_message.content
                        except Exception as e:
                            log.error(f"A message with older hop version is found. {e}\n{snews_message}")
                            snews_message = snews_message
                        # handle the input message
                        handler = CommandHandler(snews_message)
                        # if a coincidence tier message (or retraction) run through the logic
                        if handler.handle(self):
                            snews_message['received_time'] = datetime.utcnow().isoformat()
                            click.secho(f'{"-" * 57}', fg='bright_blue')
                            self.coinc_data.add_to_cache(message=snews_message)
                            # self.display_table() ## don't display on the server
                            self.hype_mode_publish()
                            self.update_message_alert()
                            self.storage.insert_mgs(snews_message)
                            sys.stdout.flush()

                        # for each read message reduce the retriable err count
                        if self.retriable_error_count > 1:
                            self.retriable_error_count -= 1

            # if there is a KafkaException, check if retriable
            except adc.errors.KafkaException as e:
                if e.retriable:
                    self.retriable_error_count += 1
                    if self.retriable_error_count >= self.max_retriable_errors:
                        log.error(f"Max retryable errors exceeded. Here is the most recent exception:\n{e}\n")
                        fatal_error = True
                    else:
                        log.error(f"Retryable error! \n{e}\n")
                        # sleep with exponential backoff and a bit of jitter.
                        time.sleep((1.5 ** self.retriable_error_count) * (1 + random.random()) / 2)
                else:
                    log.error(
                        f"(1) Something crashed the server, not a retriable error, here is the Exception raised\n{e}\n")
                    fatal_error = True

            # any other exception is logged, but not fatal (?)
            except Exception as e:
                log.error(f"(2) Something crashed the server, here is the Exception raised\n{e}\n")
                fatal_error = False  # True # maybe not a fatal error?

            finally:
                # if we are breaking on errors and there is a fatal error, break
                if self.exit_on_error and fatal_error:
                    break
                # otherwise continue by re-initiating
                continue
