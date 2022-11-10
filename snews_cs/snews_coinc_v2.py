from . import cs_utils
from .snews_db import Storage
import os, click
from datetime import datetime
from .alert_pub import AlertPublisher
import numpy as np
import pandas as pd
from hop import Stream
from . import snews_bot
from .cs_alert_schema import CoincidenceTierAlert
# from .cs_utils import CommandHandler
from .cs_remote_commands import CommandHandler
from .core.logging import getLogger
from .cs_email import send_email
from .snews_hb import HeartBeat
from .cs_stats import cache_false_alarm_rate
import sys

log = getLogger(__name__)


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

    def add_to_cache(self, message):
        """
        Takes in SNEWS message and checks if it is a retraction, update or new addition to cache.
        This is the 'core' function of CoincidenceDataHandler

        Parameters
        ----------
        message : dict
            SNEWS Message, must be PT valid


        """
        message['neutrino_time_as_datetime'] = datetime.fromisoformat(message['neutrino_time'])
        # retraction
        if 'n_retract_latest' in message.keys():
            print('RETRACTING MESSAGE FROM')
            self.cache_retraction(retraction_message=message)
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
        This methods will add a new message to cache, checks if:

            A)It is an initial message (to the entire cache) or if it:

            B)Forms a new sub-group (sends message to _check_coinc_in_subgroups)

            C)Is confident to a sub-group (sends message to _check_coinc_in_subgroups)

        Parameters
        ----------
        message

        Returns
        -------

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
            # else:
            #     is_coinc = False
            #     continue
        if not is_coinc:
            new_ini_t = message['neutrino_time_as_datetime']
            new_sub_tag = len(sub_group_tags)
            message_as_cache = pd.DataFrame([message])
            temp_cache = pd.concat([self.cache, message_as_cache], ignore_index=True)
            temp_cache = temp_cache.drop_duplicates(subset=['detector_name', 'neutrino_time'])

            temp_cache['neutrino_time_delta'] = (temp_cache['neutrino_time_as_datetime'] - new_ini_t).dt.total_seconds()
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
        names = sub_group['detector_name']
        t = sub_group['neutrino_time']
        if len(sub_group) == 1:
            return True
        for sub_tag in self.cache['sub_group'].unique():
            other_sub = self.cache.query('sub_group == @sub_tag')
            check_name = names.isin(other_sub['detector_name'])
            check_t = names.isin(other_sub['neutrino_time'])

            all_t_redundant = check_t.eq(True).all()
            all_names_redundant = check_name.eq(True).all()

            if all_names_redundant and all_t_redundant:
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
        sub_df = sub_df.drop(column='neutrino_time_delta', axis=0)
        sub_df['neutrino_time_delta'] = (sub_df['neutrino_time_as_datetime'] - initial_time).dt.total_seconds()
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
        update_message = f'UPDATING MESSAGE FROM: {message["detector_name"]}'

        print(update_message)
        detector_ind = self.cache.query('detector_name==@message["detector_name"]').index.to_list()
        # old_nu_times = self.cache['neutrino_time_as_datetime'][detector_ind]
        for ind in detector_ind:
            sub_tag = self.cache['sub_group'][ind]
            initial_time = self.cache.query('sub_group==@sub_tag')['neutrino_time_as_datetime'].min()
            if abs((message['neutrino_time_as_datetime']-initial_time).total_seconds()) > 10.0:
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
        if retraction_message['retract_id'] is not None:
            self.cache = self.cache.query('_id!=@retraction_message["retract_id"]')
        else:
            retracted_name = retraction_message['detector_name']
            self.cache = self.cache.query('detector_name!=@retracted_name')
        # in case retracted message was an initial
        for sub_tag in self.cache['sub_group'].unique():
            other_sub = self.cache.query('sub_group == @sub_tag')
            if other_sub['neutrino_time_delta'] != 0:
                new_initial_time = other_sub['neutrino_time_as_datetime'].min()
                other_sub = other_sub.drop('neutrino_time_delta', axis=0)
                other_sub['neutrino_time_delta'] = other_sub['neutrino_time_as_datetime'] - new_initial_time
                self.cache = self.cache.query('sub_group!=@sub_tag')
                self.cache = pd.concat([self.cache, other_sub], ignore_index=True)
                self.cache = self.cache.sort_values(by='neutrino_time').reset_index()


class CoincidenceDistributor:

    def __init__(self, env_path=None, use_local_db=True, is_test=True, drop_db=False, firedrill_mode=True,
                 hb_path=None, server_tag=None, send_email=False):
        """This class is in charge of sending alerts to SNEWS when CS is triggered

        Parameters
        ----------
        env_path : str
            path to env file, defaults to '/auxiliary/test-config.env'
        use_local_db:
            tells CoincDecider to use local MongoClient, defaults to True
        is_test: bool
            tells CoincDecider if it's running in test mode,
        """
        log.debug("Initializing CoincDecider\n")
        cs_utils.set_env(env_path)
        self.send_email = send_email
        self.hype_mode_ON = True
        self.hb_path = hb_path
        self.server_tag = server_tag
        self.storage = Storage(drop_db=drop_db, use_local_db=use_local_db)
        self.topic_type = "CoincidenceTier"
        self.coinc_threshold = float(os.getenv('COINCIDENCE_THRESHOLD'))
        self.cache_expiration = 86400
        self.initial_set = False
        self.alert = AlertPublisher(env_path=env_path, use_local=use_local_db, firedrill_mode=firedrill_mode)
        if firedrill_mode:
            self.observation_topic = os.getenv("FIREDRILL_OBSERVATION_TOPIC")
        else:
            self.observation_topic = os.getenv("OBSERVATION_TOPIC")
        self.alert_schema = CoincidenceTierAlert(env_path)
        # handle heartbeat
        self.store_heartbeat = bool(os.getenv("STORE_HEARTBEAT", "True"))
        self.heartbeat = HeartBeat(env_path=env_path, firedrill_mode=firedrill_mode)

        self.is_test = is_test
        self.stash_time = 86400
        self.coinc_data = CoincidenceDataHandler()

    # ----------------------------------------------------------------------------------------------------------------
    def display_table(self):
        """
        Display each sub list individually using a markdown table and sends to slack channel.

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
        This method will send out an alert if the CoincedenceData

        """
        if len(self.coinc_data.updated) == 0:
            pass
        else:
            alert_type = 'UPDATE'
            for updated_sub in self.coinc_data.updated:
                _sub_df = self.coinc_data.cache.query('sub_group==@updated_sub')
                p_vals = _sub_df['p_val'].to_list()
                p_vals_avg = np.round(_sub_df['p_val'].mean(), decimals=5)
                nu_times = _sub_df['neutrino_time'].to_list()
                detector_names = _sub_df['detector_name'].to_list()
                false_alarm_prob = cache_false_alarm_rate(cache_sub_list=_sub_df, hb_cache=self.heartbeat.cache_df)
                alert_data = cs_utils.data_cs_alert(
                    p_vals=p_vals,
                    p_val_avg=p_vals_avg,
                    sub_list_num=updated_sub,
                    nu_times=nu_times,
                    detector_names=detector_names,
                    false_alarm_prob=false_alarm_prob,
                    server_tag=self.server_tag,
                    alert_type=alert_type)

                with self.alert as pub:
                    alert = self.alert_schema.get_cs_alert_schema(data=alert_data)
                    print(alert)
                    pub.send(alert)
                    if self.send_email:
                        send_email(alert)
            self.coinc_data.updated = []

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
            # for sub_list in list(self.coinc_data.cache['sub_list_num'].unique()):
            _sub_df = self.coinc_data.cache.query('sub_group==@_sub_group')
            # self.display_table()
            if len(_sub_df) == 1:
                continue
            click.secho(f'{"NEW COINCIDENT DETECTOR.. ".upper():^100}', bg='bright_green', fg='red')
            click.secho(f'{"Published an Alert!!!".upper():^100}\n', bg='bright_green', fg='red')
            click.secho(f'{"=" * 100}', fg='bright_red')
            p_vals = _sub_df['p_val'].to_list()
            p_vals_avg = np.round(_sub_df['p_val'].mean(), decimals=5)
            nu_times = _sub_df['neutrino_time'].to_list()
            detector_names = _sub_df['detector_name'].to_list()
            false_alarm_prob = cache_false_alarm_rate(cache_sub_list=_sub_df, hb_cache=self.heartbeat.cache_df)
            alert_type = 'NEW_MESSAGE'
            if new_message_count < 0:
                alert_type = 'RETRACTION'
            # logic for update declaration

            alert_data = cs_utils.data_cs_alert(p_vals=p_vals, p_val_avg=p_vals_avg, sub_list_num=_sub_group,
                                                nu_times=nu_times, detector_names=detector_names,
                                                false_alarm_prob=false_alarm_prob, server_tag=self.server_tag,
                                                alert_type=alert_type)

            with self.alert as pub:
                alert = self.alert_schema.get_cs_alert_schema(data=alert_data)
                print(alert)
                pub.send(alert)
                if self.send_email:
                    send_email(alert)

        self.coinc_data.old_count = new_count

    # ------------------------------------------------------------------------------------------------------------------
    def run_coincidence(self):
        """
        As the name states this method runs the coincidence system.
        Starts by subscribing to the hop observation_topic.

        * If a CoincidenceTier message is received then it is passed to _check_coincidence.
        * other commands include "test-connection", "test-scenarios",
                "hard-reset", "Retraction",

        """
        stream = Stream(until_eos=False)
        with stream.open(self.observation_topic, "r") as s:
            _msg = click.style(f'{datetime.utcnow().isoformat()} Running Coincidence System for '
                               f'{self.observation_topic}\n')
            print(_msg)
            for snews_message in s:
                handler = CommandHandler(snews_message)
                try:
                    go = handler.handle(self)
                except Exception as e:
                    log.debug(f"Something crashed the server, here is the Exception raised\n{e}\n")
                    go = False
                if go:
                    snews_message['received_time'] = datetime.utcnow().isoformat()
                    click.secho(f'{"-" * 57}', fg='bright_blue')
                    self.coinc_data.add_to_cache(message=snews_message)
                    self.display_table()
                    self.hype_mode_publish()
                    self.update_message_alert()
                    self.storage.insert_mgs(snews_message)
                    sys.stdout.flush()
