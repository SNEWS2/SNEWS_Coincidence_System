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
from .cs_utils import CommandHandler
from .core.logging import getLogger
from .cs_email import send_email
from .snews_hb import HeartBeat
from .cs_stats import cache_false_alarm_rate
import sys, json

log = getLogger(__name__)


class CoincDecider:

    def __init__(self, env_path=None, use_local_db=True, is_test=True, drop_db=False, firedrill_mode=True,
                 hb_path=None, server_tag=None, send_email=False, send_on_slack=False):
        """Coincidence Decider class constructor

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
        self.send_on_slack = send_on_slack
        self.hype_mode_ON = True
        self.hb_path = hb_path
        self.server_tag = server_tag
        self.storage = Storage(drop_db=drop_db, use_local_db=use_local_db)
        self.topic_type = "CoincidenceTier"
        self.coinc_threshold = float(os.getenv('COINCIDENCE_THRESHOLD'))
        self.cache_expiration = 86400
        self.initial_set = False
        self.alert = AlertPublisher(env_path=env_path, use_local=use_local_db, firedrill_mode=firedrill_mode)
        # self.times = cs_utils.TimeStuff(env_path)
        if firedrill_mode:
            self.observation_topic = os.getenv("FIREDRILL_OBSERVATION_TOPIC")
        else:
            self.observation_topic = os.getenv("OBSERVATION_TOPIC")
        log.info(f"Set observation topic to {self.observation_topic}\n")

        self.column_names = ["_id", "detector_name", "received_time", "machine_time", "neutrino_time",
                             "p_val", "meta", "sub_list_num", "nu_delta_t"]

        self.cache_df = pd.DataFrame(columns=self.column_names)
        self.alert_schema = CoincidenceTierAlert(env_path)
        self.published_alerts = []

        # handle heartbeat
        self.store_heartbeat = bool(os.getenv("STORE_HEARTBEAT", "True"))
        self.heartbeat = HeartBeat(env_path=env_path, firedrill_mode=firedrill_mode)

        self.is_test = is_test
        self.in_coincidence = False
        self.in_list_already = False
        self.stash_time = 86400

    ### This function is not used anywhere, and tries to subtract string from datetime
    # # ------------------------------------------------------------------------------------------------------------------
    # def _is_old_message(self, message):
    #     """
    #     Checks if snews message is too old.
    #
    #     Parameters
    #     ----------
    #     message: dict
    #         incoming SNEWS message
    #
    #     Returns
    #     -------
    #     True is message is older than stash time (24hrs)
    #     """
    #     curr_t = datetime.utcnow().isoformat()
    #     # nu_t = self.times.str_to_datetime(message['neutrino_time'], fmt='%y/%m/%d %H:%M:%S:%f')
    #     nu_t = datetime.fromisoformat(message['neutrino_time'])
    #
    #     del_t = (curr_t - nu_t).total_seconds()
    #     if del_t >= self.stash_time:
    #         return True
    #     else:
    #         return False

    # ------------------------------------------------------------------------------------------------------------------
    def append_message_to_df(self, message, delta_t, sub_list_num):
        """
        Appends cache df when there is a coincident signal

        Parameters
        ----------
        message : `dict`
            dictionary of the SNEWS message
        delta_t : float
            value for time difference between message nu time and initial (0 if message set initial)
        sub_list_num : int
            numeric label for coincidence sub list

        """
        message['sub_list_num'] = sub_list_num
        message['nu_delta_t'] = delta_t
        temp_msg = {}
        for key, val in message.items():
            temp_msg[key] = [val]
        msg_df = pd.DataFrame.from_dict(temp_msg)
        self.cache_df = pd.concat([msg_df, self.cache_df], ignore_index=True)

    # ------------------------------------------------------------------------------------------------------------------
    def reset_df(self):
        """
        Resets coincidence arrays if coincidence is broken

        """
        log.debug("reset_df() called. \n")
        del self.cache_df
        self.cache_df = pd.DataFrame(columns=self.column_names)
        self.initial_set = False
        self.published_alerts = []

    # ------------------------------------------------------------------------------------------------------------------
    def _coincident_with_whole_list(self, message, sub_list_num, ):
        """
        As the name states this backend method checks if the message is coincident
        with the whole sub list (within 10secs of each message)

        Parameters
        ----------
        message: dict
            incoming message
        sub_list_num: int
            number of current sub list that _check_coincidence is on.

        """
        log.debug("Checking coincident with whole list.\n")
        # first check if detector already in sublist
        sub_list = self.cache_df.query(f'sub_list_num=={sub_list_num}')
        sub_list_detectors = list(sub_list['detector_name'])
        if message['detector_name'] in sub_list_detectors:
            self.in_coincidence = True
            log.debug("Message already in list.\n")
            return 'ALREADY_IN_LIST',
        # compare the current nu time with all other on the sublist
        # message_nu_time = self.times.str_to_datetime(message['neutrino_time'], fmt='%y/%m/%d %H:%M:%S:%f')
        message_nu_time = datetime.fromisoformat(message['neutrino_time'])
        nu_times = pd.to_datetime(sub_list.neutrino_time, format="%Y-%m-%dT%H:%M:%S.%f")
        # nu_times = datetime.fromisoformat(sub_list.neutrino_time)
        delta_ts = ((message_nu_time - nu_times).dt.total_seconds()).values  # numpy array
        # check if signal is NOT coincident with the whole list
        if all(abs_del_t > self.coinc_threshold for abs_del_t in np.abs(delta_ts)):
            self.in_coincidence = False
            log.debug("Not coincident with whole list.\n")
            return 'NOT_COINCIDENT',
        # check if signal is coincident with the whole list and arrives earlier
        elif all(0 > del_t >= -self.coinc_threshold for del_t in delta_ts):
            self.in_coincidence = True
            log.debug("Early coincidence found.\n")
            return 'EARLY_COINCIDENT', np.insert(np.sort(np.abs(delta_ts)), 0, 0)
        # check if signal is coincident with the whole list
        elif all(abs_del_t <= self.coinc_threshold for abs_del_t in np.abs(delta_ts)):
            self.in_coincidence = True
            log.debug("Coincidence found.\n")
            return 'COINCIDENT', delta_ts[0]
        # not sure if I need this
        else:
            self.in_coincidence = False
            log.debug("No coincidence event found.")
            return 'NOT_COINCIDENT',

    # ------------------------------------------------------------------------------------------------------------------
    def _concat_to_cache(self, new_list, new_sub_list_num):
        """ Performs a concat on cache df ( appends the new sub list into it, bottom join)

        Parameters
        ----------
        new_list: pandas DataFrame
            new sub list
        new_sub_list_num: int
            label of new list

        """
        self.cache_df = pd.concat([self.cache_df.query(f'sub_list_num!={new_sub_list_num}'), new_list],
                                  ignore_index=True)

    # ------------------------------------------------------------------------------------------------------------------
    def _nu_delta_organizer(self, df):
        main_temp = pd.DataFrame()
        for sub_list in list(df['sub_list_num'].unique()):
            temp = df.query(f'sub_list_num=={sub_list}')
            temp = temp.sort_values(by='neutrino_time').reset_index(drop=True)
            new_delta = []
            new_initial = temp['neutrino_time'][0]
            for time in temp['neutrino_time']:
                new_delta.append(
                    (datetime.fromisoformat(time) - datetime.fromisoformat(new_initial)).total_seconds())
            temp['nu_delta_t'] = new_delta
            main_temp = pd.concat([main_temp, temp])
        main_temp.sort_values(by=['sub_list_num', 'neutrino_time'], ascending=False, inplace=True)
        main_temp.reset_index(drop=True, inplace=True)
        return main_temp

    # ------------------------------------------------------------------------------------------------------------------
    def _new_list_find_coincidences(self, message, new_sub_list):
        """
        Checks for coincident messages with the new sub_list.
        Compares nu times to the sub list's initial nu time.

        (1)If a message is coincident with the ENTIRE new sub list,
            has an eralier time than the initial nu time:
            The early coincident time message will have its nu time
            as the initial time for the list. The message is appended to the sub list.
            A new set of nu_delta_t 's is maded and passed to the sub list.
            Finally, the sublist is sorted by 'nu times' and the indeces are rest

        (2)If a message is coincident with the ENTIRE new sub list:
            Message is appended to the sub list.
            Then sub list is concatenated to the cache

        (3)If a message is not coincident it is ignored.

        Parameters
        ----------
        message: `dict`
            input observation message
        new_sub_list: `int`
            label of new sub list

        """

        other_df = self.cache_df.query(f'sub_list_num!={new_sub_list}').sort_values(by='neutrino_time').drop_duplicates(
            subset=['_id'])
        detector_name = message['detector_name']
        initial_time = datetime.fromisoformat(message['neutrino_time'])
        other_df = other_df.query(f'detector_name != "{detector_name}"')
        new_list = self.cache_df.query(f'sub_list_num=={new_sub_list}')

        for index, row in other_df.iterrows():
            if row['detector_name'] in list(new_list['detector_name']):
                continue
            nu_time = datetime.fromisoformat(row['neutrino_time'])
            del_t = (nu_time - initial_time).total_seconds()
            # (1)
            # make sure the signal is not an initial that its nu is earlier than new list's initial
            if float(row['nu_delta_t']) != 0 and (0 > del_t >= -self.coinc_threshold):
                if self._coincident_with_whole_list(message=row.copy(deep=False),
                                                    sub_list_num=new_sub_list, )[0] == 'EARLY_COINCIDENT':
                    initial_time = nu_time
                    new_row = row.copy(deep=False)
                    new_row['sub_list_num'] = new_sub_list
                    new_list = new_list.append(new_row, ignore_index=True)
                    new_nu_time = pd.to_datetime(new_list.neutrino_time, format='%y/%m/%d %H:%M:%S:%f')
                    new_list['nu_delta_t'] = ((new_nu_time - initial_time).dt.total_seconds()).values
                    new_list = new_list.sort_values(by='neutrino_time')
                    self._concat_to_cache(new_list, new_sub_list)
                else:
                    pass
            # (2)
            if 0 < del_t <= self.coinc_threshold:
                # print(self._coincident_with_whole_list(message=row.copy(deep=False),
                #                                        sub_list_num=new_sub_list, ))
                if self._coincident_with_whole_list(message=row.copy(deep=False),
                                                    sub_list_num=new_sub_list, )[0] == 'COINCIDENT':
                    new_row = row.copy(deep=False)
                    new_row['sub_list_num'] = new_sub_list
                    new_row['nu_delta_t'] = del_t
                    new_list = new_list.append(new_row, ignore_index=True)
                    new_list = new_list.sort_values(by='neutrino_time')
                    self._concat_to_cache(new_list, new_sub_list)
                else:
                    pass
            # (3)
            else:
                continue
        # reset the index
        self.cache_df = self.cache_df.reset_index(drop=True)

    # ------------------------------------------------------------------------------------------------------------------
    def _dump_redundant_list(self):
        """Gets rid of a sub list if the whole list is contained in anther list.
        """
        for sub_list in list(self.cache_df['sub_list_num'].unique()):
            curr_ids = list(self.cache_df.query(f'sub_list_num=={sub_list}')['_id'])
            for other_sub_list in list(self.cache_df['sub_list_num'].unique()):
                other_ids = list(self.cache_df.query(f'sub_list_num=={other_sub_list}')['_id'])
                if sub_list == other_sub_list:
                    continue
                elif len(curr_ids) < len(other_ids) and set(curr_ids).issubset(other_ids):
                    curr_index = list(self.cache_df.query(f'sub_list_num=={sub_list}').index)
                    self.cache_df = self.cache_df.drop(curr_index)
                    self.cache_df = self.cache_df.reset_index(drop=True)

    # ------------------------------------------------------------------------------------------------------------------
    def _check_sub_lists(self, message, sub_list):
        """Checks _coincident_with_whole_list's return and applies actions on sub list (append, give new initial time)

        Parameters
        ----------
        message: dict
            incoming message
        sub_list: int
            current sub list that _check_coincidence is on


        """
        log.debug("Checking sub lists.\n")
        coinc_with_list = self._coincident_with_whole_list(message, sub_list)
        if coinc_with_list[0] == 'ALREADY_IN_LIST':
            self.in_list_already = True
            log.debug("Message already in list.\n")
            pass
        elif coinc_with_list[0] == 'NOT_COINCIDENT':
            log.debug("Not coincident.\n")
            pass
        elif coinc_with_list[0] == 'COINCIDENT':
            # only when COINCIDENT delta_t is float
            delta_t = coinc_with_list[1]
            self.append_message_to_df(message, delta_t, sub_list)
            msg = f"Appending message {message} to sub_list {sub_list} with delta_t {delta_t}."
            log.debug(msg)


        elif coinc_with_list[0] == 'EARLY_COINCIDENT':
            self.append_message_to_df(message, 0, sub_list)
            _sub_list = self.cache_df.query(f'sub_list_num=={sub_list}')
            _sub_list = _sub_list.sort_values(by='neutrino_time')  # first sort! dt's are sorted too
            # re-assign dt making the early message first
            _sub_list['nu_delta_t'] = coinc_with_list[1]
            self.cache_df = pd.concat([_sub_list, self.cache_df.query(f'sub_list_num!={sub_list}')],
                                      ignore_index=True)
            self.cache_df = self.cache_df.reset_index(drop=True)
            log.debug(f"Early coincidence in sublist {sub_list}.\n")

    # ------------------------------------------------------------------------------------------------------------------
    def _check_coincidence(self, message, ):
        """
        Parent method of coincidence, calls a support methods to check if a message is coincident.
        Appending actions are made by support methods.
        (1) Checks subs lists to see if messages is coincident with the whole list.
        (2) If messages is not coincident with any lists make a new subs list and looks for other messages
            in the cache to append to the new sub list.
        (3) If a new detector has been added to any subs list proceed to send an alert (both SNEWS alert and Slack post)

        Parameters
        ----------
        message: dict
            incoming message

        """
        if len(self.cache_df) == 0:
            self.append_message_to_df(message, 0, 0)
            pass

        subs_list_nums = list(self.cache_df['sub_list_num'].unique())
        self.in_coincidence = False
        self.in_list_already = False
        # (1)
        for sub_list in subs_list_nums:
            self._check_sub_lists(message=message, sub_list=sub_list)
        # (2)
        if not self.in_coincidence:
            # print('making  new list')
            new_sub_list = max(subs_list_nums,
                               default=-1) + 1  # -1 is there for the very first msg when sub_list_nums empty
            self.append_message_to_df(message, 0, new_sub_list)
            self._new_list_find_coincidences(message=message, new_sub_list=new_sub_list)
        # (3)
        # unique detector has been added to a sub_list
        # else:
        #     # not coincidence but maybe already in list
        if not self.in_list_already:
            self._dump_redundant_list()
            # self.cache_df = self.cache_df.sort_values(by=['sub_list_num', 'received_time'])
            self.cache_df = self._nu_delta_organizer(self.cache_df)
            self.hype_mode_publish()
            # self.display_table()

    # ----------------------------------------------------------------------------------------------------------------
    def display_table(self):
        """
        Display each sub list individually using a markdown table and sends to slack channel.

        """
        click.secho(
            f'Here is the current coincident table\n',
            fg='magenta', bold=True, )
        for sub_list in self.cache_df['sub_list_num'].unique():
            sub_df = self.cache_df.query(f'sub_list_num=={sub_list}')
            sub_df = sub_df.drop(columns=['meta', 'machine_time', 'schema_version'])
            sub_df = sub_df.sort_values(by=['neutrino_time'])
            # snews_bot.send_table(sub_df) # no need to print the table on the server. Logs have the full content
            print(sub_df.to_markdown())
            print('=' * 168)

    # ------------------------------------------------------------------------------------------------------------------
    def hype_mode_publish(self):
        """
        This method will publish an alert every time a new detector
        submits an observation message

        """
        click.secho(f'{"=" * 100}', fg='bright_red')
        for sub_list in list(self.cache_df['sub_list_num'].unique()):
            _sub_df = self.cache_df.query(f'sub_list_num=={sub_list}')
            if len(_sub_df) <= 1:
                continue
            p_vals = _sub_df['p_val'].to_list()
            p_vals_avg = _sub_df['p_val'].mean()
            nu_times = _sub_df['neutrino_time'].to_list()
            detector_names = _sub_df['detector_name'].to_list()
            false_alarm_prob = cache_false_alarm_rate(cache_sub_list=_sub_df, hb_cache=self.heartbeat.cache_df)
            alert_data = cs_utils.data_cs_alert(p_vals=p_vals, p_val_avg=p_vals_avg, sub_list_num=sub_list,
                                                nu_times=nu_times, detector_names=detector_names,
                                                false_alarm_prob=false_alarm_prob, server_tag=self.server_tag)

            with self.alert as pub:
                alert = self.alert_schema.get_cs_alert_schema(data=alert_data)
                hashvalue = hash(json.dumps(alert['neutrino_times']))
                if hashvalue in self.published_alerts:
                    ## This alert has already been published
                    continue
                print('> We got something publishing an alert !')
                pub.send(alert)
                self.published_alerts.append(hashvalue)
                if self.send_email:
                    send_email(alert)
                if self.send_on_slack:
                    try:
                        snews_bot.send_table(_sub_df, self.is_test, alert_data, self.observation_topic)
                    except Exception as e:
                        print(f"Bot failed to send slack message \n{e}")

                click.secho(f'{"NEW COINCIDENT DETECTOR.. ".upper():^100}', bg='bright_green', fg='red')
                click.secho(f'{"Published an Alert!!!".upper():^100}\n', bg='bright_green', fg='red')
                click.secho(f'{"=" * 100}', fg='bright_red')

    # ------------------------------------------------------------------------------------------------------------------
    def run_coincidence(self):
        try:
            self._run_coincidence()
        except Exception as e:
            print("\n\n?>>>>>",e)
            self.run_coincidence()

    def _run_coincidence(self):
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
                    self.storage.insert_mgs(snews_message)
                    click.secho(f'{"-" * 57}', fg='bright_blue')
                    self._check_coincidence(message=snews_message)
                    sys.stdout.flush()
