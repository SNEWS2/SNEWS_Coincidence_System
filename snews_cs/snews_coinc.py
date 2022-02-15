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

class CoincDecider:
    """ CoincDecider class for Supernova alerts (Coincidence Tier)
        
    Parameters
    ----------
    env_path : `str`, optional
        user can give the path a specific SNEWS env file, 
        defaults to None ./auxiliary/test-config.env)
    
    """

    def __init__(self, env_path=None, use_local_db=False, hype_mode_ON=True, is_test=True):
        cs_utils.set_env(env_path)
        self.hype_mode_ON = hype_mode_ON
        self.storage = Storage(drop_db=False, use_local_db=use_local_db)
        self.topic_type = "CoincidenceTier"
        self.coinc_threshold = float(os.getenv('COINCIDENCE_THRESHOLD'))
        self.cache_expiration = 86400
        self.coinc_cache = self.storage.coincidence_tier_cache
        self.alert = AlertPublisher(env_path=env_path,use_local=use_local_db)
        self.times = cs_utils.TimeStuff(env_path)
        self.observation_topic = os.getenv("OBSERVATION_TOPIC")
        self.column_names = ["_id", "detector_name", "received_time", "machine_time", "neutrino_time",
                             "p_value", "meta", "sub_list_num", "nu_delta_t"]

        self.cache_df = pd.DataFrame(columns=self.column_names)
        self.alert_schema = CoincidenceTierAlert(env_path)

        self.is_test = is_test
        self.in_coincidence = False
        self.in_list_already = False
        self.secs_in_day = 86400

    # ------------------------------------------------------------------------------------------------------------------
    def append_message_to_df(self, message, delta_t, sub_list_num):
        """ Appends cache df when there is a coincident signal

        Parameters
        ----------
        message : `dict`
            dictionary of the SNEWS message
        delta_t : 'float'
            value for time difference between message nu time and initial (0 if message set initial)
        sub_list_num : 'int'
            numeric label for coincidence sub list

        """
        message['received_time'] = datetime.utcnow().strftime("%y/%m/%d %H:%M:%S")
        message['sub_list_num'] = sub_list_num
        message['nu_delta_t'] = delta_t
        self.cache_df = self.cache_df.append(message, ignore_index=True)

    # ------------------------------------------------------------------------------------------------------------------
    def reset_df(self):
        """ Resets coincidence arrays if coincidence is broken

        """
        del self.cache_df
        self.cache_df = pd.DataFrame(columns=self.column_names)
        self.initial_set = False

    # ------------------------------------------------------------------------------------------------------------------
    def _coincident_with_whole_list(self, message, sub_list_num, ):
        """
        Check if signal is coincident with a whole sub list
        :param message: `dict`, new incoming message
        :param sub_list_num: `int`, the coincident sublist
        :return: (`str`,) , type of the coincidence if any
        """
        # first check if detector already in sublist
        sub_list = self.cache_df.query(f'sub_list_num=={sub_list_num}')
        sub_list_detectors = list(sub_list['detector_name'])
        if message['detector_name'] in sub_list_detectors:
            self.in_coincidence = True
            return 'ALREADY_IN_LIST',
        # compare the current nu time with all other on the sublist
        message_nu_time = self.times.str_to_datetime(message['neutrino_time'], fmt='%y/%m/%d %H:%M:%S:%f')
        nu_times = pd.to_datetime(sub_list.neutrino_time, format='%y/%m/%d %H:%M:%S:%f')
        delta_ts = ((message_nu_time - nu_times).dt.total_seconds()).values  # numpy array
        # check if signal is NOT coincident with the whole list
        if all(abs_del_t > self.coinc_threshold for abs_del_t in np.abs(delta_ts)):
            self.in_coincidence = False
            return 'NOT_COINCIDENT',
        # check if signal is coincident with the whole list and arrives earlier
        elif all(0 > del_t >= -self.coinc_threshold for del_t in delta_ts):
            self.in_coincidence = True
            return 'EARLY_COINCIDENT', np.insert(np.sort(np.abs(delta_ts)), 0, 0)
        # check if signal is coincident with the whole list
        elif all(abs_del_t <= self.coinc_threshold for abs_del_t in np.abs(delta_ts)):
            self.in_coincidence = True
            return 'COINCIDENT', delta_ts[0]
        # not sure if I need this
        else:
            self.in_coincidence = False
            return 'NOT_COINCIDENT',

    # ------------------------------------------------------------------------------------------------------------------
    def _in_between_coincidence(self, initial_time, detector_name, new_sub_list, other_df):
        """Checks for coincident messages with the new sub_list

        Parameters
        ----------
        initial_time
        detector_name
        new_sub_list
        other_df


        """
        other_df = other_df.query(f'detector_name != "{detector_name}"')
        new_list = self.cache_df.query(f'sub_list_num=={new_sub_list}')
        print(f'Checking these detectors: {list(other_df["detector_name"])}')
        for index, row in other_df.iterrows():
            print(index)
            nu_time = self.times.str_to_datetime(row['neutrino_time'], fmt='%y/%m/%d %H:%M:%S:%f')
            if row['detector_name'] in list(new_list['detector_name']):
                continue
            del_t = (nu_time - initial_time).total_seconds()
            # make sure the signal is not an initial that its nu is earlier than new list's initial
            if float(row['nu_delta_t']) != 0 and (0 > del_t >= -self.coinc_threshold):
                if self._coincident_with_whole_list(message=row.copy(deep=False),
                                                    sub_list_num=new_sub_list)[0] == 'EARLY_COINCIDENT':
                    print(f'{row["detector_name"]} is making new ini {del_t}')
                    initial_time = nu_time
                    new_row = row.copy(deep=False)
                    new_row['sub_list_num'] = new_sub_list
                    new_list = new_list.append(new_row, ignore_index=True)
                    new_nu_time = pd.to_datetime(new_list.neutrino_time, format='%y/%m/%d %H:%M:%S:%f')
                    new_list['nu_delta_t'] = ((new_nu_time - initial_time).dt.total_seconds()).values
                    print(new_list['nu_delta_t'])
                    new_list = new_list.sort_values(by='neutrino_time')
                else:
                    pass

            if 0 < del_t <= self.coinc_threshold:
                if self._coincident_with_whole_list(message=row.copy(deep=False),
                                                    sub_list_num=new_sub_list)[0] == 'COINCIDENT':
                    print(f'appending to {row["detector_name"]} new list {del_t}')
                    new_row = row.copy(deep=False)
                    new_row['sub_list_num'] = new_sub_list
                    new_row['nu_delta_t'] = del_t
                    new_list = new_list.append(new_row, ignore_index=True)
                    new_list = new_list.sort_values(by='neutrino_time')
                else:
                    pass

        self.cache_df = pd.concat([self.cache_df.query(f'sub_list_num!={new_sub_list}'), new_list], ignore_index=True)
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
    def _new_list_find_coincidences(self, message, new_sub_list):
        print('new_list_find_coincidence')
        other_df = self.cache_df.query(f'sub_list_num!={new_sub_list}').sort_values(by='neutrino_time').drop_duplicates(
            subset=['_id'])
        detector_name = message['detector_name']
        initial_time = self.times.str_to_datetime(message['neutrino_time'], fmt='%y/%m/%d %H:%M:%S:%f')
        self._in_between_coincidence(initial_time, detector_name, new_sub_list, other_df)

    # ------------------------------------------------------------------------------------------------------------------
    def _check_sub_lists(self, message, sub_list):
        """Checks _coincident_with_whole_list's return and applies actions on sub list (append, give new initial time)

        Parameters
        ----------
        message
        sub_list


        """
        coinc_with_list = self._coincident_with_whole_list(message, sub_list)
        if coinc_with_list[0] == 'ALREADY_IN_LIST':
            print('ALREADY_IN_LIST')
            self.in_list_already = True
            pass
        elif coinc_with_list[0] == 'NOT_COINCIDENT':
            pass
        elif coinc_with_list[0] == 'COINCIDENT':
            print('COINCIDENT')
            delta_t = coinc_with_list[1]
            self.append_message_to_df(message, delta_t, sub_list)
            print(f'appending to {sub_list}')

        elif coinc_with_list[0] == 'EARLY_COINCIDENT':
            self.append_message_to_df(message, 0, sub_list)
            _sub_list = self.cache_df.query(f'sub_list_num=={sub_list}')
            _sub_list = _sub_list.sort_values(by='neutrino_time')  # first sort! dt's are sorted too
            # re-assign dt making the early message first
            _sub_list['nu_delta_t'] = coinc_with_list[1]
            self.cache_df = pd.concat([_sub_list, self.cache_df.query(f'sub_list_num!={sub_list}')],
                                      ignore_index=True)
            self.cache_df = self.cache_df.reset_index(drop=True)

    # ------------------------------------------------------------------------------------------------------------------
    def _check_coincidence(self, message, ):
        click.secho(f'{message["detector_name"]}', )
        if len(self.cache_df) == 0:
            self.append_message_to_df(message, 0, 0)
            pass
        subs_list_nums = list(self.cache_df['sub_list_num'].unique())
        self.in_coincidence = False
        self.in_list_already = False
        # already_made_new_nc_list = False
        for sub_list in subs_list_nums:
            self._check_sub_lists(message=message, sub_list=sub_list)
        print(f'in_coincidence: {self.in_coincidence}')
        if not self.in_coincidence:
            print('making  new list')
            new_sub_list = max(subs_list_nums,
                               default=-1) + 1  # -1 is there for the very first msg when sub_list_nums empty
            self.append_message_to_df(message, 0, new_sub_list)
            self._new_list_find_coincidences(message=message, new_sub_list=new_sub_list)
        # unique detector has been added to a sub_list
        # else:
        #     # not coincidence but maybe already in list
        #     if not self.in_list_already:
        #         self._dump_redundant_list()
        #         self.cache_df = self.cache_df.sort_values(by=['sub_list_num', 'neutrino_time'])
        #         self.hype_mode_publish()
        #         self.display_table()

        self._dump_redundant_list()
        self.cache_df = self.cache_df.sort_values(by=['sub_list_num', 'neutrino_time'])
        self.display_table()

    # ----------------------------------------------------------------------------------------------------------------
    def display_table(self):
        click.secho(
            f'Here is the current coincident table\n',
            fg='magenta', bold=True, )
        for sub_list in self.cache_df['sub_list_num'].unique():
            sub_df = self.cache_df.query(f'sub_list_num=={sub_list}')
            sub_df.drop(columns=['meta','machine_time','schema_version'])
            snews_bot.send_table(sub_df)
            print(sub_df.to_markdown())
            print('=' * 168)

    # TODO: 27/02 update for new df format (REWORK)
    # ------------------------------------------------------------------------------------------------------------------
    def retract_from_cache(self, retrc_message):
        """
        Parses retraction message, will delete 'n' latest messages. The list is sorted by 'received_time', the latest message
        will be at the top of the list.

        """

        drop_detector = retrc_message['detector_name']
        delete_n_many = retrc_message['N_retract_latest']

        if retrc_message['N_retract_latest'] == 'ALL':
            delete_n_many = self.cache_df.groupby(by='detector_name').size().to_dict()[drop_detector]
        print(f'\nDropping latest message(s) from {drop_detector}\nRetracting: {delete_n_many} messages')
        sorted_df = self.cache_df.sort_values(by='received_time')
        for i in sorted_df.index:
            if delete_n_many > 0 and self.cache_df.loc[i, 'detector_name'] == drop_detector:
                self.cache_df.drop(index=i, inplace=True)
                delete_n_many -= 1
        self.cache_df = self.cache_df.reset_index()

    # ------------------------------------------------------------------------------------------------------------------
    def hype_mode_publish(self):
        """ This method will publish an alert every time a new detector
            submits an observation message

            Parameters
            ----------
            n_old_unique_count : `int`
                the least number of detectors required for the hype publish
        """

        click.secho(f'{"=" * 100}', fg='bright_red')
        for sub_list in list(self.cache_df['sub_list_num'].unique()):
            _sub_df = self.cache_df.query(f'sub_list_num=={sub_list}')
            p_vals = _sub_df['p_value'].to_list()
            p_vals_avg = _sub_df['p_value'].mean()
            nu_times = _sub_df['neutrino_time'].to_list()
            detector_names = _sub_df['detector_name'].to_list()

            alert_data = cs_utils.data_cs_alert(p_vals=p_vals, p_val_avg=p_vals_avg, sub_list_num=sub_list,
                                                nu_times=nu_times, detector_names=detector_names, )

            with self.alert as pub:
                alert = self.alert_schema.get_cs_alert_schema(data=alert_data)
                pub.send(alert)

        click.secho(f'{"NEW COINCIDENT DETECTOR.. ".upper():^100}\n', bg='bright_green', fg='red')
        click.secho(f'{"Published an Alert!!!".upper():^100}\n', bg='bright_green', fg='red')
        click.secho(f'{"=" * 100}', fg='bright_red')
        # snews_bot.send_table(self.cache_df, self.is_test)

    # ------------------------------------------------------------------------------------------------------------------
    def dump_old_messages(self, message):
        current_sent_time = message['sent_time']

        ind = 0
        for latest_sent_time in self.cache_df['sent_time']:
            latest_sent_time = datetime.strptime(latest_sent_time, '%d/%m/%y %H:%M:%S')
            current_sent_time = datetime.strptime(current_sent_time, '%d/%m/%y %H:%M:%S')

            del_t = (current_sent_time - latest_sent_time).total_seconds()
            if del_t >= self.secs_in_day:
                self.cache_df.drop(ind, inplace=True)
            ind += 1

    # ------------------------------------------------------------------------------------------------------------------
    def run_coincidence(self):
        ''' Main body of the class.

        '''

        stream = Stream(until_eos=False)
        with stream.open(self.observation_topic, "r") as s:
            print('Nothing here, please wait...')
            for snews_message in s:
                # Check for Coincidence
                if snews_message['_id'].split('_')[1] == self.topic_type:
                    _str = '!!! This is a Test !!!' if 'TEST' in snews_message['_id'] else '!!!'
                    click.secho(f'{"-" * 57}', fg='bright_blue')
                    click.secho(f'Incoming message {_str}'.upper(), bold=True, fg='red')
                    self._check_coincidence(message=snews_message)

                # Check for Retraction (NEEDS WORK)
                elif snews_message['_id'].split('_')[1] == 'Retraction':
                    if snews_message['which_tier'] == 'CoincidenceTier' or snews_message['which_tier'] == 'ALL':
                        self.retract_from_cache(snews_message)
                    else:
                        pass

                elif snews_message['_id'].split('_')[0] == 'hard-reset':
                    self.reset_df()
                    click.secho('Cache restrated', fg='yellow')

                else:
                    print(snews_message['_id'].split('_')[0], ' is not recognized!')
