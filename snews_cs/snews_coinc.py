from . import snews_utils
from .snews_db import Storage
import os, click
from datetime import datetime
from .hop_pub import Publish_Alert
import numpy as np
import pandas as pd
from hop import Stream
from . import snews_bot

class CoincDecider:
    """ CoincDecider class for Supernova alerts (Coincidence Tier)
        
    Parameters
    ----------
    env_path : `str`, optional
        user can give the path a specific SNEWS env file, 
        defaults to None ./auxiliary/test-config.env)
    
    """

    def __init__(self, env_path=None, use_local_db=False, hype_mode_ON=True, is_test=True):
        snews_utils.set_env(env_path)
        self.hype_mode_ON = hype_mode_ON
        self.storage = Storage(drop_db=False, use_local_db=use_local_db)
        self.topic_type = "CoincidenceTier"
        self.coinc_threshold = float(os.getenv('COINCIDENCE_THRESHOLD'))
        self.cache_expiration = 86400
        self.coinc_cache = self.storage.coincidence_tier_cache
        self.alert = Publish_Alert(use_local=True)
        self.times = snews_utils.TimeStuff(env_path)
        self.observation_topic = os.getenv("OBSERVATION_TOPIC")
        self.column_names = ["_id", "detector_name", "sent_time", "machine_time", "neutrino_time",
                             "p_value", "meta", "sub_list_num", "nu_delta_t"]

        self.cache_df = pd.DataFrame(columns = self.column_names)

        self.is_test = is_test

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

        message['sub_list_num'] = sub_list_num
        message['nu_delta_t'] = delta_t
        self.cache_df = self.cache_df.append(message, ignore_index=True)

    # ------------------------------------------------------------------------------------------------------------------
    def reset_df(self):
        """ Resets coincidence arrays if coincidence is broken

        """
        del self.cache_df
        self.cache_df = pd.DataFrame(columns = self.column_names)
        self.initial_set = False

    # ------------------------------------------------------------------------------------------------------------------
    def coincident_with_whole_list(self, message, sub_list_num):
        """
        Look for coincidences with any items in the data
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
        delta_ts = ((message_nu_time - nu_times).dt.total_seconds()).values # numpy array

        # if any delta t is negative, current message has to be made initial, before!
        p_dt = delta_ts[delta_ts >= 0]
        n_dt = delta_ts[delta_ts < 0]
        if any(-n_dt <= self.coinc_threshold):
            self.in_coincidence = True
            return 'EARLY_COINCIDENT', np.insert(np.sort(np.abs(delta_ts)), 0, 0)
        # if not check if current message is in the list
        elif any(p_dt <= self.coinc_threshold):
            self.in_coincidence = True
            return 'COINCIDENT', delta_ts[0]
        elif not any(np.abs(delta_ts) <= self.coinc_threshold):
            # this condition is a must!
            # otherwise it duplicates because it can satisfy two conditions at the same time
            return 'NOT_COINCIDENT',
        else:
            raise "Something weird happenning"

    # ------------------------------------------------------------------------------------------------------------------
    def check_coincidence_melih_seb_edition(self, message):

        subs_list_nums = list(self.cache_df['sub_list_num'].unique())
        self.in_coincidence = False
        in_list_already = False
        for sub_list in subs_list_nums:
            coinc_with_list = self.coincident_with_whole_list(message, sub_list)
            if coinc_with_list[0] == 'ALREADY_IN_LIST':
                in_list_already = True
                continue
            elif coinc_with_list[0] == 'COINCIDENT':
                delta_t = coinc_with_list[1]
                self.append_message_to_df(message, delta_t, sub_list)

            elif coinc_with_list[0] == 'EARLY_COINCIDENT':
                self.append_message_to_df(message, 0, sub_list)
                _sub_list = self.cache_df.query(f'sub_list_num=={sub_list}')
                _sub_list = _sub_list.sort_values(by='neutrino_time') # first sort! dt's are sorted too
                # re-assign dt making the early message first
                _sub_list['nu_delta_t'] = coinc_with_list[1]
                self.cache_df = pd.concat([_sub_list, self.cache_df.query(f'sub_list_num!={sub_list}')],
                                          ignore_index=True)
                self.cache_df = self.cache_df.reset_index(drop=True)

        # after checking all the sublist, if the message not in coincidence with any, only then make a new list
        if not self.in_coincidence:
            new_sub_list = max(subs_list_nums, default=-1) + 1 #-1 is there for the very first msg when sub_list_nums empty
            self.append_message_to_df(message, 0, new_sub_list)

            # check if any other non-initial signal might be in coincidence
            other_df = self.cache_df.query(f'sub_list_num!={new_sub_list}')
            for index, row in other_df.iterrows():
                self.check_coincidence_melih_seb_edition(row)
        else:
            # not coincidence but maybe already in list
            if not in_list_already:
                # self.hype_mode_publish()
                self.display_table()

    # ----------------------------------------------------------------------------------------------------------------
    def display_table(self):
        click.secho(
            f'Here is the current coincident table\n',
            fg='magenta', bold=True, )
        for sub_list in self.cache_df['sub_list_num'].unique():
            print(self.cache_df.query(f'sub_list_num=={sub_list}').to_markdown())
            print('=' * 168)

    # TODO: 27/02 update for new df format (REWORK)
    # ------------------------------------------------------------------------------------------------------------------
    def retract_from_cache(self, retrc_message):
        """
        loops through false warnings collection looks for
        coincidence tier false warnings, if a warning is found,
        it then loops through coincidence cache, if the false message
        is then all its corresponding features are deleted
        from the coincidence arrays.

        """
        if retrc_message['N_retract_latest'] != 0:
            drop_detector = retrc_message['detector_name']
            delete_n_many = retrc_message['N_retract_latest']
            if retrc_message['N_retract_latest'] == 'ALL':
                delete_n_many = self.cache_df.groupby(by='detector_name').size().to_dict()[drop_detector]
                print(f'\nDropping latest message(s) from {drop_detector}\nRetracting: {delete_n_many} messages')
            for i in self.cache_df.index:
                if delete_n_many > 0 and self.cache_df.loc[i, 'detector_name'] == drop_detector:
                    self.cache_df.drop(index=i, inplace=True)
                    self.cache_df.reset_index()
                    delete_n_many -= 1
            print(f'\nTotal Number of coincident events left: {len(self.cache_df.index)}')

        if retrc_message['false_id'] != None and retrc_message['false_id'].split('_')[1] == 'CoincidenceTier':
            false_id = retrc_message['false_id']
            for i in self.cache_df.index:
                if self.cache_df.loc[i, '_id'] == false_id:
                    self.cache_df.drop(index=i, inplace=True)

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
        p_vals = self.cache_df['p_value'].to_list()
        nu_times = self.cache_df['neutrino_time'].to_list()
        detector_names = self.cache_df['detector_name'].to_list()
        alert_data = snews_utils.data_cs_alert(p_vals=p_vals, nu_times=nu_times, detector_names=detector_names, )
        self.alert.publish(msg_type=self.topic_type, data=alert_data)
        click.secho(f'{"Hype Mode: NEW COINCIDENT DETECTOR.. ".upper():^100}\n', bg='bright_green', fg='red')
        click.secho(f'{"Published an Alert!!!".upper():^100}\n', bg='bright_green', fg='red')
        click.secho(f'{"=" * 100}', fg='bright_red')
        # snews_bot.send_table(self.cache_df, self.is_test)

    # ------------------------------------------------------------------------------------------------------------------
    def dump_old_messages(self, message):
        current_sent_time = message['sent_time']
        secs_in_day = 86400
        ind = 0
        for latest_sent_time in self.cache_df['sent_time']:
            latest_sent_time = datetime.strptime(latest_sent_time, '%d/%m/%y %H:%M:%S')
            current_sent_time = datetime.strptime(current_sent_time, '%d/%m/%y %H:%M:%S')

            del_t = (current_sent_time - latest_sent_time).total_seconds()
            if del_t >= secs_in_day:
                self.cache_df.drop(ind, inplace=True)
            ind += 1

    # ------------------------------------------------------------------------------------------------------------------
    def debloater(self):
        for sub_list in self.cache_df['sub_list_num'].unique():
            pass

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
                    self.check_coincidence_melih_seb_edition(message=snews_message)

                # Check for Retraction (NEEDS WORK)
                elif snews_message['_id'].split('_')[1] == 'FalseOBS':
                    if snews_message['which_tier'] == 'CoincidenceTier' or snews_message['which_tier'] == 'ALL':
                        self.retract_from_cache(snews_message)
                    else:
                        pass

                elif snews_message['_id'].split('_')[0] == 'hard-reset':
                    self.reset_df()
                    click.secho('Cache restrated', fg='yellow')

                else:
                    print(snews_message['_id'].split('_')[0], ' is not recognized!')
