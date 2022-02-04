from . import snews_utils
from .snews_db import Storage
import os, click, sys
from datetime import datetime
import time
from .hop_pub import Publish_Alert
import numpy as np
import pandas as pd
from hop import Stream
from . import snews_bot


# TODO: Implement confidence level on coincidence list.
# TODO: Archive old cache and parse it
# TODO: Archive Melih's Lonely Signals
# TODO: stash
# TODO: pandas failsafe

#  have 3 coinc detectors (5,10,23)
#  get bs signal outside SN window
#  normally this would kill coinc
#  coinc_list.confidence_lvl()
#  0.0-1.0 and if above c_l ignore bs signals


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

        self.cache_df = pd.DataFrame()

        self.cache_reset = False
        self.initial_set = False
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
        if self.coinc_broken:
            del self.cache_df
            self.cache_df = pd.DataFrame()
        else:
            pass

    # ------------------------------------------------------------------------------------------------------------------
    def reset_cache(self):
        """ Resets mongo cache and all coincidence arrays if coincidence is broken

        """
        if self.coinc_broken:
            self.reset_df()
            # self.storage.purge_cache(coll='CoincidenceTier')
            self.coinc_broken = False
            self.delta_t = None
            self.cache_reset = True
        else:
            pass

    # ------------------------------------------------------------------------------------------------------------------
    def copy_row_and_append(self, ind, delta_t, sub_list_num):
        duplicate_row = self.cache_df.iloc[ind].copy(deep=False)
        duplicate_row['nu_delta_t'] = np.abs(delta_t)
        duplicate_row['sub_list_num'] = sub_list_num
        return self.cache_df.append(duplicate_row, ignore_index=True)

    # ------------------------------------------------------------------------------------------------------------------
    def coincident_with_whole_list(self, message, sub_list_num):
        message_nu_time = self.times.str_to_hr(message['neutrino_time'])
        sub_list = self.cache_df.query(f'sub_list_num=={sub_list_num}')
        nu_times = list(pd.to_datetime(sub_list.neutrino_time, format='%H:%M:%S:%f'))
        delta_ts = (message_nu_time - nu_times).dt.total_seconds()
        sub_list_detectors = list(sub_list['Name'])
        if message['detector_name'] in sub_list_detectors:
            return 'ALREADY_IN_LIST',
        elif any(0 < delta_ts <= self.coinc_threshold):
            return 'COINCIDENT', delta_ts[0]
        elif any(0 > delta_ts >= self.coinc_threshold):
            return 'EARLY_COINCIDENT', list(delta_ts.abs()).insert(0, 0)
        else:
            return 'NOT_COINCIDENT',

    # ------------------------------------------------------------------------------------------------------------------
    def check_coincidence_melih_seb_edition(self, message):

        subs_list_nums = list(self.cache_df['sub_list_num'].unique())
        nu_times = list(pd.to_datetime(self.cache_df.neutrino_time, format='%H:%M:%S:%f'))
        for sub_list in subs_list_nums:
            coinc_with_list = self.coincident_with_whole_list(message, sub_list)
            if coinc_with_list[0] == 'ALREADY_IN_LIST':
                continue
            elif coinc_with_list[0] == 'COINCIDENT':
                delta_t = coinc_with_list[1]
                self.append_message_to_df(message, delta_t, sub_list)
            elif coinc_with_list[0] == 'EARLY_COINCIDENT':
                self.append_message_to_df(message, 0, sub_list)
                sub_list = self.cache_df.query(f'sub_list_num=={sub_list}')
                sub_list['nu_delta_t'] = coinc_with_list[1]
                sub_list = sub_list.sort_value(by='neutrino_time')
                self.cache_df = pd.concat([sub_list, self.cache_df.query(f'sub_list_num!={sub_list}')],
                                          ignore_index=True)
                self.cache_df = self.cache_df.reset_index(drop=True)
            elif coinc_with_list[0] == 'NOT_COINCIDENT':
                new_sub_list = subs_list_nums.max() + 1
                self.append_message_to_df(message, 0, new_sub_list)
                for i, nu_t in enumerate(nu_times):
                    message_nu_time = self.times.str_to_hr(message['neutrino_time'])
                    if self.cache_df['detector_name'][i] == message['detector_name']:
                        continue
                    elif np.abs((nu_t - message_nu_time).total_seconds()) <= self.coinc_threshold:
                        delta_t = np.abs((nu_t - message_nu_time).total_seconds())
                        self.cache_df = self.copy_row_and_append(i, delta_t, new_sub_list)
                    else:
                        continue

    # ------------------------------------------------------------------------------------------------------------------
    def check_coincidence(self, message):
        message_nu_time = self.times.str_to_hr(message['neutrino_time'])
        max_sub_list_num = self.cache_df['sub_list_num'].max()
        nu_times = list(pd.to_datetime(self.cache_df.neutrino_time, format='%H:%M:%S:%f'))
        index = list(self.cache_df.index)
        subs_list_nums = list(self.cache_df['sub_list_num'])
        building_new_list = False
        new_sub_list_num = None
        for ind, nu_t, curr_sub_list_num in zip(index, nu_times, subs_list_nums):
            delta_t = (message_nu_time - nu_t).total_seconds()
            print(f'current delta {delta_t}')
            if self.detector_in_sub_list(message_detector=message['detector_name'], sub_list_num=curr_sub_list_num):
                continue
            elif self.cache_df['nu_delta_t'][ind] == 0.0 and not building_new_list:
                if message['detector_name'] in list(self.cache_df.query(f'sub_list_num=={curr_sub_list_num}')):
                    continue
                print(f'Checking this sub list {curr_sub_list_num} ini {self.cache_df["detector_name"][ind]}')
                if 0 < np.abs(delta_t) <= self.coinc_threshold:
                    if delta_t < 0:
                        print('In coincidence with current ini but early')
                        new_sub_list_num = max_sub_list_num + 1
                        self.append_message_to_df(message, 0, new_sub_list_num)
                        self.cache_df = self.copy_row_and_append(ind=ind, delta_t=delta_t,
                                                                 sub_list_num=new_sub_list_num)
                        building_new_list = True
                        continue
                    else:
                        print('In coincidence with current list')
                        self.append_message_to_df(message, delta_t, curr_sub_list_num)
                        continue
                elif (np.abs(delta_t) > self.coinc_threshold) and max_sub_list_num == curr_sub_list_num:
                    print('Building new list: late nu time out of coincidence')
                    new_sub_list_num = max_sub_list_num + 1
                    self.append_message_to_df(message, 0, new_sub_list_num)
                    building_new_list = True
                    continue
            if building_new_list and np.abs(delta_t) <= self.coinc_threshold:
                print('appending to new list')
                if self.cache_df['detector_name'][ind] in list(self.cache_df.query(f'sub_list_num=={new_sub_list_num}')[
                                                                   'detector_name']):
                    print('This detector is already in the list, skipping')
                    continue
                else:
                    print('Adding detector to new sub list.')
                    self.cache_df = self.copy_row_and_append(ind=ind, delta_t=delta_t,
                                                             sub_list_num=new_sub_list_num)
                    continue

    # ----------------------------------------------------------------------------------------------------------------

    def display_table(self):
        click.secho(
            f'Here is the current coincident table\n',
            fg='magenta', bold=True, )
        for sub_list in self.cache_df['sub_list_num'].unique():
            print(self.cache_df.query(f'sub_list_num=={sub_list}').to_markdown())
            print('=' * 100)

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
    def hype_mode_publish(self, n_old_unique_count):
        """ This method will publish an alert every time a new detector
            submits an observation message

            Parameters
            ----------
            n_old_unique_count : `int`
                the least number of detectors required for the hype publish
        """

        if self.hype_mode_ON and n_old_unique_count < self.cache_df['detector_name'].nunique():
            click.secho(f'{"=" * 100}', fg='bright_red')
            p_vals = self.cache_df['p_value'].to_list()
            nu_times = self.cache_df['neutrino_time'].to_list()
            detector_names = self.cache_df['detector_name'].to_list()
            alert_data = snews_utils.data_cs_alert(p_vals=p_vals, nu_times=nu_times, detector_names=detector_names, )
            self.alert.publish(msg_type=self.topic_type, data=alert_data)
            click.secho(f'{"Hype Mode: NEW COINCIDENT DETECTOR.. ".upper():^100}\n', bg='bright_green', fg='red')
            click.secho(f'{"Published an Alert!!!".upper():^100}\n', bg='bright_green', fg='red')
            click.secho(f'{"=" * 100}', fg='bright_red')

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
                    click.secho(f'{"-" * 57}', fg='bright_blue')
                    click.secho('Incoming message !!!'.upper(), bold=True, fg='red')
                    if not self.initial_set:
                        self.append_message_to_df(snews_message, delta_t=0, sub_list_num=0)
                        self.display_table()
                        self.initial_set = True
                        continue
                    self.check_coincidence(message=snews_message)

                    # TODO: Rework needed !!
                    # if len(self.cache_df.index) > 1:
                    #     self.hype_mode_publish(n_old_unique_count=self.n_unique_detectors)
                    # self.n_unique_detectors = self.cache_df['detector_name'].nunique()

                    # snews_bot.send_table(self.cache_df, self.is_test)
                    self.display_table()
                # Check for Retraction (NEEDS WORK)
                elif snews_message['_id'].split('_')[1] == 'FalseOBS':
                    if snews_message['which_tier'] == 'CoincidenceTier' or snews_message['which_tier'] == 'ALL':
                        self.retract_from_cache(snews_message)
                    else:
                        pass

                elif snews_message['_id'].split('_')[0] == 'SNEWS-Updater':
                    self.dump_old_messages(snews_message)
                else:
                    print('garbage')
