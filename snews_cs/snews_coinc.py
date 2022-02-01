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

        self.n_unique_detectors = 0

        self.initial_nu_time = None
        self.curr_nu_time = None
        self.delta_t = None

        self.cache_df = pd.DataFrame()

        self.coinc_broken = False
        self.cache_reset = False
        self.initial_set = False
        self.is_test = is_test

    # ------------------------------------------------------------------------------------------------------------------
    def append_df(self, mgs):
        """ Appends cache df when there is a coincident signal

        Parameters
        ----------
        mgs : `dict`
            dictionary of the SNEWS message

        """
        self.cache_df = self.cache_df.append(mgs, ignore_index=True)

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
    def set_initial_signal(self, snews_message):
        """ Sets up the initial signal

        Parameters
        ----------
        snews_message : `dict`
            dictionary of the SNEWS message

        """
        if len(self.cache_df.index) == 0:
            print('Setting initial values')
            snews_message['nu_delta_t'] = 0
            snews_message['sub_list_num'] = 0
            self.append_df(snews_message)
            self.coinc_broken = False
            self.cache_reset = False
            self.initial_set = True
        else:
            pass

    # ------------------------------------------------------------------------------------------------------------------
    def message_out_of_order(self, sub_list_num):
        """ This method will reorder the cache if a coincident message arrives
        with a nu time earlier than the set initial time.
        """
        print('MESSAGE OUT OF ORDER')
        bad_df = self.cache_df.query(f'sub_list_num=={sub_list_num}')
        bad_df['neutrino_time'] = pd.to_datetime(bad_df.neutrino_time, format='%H:%M:%S:%f')
        bad_df.sort_values(by='neutrino_time', inplace=True)
        bad_df.reset_index(inplace=True, drop=True)
        initial_nu_time = bad_df['neutrino_time'][0]
        del_ts = []
        nu_t_strs = []
        for nu_time in bad_df['neutrino_time']:
            del_t = (nu_time - initial_nu_time).total_seconds()
            del_ts.append(del_t)
            nu_t_str = nu_time.strftime('%H:%M:%S:%f')
            nu_t_strs.append(nu_t_str)
        bad_df['nu_delta_t'] = del_ts
        print(del_ts)
        bad_df['neutrino_time'] = nu_t_strs
        if len(self.cache_df['sub_list_num'].unique()) > 1:
            df_chunk = self.cache_df.query(f'sub_list_num != {sub_list_num}')
            self.cache_df = pd.concat([bad_df, df_chunk])
            self.check_for_in_between_messages(ini_time=initial_nu_time, sub_list_num=sub_list_num)
        else:
            self.cache_df = bad_df
        self.cache_df.sort_values(by='sub_list_num', inplace=True)
        self.cache_df.reset_index(inplace=True, drop=True)

    # ------------------------------------------------------------------------------------------------------------------
    def check_for_in_between_messages(self, ini_time, sub_list_num):
        # nu_times_strs = self.cache_df['neutrino_time']
        self.cache_df['neutrino_time'] = pd.to_datetime(self.cache_df.neutrino_time, format='%H:%M:%S:%f')
        chunk_df = self.cache_df.query(f'sub_list_num!={sub_list_num}')
        chunk_inds = list(chunk_df.index)

        for ind in chunk_inds:
            delta_t = (chunk_df['neutrino_time'][ind] - ini_time).total_seconds()
            if delta_t <= self.coinc_threshold:
                sub_list_duplicate = pd.Series(chunk_df)
                sub_list_duplicate['sub_list_num'] = sub_list_num
                self.cache_df.append(sub_list_duplicate, ignore_index=True)
        self.cache_df.sort_values(by=['sub_list_num', 'neutrino_time'])
        self.cache_df.reset_index(inplace=True, drop=True)

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
    def check_coinc_v2(self, message):
        message_nu_time = self.times.str_to_hr(message['neutrino_time'])
        nu_times = pd.to_datetime(self.cache_df.neutrino_time, format='%H:%M:%S:%f')
        max_sub_list_num = self.cache_df['sub_list_num'].max()
        ind = 0
        building_new_list = False
        for nu_t in nu_times:
            delta_t_post = (message_nu_time - nu_t).total_seconds()
            delta_t_pre = (nu_t - message_nu_time).total_seconds()
            curr_sub_list_num = self.cache_df['sub_list_num'][ind]
            delta_t = delta_t_post
            # current row is an initial nu signal
            if self.cache_df['nu_delta_t'][ind] == 0.0:
                # if message arrives within it's coincidence window append it
                if 0 < delta_t_post <= self.coinc_threshold:
                    print('1')
                    message['sub_list_num'] = curr_sub_list_num
                    message['nu_delta_t'] = delta_t_post
                    self.append_df(message)
                    ind += 1
                    continue
                # if message is outside it's coincidence window and all lists have been loop through
                # append it, but in a new list
                if delta_t_post > self.coinc_threshold and max_sub_list_num == curr_sub_list_num:
                    print('2')
                    message['sub_list_num'] = max_sub_list_num + 1
                    message['nu_delta_t'] = 0
                    self.append_df(message)
                    building_new_list = True
                    ind += 1
                    continue
                if 0 > delta_t_post >= -1 * self.coinc_threshold:
                    print('3')
                    curr_sub_list_num = max_sub_list_num + 1
                    message['sub_list_num'] = curr_sub_list_num
                    message['nu_delta_t'] = 0
                    self.append_df(message)
                    building_new_list = True
                    ind += 1
                    continue
                    # set this message as initial
                    # fix the sub list (reorder and write new delta_t_post)
                    # use sep method
                if delta_t_post > -1 * self.coinc_threshold:
                    print('4')
                    curr_sub_list_num = max_sub_list_num + 1
                    message['sub_list_num'] = curr_sub_list_num
                    message['nu_delta_t'] = 0
                    self.append_df(message)
                    building_new_list = True
                    ind += 1
                    continue
            if building_new_list:
                print('4')
                if delta_t_pre <= -1*self.coinc_threshold or delta_t_post > self.coinc_threshold:
                    pass
                
                duplicate_row = self.cache_df.iloc[ind]
                duplicate_row['nu_delta_t'] = delta_t_pre
                duplicate_row['sub_list_num'] = curr_sub_list_num
                self.cache_df.append(duplicate_row, ignore_index=True)
                ind += 1
                continue

    # ----------------------------------------------------------------------------------------------------------------

    def checking_for_coincidence(self, message):
        """
        Checks new message in the stream,
        if message is within SN time window (10sec)
        it is added to the coincidence list, if not
        the message is added to separate coincidence list.
        An alter will be published if a new coincident detector is added to the list.

        Parameters
        ----------
        message : `dict`
            dictionary of the SNEWS message

        """

        # only run if there is 1 or more messages in the df
        if len(self.cache_df.index) >= 1:
            message_nu_time = self.times.str_to_hr(message['neutrino_time'])
            # If there are more multiple sub-lists begin recursion call
            out_of_ord = False
            within_sub_list = False
            print('Checking coinc')
            for sub_list_num in self.cache_df['sub_list_num'].unique():
                # define a temporary df by querying for the sub_list_num
                temp_df = self.cache_df.query(f'sub_list_num=={sub_list_num}')
                temp_df.reset_index(inplace=True, drop=True)
                initial_nu_time = self.times.str_to_hr(temp_df['neutrino_time'][0])
                in_coinc = True
                ind = temp_df.index[0]
                initial_delta = (message_nu_time - initial_nu_time).total_seconds()
                # if the message has an earlier nu_time than the set initial_nu_time
                if 0 > initial_delta:
                    if initial_delta >= -1 * self.coinc_threshold:
                        out_of_ord = True
                        within_sub_list = True
                    else:
                        out_of_ord = True
                        within_sub_list = False
                if (0 > initial_delta >= -1 * self.coinc_threshold) and out_of_ord and within_sub_list:
                    message['sub_list_num'] = sub_list_num
                    message['nu_delta_t'] = ' '
                    self.append_df(message)
                    print(message['_id'])
                    self.message_out_of_order(sub_list_num)
                    return
                print(f'out_of_ord {out_of_ord}')
                print(f'within_sub_list {within_sub_list}')
                print(f'sub_list_num {sub_list_num}')
                if out_of_ord and sub_list_num == self.cache_df['sub_list_num'].max() and not within_sub_list:
                    print('no coinc and early')
                    message['sub_list_num'] = sub_list_num + 1
                    message['nu_delta_t'] = 0
                    self.append_df(message)
                    return

                # loop through all the nu times
                for nu_time in temp_df['neutrino_time']:
                    nu_time = self.times.str_to_hr(nu_time)
                    delta_t = (message_nu_time - nu_time).total_seconds()
                    # checks if coincidence holds with all message in current sub coincidence list
                    if delta_t <= self.coinc_threshold:
                        in_coinc = True
                    # if not coincidence is broken, break the for loop
                    else:
                        in_coinc = False
                        break
                    ind += 1
                if in_coinc:
                    # if the message is coincident with the entire sub list append it with the initial delta and current
                    # coinc num
                    print('appending')
                    message['sub_list_num'] = sub_list_num
                    message['nu_delta_t'] = initial_delta
                    self.append_df(message)
                if not in_coinc and sub_list_num == self.cache_df['sub_list_num'].max():
                    message['nu_delta_t'] = 0
                    message['sub_list_num'] = sub_list_num + 1
                    self.append_df(message)

    # ------------------------------------------------------------------------------------------------------------------
    def display_table(self):
        click.secho(
            f'Here is the current coincident table\n',
            fg='magenta', bold=True, )
        for sub_list in self.cache_df['sub_list_num'].unique():
            print(self.cache_df.query(f'sub_list_num=={sub_list}').to_markdown())

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

    # TODO: NEEDS REWORK
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
                        self.set_initial_signal(snews_message)
                        self.display_table()
                        continue
                    self.check_coinc_v2(message=snews_message)
                    # self.checking_for_coincidence(snews_message)
                    self.display_table()
                    # TODO: Rework needed !!
                    # if len(self.cache_df.index) > 1:
                    #     self.hype_mode_publish(n_old_unique_count=self.n_unique_detectors)
                    # self.n_unique_detectors = self.cache_df['detector_name'].nunique()

                    # snews_bot.send_table(self.cache_df, self.is_test)

                # Check for Retraction (NEEDS WORK)
                if snews_message['_id'].split('_')[1] == 'FalseOBS':
                    if snews_message['which_tier'] == 'CoincidenceTier' or snews_message['which_tier'] == 'ALL':
                        self.retract_from_cache(snews_message)
                    else:
                        pass

                if snews_message['_id'].split('_')[0] == 'SNEWS-Updater':
                    self.dump_old_messages(snews_message)
