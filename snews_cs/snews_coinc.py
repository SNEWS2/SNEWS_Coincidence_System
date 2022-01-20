from . import snews_utils
from .snews_db import Storage
import os, click
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

    def __init__(self, env_path=None, use_local_db=False, hype_mode_ON=True):
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

    def append_df(self, mgs):
        """ Appends cache df when there is a coincident signal

        Parameters
        ----------
        mgs : `dict`
            dictionary of the SNEWS message

        """
        self.cache_df = self.cache_df.append(mgs, ignore_index=True)
        df_len = len(self.cache_df.index)
        if df_len == 1:
            self.cache_df.at[df_len - 1, 'nu_delta_t'] = 0
        elif df_len > 1:
            self.cache_df.at[df_len - 1, 'nu_delta_t'] = self.delta_t

    def reset_df(self):
        """ Resets coincidence arrays if coincidence is broken

        """
        if self.coinc_broken:
            del self.cache_df
            self.cache_df = pd.DataFrame()
        else:
            pass

    def set_initial_signal(self, mgs):
        """ Sets up the initial signal

        Parameters
        ----------
        mgs : `dict`
            dictionary of the SNEWS message

        """
        if len(self.cache_df.index) == 0:
            print('Setting initial values')
            self.initial_nu_time = self.times.str_to_hr(mgs['neutrino_time'])
            self.append_df(mgs)
            self.coinc_broken = False
            self.cache_reset = False
            self.initial_set = True
        else:
            pass

    def message_out_of_order(self):
        """ This method will reorder the cache if a coincident message arrives
        with a nu time earlier than the set initial time.
        """
        self.cache_df['neutrino_time'] = pd.to_datetime(self.cache_df.neutrino_time)
        self.cache_df.sort_values(by='neutrino_time', inplace=True)
        self.cache_df.reset_index(inplace=True)
        self.initial_nu_time = self.cache_df['neutrino_time'][0]
        del_ts = []
        nu_t_strs = []
        for nu_time in self.cache_df['neutrino_time']:
            del_t = (nu_time - self.initial_nu_time).total_seconds()
            del_ts.append(del_t)
            nu_t_strs.append(nu_time.datetime.strftime('%H:%M:%S:%f'))
        self.cache_df['nu_delta_t'] = del_ts
        self.cache_df['neutrino_time'] = nu_t_strs

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

    def check_for_coinc(self, mgs):
        """ 
        Checks new message in the stream, 
        if message is within SN time window (10sec) 
        it is added to the coincidence list, if not 
        coincidence is broken. Then the publish method is called. 
        Finally a new stream and coincidence list is made.

        Parameters
        ----------
        mgs : `dict`
            dictionary of the SNEWS message

        """

        if len(self.cache_df.index) >= 1:
            self.curr_nu_time = self.times.str_to_hr(mgs['neutrino_time'])
            self.delta_t = (self.curr_nu_time - self.initial_nu_time).total_seconds()
            if self.delta_t <= self.coinc_threshold:
                self.append_df(mgs)
                click.secho('got something'.upper(), fg='white', bg='red')
                # self.coinc_broken = True
                # self.pub_alert()  # why it was not publishing alert?

            if 0 > self.delta_t >= -1 * self.coinc_threshold:
                click.secho('got something'.upper(), fg='white', bg='red')
                click.secho('Current message has an earlier nu time..'.upper(), fg='white', bg='red')
                self.append_df(mgs)
                self.message_out_of_order()
            # the conditional below, repeats itself
            elif self.delta_t > self.coinc_threshold:
                print('Outside SN window')
                print('Coincidence is broken, checking to see if an ALERT can be published...\n\n')
                self.coinc_broken = True
                self.pub_alert()
                print('Resetting the cache')
                self.reset_cache()
                self.set_initial_signal(mgs)
                # self.storage.coincidence_tier_cache.insert_one(mgs)
                # Start recursion
                click.secho('Starting new stream..'.upper(), bold=True, fg='bright_white', underline=True)
                self.run_coincidence()
        else:
            pass

    def display_table(self):
        click.secho(
            f'Here is the current coincident table\n',
            fg='magenta', bold=True, )
        print(self.cache_df.to_markdown())

    # Needs df update
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

    def hype_mode_publish(self, n_old_unique_count):
        """ This method will publish an alert every time a new detector
            submits an observation message

            Parameters
            ----------
            n_old_unique_count : `int`
                the least number of detectors required for the hype publish
        """

        if self.hype_mode_ON and n_old_unique_count < self.cache_df['detector_name'].nunique():
            click.secho(f'{"=" * 57}', fg='bright_red')
            p_vals = self.cache_df['p_value'].to_list()
            nu_times = self.cache_df['neutrino_time'].to_list()
            detector_names = self.cache_df['detector_name'].to_list()
            alert_data = snews_utils.data_cs_alert(p_vals=p_vals, nu_times=nu_times, detector_names=detector_names, )
            self.alert.publish(msg_type=self.topic_type, data=alert_data)
            click.secho(f'{"Hype Mode: NEW COINCIDENT DETECTOR.. ".upper():^100}\n', bg='bright_green', fg='red')
            click.secho(f'{"Published an Alert!!!".upper():^100}\n', bg='bright_green', fg='red')
            click.secho(f'{"=" * 57}', fg='bright_red')

    def pub_alert(self):
        """ When the coincidence is broken publish alert
            if there were more than 1 detectors in the 
            given coincidence window

        """
        if self.coinc_broken and self.cache_df['detector_name'].nunique() > 1:
            # click.secho(f'{"=" * 57}', fg='bright_red')
            p_vals = self.cache_df['p_value'].to_list()
            nu_times = self.cache_df['neutrino_time'].to_list()
            detector_names = self.cache_df['detector_name'].to_list()
            alert_data = snews_utils.data_cs_alert(p_vals=p_vals, nu_times=nu_times, detector_names=detector_names, )
            self.alert.publish(msg_type=self.topic_type, data=alert_data)
            click.secho('Published an Alert using pub_alert!!!'.upper(), bg='bright_green', fg='red')
            click.secho(f'{"=" * 57}', fg='bright_red')
        else:
            print('Nothing to send :(')
            pass

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
                    self.check_for_coinc(snews_message)
                    if len(self.cache_df.index) > 1:
                        self.hype_mode_publish(n_old_unique_count=self.n_unique_detectors)
                    self.n_unique_detectors = self.cache_df['detector_name'].nunique()
                    self.display_table()
                    snews_bot.send_table(self.cache_df)

                # Check for Retraction (NEEDS WORK)
                if snews_message['_id'].split('_')[1] == 'FalseOBS':
                    if snews_message['which_tier'] == 'CoincidenceTier' or snews_message['which_tier'] == 'ALL':
                        self.retract_from_cache(snews_message)
                    else:
                        pass
