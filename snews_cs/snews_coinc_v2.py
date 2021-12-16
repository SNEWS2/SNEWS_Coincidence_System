from . import snews_utils
from .snews_db import Storage
import os, click
from datetime import datetime
import time
from .hop_pub import Publish_Alert
import numpy as np
import pandas as pd
from hop import Stream


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
            print(self.delta_t)
            if self.delta_t <= self.coinc_threshold:
                self.append_df(mgs)
                click.secho('got something'.upper(), fg='white', bg='red')


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
            alert_data = snews_utils.data_cs_alert(p_vals=p_vals, nu_times=nu_times)
            self.alert.publish(msg_type=self.topic_type, data=alert_data)
            click.secho(f'{"Published an Alert!!!".upper():^100}\n', bg='bright_green', fg='red')
            click.secho(f'{"=" * 57}', fg='bright_red')

    # TODO: df update
    def pub_alert(self):
        """ When the coincidence is broken publish alert
            if there were more than 1 detectors in the 
            given coincidence window

        """
        if self.coinc_broken and self.cache_df['detector_name'].nunique() > 1:
            click.secho(f'{"=" * 57}', fg='bright_red')
            p_vals = self.cache_df['p_value'].to_list()
            nu_times = self.cache_df['neutrino_time'].to_list()
            alert_data = snews_utils.data_cs_alert(p_vals=p_vals, nu_times=nu_times, ids=None)
            self.alert.publish(msg_type=self.topic_type, data=alert_data)
            click.secho('Published an Alert!!!'.upper(), bg='bright_green', fg='red')
            click.secho(f'{"=" * 57}', fg='bright_red')
        else:
            print('Nothing to send :(')
            pass

    def run_coincidence(self):
        ''' Main body of the class.

        '''

        stream = Stream(persist=True)
        with stream.open(self.observation_topic, "r") as s:
            print('Nothing here, please wait...')
            for snews_message in s:
                # Check for Coincidence
                if snews_message['_id'].split('_')[1] == self.topic_type:
                    click.secho(f'{"-" * 57}', fg='bright_blue')
                    click.secho('Incoming message !!!'.upper(), bold=True, fg='red')
                    if not self.initial_set:
                        self.set_initial_signal(snews_message)
                        continue
                    self.check_for_coinc(snews_message)
                    self.n_unique_detectors = self.cache_df['detector_name'].nunique()
                    self.hype_mode_publish(n_old_unique_count=self.n_unique_detectors)

                # Check for Retraction (NEEDS WORK)
                if snews_message['_id'].split('_')[1] == 'FalseOBS':
                    if snews_message['which_tier'] == 'CoincidenceTier' or snews_message['which_tier'] == 'ALL':
                        self.retract_from_cache(snews_message)
                    else:
                        pass

                print(self.display_table())

        #
        # with self.coinc_cache.watch() as stream:
        #     # should it be: for mgs in stream ?
        #     if self.storage.empty_coinc_cache():
        #         # click.secho(f'{"-" * 57}', fg='bright_blue')
        #
        #
        #
        #     for doc in stream:
        #         if 'fullDocument' not in doc.keys():
        #             self.run_coincidence()
        #         snews_message = doc['fullDocument']
        #
        #         click.secho(f'{snews_message["_id"]}'.upper(), fg='bright_green')
        #         n_unique_detectors = len(np.unique(self.detectors))
        #         self.set_initial_signal(snews_message)
        #         self.check_for_coinc(snews_message)  # adds +1 detector
        #         self.hype_mode_publish(n_old_unique_count=n_unique_detectors)
        #         self.waited_long_enough()
