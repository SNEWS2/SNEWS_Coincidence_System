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

    def __init__(self, env_path=None, use_local=False, hype_mode_ON=True):
        snews_utils.set_env(env_path)
        self.hype_mode_ON = hype_mode_ON
        self.storage = Storage(drop_db=False, use_local=use_local)
        self.topic_type = "CoincidenceTier"
        self.coinc_threshold = float(os.getenv('COINCIDENCE_THRESHOLD'))
        self.mgs_expiration = 300
        self.coinc_cache = self.storage.coincidence_tier_cache
        self.alert = Publish_Alert(use_local=True)
        self.times = snews_utils.TimeStuff(env_path)
        self.observation_topic = os.getenv("OBSERVATION_TOPIC")

        self.message_counter = 0

        self.n_unique_detectors = 0

        self.initial_nu_time = None
        self.curr_nu_time = None
        self.delta_t = None

        self.cache_df = pd.DataFrame(
            columns=['_id', 'detector_name', 'sent_time', 'machine_time', 'neutrino_time', 'p_value', 'nu_delta_t'])

        self.coinc_broken = False
        self.cache_reset = False

    def append_df(self, mgs):
        """ Appends cache df when there is a coincident signal

        Parameters
        ----------
        mgs : `dict`
            dictionary of the SNEWS message

        """
        self.cache_df.append(mgs)
        if self.counter == 0:
            self.cache_df.at[self.counter, 'nu_delta_t'] = 0
        elif self.counter != 0:
            self.cache_df.at[self.counter, 'nu_delta_t'] = self.delta_t

    def reset_df(self):
        """ Resets coincidence arrays if coincidence is broken

        """
        if self.coinc_broken:
            del self.cache_df
            self.cache_df = pd.DataFrame(
                columns=['_id', 'detector_name', 'sent_time', 'machine_time', 'neutrino_time', 'p_value', 'nu_delta_t'])
        else:
            pass

    def set_initial_signal(self, mgs):
        """ Sets up the initial signal

        Parameters
        ----------
        mgs : `dict`
            dictionary of the SNEWS message

        """
        if self.counter == 0:
            print('Setting initial values')
            self.initial_nu_time = self.times.str_to_hr(mgs['neutrino_time'])
            self.append_df(mgs)
            self.coinc_broken = False
            self.cache_reset = False
        else:
            pass

    def reset_cache(self):
        """ Resets mongo cache and all coincidence arrays if coincidence is broken

        """
        if self.coinc_broken:
            self.counter = 0
            self.reset_df()
            # self.storage.purge_cache(coll='CoincidenceTier')
            self.coinc_broken = False
            self.delta_t = None
            self.cache_reset = True
        else:
            pass

    def hype_mode_publish(self, n_old_unique_count):
        """ This method will publish an alert every time a new detector
            submits an observation message

            Parameters
            ----------
            n_old_unique_count : `int`
                the least number of detectors required for the hype publish
        """
        # TODO: change alert pub to SNEWS_PT like
        # TODO: can use set()
        if self.hype_mode_ON and n_old_unique_count < len(np.unique(self.detectors)):
            click.secho(f'{"=" * 57}', fg='bright_red')
            alert_data = snews_utils.data_alert(detector_events=self.detector_events,
                                                ids=self.cache_df['_id'].to_list(),
                                                p_vals=self.cache_df['p_value'].to_list(),
                                                nu_times=self.self.cache_df['neutrino_time'].to_list(),
                                                machine_times=self.self.cache_df['machine_time'].to_list())
            self.alert.publish(msg_type=self.topic_type, data=alert_data)
            click.secho(f'{"Published an Alert!!!".upper():^100}\n', bg='bright_green', fg='red')
            click.secho(f'{"=" * 57}', fg='bright_red')

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
        if self.cache_reset:
            pass

        if self.counter != 0:
            self.curr_nu_time = self.times.str_to_hr(mgs['neutrino_time'])
            self.delta_t = (self.curr_nu_time - self.initial_nu_time).total_seconds()

            if self.delta_t <= self.coinc_threshold:
                self.append_df(mgs)
                click.secho('got something'.upper(), fg='white', bg='red')
                # self.counter += 1

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
                self.counter += 1
                self.run_coincidence()
        else:
            pass

    # def waited_long_enough(self):
    #     """
    #     Waits for a new message, if a new message does not come
    #     in before 120sec then coincidence is broken.
    #     Publishing method is called and then the stream and cache are reset
    #
    #     """
    #     curr_cache_len = self.storage.coincidence_tier_cache.count()
    #     stagnant_cache = True
    #     t0 = time.time()
    #     click.secho('waiting..'.upper(), fg='cyan', bold=True, )
    #     while stagnant_cache:
    #         t1 = time.time()
    #         delta_t = t1 - t0
    #         self.in_cache_retract()  # first, check for false messages and clean
    #         if self.storage.coincidence_tier_cache.count() > curr_cache_len and delta_t < self.mgs_expiration:
    #             self.counter += 1
    #             break
    #         # for every 2 minutes
    #         if np.round(delta_t) > 10 and delta_t % 120.0 == 0.0:
    #             click.secho(
    #                 f'Here is the current coincident list\n',
    #                 fg='magenta', bold=True, )
    #             click.secho(
    #                 f'Total Number of detectors: {np.unique(self.detectors)} \n',
    #                 fg='magenta', bold=True, )
    #             click.secho(
    #                 f'Total number of coincident events: {len(self.ids)}\n',
    #                 fg='magenta', bold=True, )
    #         elif delta_t > self.mgs_expiration:
    #             click.secho('\nWaited too long !!'.upper(), fg='cyan', bold=True)
    #             self.coinc_broken = True
    #             self.pub_alert()
    #             print('\nResetting cache')
    #             self.reset_cache()
    #             # Run recursion
    #             click.secho('\n\nStarting new stream..\n\n'.upper(), bold=True, fg='bright_white', underline=True)
    #             self.run_coincidence()
    #         if self.storage.coincidence_tier_cache.count() == 0:
    #             self.run_coincidence()

    # Needs df update
    def in_cache_retract(self, retrc_message):
        """ 
        loops through false warnings collection looks for 
        coincidence tier false warnings, if a warning is found,
        it then loops through coincidence cache, if the false message
        is then all its corresponding features are deleted
        from the coincidence arrays.

        """
        if self.storage.empty_false_warnings():
            # print('No false messages...yet')
            pass

        if len(self.ids) == 0:
            # print('cache is empty')
            pass

        if retrc_message['N_retract_latest'] != 0 and (
                retrc_message['which_tier'] == 'CoincidenceTier' or retrc_message['which_tier'] == 'ALL'):

            drop_detector = retrc_message['detector_name']
            delete_n_many = retrc_message['N_retract_latest']
            if retrc_message['N_retract_latest'] == 'ALL':
                delete_n_many = self.detector_events = self.cache_df['detector_name'].value_counts().to_dict()[
                    'drop_detector']
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

    def count_detector_events(self, detector_name, add_or_pop):
        """ Count the events from detectors.
            If no events exists, remove the detector from dict.

        """
        self.detector_events = self.cache_df['detector_name'].value_counts().to_dict()

        for detector_name in self.detector_events.keys():
            if detector_name in self.detector_events.keys():
                self.detector_events[detector_name] += add_or_pop
            else:
                self.detector_events[detector_name] = 1
        if self.detector_events[detector_name] == 0:
            self.detector_events.pop(detector_name, None)

    # TODO: df update
    def pub_alert(self):
        """ When the coincidence is broken publish alert
            if there were more than 1 detectors in the 
            given coincidence window

        """
        if self.coinc_broken and len(self.detector_events.keys()) > 1:
            click.secho(f'{"=" * 57}', fg='bright_red')
            alert_data = snews_utils.data_alert(detector_events=self.detector_events,
                                                ids=self.ids,
                                                p_vals=self.p_vals,
                                                nu_times=self.nu_times,
                                                machine_times=self.machine_times)
            self.alert.publish(msg_type=self.topic_type, data=alert_data)
            click.secho('Published an Alert!!!'.upper(), bg='bright_green', fg='red')
            click.secho(f'{"=" * 57}', fg='bright_red')
        else:
            print('Nothing to send :(')
            pass

    def run_coincidence(self):
        ''' Main body of the class.

        '''
        #  Pipeline: sub->Mongo->coinc->alert-> :)
        #  New Pipeline: sub->coinc->(DB stash)->alert
        stream = Stream(persist=True)
        with stream.open(self.observation_topic, "r") as s:
            print('Nothing here, please wait...')
            for snews_message in s:
                # Check for Coincidence
                if snews_message['_id'].split('_')[1] == self.topic_type:
                    click.secho(f'{"-" * 57}', fg='bright_blue')
                    click.secho('Incoming message !!!'.upper(), bold=True, fg='red')
                    self.set_initial_signal(snews_message)
                    self.check_for_coinc(snews_message)  # adds +1 detector
                    self.n_unique_detectors = self.cache_df['detector_name'].nunique()
                    self.hype_mode_publish(n_old_unique_count=self.n_unique_detectors)
                    # self.waited_long_enough()
                    self.counter += 1
                # Check for Retraction (NEEDS WORK)
                if snews_message['_id'].split('_')[1] == 'FalseOBS':
                    if snews_message['which_tier'] == 'CoincidenceTier':
                        pass
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
