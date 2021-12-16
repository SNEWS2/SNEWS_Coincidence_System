from . import snews_utils
from .snews_db import Storage
import os, click
from datetime import datetime
import time
from .hop_pub import Publish_Alert
import numpy as np
import pandas as pd

# TODO Need to turn detector names into a unique arr
# TODO Implement silver/gold
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
        self.storage = Storage(drop_db=False, use_local_db=use_local)
        self.topic_type = "CoincidenceTierAlert"
        self.coinc_threshold = float(os.getenv('COINCIDENCE_THRESHOLD'))
        self.mgs_expiration = 300
        self.coinc_cache = self.storage.coincidence_tier_cache
        self.alert = Publish_Alert(use_local=True)
        self.times = snews_utils.TimeStuff(env_path)

        self.counter = 0

        self.initial_nu_time = None
        self.curr_nu_time = None
        self.delta_t = None

        self.cache_df = pd.DataFrame(columns=['_id','detector_name','sent_time','machine_time','neutrino_time','p_value','nu_delta_t'])

        self.ids = []
        self.delta_ts = []
        self.detectors = []
        self.nu_times = []
        self.machine_times = []
        self.p_vals = []
        self.detector_events = {}

        self.coinc_broken = False

        self.cache_reset = False

    def append_arrs(self, mgs):
        """ Appends coincidence arrays when there is a coincident signal

        Parameters
        ----------
        mgs : `dict`
            dictionary of the SNEWS message

        """
        self.nu_times.append(mgs['neutrino_time'])
        self.detectors.append(mgs['detector_name'])
        self.count_detector_events(detector_name=mgs['detector_name'], add_or_pop=1)
        self.machine_times.append(mgs['machine_time'])
        self.p_vals.append(mgs['p_value'])
        self.ids.append(mgs['_id'])
        if self.counter == 0:
            self.delta_ts.append(0)
        elif self.counter != 0:
            self.delta_ts.append(self.delta_t)

    def reset_arr(self):
        """ Resets coincidence arrays if coincidence is broken

        """
        if self.coinc_broken:
            self.ids = []
            self.delta_ts = []
            self.nu_times = []
            self.machine_times = []
            self.p_vals = []
            self.detector_events = {}
            self.detectors = []
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
            # self.old_loc = mgs['location']
            # self.old_detector = mgs['detector_name']
            self.append_arrs(mgs)
            # self.delta_t = 0
            self.coinc_broken = False
            self.cache_reset = False
        else:
            pass

    def kill_false_element(self, detector_name, index):
        """ Remove the information at a given index

        Parameters
        ----------
        index : `int`
            The index in which the information is removed

        """
        self.count_detector_events(detector_name=detector_name, add_or_pop=-1)
        self.ids.pop(index)
        self.nu_times.pop(index)
        self.delta_ts.pop(index)
        self.p_vals.pop(index)
        self.machine_times.pop(index)
        self.detectors.pop(index)

    def reset_cache(self):
        """ Resets mongo cache and all coincidence arrays if coincidence is broken

        """
        if self.coinc_broken:
            self.counter = 0
            self.reset_arr()
            self.storage.purge_cache(coll='CoincidenceTier')
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
        if self.hype_mode_ON and n_old_unique_count < len(np.unique(self.detectors)):
            click.secho(f'{"=" * 57}', fg='bright_red')
            alert_data = snews_utils.data_cs_alert(p_vals=self.p_vals, nu_times=self.nu_times, ids=self.ids,
                                                   machine_times=self.machine_times)
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
                self.append_arrs(mgs)
                click.secho('got something'.upper(), fg='white', bg='red')
                self.counter += 1

            # the conditional below, repeats itself
            elif self.delta_t > self.coinc_threshold:
                print('Outside SN window')
                print('Coincidence is broken, checking to see if an ALERT can be published...\n\n')
                self.coinc_broken = True
                self.pub_alert()
                print('Resetting the cache')
                self.reset_cache()
                self.set_initial_signal(mgs)
                self.storage.coincidence_tier_cache.insert_one(mgs)
                # Start recursion
                click.secho('Starting new stream..'.upper(), bold=True, fg='bright_white', underline=True)
                self.counter += 1
                self.run_coincidence()
        else:
            pass

    def waited_long_enough(self):
        """
        Waits for a new message, if a new message does not come
        in before 120sec then coincidence is broken.
        Publishing method is called and then the stream and cache are reset

        """
        curr_cache_len = self.storage.coincidence_tier_cache.count()
        stagnant_cache = True
        t0 = time.time()
        click.secho('waiting..'.upper(), fg='cyan', bold=True, )
        while stagnant_cache:
            t1 = time.time()
            delta_t = t1 - t0
            self.in_cache_retract()  # first, check for false messages and clean
            if self.storage.coincidence_tier_cache.count() > curr_cache_len and delta_t < self.mgs_expiration:
                self.counter += 1
                break
            # for every 2 minutes
            if np.round(delta_t) > 10 and delta_t % 120.0 == 0.0:
                click.secho(
                    f'Here is the current coincident list\n',
                    fg='magenta', bold=True, )
                click.secho(
                    f'Total Number of detectors: {np.unique(self.detectors)} \n',
                    fg='magenta', bold=True, )
                click.secho(
                    f'Total number of coincident events: {len(self.ids)}\n',
                    fg='magenta', bold=True, )
            elif delta_t > self.mgs_expiration:
                click.secho('\nWaited too long !!'.upper(), fg='cyan', bold=True)
                self.coinc_broken = True
                self.pub_alert()
                print('\nResetting cache')
                self.reset_cache()
                # Run recursion
                click.secho('\n\nStarting new stream..\n\n'.upper(), bold=True, fg='bright_white', underline=True)
                self.run_coincidence()
            if self.storage.coincidence_tier_cache.count() == 0:
                self.run_coincidence()

    def in_cache_retract(self):
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

        for mgs in self.storage.get_false_warnings():
            # Delete N many false messages
            if mgs['N_retract_latest'] != 0 and (mgs['which_tier'] == 'CoincidenceTier' or mgs['which_tier'] == 'ALL'):
                i = len(self.ids) - 1
                drop_detector = mgs['detector_name']
                delete_n_many = mgs['N_retract_latest']
                if mgs['N_retract_latest'] == 'ALL':
                    delete_n_many = self.detectors.count(drop_detector)

                print(f'\nDropping latest message(s) from {drop_detector}\nRetracting: {delete_n_many} messages')
                for detector_name in reversed(self.detectors):
                    if delete_n_many > 0 and detector_name == drop_detector:
                        delete_n_many -= 1
                        self.kill_false_element(index=i, detector_name=drop_detector)
                    i -= 1
                query = {'_id': mgs['_id']}
                self.storage.false_warnings.delete_one(query)
                print(f'\nTotal Number of coincident events left: {len(self.ids)}')

            if mgs['false_id'] != None and mgs['false_id'].split('_')[1] == 'CoincidenceTier':
                false_id = mgs['false_id']
                i = 0
                for id in self.ids:
                    if false_id == id:
                        self.kill_false_element(index=i, detector_name=mgs['detector_name'])
                        print(f'\nFalse mgs found {id}\nPurging it from coincidence list\n')
                        break
                        # print(f'\nNew list of coincident detectors:\n{self.detectors}')
                    i += 1

                if not self.hype_mode_ON:  # what if it is ON ?
                    query = {'_id': mgs['_id']}
                    self.storage.false_warnings.delete_one(query)

    def count_detector_events(self, detector_name, add_or_pop):
        """ Count the events from detectors.
            If no events exists, remove the detector from dict.

        """
        if detector_name in self.detector_events.keys():
            self.detector_events[detector_name] += add_or_pop
        else:
            self.detector_events[detector_name] = 1
        if self.detector_events[detector_name] == 0:
            self.detector_events.pop(detector_name, None)

    def pub_alert(self):
        """ When the coincidence is broken publish alert
            if there were more than 1 detectors in the 
            given coincidence window

        """
        if self.coinc_broken and len(self.detector_events.keys()) > 1:
            click.secho(f'{"=" * 57}', fg='bright_red')
            alert_data = snews_utils.data_cs_alert(p_vals=self.p_vals, nu_times=self.nu_times, ids=self.ids,
                                                   machine_times=self.machine_times)
            self.alert.publish(msg_type=self.topic_type, data=alert_data)
            click.secho('Published an Alert!!!'.upper(), bg='bright_green', fg='red')
            click.secho(f'{"=" * 57}', fg='bright_red')
        else:
            print('Nothing to send :(')
            pass

    def run_coincidence(self):
        ''' Main body of the class. Reads the 
            mongodb as a stream to look for coincidences.

        '''
        with self.coinc_cache.watch() as stream:
            # should it be: for mgs in stream ?
            if self.storage.empty_coinc_cache():
                # click.secho(f'{"-" * 57}', fg='bright_blue')
                print('Nothing here, please wait...')

            for doc in stream:
                if 'fullDocument' not in doc.keys():
                    self.run_coincidence()
                snews_message = doc['fullDocument']
                click.secho(f'{"-" * 57}', fg='bright_blue')
                click.secho('Incoming message !!!'.upper(), bold=True, fg='red')
                click.secho(f'{snews_message["_id"]}'.upper(), fg='bright_green')
                n_unique_detectors = len(np.unique(self.detectors))
                self.set_initial_signal(snews_message)
                self.check_for_coinc(snews_message)  # adds +1 detector
                self.hype_mode_publish(n_old_unique_count=n_unique_detectors)
                self.waited_long_enough()
