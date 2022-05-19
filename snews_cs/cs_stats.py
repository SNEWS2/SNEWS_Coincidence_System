import numpy as np
from scipy.stats import poisson
import pandas as pd
import json


class CoincStat:

    def cache_false_alarm_rate(self, cache_sub_list, path_to_hb='heartboeats/heartboeats.json'):
        '''
        Generates flase alarm rates for a set of detector heartbeats

        Parameters
        ----------
        path_to_hb: str
            Path to HB log file (JSON ?)

        Returns

        -------

        '''
        with open(path_to_hb) as json_file:
            num_coinc_detectors = len(cache_sub_list['detector_name'])

            num_detectors_online = len(json.load(json_file)['detectors'])

            mu = 1 * num_detectors_online  # expected number of false coincidence for a week

            prob_false_alarm_rate = poisson.pmf(k=num_coinc_detectors, mu=mu)
            return prob_false_alarm_rate
