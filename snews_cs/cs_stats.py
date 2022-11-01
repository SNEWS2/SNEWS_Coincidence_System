import numpy as np
from scipy.stats import poisson
import pandas as pd
import json


def cache_false_alarm_rate(cache_sub_list, hb_cache):
    """
    Generates false alarm rates for a set of detector heartbeats

    Parameters
    ----------


    Returns
    -------
    false alarm probability: float
        probability that SNEWS alert is  a false alarm

    """
    # convert our n-fold coicidence from n-fold/day to n-fold/per week
    num_coinc_detectors = len(cache_sub_list['detector_name']) * (1/7)
    num_detectors_online = len(hb_cache)
    mu = 1 * num_detectors_online   # expected number of false coincidence for a week
    prob_false_alarm_rate = poisson.pmf(k=num_coinc_detectors, mu=mu)
    return np.round(prob_false_alarm_rate, decimals=5)


