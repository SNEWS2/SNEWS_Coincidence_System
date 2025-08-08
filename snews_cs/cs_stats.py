import numpy as np
import math


def ncr(n, r):
    f = math.factorial
    # if the HB cache is empty, n-r returns negative number for which factorial is not defined
    # as a temporary fix, let's return 0
    if n - r < 0:
        return 0
    return int(f(n) / f(r) / f(n - r))


def cache_false_alarm_rate(cache_sub_list, hb_cache):
    """Assume false alarm rate of 1 per week
    returns the combined false alarm rate in years
    meaning; if there are 8 active detectors, each with false alarm rate of 1/week
    We would get a false alarm with 2-fold coincidence every X years
    The formula;
        n = number of detectors
        r = number of coincidences
        C(n,r) = \frac{n!}{r!(n-r)!}

        R_{combined} = C(n,r)+1 \times F_{im, d1} * F_{im, d2} ... * F_{im, dn} \times δt^{r-1}

    """
    seconds_year = 31_556_926
    seconds_week = 604800
    single_imitation_freq = 1 / seconds_week  # 1/week in seconds
    online_detectors = len(hb_cache.Detector.unique())  # n
    coincident_detectors = len(cache_sub_list["detector_name"])  # r
    time_window = 10  # seconds
    combinations = ncr(online_detectors, coincident_detectors)
    combined_imitation_freq = (
        (combinations + 1)
        * np.power(single_imitation_freq, coincident_detectors)
        * np.power(time_window, coincident_detectors - 1)
    )
    comb_Fim_year = combined_imitation_freq / seconds_year
    return 1 / comb_Fim_year
