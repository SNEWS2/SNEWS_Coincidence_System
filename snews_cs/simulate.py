"""
module to simulate observation messages
and trigger alerts
"""

import os , json
from collections import namedtuple
import numpy as np
from . import snews_utils

def _retrieve_detectors():
    ''' Retrieve the name-ID-location of the participating detectors.

    '''
    detectors_path = os.path.dirname(__file__) + "/auxiliary/detector_properties.json"
    if not os.path.isfile(detectors_path):
        os.system(f'python {os.path.dirname(__file__)}/auxiliary/make_detector_file.py')

    with open(detectors_path) as json_file:
        detectors = json.load(json_file)

    # make a namedtuple
    Detector = namedtuple("Detector", ["name", "id", "location"])
    for k, v in detectors.items():
        detectors[k] = Detector(v[0], v[1], v[2])
    return detectors


def get_detector(detector):
    """ Return the selected detector properties
    """
    Detector = namedtuple("Detector", ["name", "id", "location"])
    if isinstance(detector, Detector): return detector  # not needed?
    # search for the detector name in `detectors`
    detectors = _retrieve_detectors()
    if isinstance(detector, str):
        try:
            return detectors[detector]
        except KeyError:
            print(f'{detector} is not a valid detector!')
            return detectors['TEST']

def randomly_select_detector():
    detectors = _retrieve_detectors()
    return np.random.choice(list(detectors.keys()))


def get_simulated_message(neutrino_time, detector_name):
    times = snews_utils.TimeStuff()
    date_time = times.get_snews_time(fmt="%y/%m/%d_%H:%M:%S:%f")
    _id =  f'(SIMULATED)-{detector_name}_CoincidenceTier_{date_time}'

    message = {"_id": _id,
               "detector_name": detector_name,
               "sent_time": times.get_snews_time(),
               "machine_time": "",
               "neutrino_time" : neutrino_time,
               "p_value" : 0
               }
    return message
