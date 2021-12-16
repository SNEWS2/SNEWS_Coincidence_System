"""
Example initial dosctring
"""
from dotenv import load_dotenv
from datetime import datetime

import os, json
from pathlib import Path
import sys


def set_env(env_path=None):
    """ Set environment parameters

    Parameters
    ----------
    env_path : `str`, (optional)
        path for the environment file.
        Use default settings if not given

    """
    dirname = os.path.dirname(__file__)
    default_env_path = os.path.dirname(__file__) + '/auxiliary/test-config.env'
    env = env_path or default_env_path
    load_dotenv(env)


# this is same as os.makedirs(path, exist_ok=True) # Also, not used
# def make_dir(path):
#     """ make a directory in a given path """
#     if Path(path).is_dir():
#         pass
#     else:
#         os.makedirs(path)


class TimeStuff:
    ''' SNEWS format datetime objects

    '''

    def __init__(self, env_path=None):
        set_env(env_path)
        self.snews_t_format = os.getenv("TIME_STRING_FORMAT")
        self.hour_fmt = "%H:%M:%S"
        self.date_fmt = "%y_%m_%d"
        self.get_datetime = datetime.utcnow()
        self.get_snews_time = lambda fmt=self.snews_t_format: datetime.utcnow().strftime(fmt)
        self.get_hour = lambda fmt=self.hour_fmt: datetime.utcnow().strftime(fmt)
        self.get_date = lambda fmt=self.date_fmt: datetime.utcnow().strftime(fmt)

    def str_to_datetime(self, nu_time, fmt='%y/%m/%d %H:%M:%S'):
        """ string to datetime object """
        return datetime.strptime(nu_time, fmt)

    def str_to_hr(self, nu_time, fmt='%H:%M:%S:%f'):
        """ string to datetime hour object """
        return datetime.strptime(nu_time, fmt)

def isnotebook():
    """ Tell if the script is running on a notebook

    """
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True  # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False  # Probably standard Python interpreter

# TODO: needs work
def get_logger(scriptname, logfile_name):
    """ Logger

    .. note:: Deprecated

    """
    import logging
    # Gets or creates a logger
    logger = logging.getLogger(scriptname)

    # set log level
    logger.setLevel(logging.INFO)
    # define file handler and set formatter
    file_handler = logging.FileHandler(logfile_name)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(message)s')
    file_handler.setFormatter(formatter)
    # add file handler to logger
    logger.addHandler(file_handler)
    return logger


def display_gif():
    """ Some fun method to display an alert gif
        If running on notebook

    """
    if isnotebook():
        from IPython.display import HTML, display
        giphy_snews = "https://raw.githubusercontent.com/SNEWS2/hop-SNalert-app/snews2_dev/hop_comms/auxiliary/snalert.gif"
        display(HTML(f'<img src={giphy_snews}>'))


# TODO: Change to SNEWS_PT struc
def data_alert(p_vals=None, detector_events=None, t_series=None, nu_times=None,
               ids=None, locs=None, status=None, machine_times=None):
    """ Default alert message data
        
        Parameters
        ----------
        p_vals : `list`
            list with p-values of the observations involved in the alert
        detectors_events : `dict`
            dict of detectors and their number of events involved in the alert
        t_series : `list`
            list of timeseries (if applicable)
        nu_time : `list`
            list of neutrino arrival times
        ids : `list`
            list of ids of the detectors involved in the alert
        locs : `list`
            list of locations of the experiments involved in the alert
        status : `list`
            Depracted?              
        machine_times : `list`
            The machine times of the experiments involved in the alert

        Returns        
        -------
            `dict`
                dictionary of the complete alert data

    """
    keys = ['p_vals', 'detector_events', 't_series', 'neutrino_times', 'ids', 'locs', 'status', 'machine_times']
    values = [p_vals, detector_events, t_series, nu_times, ids, locs, status, machine_times]
    return dict(zip(keys, values))

