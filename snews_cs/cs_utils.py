"""
Example initial dosctring
"""
from dotenv import load_dotenv
from datetime import datetime
import os
import json


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


class TimeStuff:
    """ SNEWS format datetime objects

    """

    def __init__(self, env_path=None):
        set_env(env_path)
        self.snews_t_format = os.getenv("TIME_STRING_FORMAT")
        self.hour_fmt = "%H:%M:%S"
        self.date_fmt = "%y_%m_%d"
        self.get_datetime = datetime.utcnow()
        self.get_snews_time = lambda fmt=self.snews_t_format: datetime.utcnow().strftime(fmt)
        self.get_hour = lambda fmt=self.hour_fmt: datetime.utcnow().strftime(fmt)
        self.get_date = lambda fmt=self.date_fmt: datetime.utcnow().strftime(fmt)

    def str_to_datetime(self, nu_time, fmt='%y/%m/%d %H:%M:%S:%f'):
        """ string to datetime object """
        return datetime.strptime(nu_time, fmt)

    def str_to_hr(self, nu_time, fmt='%H:%M:%S:%f'):
        """ string to datetime hour object """
        return datetime.strptime(nu_time, fmt)


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


# TODO: Change to SNEWS_PT struc
def data_cs_alert(p_vals=None, nu_times=None,
                  detector_names=None, p_val_avg=None, sub_list_num=None, false_alarm_prob=None, server_tag=None):
    """ Default alert message data
        
        Parameters
        ----------
        p_vals : `list`
            list with p-values of the observations involved in the alert
        nu_time : `list`
            list of neutrino arrival times
        detector_names : `list`
            list of ids of the detectors involved in the alert
        p_val_avg : `float`
            Naive, average p value 
        sub_list_num : `int`
            The sublist number of the triggered alert

        Returns        
        -------
            `dict`
                dictionary of the complete alert data

    """
    keys = ['p_vals', 'neutrino_times', 'detector_names', 'p_val_avg', 'sub_list_num', 'false_alarm_prob', 'server_tag']
    values = [p_vals, nu_times, detector_names, p_val_avg, sub_list_num, false_alarm_prob, server_tag]
    return dict(zip(keys, values))


def is_garbage_message(snews_message):
    """ This method checks to see if message meets SNEWS standards

    Parameters
    ----------
    snews_message : dict
        incoming SNEWS message

    Returns
    -------
        bool
            True if message does not meet CS standards, else

    """
    time = TimeStuff()
    detector_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'auxiliary/detector_properties.json'))
    with open(detector_file) as file:
        snews_detectors = json.load(file)
    snews_detectors = list(snews_detectors.keys())
    message_key = snews_message.keys()
    is_garbage = False
    missing_key = False
    warning = f'The following Message does not meet SNEWS CS standards !\n{snews_message}\n'
    if 'detector_name' not in message_key:
        warning += f'* Does not have required key: "detector_name"\n'
        is_garbage = True
        missing_key = True
    if 'neutrino_time' not in message_key:
        warning += f'* Does not have required key: "neutrino_time"\n'
        is_garbage = True
        missing_key = True
    if 'p_val' not in message_key:
        warning += f'* Does not have required key: "p_val"\n'
        is_garbage = True
        missing_key = True
    if missing_key:
        print(warning)
        return is_garbage
    contents_suck = False
    if type(snews_message['p_val']) is not float:
        contents_suck = True
        is_garbage = True
        warning += f'* p value needs to be a float type, type given: {type(snews_message["p_val"])}\n'
    if type(snews_message['p_val']) is float and (snews_message['p_val'] >= 1.0 or snews_message['p_val'] <= 0):
        warning += f'* {snews_message["p_val"]} is not a valid p value !\n'
        contents_suck = True
        is_garbage = True
    if type(snews_message['detector_name']) is not str:
        contents_suck = True
        is_garbage = True
        warning += f'* "detector_name" must be a str, type given: {type(snews_message["detector_name"])}\n'
    if type(snews_message['neutrino_time']) is not str:
        contents_suck = True
        is_garbage = True
        warning += f'* neutrino time must be a str, type given: {type(snews_message["neutrino_time"])}\n'
    if snews_message['detector_name'] not in snews_detectors:
        contents_suck = True
        is_garbage = True
        warning += f'* "{snews_message["detector_name"]}" is not in the SNEWS detector list\n\t-Please see README file for detector list\n'
    if contents_suck:
        print(warning)
        return is_garbage
    shitty_nu_time = False

    try:
        time.str_to_datetime(snews_message['neutrino_time'])
    except ValueError:
        warning += f'* neutrino time: {snews_message["neutrino_time"]} does not match SNEWS CS format: "%y/%m/%d %H:%M:%S:%f"'
        shitty_nu_time = True
        is_garbage = True

    if (time.str_to_datetime(snews_message['neutrino_time']) - datetime.utcnow()).total_seconds() <= -172800.0:
        warning += f'* neutrino time is more than 48 hrs olds !'
        shitty_nu_time = True
        is_garbage = True

    if (time.str_to_datetime(snews_message['neutrino_time']) - datetime.utcnow()).total_seconds() > 0:
        warning += f'* neutrino time comes from the future, please stop breaking causality'
        shitty_nu_time = True
        is_garbage = True

    if shitty_nu_time:
        print(warning)
        return is_garbage

    return is_garbage

def test_connection(message, broker):
    """ When received a test_connection key
        reinstert the message with updated status
        this way user can test if their message
        goes and comes back from the server
    """
    from hop import Stream
    stream = Stream(until_eos=False)
    with stream.open(broker, "w") as s:
        # insert back with a "received" status
        message["status"] = "received"
        s.write(message)
        print(message)
        print(f"{message['name']} tested their connection {message['time']}")
