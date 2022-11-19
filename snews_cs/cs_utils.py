"""
Example initial docstring
"""
from dotenv import load_dotenv
import os

def set_env(env_path=None):
    """ Set environment parameters

    Parameters
    ----------
    env_path : `str`, (optional)
        path for the environment file.
        Use default settings if not given

    """
    default_env_path = os.path.dirname(__file__) + '/auxiliary/test-config.env'
    env = env_path or default_env_path
    load_dotenv(env)

def make_beat_directory(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)



def data_cs_alert(p_vals=None,
                  nu_times=None,
                  detector_names=None,
                  p_val_avg=None,
                  sub_list_num=None,
                  false_alarm_prob=None,
                  server_tag=None,
                  alert_type=None):
    """ Default alert message data
        
        Parameters
        ----------
        p_vals : `list`
            list with p-values of the observations involved in the alert
        nu_times : `list`
            list of neutrino arrival times
        detector_names : `list`
            list of ids of the detectors involved in the alert
        p_val_avg : `float`
            Naive, average p value 
        sub_list_num : `int`
            The sublist number of the triggered alert
        false_alarm_prob : `float`
            The proba of the alert being random coincidence based on number of active detectors
        server_tag : `str`
            The name of the server issuing the alert
        alert_type : `str`
            Reason of the alert, new / update / retraction

        Returns        
        -------
            `dict`
                dictionary of the complete alert data

    """
    keys = ['p_vals', 'neutrino_times', 'detector_names', 'p_val_avg', 'sub_list_num',
            'false_alarm_prob', 'server_tag', 'alert_type']
    values = [p_vals, nu_times, detector_names, p_val_avg, sub_list_num,
              false_alarm_prob, server_tag, alert_type]
    return dict(zip(keys, values))


