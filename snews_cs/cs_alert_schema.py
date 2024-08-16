from .cs_utils import set_env
from datetime import datetime

class CoincidenceTierAlert:
    """ The Message scheme for the alert and observations

    Parameters
    ----------
    env_path : `str`, optional
        The path containing the environment configuration file
        If None, uses the default file in '/etc/test-config.env'

    """

    def __init__(self, env_path=None):
        set_env(env_path)

    def id_format(self, num_detectors):
        """ Returns formatted message ID
            time format should always be same for all detectors.
            num_detectors: `int`
                Number of detectors in the alert. If more than 2,
                it is an update to an earlier alert, and will be 
                appended with -UPDATE_ string

        Returns
            :`str`
                The formatted id as a string
            
        """
        date_time = datetime.utcnow().isoformat()
        if num_detectors == 2:
            return f'SNEWS_Coincidence_ALERT {date_time}'
        else:
            return f'SNEWS_Coincidence_ALERT-UPDATE {date_time}'

    def get_cs_alert_schema(self, data, is_test=False):
        """ Create a message schema for alert.
            Internally called in hop_pub
        
            Parameters
            ----------
            data : `named tuple`
                cs_utils data tuple with predefined field.
            
            Returns
            -------
                :`dict`
                    message with the correct scheme 

        """
        id = self.id_format(len(data['detector_names']))
        alert_type = "TEST "+ data['alert_type'] if is_test else data['alert_type']
        try:
            far = f"Would happen every {data['false_alarm_prob']:.2e} year"
        except:
            far = data['false_alarm_prob']
        return {"_id": id,
                "alert_type":alert_type,
                "server_tag": data['server_tag'],
                "False Alarm Prob": far,
                "detector_names": data['detector_names'],
                "sent_time": id.split(' ')[1],
                "p_values": data['p_vals'],
                "neutrino_times": data['neutrino_times'],
                "p_values average": data['p_val_avg'],
                "sub list number": data['sub_list_num']
                }
