# from .cs_utils import TimeStuff
from datetime import datetime
import sys


class CoincidenceTierAlert:
    """ The Message scheme for the alert and observations

    Parameters
    ----------
    env_path : `str`, optional
        The path containing the environment configuration file
        If None, uses the default file in '/auxiliary/test-config.env'

    """

    # def __init__(self, env_path=None):
        # self.times = TimeStuff(env_path)

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
        # date_time = self.times.get_snews_time(fmt="%y/%m/%d %H:%M:%S:%f")
        date_time = datetime.utcnow().isoformat()
        if num_detectors == 2:
            return f'SNEWS_Coincidence_ALERT {date_time}'
        else:
            return f'SNEWS_Coincidence_ALERT-UPDATE {date_time}'

    def get_cs_alert_schema(self, data):
        """ Create a message schema for alert.
            Internally called in hop_pub
        
            Parameters
            ----------
            data : `named tuple`
                cs_utils data tuple with predefined field.
            sent_time : `str`
                time as a string
            
            Returns
            -------
                :`dict`
                    message with the correct scheme 

        """
        id = self.id_format(len(data['detector_names']))
        return {"_id": id,
                "server_tag": data['server_tag'],
                "false_alarm_probability": data['false_alarm_prob'],
                "detector_names": data['detector_names'],
                "sent_time": id.split(' ')[1]+' '+id.split(' ')[2],
                "p_values": data['p_vals'],
                "neutrino_times": data['neutrino_times'],
                "p_values average": data['p_val_avg'],
                "sub list number": data['sub_list_num']
                }
