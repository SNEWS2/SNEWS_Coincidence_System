from .cs_utils import TimeStuff
import sys


class CoincidenceTierAlert:
    """ The Message scheme for the alert and observations

    Parameters
    ----------
    env_path : `str`, optional
        The path containing the environment configuration file
        If None, uses the default file in '/auxiliary/test-config.env'
    detector_key : `str`, optional
        The name of the detector. If None, uses "TEST"
    alert : `bool`, optional
        True if the message is ALERT message. Default is False.

    """

    def __init__(self, env_path=None):
        self.times = TimeStuff(env_path)

    def id_format(self, num_detectors):
        """ Returns formatted message ID
            time format should always be same for all detectors.

        Returns
            :`str`
                The formatted id as a string
            
        """
        date_time = self.times.get_snews_time(fmt="%y/%m/%d %H:%M:%S:%f")
        if num_detectors == 2:
            return f'SNEWS_Coincidence-ALERT_{date_time}'
        else:
            return f'SNEWS_Coincidence-ALERT-UPDATE_{date_time}'


    def get_cs_alert_schema(self, data):
        """ Create a message schema for given topic type.
            Internally called in hop_pub
        
            Parameters
            ----------
            msg_type : `str`
                type of the message to be published. Can be;
                'TimeTier', 'SigTier', 'CoincidenceTier' for
                observation messages and, 'HeartBeat' for 
                heartbeat messages
            data : `named tuple`
                snews_utils data tuple with predefined field.
            sent_time : `str`
                time as a string
            
            Returns
            -------
                :`dict`
                    message with the correct scheme 

        """
        id = self.id_format(len(data['detector_names']))
        return {"_id": id,
                "detector_names": data['detector_names'],
                "sent_time": id.split('_')[2],
                "p_values": data['p_vals'],
                "neutrino_times": data['neutrino_times'],
                "p_values average": data['p_val_avg'],
                "sub list number":data['sub_list_num']
                }
