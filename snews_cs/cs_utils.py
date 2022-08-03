"""
Example initial dosctring
"""
from dotenv import load_dotenv
from datetime import datetime
import os
import json
import click

from .core.logging import getLogger

log = getLogger(__name__)

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


def is_garbage_message(snews_message, is_test=False):
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
    if missing_key:
        log.warning(warning)
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
        log.warning(warning)
        return is_garbage
    shitty_nu_time = False

    try:
        time.str_to_datetime(snews_message['neutrino_time'])
    except ValueError:
        warning += f'* neutrino time: {snews_message["neutrino_time"]} does not match SNEWS CS format: "%y/%m/%d %H:%M:%S:%f"\n'
        shitty_nu_time = True
        is_garbage = True

    if (time.str_to_datetime(snews_message['neutrino_time']) - datetime.utcnow()).total_seconds() <= -172800.0:
        warning += f'* neutrino time is more than 48 hrs olds !\n'
        shitty_nu_time = True
        is_garbage = True

    if (time.str_to_datetime(snews_message['neutrino_time']) - datetime.utcnow()).total_seconds() > 0:
        if is_test:
            pass
        else:
            warning += f'* neutrino time comes from the future, please stop breaking causality\n'
            shitty_nu_time = True
            is_garbage = True

    if shitty_nu_time:
        log.warning(warning)
        return is_garbage

    return is_garbage


class CommandHandler:
    """ class to handle the manual command issued by the admins
        These commands can be
        - Garbage message handling
        - Reset the cache
        - Test connection
        - Retract messages
        - Testing-purpose submissions
        - Get logs
        - Change Broker
    """
    def __init__(self, message):
        self.known_commands = ["test-connection", "test-scenarios",
                               "hard-reset", "Retraction", "broker-change", "Heartbeat"]
        self.known_command_functions = {"test-connection": self.test_connection,
                                        "hard-reset": self.hard_reset,
                                        "Retraction": self.retract,
                                        "broker-change": self.change_broker,
                                        "Heartbeat": self.heartbeat_handle}
        self.input_message = message
        self.command = None
        self.username = self.input_message.get("detector_name", "NoName")
        self.times = TimeStuff()
        self.entry = lambda : f"\n|{self.username}|"

    def handle(self, CoincDeciderInstance):
        if not self.check_id():
            return False
        self.command = self.input_message['_id'].split('_')[1]
        return self.check_command(CoincDeciderInstance)

    def check_id(self):
        """ check if the format is correct
            snews_pt sends messages in mongodb format
            which HAS TO contain an _id field
        """
        if "_id" not in self.input_message.keys():
            msg = f"{self.entry()} message without '_id' field\n" \
                  f"{self.input_message}\n"
            log.error(msg)
            return False
        else:
            return True

    def check_command(self, CoincDeciderInstance):
        if self.command in self.known_commands:
            msg = f"{self.entry()} {self.command} is passed!"
            log.info(msg)
            return self.known_command_functions[self.command](CoincDeciderInstance)
        else:
            # for now assume it is an observation message
            if "meta" not in self.input_message.keys():
                msg = f"{self.entry()} message with no meta key received. Ignoring!"
                log.warning(msg)
                return False
            if "this is a test" in self.input_message['meta'].values():
                is_test = True
                msg = f"{self.entry()} {self.command} TEST SCENARIO Message Received!\n"
                log.info(msg)
                if "test" or "firedrill" in CoincDeciderInstance.observation_topic:
                    pass
                else:
                    msg = f"\nThe {CoincDeciderInstance.observation_topic} does not allow for tests!\n"
                    log.warning(msg)
                    return False
            else:
                msg = f"{self.entry()} {self.command} Observation Message Received!\n"
                log.info(msg)
                is_test = False
            is_garbage = is_garbage_message(self.input_message, is_test=is_test)
            is_correct_topic = (self.input_message['_id'].split('_')[1] == CoincDeciderInstance.topic_type)
            if (not is_garbage) and is_correct_topic:
                msg += "\t valid message\n"
                log.info(msg)
                return True
            else:
                msg += "\t NOT a valid message\n"
                log.warning(msg)
                return  False

    def test_connection(self, CoincDeciderInstance):
        """ When received a test_connection key
            reinstert the message with updated status
            this way user can test if their message
            goes and comes back from the server
        """
        if self.input_message["status"] == "received":
            msg = f"{self.entry()} confirm received\n"
            log.info(msg)
            return False
        from hop import Stream
        stream = Stream(until_eos=True)
        with stream.open(CoincDeciderInstance.observation_topic, "w") as s:
            # insert back with a "received" status
            msg = self.input_message.copy()
            msg["status"] = "received"
            s.write(msg)
            log.info(f"{self.entry()} tested their connection\n")
        return False

    def _check_rights(self):
        if self.input_message['pass'] == os.getenv('snews_cs_admin_pass'):
            return True
        else:
            return False

    def hard_reset(self, CoincDeciderInstance):
        if self._check_rights():
            CoincDeciderInstance.reset_df()
            msg = click.style(f"{self.entry()} Cache restarted", fg='yellow')
        else:
            msg = click.style(f'{self.entry()} The user has no right to reset the cache', fg='yellow')
        log.info(f'{msg}\n')
        return False

    def retract(self, CoincDeciderInstance):
        retrc_message = self.input_message
        if not retrc_message.get('N_retract_latest', False):
            msg = f"{self.entry()} Tried retracting message without 'N_retract_latest' key, setting to 'ALL'"
            log.warning(msg)
            retrc_message['N_retract_latest'] = 'ALL'

        drop_detector = retrc_message['detector_name']
        delete_n_many = retrc_message['N_retract_latest']

        if retrc_message['N_retract_latest'] == 'ALL':
            delete_n_many = CoincDeciderInstance.cache_df.groupby(by='detector_name').size().to_dict()[drop_detector]
        msg = click.style(f'{self.entry()} Dropping latest message(s) from {drop_detector}\nRetracting: {delete_n_many} messages\n')
        log.info(msg)
        sorted_df = CoincDeciderInstance.cache_df.sort_values(by='received_time')
        for i in sorted_df.index:
            if delete_n_many > 0 and CoincDeciderInstance.cache_df.loc[i, 'detector_name'] == drop_detector:
                CoincDeciderInstance.cache_df.drop(index=i, inplace=True)
                delete_n_many -= 1
        CoincDeciderInstance.cache_df = CoincDeciderInstance.cache_df.reset_index(drop=True)
        return False

    def change_broker(self, CoincDeciderInstance):
        auth = self._check_rights()
        new_broker_name = self.input_message["_id"]
        if auth:
            msg = click.style(f"{self.entry()} tried to change the broker but it is not implemented")
        else:
            msg = click.style(f"{self.entry()} tried to change the broker.")
        log.info(msg)
        # raise NotImplementedError # do not crash the server
        return False

    def heartbeat_handle(self, CoincDeciderInstance):
        msg = f"{self.entry()} Heartbeat Received (Not implemented Yet)!"
        log.info(msg)
        return False

    def display_logs(self, CoincDeciderInstance):
        auth = self._check_rights()
        new_broker_name = self.input_message["_id"]
        if auth:
            msg = click.style(f"{self.entry()} tried to display the logs but it is not implemented")
        else:
            msg = click.style(f"{self.entry()} tried to display the logs.")
        log.info(msg)
        return False
