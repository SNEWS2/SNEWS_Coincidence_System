import os
import json
import click
from snews_pt.snews_format_checker import SnewsFormat
import pandas as pd
from .heartbeat_feedbacks import check_frequencies_and_send_mail, delete_old_figures
from .core.logging import getLogger
from hop.models import JSONBlob

log = getLogger(__name__)

"""
Command Handler should take care of the stability of the input message
Then check if the message is a remote command or actually a Tier message (use new SnewsFormat)
Then give a "go" to coincidence decider if valid.
Only if it is not a tier message and a command should invoke 'Commands'

"""

known_commands = [
    "test-connection",
    "hard-reset",
    "broker-change",
    "Heartbeat",
    "display-heartbeats",
    "Retraction",
    "Get-Feedback"
]

contact_list_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'auxiliary/contact_list.json'))
with open(contact_list_file) as file:
    contact_list = json.load(file)

# this csv file is mirroring the existing heartbeat cache
beats_path = os.path.join(os.path.dirname(__file__), "../beats")
csv_path = os.path.join(beats_path, f"cached_heartbeats_mirror.csv")

# should I allow people to change their passwords? I can use simple encryption: from cryptography.fernet import Fernet
class Commands:
    """ Class for remote commands"""

    def __init__(self):
        self.known_command_functions = {"test-connection": self.test_connection,
                                        "hard-reset": self.hard_reset,
                                        "broker-change": self.change_broker,
                                        "Heartbeat": self.heartbeat_handle,
                                        "display-heartbeats": self.display_heartbeats,
                                        "Retraction":self.retract_message,
                                        "Get-Feedback":self.send_feedback}
        self.passw = os.getenv('snews_cs_admin_pass', 'False')

    def _check_rights(self, message):
        try:
            if message['pass'] == self.passw:
                return True
            else:
                log.error(f"\t> Authorization check failed")
                return False
        except Exception as e:
            log.error(f"\t> Authorization check failed\n{e}")
            return False

    def execute(self, command_name, message, CoincDeciderInstance):
        """ If Command Handler finds a Remote Command from known_commands
            It returns the name, message and the CoincidenceInstance
            Here we perform some remote commands on that CoincidenceInstance
            such as resetting the cache.
        """
        # get the function that executes given command
        command = self.known_command_functions[command_name]
        # execute that function
        command(message, CoincDeciderInstance)
        # return default NO-GO, this is only changed if the message is Retraction!
        # for retraction message we need to let it enter the cache. So updated alerts can be checked

    def test_connection(self, message, CoincDeciderInstance):
        """ When received a test_connection key in observation topic
            reinstert the message with updated status to connection topic
            this way user can test if their message
            goes and comes back from the server, by looking into the connection topic
        """
        log.debug("\t> Executing Test Connection Command.")
        default_connection_topic = "kafka://kafka.scimma.org/snews.connection-testing"
        connection_broker = os.getenv("CONNECTION_TEST_TOPIC", default_connection_topic)
        # it might be the second bounce, if so, log and exit
        # if message["status"] == "received":
        #     log.debug("\t> Confirm Received.")
        #     return None

        from hop import Stream
        stream = Stream(until_eos=True)
        msg = message.copy()
        msg["status"] = "received"
        with stream.open(connection_broker, "w") as s:
            # insert back with a "received" status
            s.write(JSONBlob(msg))
            log.info(f"\t> Connection Tested. 'Received' message is reinserted to connection stream.")

    def hard_reset(self, message, CoincDeciderInstance):
        """ Authorized User (passing a correct password)
        """
        authorized = self._check_rights(message)
        if authorized:
            log.info("\t> Cache wanted to be reset. User is authorized.")
            # CoincDeciderInstance.reset_df()
            CoincDeciderInstance.clear_cache()
            log.info("\t> Cache is reset.")
            return None
        else:
            log.debug("\t> Cache wanted to be reset. User is NOT authorized.")

    def change_broker(self, message, CoincDeciderInstance):
        try:
            new_broker_name = message["_id"][2]
        except Exception as e:
            new_broker_name = "NotImplemented"

        authorized = self._check_rights(message)
        if authorized:
            log.info("\t> Broker change requested. User is authorized.")
        log.debug(f" broker requested to changed to '{new_broker_name}' but it is not implemented yet.\n")

    def heartbeat_handle(self, message, CoincDeciderInstance):
        """handle heartbeat"""
        success = CoincDeciderInstance.heartbeat.electrocardiogram(message)
        if success:
            log.debug(f"\t> Heartbeat registered.")
        else:
            log.debug(f"\t> Heartbeat failed to register!")

    def display_heartbeats(self, message, CoincDeciderInstance):
        authorized = self._check_rights(message)
        if authorized:
            log.info("\t> User wants to display the Heartbeat table. User is authorized.")
            click.secho("\nCurrent heartbeats table as requested by the user\n")
            print(CoincDeciderInstance.heartbeat.cache_df.to_markdown(), "\n\n")
            log.debug(f"\t> Logs printed as stdout, user should check the remote logs.")
        else:
            log.error("\t> User wants to display the Heartbeat table. User is NOT authorized.")

    def retract_message(self, message, CoincDeciderInstance):
        log.info(f"\t> Retracting message in the snews_coinc.")
                  # f"This requires a GO so that message can be added and compared in the cache!")

    def send_feedback(self, message, CoincDeciderInstance):
        """ Check the user and pre-compiled email list
            send an email with a feedback from past 24H
            multiple mails are allowed by separating semicolon ";"
            Expected message format
            message = {'_id': '0_Get-Feedback',
                       'email': email_address,
                       'detector_name': detector_name,
                       'meta': {}}
        """
        given_mail = message.get('email', None)
        if given_mail is None:
            log.error(f"\t> No email is given, ignoring.")
            return None

        # first check if request email is in our list
        detector = message['detector_name']
        none_valid = True
        # avoid empty lines, and allow multiple emails
        given_mail =  [mail.strip() for mail in given_mail.split(";") if len(mail.strip())]
        log.debug(f"> [DEBUG] These mails are passed {'; '.join(given_mail)} for detector: {detector}")
        for email in given_mail:
            if not email in contact_list[detector]["emails"]:
                log.error(f"\t> The given email: {email} is not registered for {detector}!")
                print(f"> [DEBUG] The given email: {email} is not registered for {detector}!")
            else:
                none_valid = False
        if none_valid:
            log.error(f"\t> None of the the given email: {';'.join(given_mail)} is registered, ignoring all!")
            return None
        try:
            attachment_name, out = check_frequencies_and_send_mail(detector, given_contact=given_mail)
            if out:
                log.info(f"\t> The feedback file: {attachment_name} is sent to the registered mails for {detector}")
            else:
                log.debug(f"\t> The feedback file: {attachment_name} is created but could not be sent.")
        except Exception as e:
            log.info(f"\t> Something went wrong for {detector}, couldn't send mail, see the exception;\n{e}")

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
            - Request Feedback
        """

    def __init__(self, message):
        

        self.input_message = message
        self.input_json = json.dumps(message, sort_keys=True, indent=4)
        self.command_name = None
        self.is_test = False
        self.Command_Executer = Commands()

        self.username = self.input_message.get("detector_name", "TEST")
        self.entry = f"\n|{self.username}|"

    def check_message_format(self):
        formatter = SnewsFormat(self.input_message, log=log)
        return formatter()

    def handle(self, CoincDeciderInstance):
        log.debug(f"\t> Handling message..\n")
        # check if the message in stream has SnewsFormat
        if not self.check_message_format():
            log.error("\t> Message not in SnewsFormat! NO-GO")
            return False
        else:
            # if passed, there has to be an _id field
            log.info(f"\t> Message is in SnewsFormat. '_id':{self.input_message['_id']} ")
            # temporary fix for the test messages
            if "meta" in self.input_message.keys():
                self.is_test = self.input_message['meta'].get('is_test', False)
            else:
                if "is_test" in self.input_message.keys():
                    self.is_test = self.input_message['is_test']
                else:
                    self.is_test = False
            log.info(f"\t> Received Message is {'NOT ' if not self.is_test else ''}a test message!")

        # check what the _id field specifies
        self.command_name = self.input_message['_id'].split('_')[1]
        return self.check_command(CoincDeciderInstance)  # GO / NO-GO

    def check_command(self, CoincDeciderInstance):
        # if it is a Remote-Command, find and execute it
        # return No-Go so that it doesn't try to check for coincidence
        if self.command_name in known_commands:
            log.info(f"\t> [COMMAND] {self.command_name} command is passed!")
            self.Command_Executer.execute(self.command_name, self.input_message, CoincDeciderInstance)
            if self.command_name == "Retraction":
                log.info(f"\t> {self.command_name} command executed coincidence check is still a GO!")
                # it is a retraction message, requires to return a GO
                return True
            else:
                log.info(f"\t> {self.command_name} command executed coincidence check is a NO-GO!")
                return False

        # if it is a CoincidenceTier message or a Retraction Message, give a Go
        elif self.command_name in ["CoincidenceTier", "Retraction"]:
            log.info(f"\t> {self.command_name} message is received, coincidence check is GO!")
            return True

        # if it is something else (e.g. SigTier) log it and return No-Go for coincidence check
        else:
            log.error(f"\t> {self.command_name} is received (NOT KNOWN), coincidence check is NO-GO!\n")
            return False
