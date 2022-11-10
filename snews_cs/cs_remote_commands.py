
import os
import json
import click
from snews_pt.snews_format_checker import SnewsFormat
from .core.logging import getLogger
log = getLogger(__name__)


"""
Command Handler should take care of the stability of the input message
Then check if the message is a remote command or actually a Tier message (use new SnewsFormat)
Then give a "go" to coincidence decider if valid.
Only if it is not a tier message and a command should invoke 'Commands'

"""

# TODO: Retract handling, broker change?

known_commands = ["test-connection",
                  "hard-reset",
                  "broker-change",
                  "Heartbeat",
                  "display-heartbeats",
                  "Retraction"]

class Commands:
    """ Class for remote commands"""
    def __init__(self):
        self.known_command_functions = {"test-connection": self.test_connection,
                                        "hard-reset": self.hard_reset,
                                        "broker-change": self.change_broker,
                                        "Heartbeat": self.heartbeat_handle,
                                        "display-heartbeats": self.display_heartbeats,
                                        "Retraction":self.retract_message}
        self.passw = os.getenv('snews_cs_admin_pass', 'False')

    def _check_rights(self, message):
        try:
            if message['pass'] == self.passw:
                return True
            else:
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

    def test_connection(self, message, CoincDeciderInstance):
        """ When received a test_connection key
            reinstert the message with updated status
            this way user can test if their message
            goes and comes back from the server
        """
        log.debug("\t> Executing Test Connection Command.")
        # it might be the second bounce, if so, log and exit
        if message["status"] == "received":
            log.debug("\n> Confirm Received.")
            return None

        from hop import Stream
        stream = Stream(until_eos=True)
        with stream.open(CoincDeciderInstance.observation_topic, "w") as s:
            # insert back with a "received" status
            msg = message.copy()
            msg["status"] = "received"
            s.write(msg)
            log.info(f"\t> Connection Tested. 'Received' message is reinserted to stream.")


    def hard_reset(self, message, CoincDeciderInstance):
        """ Authorized User (passing a correct password)
        """
        authorized = self._check_rights(message)
        if authorized:
            log.info("\t> Cache wanted to be reset. User is authorized.")
            CoincDeciderInstance.reset_df()
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
        log.debug(f"\t> Retracting message.. -NOT Implemented Yet!")
        return None

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
        log.debug(f"\t> Handling message..\n{self.input_json}\n")
        # check if the message in stream has SnewsFormat
        if not self.check_message_format():
            log.error("\t> Message not in SnewsFormat! NO-GO")
            return False
        else:
            # if passed, there has to be an _id field
            log.info(f"\t> Message is in SnewsFormat. '_id':{self.input_message['_id']} ")
            self.is_test = self.input_message['meta']['is_test']
            log.info(f"\t> Received Message is {'NOT' if not self.is_test else ''} test message!")

        # check what the _id field specifies
        self.command_name = self.input_message['_id'].split('_')[1]
        return self.check_command(CoincDeciderInstance) # GO / NO-GO

    def check_command(self, CoincDeciderInstance):
        # if it is a Remote-Command, find and execute it
        # return No-Go so that it doesn't try to check for coincidence
        if self.command_name in known_commands:
            log.info(f"\t> {self.command_name} command is passed!\n")
            self.Command_Executer.execute(self.command_name, self.input_message, CoincDeciderInstance)
            log.info(f"\t> {self.command_name} command Executed coincidence check is NO-GO!\n")
            return False

        # if it is a CoincidenceTier message, give a Go
        elif self.command_name == "CoincidenceTier":
            log.info(f"\t> Coincidence Tier message is received, coincidence check is GO!\n")
            return True

        # if it is something else (e.g. SigTier) log it and return No-Go for coincidence check
        else:
            log.error(f"\t> {self.command_name} is received, coincidence check is NO-GO!\n")
            return False


