"""
An interface for SNEWS alert publisher

Created: 
August 2021
Authors: 
Melih Kara
Sebastian Torres-Lara
"""
import os, click
import time
from hop import Stream
import adc.errors

from . import cs_utils
from .snews_db import Storage
from .core.logging import getLogger

# mwl
log = getLogger(__name__)

class AlertPublisher:
    """ Class to publish SNEWS SuperNova Alerts based on coincidence

    """
    def __init__(self, env_path=None, verbose=True, auth=True, firedrill_mode=True):
        """
        Alert publisher constructor 
        Parameters
        ----------
        env_path: str
            path to env file, defaults to
        verbose: bool
            Show alert, defaults to True
        auth: bool
            Use hop-auth credentials, defaults to True
        """
        cs_utils.set_env(env_path)
        self.auth = auth
        self.broker = os.getenv("HOP_BROKER")
        if firedrill_mode:
            self.alert_topic = os.getenv("FIREDRILL_ALERT_TOPIC")
        else:
            self.alert_topic = os.getenv("ALERT_TOPIC")
        self.verbose = verbose

    def __enter__(self):
        self.stream = Stream(until_eos=True, auth=self.auth).open(self.alert_topic, 'w')
        return self

    def __exit__(self, *args):
        self.stream.close()

    def send(self, message):
        """This method will set the sent_time and send the message to the hop broker.

        Parameters
        ----------
        message: dict
            dict containing observation message.

        """
        # Log it for now
        log.info(f"Want to publish: {message}")
        #self.stream.write(message)
        self.display_message(message)

    def display_message(self, message):
        if self.verbose:
            tier = 'TEST ALERT'
            click.secho(f'{"-" * 64}', fg='bright_blue')
            click.secho(f'Sending {tier}', fg='bright_red')
            for k, v in message.items():
                print(f'{k:<35s}:{v}')

# Display message prints out the following on the server logs
#
# SNEWS_Coincidence_ALERT 2022-09-28T08:21:21.954651
# ----------------------------------------------------------------
# Sending TEST ALERT
#
# maybe we can get rid of "Sending TEST ALERT" message


# XXX
# Perhaps this doesn't belong in this file

# XXX - TODO
#   sent_time threshold

class AlertListener:
    """ Class to receive SNEWS SuperNova Alerts based on coincidence

    """
    def __init__(self, env_path=None, verbose=True, auth=True, use_local=False, firedrill_mode=True, topic=None,
                 remotecomm=False):
        """
        Alert listener constructor
        Parameters
        ----------
        env_path: str
            path to env file, defaults to
        verbose: bool
            Show alert, defaults to True
        auth: bool
            Use hop-auth credentials, defaults to True
        use_local: bool
            Use local MongoClient, defaults to False
        topic: str
            Allows multiple instances of this class for the multiple alert tiers
        """
        cs_utils.set_env(env_path)
        self.auth = auth
        self.broker = os.getenv("HOP_BROKER")

        if firedrill_mode:
            self.alert_topic = os.getenv("FIREDRILL_ALERT_TOPIC")
        else:
            self.alert_topic = os.getenv("ALERT_TOPIC")

        # Override the default
        if topic is not None:
            self.alert_topic = topic

        # Some Kafka errors are retryable.
        self.retriable_error_count = 0
        self.max_retriable_errors = 20
        self.exit_on_error = False  # True

        self.verbose = verbose
        self.storage = Storage(drop_db=False, use_local_db=use_local)
        self.stream = Stream(until_eos=True, auth=self.auth).open(self.alert_topic, 'r')

#    def __enter__(self):
#        self.stream = Stream(until_eos=True, auth=self.auth).open(self.alert_topic, 'r')
#        return self

    def __exit__(self, *args):
        self.stream.close()

    def run(self):
        while True:
            try:
                for message in self.stream:
                    snews_message = None
                    # XXX - Debug
                    self.display_message(message)

                    # check for the hop version
                    try:
                        snews_message = message.content
                    except Exception as e:
                        log.error(f"A message with older hop version is found. {e}\n{snews_message}")
                        snews_message = message

                    self.storage.insert_mgs(snews_message)

                time.sleep(10)
            # handle a keyboard interrupt (ctrl+c)
            except KeyboardInterrupt:
                print("Caught a keyboard interrupt.  Goodbye world!")
                log.error(f"(2) Caught a keyboard interrupt. Exiting.\n")
                fatal_error = True
                self.exit_on_error = True
                sys.exit(0)

            # if there is a KafkaException, check if retriable
            except adc.errors.KafkaException as e:
                if e.retriable:
                    self.retriable_error_count += 1
                    if self.retriable_error_count >= self.max_retriable_errors:
                        log.error(f"Max retryable errors exceeded. Here is the most recent exception:\n{e}\n")
                        fatal_error = True
                    else:
                        log.error(f"Retryable error! \n{e}\n")
                        # sleep with exponential backoff and a bit of jitter.
                        time.sleep((1.5 ** self.retriable_error_count) * (1 + random.random()) / 2)
                else:
                    log.error(
                        f"(1) Something crashed the server, not a retriable error, here is the Exception raised\n{e}\n")
                    fatal_error = True

            # any other exception is logged, but not fatal (?)
            except Exception as e:
                log.error(f"(2) Something crashed the server, here is the Exception raised\n{e}\n")
                fatal_error = False  # True # maybe not a fatal error?

            finally:
                # if we are breaking on errors and there is a fatal error, break
                if self.exit_on_error and fatal_error:
                    break
                # otherwise continue by re-initiating
                continue

    def display_message(self, message):
        if self.verbose:
            click.secho(f'{"-" * 64}', fg='bright_blue')
            for k, v in message.items():
                print(f'{k:<35s}:{v}')

