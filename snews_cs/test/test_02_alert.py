# -*- coding: utf-8 -*-
""" Initialization unit tests for the snews_cs alert tests
"""

import os
import signal
import unittest

from hop import Stream
from hop.io import StartPosition
from snews.models import messages
from snews_pt.messages import Publisher
from snews_pt.remote_commands import reset_cache

from snews_cs.snews_coinc import CoincidenceDistributor


class TestServer(unittest.TestCase):
    def timeout_handler(self, signum, frame):
        raise TimeoutError("Test timed out waiting for coincidence message.")

    timeout_secs = 30
    def test_alerts(self):
        coin1 = messages.CoincidenceTierMessage(detector_name='KamLAND',
                               neutrino_time_utc='2012-12-21T15:31:08.465011',
                               p_val=0.98,
                               is_test=True,
                               is_firedrill=False)

        coin2 = messages.CoincidenceTierMessage(detector_name='XENONnT',
                                    neutrino_time_utc='2012-12-21T15:31:07.465011',
                                    p_val=0.98,
                                    is_test=True,
                                    is_firedrill=False)

        # make sure the test cache is empty
        reset_cache(is_test=True)

        publisher = Publisher("kafka://kafka.scimma.org/snews.experiments-test")            
        publisher.add_message(coin1)
        publisher.add_message(coin2)

        # we need to think of a way for github actions to run the server
        # coincidence_searcher = CoincidenceDistributor(env_path='/etc/test-config.env', firedrill_mode=False, server_tag='test')

        # Next, manually open the stream and search for coincidences
        # this tests the coincidence logic ALREADY RUNNING on the server

        default_connection_topic = "kafka://kafka.scimma.org/snews.experiments-test"
        test_alert_topic = os.getenv("CONNECTION_TEST_TOPIC", 
                                     default_connection_topic)

        _start_at = StartPosition.LATEST #if start_at=="LATEST" else StartPosition.EARLIEST
        substream = Stream(until_eos=False, auth=True, start_at=_start_at)

        message_expected = {'False Alarm Prob': 'N/A',
                            'detector_names': ['XENONnT', 'KamLAND'],
                            'p_values': [0.98, 0.98],
                            'neutrino_times': ['2012-12-21T15:31:07.465011000Z', 
                                               '2012-12-21T15:31:08.465011000Z'],
                            'p_values average': 0.98
                            }

        fields_must_match = ["False Alarm Prob", 
                             "detector_names", "neutrino_times", 
                             "p_values", "p_values average"]
                             
        signal.signal(signal.SIGALRM, self.timeout_handler)
        signal.alarm(self.timeout_secs)

        with substream.open(test_alert_topic, "r") as ss:
            publisher.send()
            for read in ss:
                read = read.content
                print(read['detector_names'])
                for field in fields_must_match:
                    self.assertTrue(read[field] == message_expected[field], 
                                    f"Field {field} does not match!")
                break # stop otherwise we run forever (or timeout)

        # clear the timeout and alarm cache
        signal.alarm(0)
        reset_cache(is_test=True)
