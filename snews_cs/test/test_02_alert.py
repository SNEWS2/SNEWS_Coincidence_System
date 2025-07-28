# -*- coding: utf-8 -*-
""" Initialization unit tests for the snews_cs alert tests
"""

import os
import unittest

from hop import Stream
from hop.io import StartPosition
from snews.models import messages
from snews_pt.messages import Publisher
from snews_pt.remote_commands import reset_cache

from snews_cs.snews_coinc import CoincidenceDistributor


class TestServer(unittest.TestCase):
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

        # First, send two coinciding messages
        for coin in [coin1, coin2]:
            try:
                publisher.add_message(coin1)
                publisher.add_message(coin2)

            except Exception as exc:
                print('test_alert test failed trying to send messages with SNEWSMessageBuilder.send_messages() !\n')
                assert False, f"Exception raised:\n {exc}"

        # we need to think of a way for github actions to run the server
        # coincidence_searcher = CoincidenceDistributor(env_path='/etc/test-config.env', firedrill_mode=False, server_tag='test')

        # Next, manually open the stream and search for coincidences
        # this tests the coincidence logic ALREADY RUNNING on the server
        default_connection_topic = "kafka://kafka.scimma.org/snews.experiments-test"
        test_alert_topic = os.getenv("CONNECTION_TEST_TOPIC", 
                                     default_connection_topic)

        _start_at = StartPosition.LATEST #if start_at=="LATEST" else StartPosition.EARLIEST
        substream = Stream(until_eos=True, auth=True, start_at=_start_at)

        message_expected = {'False Alarm Prob': 'N/A',
                            'detector_names': ['XENONnT', 'DS-20K'],
                            'p_values': [0.98, 0.98],
                            'neutrino_times': ['2030-01-01T12:34:45.678999000Z',
                            '2030-01-01T12:34:47.678999000Z'],
                            'p_values average': 0.98
                            }

        fields_must_match = ["False Alarm Prob", "alert_type", 
                             "detector_names", "neutrino_times", 
                             "p_values", "p_values average"]

        with substream.open(test_alert_topic, "r") as ss:
            publisher.send()
            for read in ss:
                read = read.content
                print(read['detector_names'])
                for field in fields_must_match:
                    self.assertTrue(read[field] == message_expected[field], 
                                    f"Field {field} does not match!")

        # clear the cache again afterwards
        reset_cache(is_test=True)
