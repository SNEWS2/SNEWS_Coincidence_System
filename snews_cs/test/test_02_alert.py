# -*- coding: utf-8 -*-
"""Initialization unit tests for the snews_cs alert tests"""

import os
import unittest

from hop import Stream
from hop.io import StartPosition
from snews_pt.messages import SNEWSMessageBuilder
from snews_pt.remote_commands import reset_cache


class TestServer(unittest.TestCase):
    def test_alerts(self):
        coin1 = SNEWSMessageBuilder(
            detector_name="KamLAND",
            machine_time_utc="2012-06-09T15:30:00.000501",
            neutrino_time="2012-06-09T15:31:08.891011",
            firedrill_mode=False,
            p_val=0.98,
            is_test=True,
        )

        coin2 = SNEWSMessageBuilder(
            detector_name="XENONnT",
            machine_time_utc="2012-06-09T15:30:00.000501",
            neutrino_time="2012-06-09T15:31:07.465011",
            firedrill_mode=False,
            p_val=0.98,
            is_test=True,
        )
        # make sure the test cache is empty
        reset_cache(is_test=True)

        # First, send two coinciding messages
        for coin in [coin1, coin2]:
            try:
                coin.send_messages()
            except Exception as exc:
                print(
                    (
                        "test_alert test failed trying to send messages with "
                        "SNEWSMessageBuilder.send_messages() !\n"
                    )
                )
                assert False, f"Exception raised:\n {exc}"

        # we need to think of a way for github actions to run the server
        # coincidence_searcher = CoincidenceDistributor(
        # env_path='/etc/test-config.env',
        # firedrill_mode=False,
        # server_tag='test'
        # )

        # Next, manually open the stream and search for coincidences
        # this tests the coincidence logic ALREADY RUNNING on the server
        default_connection_topic = "kafka://kafka.scimma.org/snews.connection-testing"
        test_alert_topic = os.getenv("CONNECTION_TEST_TOPIC", default_connection_topic)

        _start_at = StartPosition.LATEST  # if start_at=="LATEST" else StartPosition.EARLIEST
        substream = Stream(until_eos=True, auth=True, start_at=_start_at)

        message_expected = {
            "False Alarm Prob": "N/A",
            "id": "SNEWS_Coincidence_ALERT XXXXXXXXXXX",
            "alert_type": "TEST COINC_MSG",
            "detector_names": ["KamLAND", "XENONnT"],
            "neutrino_times": [
                "2012-06-09T15:31:08.891011000",
                "2012-06-09T15:31:07.465011000",
            ],
            "p_values": [0.98, 0.98],
            "p_values average": 0.98,
            "sent_time": "XXXXXXX",
            "server_tag": "XXXXXXXX",
            "sub list number": 0,
        }

        fields_must_match = [
            "False Alarm Prob",
            "alert_type",
            "detector_names",
            "neutrino_times",
            "p_values",
            "p_values average",
        ]

        with substream.open(test_alert_topic, "r") as ss:
            for read in ss:
                read = read.content
                for field in fields_must_match:
                    self.assertTrue(
                        read[field] == message_expected[field],
                        f"Field {field} does not match!",
                    )

        # clear the cache again afterwards
        reset_cache(is_test=True)
