# -*- coding: utf-8 -*-
"""Initialization unit tests for the snews_cs alert tests"""

import os
import unittest
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

from hop import Stream
from hop.io import StartPosition
from snews_cs.snews_coinc import CoincidenceDistributor
from snews_cs.cs_utils import set_env


class TestServer(unittest.TestCase):
    def setUp(self):
        """Set up test environment"""
        # Set up test environment variables
        os.environ["COINCIDENCE_THRESHOLD"] = "10"  # 10 second threshold
        os.environ["CONNECTION_TEST_TOPIC"] = "kafka://kafka.scimma.org/snews.connection-testing"

    @patch('snews_cs.snews_coinc.AlertPublisher')
    def test_alerts(self, mock_alert_publisher):
        """Test the alert generation and coincidence detection"""
        # Mock the alert publisher
        mock_publisher = MagicMock()
        mock_alert_publisher.return_value.__enter__.return_value = mock_publisher
        
        # Initialize coincidence distributor for testing
        self.coinc = CoincidenceDistributor(
            env_path=None,  # Use default test config
            firedrill_mode=False,
            server_tag="test",
            send_email=False,
            send_slack=False
        )
        
        # Clear any existing test cache
        self.coinc.clear_cache(is_test=True)
        
        # Create test messages with coinciding neutrino times
        base_time = datetime.utcnow()
        
        # First message
        message1 = {
            "_id": "test1_CoincidenceTier_" + base_time.isoformat(),
            "detector_name": "KamLAND",
            "machine_time_utc": base_time.isoformat(),
            "neutrino_time_utc": base_time.isoformat(),
            "p_val": 0.98,
            "meta": {"is_test": True},
            "schema_version": "1.2.0"
        }
        
        # Second message within coincidence window
        message2 = {
            "_id": "test2_CoincidenceTier_" + base_time.isoformat(),
            "detector_name": "XENONnT",
            "machine_time_utc": base_time.isoformat(),
            "neutrino_time_utc": (base_time + timedelta(seconds=5)).isoformat(),
            "p_val": 0.98,
            "meta": {"is_test": True},
            "schema_version": "1.2.0"
        }

        # Process messages through coincidence system
        self.coinc.deal_with_the_cache(message1)
        self.coinc.deal_with_the_cache(message2)

        # Verify that the alert was published with correct content
        self.assertTrue(mock_publisher.send.called, "Alert was not published")
        
        # Get the alert message that was published
        alert_message = mock_publisher.send.call_args[0][0]
        
        # Verify alert message content
        self.assertEqual(alert_message["alert_type"], "TEST COINC_MSG")
        self.assertEqual(alert_message["detector_names"], ["KamLAND", "XENONnT"])
        self.assertEqual(alert_message["p_values"], [0.98, 0.98])
        self.assertEqual(alert_message["p_values average"], 0.98)
        self.assertEqual(alert_message["sub list number"], 0)
        self.assertEqual(alert_message["False Alarm Prob"], "N/A")
        
        # Verify neutrino times are within coincidence window
        nu_times = [datetime.fromisoformat(t) for t in alert_message["neutrino_times"]]
        self.assertTrue(
            abs(nu_times[0] - nu_times[1]) <= timedelta(seconds=10),
            "Neutrino times are not within coincidence window!"
        )

    def tearDown(self):
        """Clean up after tests"""
        if hasattr(self, 'coinc'):
            self.coinc.clear_cache(is_test=True)
