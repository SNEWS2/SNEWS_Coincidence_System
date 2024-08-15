# -*- coding: utf-8 -*-
"""Initialization unit tests for the snews_cs module.
"""
import unittest
import snews_cs
from snews_pt.messages import SNEWSMessageBuilder

def test_alert(unittest.TestCase):
    """Test alert message builder."""
    message1 = SNEWSMessageBuilder(detector_name="XENONnT",
                                   machine_time="2022-01-01T00:00:00.000",
                                   detector_status="ON",)



