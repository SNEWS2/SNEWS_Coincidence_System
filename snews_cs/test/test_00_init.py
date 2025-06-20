# -*- coding: utf-8 -*-
"""Initialization unit tests for the snews_cs module."""
import unittest

import snews_cs


class TestInit(unittest.TestCase):
    def test_version_exists(self):
        self.assertTrue(hasattr(snews_cs, "__version__"))
