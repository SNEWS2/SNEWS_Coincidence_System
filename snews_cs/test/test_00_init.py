import snews_cs
import unittest

class TestInit(unittest.TestCase):
    def test_version_exists(self):
        self.assertTrue(hasattr(snews_cs, '__version__'))
