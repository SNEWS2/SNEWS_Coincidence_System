# -*- coding: utf-8 -*-
"""Initialization unit tests for the snews_cs module."""
import unittest

import snews_pt.remote_commands as sptrc

import io
import contextlib


class TestServer(unittest.TestCase):
    def test_connection(self):
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            # - Connect to server
            sptrc.test_connection(
                detector_name="XENONnT",
                firedrill=False,
                start_at="LATEST",
                patience=8,
            )

            # - Check the output message; it should say "You (XENONnT) have a connection"
            confirm_msg = "You (XENONnT) have a connection to the server"
            self.assertTrue(confirm_msg in f.getvalue())
