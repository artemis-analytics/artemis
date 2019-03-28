#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import unittest
import logging
import tempfile

from artemis.io.collector import Collector
from artemis.core.tree import Element
import pandas as pd
import numpy as np
import pyarrow as pa
logging.getLogger().setLevel(logging.INFO)


class CollectorTestCase(unittest.TestCase):
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        pass

    def test(self):
        pass

if __name__ == "__main__":
    unittest.main()
