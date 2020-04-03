#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Testing the configurable classes
"""
import unittest
import logging
import tempfile
import os

from artemis.artemis import Artemis, ArtemisFactory
from artemis.configurables.factories import MenuFactory, JobConfigFactory
from artemis.configurables.configs.csvgenconfig import CsvGenConfig

from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets
from artemis.core.gate import ArtemisGateSvc
from artemis_format.pymodels.artemis_pb2 import JobInfo as JobInfo_pb

logging.getLogger().setLevel(logging.INFO)


class ConfigurableTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_config(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        mb = MenuFactory("csvgen")
        msgmenu = mb.build()
        config = CsvGenConfig(msgmenu, table_id="dummy")
        config.configure()

    def test_config_artemis(self):
        """
        See test_artemis.py
        """
        pass


if __name__ == "__main__":
    unittest.main()
