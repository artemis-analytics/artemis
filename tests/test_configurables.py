#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

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

from artemis.core.tree import Tree
from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets
from artemis.core.properties import JobProperties
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb
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
        mb = MenuFactory('csvgen')
        msgmenu = mb.build()
        config = CsvGenConfig(msgmenu, table_id='dummy')
        config.configure()

    def test_config_artemis(self):
        '''
        See test_artemis.py
        '''
        pass


if __name__ == "__main__":
    unittest.main()
