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

from artemis.artemis import Artemis
from artemis.configurables.factories import MenuFactory, JobConfigFactory
from artemis.configurables.configs.csvgenconfig import CsvGenConfig

from artemis.core.tree import Tree
from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets
from artemis.core.properties import JobProperties

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

        config = CsvGenConfig()
        config.configure()

    def test_config_artemis(self):
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        
        mb = MenuFactory('csvgen')
        with tempfile.TemporaryDirectory() as dirpath:
            self.prtcfg = os.path.join(dirpath, 'test_configurable.dat')
            try:
                msgmenu = mb.build()
            except Exception:
                raise
            
            config = JobConfigFactory('csvgen', msgmenu)
            config.configure()
            msg = config.job_config
            try:
                with open(self.prtcfg, "wb") as f:
                    f.write(msg.SerializeToString())
            except IOError:
                self.__logger.error("Cannot write message")
            except Exception:
                raise
            bow = Artemis("arrowproto", 
                          protomsg=self.prtcfg,
                          loglevel='INFO',
                          jobname='test',
                          path=dirpath)
            bow.control()


if __name__ == "__main__":
    unittest.main()
