#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""

"""
import coverage
import unittest
import logging
import tempfile

from artemis.artemis import Artemis
from artemis.configurables.factories import MenuFactory, JobConfigFactory
from artemis.core.singleton import Singleton
from artemis.core.tree import Tree
from artemis.core.datastore import ArrowSets
from artemis.core.properties import JobProperties

logging.getLogger().setLevel(logging.INFO)


class ArtemisTestCase(unittest.TestCase):
        
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)

    def test_proto(self):
        cov = coverage.Coverage()
        cov.start()
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        mb = MenuFactory('csvgen')
        with tempfile.TemporaryDirectory() as dirpath:
            self.prtcfg = dirpath + 'test_configurable.dat'
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
            
            cov.stop()
            cov.save()


if __name__ == '__main__':
    unittest.main()
