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
import unittest
import logging
from artemis.artemis import Artemis
from artemis.core.singleton import Singleton
from artemis.core.properties import JobProperties
from artemis.core.tree import Tree
from artemis.core.datastore import ArrowSets
from artemis.generators.csvgen import GenCsvLikeArrow
from artemis.configurables.factories import MenuFactory, JobConfigFactory

logging.getLogger().setLevel(logging.INFO)


class ArtemisTestCase(unittest.TestCase):
        
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        Singleton.reset(JobProperties)
    
    def test_fileio(self):
        '''
        Write csv to disk
        Read back in artemis
        '''
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        self.prtcfg = 'arrowproto_proto.dat'

        generator = GenCsvLikeArrow('generator',
                                    nbatches=10,
                                    num_cols=20,
                                    num_rows=10000,
                                    suffix='.csv',
                                    prefix='testio',
                                    path='/tmp')
        generator.write()
        mb = MenuFactory('csvgen')
        self.prtcfg = 'test_configurable.dat'
        try:
            msgmenu = mb.build()
        except Exception:
            raise
        
        config = JobConfigFactory('csvio', msgmenu)
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
                      jobname='test')
        bow.control()


if __name__ == '__main__':
    unittest.main()
