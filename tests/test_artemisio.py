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
import tempfile

from artemis.artemis import Artemis, ArtemisFactory
from artemis.core.singleton import Singleton
from artemis.core.properties import JobProperties
from artemis.core.tree import Tree
from artemis.core.datastore import ArrowSets
from artemis.core.physt_wrapper import Physt_Wrapper
from artemis.core.timerstore import TimerSvc
from artemis.generators.csvgen import GenCsvLikeArrow
from artemis.configurables.factories import MenuFactory, JobConfigFactory
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb
logging.getLogger().setLevel(logging.INFO)

# Improve temporary outputs and context handling
# stackoverflow 3223604


class ArtemisTestCase(unittest.TestCase):
        
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        Singleton.reset(Physt_Wrapper)
        Singleton.reset(TimerSvc)
    
    def test_fileio(self):
        '''
        Write csv to disk
        Read back in artemis
        '''
        with tempfile.TemporaryDirectory() as dirpath:
            mb = MenuFactory('csvgen')
            msgmenu = mb.build()
            generator = GenCsvLikeArrow('generator',
                                        nbatches=1,
                                        num_cols=20,
                                        num_rows=10000,
                                        suffix='.csv',
                                        prefix='testio',
                                        path=dirpath)
            generator.write()
            
            config = JobConfigFactory('csvio', msgmenu,
                                      jobname='arrowproto',
                                      generator_type='file',
                                      filehandler_type='csv',
                                      nbatches=1,
                                      num_rows=10000,
                                      max_file_size=1073741824,
                                      write_csv=True,
                                      input_repo=dirpath,
                                      input_glob='testio*.csv',
                                      output_repo=dirpath
                                      )

            config.configure()
            msg = config.job_config
            job = JobInfo_pb()
            job.name = 'arrowproto'
            job.job_id = 'example'
            job.output.repo = dirpath
            job.config.CopyFrom(msg)
            #job.job_id = str(uuid.uuid4())
            print(job)
            bow = ArtemisFactory(job, 'INFO')
            bow.control()

if __name__ == '__main__':
    unittest.main()
