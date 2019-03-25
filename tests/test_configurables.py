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
        config = CsvGenConfig(msgmenu)
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
            mb = MenuFactory('csvgen')
            msgmenu = mb.build()
            config = JobConfigFactory('csvgen', msgmenu,
                                      jobname='arrowproto',
                                      generator_type='csv',
                                      filehandler_type='csv',
                                      nbatches=10,
                                      num_cols=20,
                                      num_rows=10000,
                                      linesep='\r\n',
                                      delimiter=',',
                                      max_file_size=10485760,
                                      write_csv=True,
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


if __name__ == "__main__":
    unittest.main()
