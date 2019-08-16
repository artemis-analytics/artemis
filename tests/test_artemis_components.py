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
import os
import shutil

from artemis.core.steering import Steering
from artemis.artemis import Artemis, ArtemisFactory
from artemis.core.gate import ArtemisGateSvc
from artemis.logger import Logger
from artemis.configurables.factories import MenuFactory, JobConfigFactory

from artemis.core.tree import Tree
from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets

import artemis.io.protobuf.artemis_pb2 as artemis_pb2
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb

logging.getLogger().setLevel(logging.INFO)


class ArtemisTestCase(unittest.TestCase):
     
    def reset(self):
        Singleton.reset(ArtemisGateSvc)
        Singleton.reset(ArrowSets)

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        self.reset()
        
        mb = MenuFactory('csvgen')
        msgmenu = mb.build()
        config = JobConfigFactory('csvgen',msgmenu,
                                  jobname='arrowproto',
                                  output_repo='')
        config.configure()
        msg = config.job_config
       
        self.job = JobInfo_pb()
        self.job.name = 'arrowproto'
        self.job.job_id = 'example'
        self.job.output.repo = ''
        self.job.config.CopyFrom(msg)

    def tearDown(self):
        self.reset()
        # Should be able to call self.tmppath.cleanup()
        # But above, cannot join str and TemporaryDirectory types
    ''' 
    def test_launch(self):
        self.reset()
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_RUNNING
            print('Launching')
            bow.launch()
            print('End Launch')

    def test_configure(self):
        Singleton.reset(JobProperties)
        Singleton.reset(ArrowSets)
        Singleton.reset(Tree)
        Singleton.reset(Physt_Wrapper)
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_RUNNING
            print('Configuring')
            bow.configure()

    def test_lock(self):
        self.reset()
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_RUNNING
            print('Locking')
            bow.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)
            bow.lock()

    def test_initialize(self):
        self.reset()
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_INITIALIZE
            print('Initializing')
            bow.configure()
            bow.initialize()

    def test_book(self):
        self.reset() 
        self.prtcfg = 'arrowproto_proto.dat'
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_BOOK
            bow.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)
            print('Booking')
            bow.book()

    def test_run(self):
        self.reset()
        return True
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_RUNNING
            bow.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)
            bow.steer._hbook = Physt_Wrapper()
            bow.steer._hbook.book('steer.time', 'dummy', range(10))
            bow.steer._hbook.book('steer.time', 'csvparser', range(10))
            bow.steer._hbook.book('steer.time', 'profiler', range(10))
            print('Running')
            bow.hbook = Physt_Wrapper()
            bow.hbook.book('artemis', 'counts', range(10))
            bow.hbook.book('artemis', 'time.prepschema', range(10))
            bow.hbook.book('artemis', 'time.prepblks', range(10))
            bow.hbook.book('artemis', 'payload', range(10))
            bow.hbook.book('artemis', 'nblocks', range(10))
            bow.hbook.book('artemis', 'time.execute', range(10))
            bow.hbook.book('artemis', 'blocksize', range(10))
            bow.hbook.book('artemis', 'time.collect', range(10))
            bow.configure()
            bow.initialize()
            tree = Tree('artemis')
            try:
                bow._execute()
            except StopIteration:
                print("Process complete")
            except Exception:
                raise

    def test_finalize(self):
        # TODO
        # Disable finalize until collector class properly initialized
        # Requires a Tree with nodes and payload
        return True
        
        self.reset()
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_RUNNING
            bow.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)
            bow.steer._hbook = Physt_Wrapper()
            bow.steer._hbook.book('steer.time', 'dummy', range(10))
            bow.steer._hbook.book('steer.time', 'csvparser', range(10))
            bow.steer._hbook.book('steer.time', 'profiler', range(10))
            print('Running')
            bow.hbook = Physt_Wrapper()
            bow.hbook.book('artemis', 'counts', range(10))
            bow.hbook.book('artemis', 'payload', range(10))
            bow.hbook.book('artemis', 'blocksize', range(10))
            bow.hbook.book('artemis', 'time.prepblks', range(10))
            bow.hbook.book('artemis', 'time.prepschema', range(10))
            bow.hbook.book('artemis', 'time.execute', range(10))
            bow.hbook.book('artemis', 'time.collect', range(10))
            bow.configure()
            bow.initialize()
            bow.book()
            tree = Tree('artemis')
            bow.collector.initialize()
            print('Finalizing')
            bow.finalize()
            print('Job finished')
    
    def test_abort(self):
        self.reset()
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_RUNNING
            bow._jp.meta.state = artemis_pb2.JOB_CONFIGURE
            bow.configure()
            bow._jp.meta.state = artemis_pb2.JOB_INITIALIZE
            bow.initialize()
            bow._jp.meta.state = artemis_pb2.JOB_BOOK
            bow.book()
            bow._jp.meta.state = artemis_pb2.JOB_SAMPLE
            bow.execute()
            bow.collector.initialize()
            print('Finalizing')
            bow.abort("abort")
            print('Job finished')
    '''
def suite():
    pass
    #suite = unittest.TestSuite()
    #suite.addTest(ArtemisTestCase('test_launch'))
    #suite.addTest(ArtemisTestCase('test_configure'))
    #suite.addTest(ArtemisTestCase('test_lock'))
    #suite.addTest(ArtemisTestCase('test_initialize'))
    #suite.addTest(ArtemisTestCase('test_book'))
    #suite.addTest(ArtemisTestCase('test_run'))
    #suite.addTest(ArtemisTestCase('test_finalize'))

if __name__ == '__main__':
    runner = unittest.ArtemisTestCase()
    runner.run(suite())
    pass
