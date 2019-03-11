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
from artemis.core.properties import JobProperties
from artemis.logger import Logger
from artemis.core.physt_wrapper import Physt_Wrapper
from artemis.configurables.factories import MenuFactory, JobConfigFactory

from artemis.core.tree import Tree
from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets

import artemis.io.protobuf.artemis_pb2 as artemis_pb2
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb

logging.getLogger().setLevel(logging.INFO)


class ArtemisTestCase(unittest.TestCase):
     
    def reset(self):
        Singleton.reset(JobProperties)
        Singleton.reset(ArrowSets)
        Singleton.reset(Tree)
        Singleton.reset(Physt_Wrapper)

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
    
    def test_launch(self):
        self.reset()
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_RUNNING
            print('Launching')
            bow._launch()
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
            bow._configure()

    def test_lock(self):
        self.reset()
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_RUNNING
            print('Locking')
            bow.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)
            bow._lock()

    def test_initialize(self):
        self.reset()
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_RUNNING
            bow.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)
            print('Initializing')
            bow._initialize()

    def test_book(self):
        self.reset() 
        self.prtcfg = 'arrowproto_proto.dat'
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_RUNNING
            bow.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)
            print('Booking')
            bow.hbook = Physt_Wrapper()
            bow._book()

    def test_run(self):
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
            bow.hbook.book('artemis', 'time.prepschema', range(10))
            bow.hbook.book('artemis', 'time.prepblks', range(10))
            bow.hbook.book('artemis', 'payload', range(10))
            bow.hbook.book('artemis', 'nblocks', range(10))
            bow.hbook.book('artemis', 'time.execute', range(10))
            bow.hbook.book('artemis', 'blocksize', range(10))
            bow.hbook.book('artemis', 'time.collect', range(10))
            bow._gen_config()
            tree = Tree('artemis')
            try:
                bow._run()
            except StopIteration:
                print("Process complete")
            except Exception:
                raise

    def test_finalize(self):
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
            bow._gen_config()
            print('Finalizing')
            bow._finalize()
            print('Job finished')
    
    def test_abort(self):
        self.reset()
        with tempfile.TemporaryDirectory() as dirpath:
            self.job.output.repo = dirpath
            bow = ArtemisFactory(self.job, 'INFO')
            print('State change -> RUNNING')
            bow._jp.meta.state = artemis_pb2.JOB_RUNNING
            bow._configure() 
            bow._initialize()
            bow._book()
            bow._sample_chunks()
            bow._init_buffers()
            print('Finalizing')
            bow.abort("abort")
            print('Job finished')

def suite():
    suite = unittest.TestSuite()
    suite.addTest(ArtemisTestCase('test_launch'))
    suite.addTest(ArtemisTestCase('test_configure'))
    suite.addTest(ArtemisTestCase('test_lock'))
    suite.addTest(ArtemisTestCase('test_initialize'))
    suite.addTest(ArtemisTestCase('test_book'))
    suite.addTest(ArtemisTestCase('test_run'))
    suite.addTest(ArtemisTestCase('test_finalize'))

if __name__ == '__main__':
    runner = unittest.ArtemisTestCase()
    runner.run(suite())
