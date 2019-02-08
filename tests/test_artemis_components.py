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
from google.protobuf import text_format

from artemis.core.dag import Sequence, Chain, Menu
from artemis.core.steering import Steering
from artemis.algorithms.dummyalgo import DummyAlgo1
from artemis.algorithms.csvparseralgo import CsvParserAlgo
from artemis.algorithms.profileralgo import ProfilerAlgo
from artemis.artemis import Artemis
from artemis.core.singleton import Singleton
from artemis.core.properties import JobProperties
from artemis.generators.csvgen import GenCsvLikeArrow
from artemis.logger import Logger
from artemis.core.physt_wrapper import Physt_Wrapper
from artemis.core.datastore import ArrowSets
from artemis.core.tree import Tree

import artemis.io.protobuf.artemis_pb2 as artemis_pb2


logging.getLogger().setLevel(logging.INFO)


class ArtemisTestCase(unittest.TestCase):
        
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        Singleton.reset(JobProperties)
        Singleton.reset(ArrowSets)
        Singleton.reset(Tree)
        Singleton.reset(Physt_Wrapper)
        self.menucfg = ''
        self.gencfg = ''
        self.prtcfg = ''
        testalgo = DummyAlgo1('dummy', myproperty='ptest', loglevel='INFO')
        csvalgo = CsvParserAlgo('csvparser', loglevel='INFO')
        profileralgo = ProfilerAlgo('profiler', loglevel='INFO')

        seq1 = Sequence(["initial"], (testalgo, testalgo), "seq1")
        seq2 = Sequence(["initial"], (testalgo, testalgo), "seq2")
        seq3 = Sequence(["seq1", "seq2"], (testalgo,), "seq3")
        seq4 = Sequence(["seq3"], (testalgo,), "seq4")

        dummyChain1 = Chain("dummy1")
        dummyChain1.add(seq1)
        dummyChain1.add(seq4)
        dummyChain1.add(seq3)
        dummyChain1.add(seq2)

        seq5 = Sequence(["initial"], (testalgo, testalgo), "seq5")
        seq6 = Sequence(["seq5"], (testalgo, testalgo), "seq6")
        seq7 = Sequence(["seq6"], (testalgo,), "seq7")

        dummyChain2 = Chain("dummy2")
        dummyChain2.add(seq5)
        dummyChain2.add(seq6)
        dummyChain2.add(seq7)

        csvChain = Chain("csvchain")
        seqX = Sequence(["initial"], (csvalgo,), "seqX")
        seqY = Sequence(["seqX"], (profileralgo,), "seqY")
        csvChain.add(seqX)
        csvChain.add(seqY)
        
        self.testmenu = Menu("test")
        self.testmenu.add(dummyChain1)
        self.testmenu.add(dummyChain2)
        self.testmenu.add(csvChain)
        self.testmenu.generate()

    def tearDown(self):
        Singleton.reset(JobProperties)
    
    def test_launch(self):
        Singleton.reset(JobProperties)
        self.prtcfg = 'arrowproto_proto.dat'
        try:
            msgmenu = self.testmenu.to_msg()
        except Exception:
            raise
        
        generator = GenCsvLikeArrow('generator',
                                    nbatches=1,
                                    num_cols=20,
                                    num_rows=10000)
        msggen = generator.to_msg()

        msg = artemis_pb2.JobConfig()
        msg.input.generator.config.CopyFrom(msggen)
        msg.menu.CopyFrom(msgmenu)
        parser = msg.parser.csvparser
        parser.block_size = 2**16
        parser.delimiter = '\r\n'
        parser.skip_header = True

        try:
            with open(self.prtcfg, "wb") as f:
                f.write(msg.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write message")
        except Exception:
            raise
        bow = Artemis("arrowproto", 
                      protomsg=self.prtcfg,
                      blocksize=2**16,
                      skip_header=True,
                      loglevel='INFO')
        print('State change -> RUNNING')
        bow._jp.meta.state = artemis_pb2.JOB_RUNNING
        print('Launching')
        bow._launch()
        print('End Launch')

    def test_configure(self):
        Singleton.reset(JobProperties)
        self.prtcfg = 'arrowproto_proto.dat'
        try:
            msgmenu = self.testmenu.to_msg()
        except Exception:
            raise
        
        generator = GenCsvLikeArrow('generator',
                                    nbatches=1,
                                    num_cols=20,
                                    num_rows=10000)
        msggen = generator.to_msg()

        msg = artemis_pb2.JobConfig()
        msg.input.generator.config.CopyFrom(msggen)
        msg.menu.CopyFrom(msgmenu)
        parser = msg.parser.csvparser
        parser.block_size = 2**16
        parser.delimiter = '\r\n'
        parser.skip_header = True

        try:
            with open(self.prtcfg, "wb") as f:
                f.write(msg.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write message")
        except Exception:
            raise
        bow = Artemis("arrowproto", 
                      protomsg=self.prtcfg,
                      blocksize=2**16,
                      skip_header=True,
                      loglevel='INFO')
        print('State change -> RUNNING')
        bow._jp.meta.state = artemis_pb2.JOB_RUNNING
        print('Configuring')
        bow._configure()

    def test_lock(self):
        Singleton.reset(JobProperties)
        self.prtcfg = 'arrowproto_proto.dat'
        try:
            msgmenu = self.testmenu.to_msg()
        except Exception:
            raise
        
        generator = GenCsvLikeArrow('generator',
                                    nbatches=1,
                                    num_cols=20,
                                    num_rows=10000)
        msggen = generator.to_msg()

        msg = artemis_pb2.JobConfig()
        msg.input.generator.config.CopyFrom(msggen)
        msg.menu.CopyFrom(msgmenu)
        parser = msg.parser.csvparser
        parser.block_size = 2**16
        parser.delimiter = '\r\n'
        parser.skip_header = True

        try:
            with open(self.prtcfg, "wb") as f:
                f.write(msg.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write message")
        except Exception:
            raise
        bow = Artemis("arrowproto", 
                      protomsg=self.prtcfg,
                      blocksize=2**16,
                      skip_header=True,
                      loglevel='INFO')
        print('State change -> RUNNING')
        bow._jp.meta.state = artemis_pb2.JOB_RUNNING
        print('Locking')
        bow.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)
        bow._lock()

    def test_initialize(self):
        Singleton.reset(JobProperties)
        self.prtcfg = 'arrowproto_proto.dat'
        try:
            msgmenu = self.testmenu.to_msg()
        except Exception:
            raise
        
        generator = GenCsvLikeArrow('generator',
                                    nbatches=1,
                                    num_cols=20,
                                    num_rows=10000)
        msggen = generator.to_msg()

        msg = artemis_pb2.JobConfig()
        msg.input.generator.config.CopyFrom(msggen)
        msg.menu.CopyFrom(msgmenu)
        parser = msg.parser.csvparser
        parser.block_size = 2**16
        parser.delimiter = '\r\n'
        parser.skip_header = True

        try:
            with open(self.prtcfg, "wb") as f:
                f.write(msg.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write message")
        except Exception:
            raise
        bow = Artemis("arrowproto", 
                      protomsg=self.prtcfg,
                      blocksize=2**16,
                      skip_header=True,
                      loglevel='INFO')
        print('State change -> RUNNING')
        bow._jp.meta.state = artemis_pb2.JOB_RUNNING
        bow.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)
        print('Initializing')
        bow._initialize()

    def test_book(self):
        Singleton.reset(JobProperties)
        self.prtcfg = 'arrowproto_proto.dat'
        try:
            msgmenu = self.testmenu.to_msg()
        except Exception:
            raise
        
        generator = GenCsvLikeArrow('generator',
                                    nbatches=1,
                                    num_cols=20,
                                    num_rows=10000)
        msggen = generator.to_msg()

        msg = artemis_pb2.JobConfig()
        msg.input.generator.config.CopyFrom(msggen)
        msg.menu.CopyFrom(msgmenu)
        parser = msg.parser.csvparser
        parser.block_size = 2**16
        parser.delimiter = '\r\n'
        parser.skip_header = True

        try:
            with open(self.prtcfg, "wb") as f:
                f.write(msg.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write message")
        except Exception:
            raise
        bow = Artemis("arrowproto", 
                      protomsg=self.prtcfg,
                      blocksize=2**16,
                      skip_header=True,
                      loglevel='INFO')
        print('State change -> RUNNING')
        bow._jp.meta.state = artemis_pb2.JOB_RUNNING
        bow.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)
        print('Booking')
        bow.hbook = Physt_Wrapper()
        bow._book()

    def test_run(self):
        Singleton.reset(JobProperties)
        self.prtcfg = 'arrowproto_proto.dat'
        try:
            msgmenu = self.testmenu.to_msg()
        except Exception:
            raise
        
        generator = GenCsvLikeArrow('generator',
                                    nbatches=1,
                                    num_cols=20,
                                    num_rows=10000)
        msggen = generator.to_msg()

        msg = artemis_pb2.JobConfig()
        msg.input.generator.config.CopyFrom(msggen)
        msg.menu.CopyFrom(msgmenu)
        parser = msg.parser.csvparser
        parser.block_size = 2**16
        parser.delimiter = '\r\n'
        parser.skip_header = True

        try:
            with open(self.prtcfg, "wb") as f:
                f.write(msg.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write message")
        except Exception:
            raise
        bow = Artemis("arrowproto", 
                      protomsg=self.prtcfg,
                      blocksize=2**16,
                      skip_header=True,
                      loglevel='INFO')
        print('State change -> RUNNING')
        bow._jp.meta.state = artemis_pb2.JOB_RUNNING
        _msgcfg = bow._jp.meta.config
        with open(bow.properties.protomsg, 'rb') as f:
            _msgcfg.ParseFromString(f.read())
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
        Singleton.reset(JobProperties)
        self.prtcfg = 'arrowproto_proto.dat'
        try:
            msgmenu = self.testmenu.to_msg()
        except Exception:
            raise
        
        generator = GenCsvLikeArrow('generator',
                                    nbatches=1,
                                    num_cols=20,
                                    num_rows=10000)
        msggen = generator.to_msg()

        msg = artemis_pb2.JobConfig()
        msg.input.generator.config.CopyFrom(msggen)
        msg.menu.CopyFrom(msgmenu)
        parser = msg.parser.csvparser
        parser.block_size = 2**16
        parser.delimiter = '\r\n'
        parser.skip_header = True

        try:
            with open(self.prtcfg, "wb") as f:
                f.write(msg.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write message")
        except Exception:
            raise
        bow = Artemis("arrowproto", 
                      protomsg=self.prtcfg,
                      blocksize=2**16,
                      skip_header=True,
                      loglevel='INFO')
        print('State change -> RUNNING')
        bow._jp.meta.state = artemis_pb2.JOB_RUNNING
        _msgcfg = bow._jp.meta.config
        with open(bow.properties.protomsg, 'rb') as f:
            _msgcfg.ParseFromString(f.read())
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
