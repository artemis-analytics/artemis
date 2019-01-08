#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import unittest
import logging
from google.protobuf import text_format

from artemis.core.dag import Sequence, Chain, Menu
from artemis.algorithms.dummyalgo import DummyAlgo1
from artemis.algorithms.csvparseralgo import CsvParserAlgo
from artemis.algorithms.profileralgo import ProfilerAlgo
from artemis.artemis import Artemis
from artemis.core.singleton import Singleton
from artemis.core.properties import JobProperties
from artemis.generators.generators import GenCsvLikeArrow
from artemis.io.filehandler import FileHandlerTool
from artemis.io.writer import BufferOutputWriter

import artemis.io.protobuf.artemis_pb2 as artemis_pb2


logging.getLogger().setLevel(logging.INFO)


class ArtemisTestCase(unittest.TestCase):
        
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
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
    
    def test_proto(self):
        Singleton.reset(JobProperties)
        self.prtcfg = 'arrowproto_proto.dat'
        try:
            msgmenu = self.testmenu.to_msg()
        except Exception:
            raise

        generator = GenCsvLikeArrow('generator',
                                    nbatches=2,
                                    num_cols=20,
                                    num_rows=10000)
        msggen = generator.to_msg()

        filetool = FileHandlerTool('filehandler',
                                   blocksize=2**16,
                                   skip_header=True,
                                   loglevel='INFO')
        filetoolcfg = filetool.to_msg()

        defaultwriter = BufferOutputWriter('bufferwriter', 
                                           BUFFER_MAX_SIZE=2147483648,  
                                           write_csv=True)
        defwtrcfg = defaultwriter.to_msg()

        msg = artemis_pb2.JobConfig()
        msg.input.generator.config.CopyFrom(msggen)
        msg.menu.CopyFrom(msgmenu)
        parser = msg.parser.csvparser
        parser.block_size = 2**16
        parser.delimiter = '\r\n'
        parser.skip_header = True

        writer = msg.writers.add()
        csvwriter = writer.csvwriter
        csvwriter.suffix = '.txt'
        writer = msg.writers.add()
        parquetwriter = writer.parquetwriter
        parquetwriter.suffix = '.parquet'

        sampler = msg.sampler
        sampler.ndatums = 1
        sampler.nchunks = 10

        msg.max_malloc_size_bytes = 2147483648
        
        filetoolmsg = msg.tools.add()
        filetoolmsg.CopyFrom(filetoolcfg)
        
        defwrtmsg = msg.tools.add()
        defwrtmsg.CopyFrom(defwtrcfg)

        try:
            with open(self.prtcfg, "wb") as f:
                f.write(msg.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write message")
        except Exception:
            raise
        bow = Artemis("arrowproto", 
                      protomsg=self.prtcfg,
                      loglevel='INFO')
        bow.control()


if __name__ == '__main__':
    unittest.main()
