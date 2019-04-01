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
import os
import sys
import uuid
from shutil import copyfile
from google.protobuf import text_format

from artemis.core.dag import Sequence, Chain, Menu
from artemis.algorithms.dummyalgo import DummyAlgo1
from artemis.algorithms.csvparseralgo import CsvParserAlgo
from artemis.algorithms.profileralgo import ProfilerAlgo
from artemis.artemis import Artemis

from artemis.configurables.factories import MenuFactory, JobConfigFactory
from artemis.core.singleton import Singleton
from artemis.core.tree import Tree
from artemis.core.datastore import ArrowSets
from artemis.core.physt_wrapper import Physt_Wrapper
from artemis.core.timerstore import TimerSvc
from artemis.core.properties import JobProperties
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb
try:
    from artemis.generators.csvgen import GenCsvLikeArrow
except ModuleNotFoundError:
    from artemis.generators.generators import GenCsvLikeArrow

from artemis.io.filehandler import FileHandlerTool
from artemis.io.writer import BufferOutputWriter
from artemis.tools.csvtool import CsvTool
import artemis.io.protobuf.artemis_pb2 as artemis_pb2

use_factories_test = True
try:
    from artemis.artemis import ArtemisFactory
except ModuleNotFoundError:
    use_factories_test = False

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
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        Singleton.reset(Physt_Wrapper)
        Singleton.reset(TimerSvc)
    
    def factory_example(self):
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        dirpath = ''
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
                                  delimiter=",",
                                  max_buffer_size=10485760,
                                  max_malloc=2147483648,
                                  write_csv=True,
                                  output_repo=dirpath,
                                  seed=42
                                  )
        config.configure()
        msg = config.job_config
        job = JobInfo_pb()
        job.name = 'arrowproto'
        job.job_id = 'example'
        job.output.repo = dirpath
        job.config.CopyFrom(msg)
        # job.job_id = str(uuid.uuid4())
        print(job)
        bow = ArtemisFactory(job, 'INFO')
        bow.control()
        copyfile('arrowproto-example.log', 'test.log')
        nrecords = 0
        for table in bow._jp.meta.summary.tables:
            nrecords += table.num_rows
        #time1 = bow._jp.hbook['artemis.time.execute']
        #time2 = bow.hbook.get_histogram('artemis','time.execute')
        #print(time1)
        #print(time2)
        #self.assertEqual(time1.frequencies.tolist(), time2.frequencies.tolist())
        #print(bow._jp.hbook.keys())
        #time1 = bow._jp.hbook['steer.time.csvparser']
        #time2 = bow.hbook.get_histogram('steer','time.csvparser')
        #self.assertEqual(time1.frequencies.tolist(), time2.frequencies.tolist())
        assert(nrecords == 100000)

    def test_proto(self):
        if use_factories_test is True:
            self.factory_example()
        else:
            Singleton.reset(JobProperties)
            self.prtcfg = 'arrowproto_proto.dat'
            try:
                msgmenu = self.testmenu.to_msg()
            except Exception:
                raise

            generator = GenCsvLikeArrow('generator',
                                        nbatches=10,
                                        num_cols=20,
                                        num_rows=10000)
            msggen = generator.to_msg()

            filetool = FileHandlerTool('filehandler',
                                       blocksize=2**16,
                                       skip_header=True,
                                       linesep='\r\n',
                                       delimiter=",",
                                       loglevel='INFO')
            filetoolcfg = filetool.to_msg()

            csvtool = CsvTool('csvtool', block_size=2**24)
            csvtoolcfg = csvtool.to_msg()

            defaultwriter = BufferOutputWriter('bufferwriter',
                                               BUFFER_MAX_SIZE=10485760,
                                               # BUFFER_MAX_SIZE=2147483648,
                                               write_csv=True)
            defwtrcfg = defaultwriter.to_msg()

            msg = artemis_pb2.JobConfig()

            # Support old format, evolve schema
            if hasattr(msg, 'config_id'):
                print('add config id')
                msg.config_id = str(uuid.uuid4())
            msg.input.generator.config.CopyFrom(msggen)
            msg.menu.CopyFrom(msgmenu)

            sampler = msg.sampler
            sampler.ndatums = 0
            sampler.nchunks = 0

            msg.max_malloc_size_bytes = 2147483648

            filetoolmsg = msg.tools.add()
            filetoolmsg.CopyFrom(filetoolcfg)

            defwrtmsg = msg.tools.add()
            defwrtmsg.CopyFrom(defwtrcfg)

            csvtoolmsg = msg.tools.add()
            csvtoolmsg.CopyFrom(csvtoolcfg)
            print(text_format.MessageToString(csvtoolmsg))
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
            print(os.path.abspath(os.path.dirname(sys.argv[0])))


if __name__ == '__main__':
    unittest.main()
