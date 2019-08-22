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
import itertools
import urllib

from shutil import copyfile
from google.protobuf import text_format

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
from artemis.core.gate import ArtemisGateSvc
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
from artemis.meta.cronus import BaseObjectStore
from artemis.io.protobuf.cronus_pb2 import MenuObjectInfo, ConfigObjectInfo, FileObjectInfo, TableObjectInfo
from artemis.io.protobuf.table_pb2 import Table

logging.getLogger().setLevel(logging.INFO)


class ArtemisTestCase(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        Singleton.reset(ArtemisGateSvc)
        Singleton.reset(ArrowSets)
    
    def factory_example(self):
        Singleton.reset(ArtemisGateSvc)
        Singleton.reset(ArrowSets)
        dirpath = os.getcwd()
        mb = MenuFactory('csvgen')
        msgmenu = mb.build()
        menuinfo = MenuObjectInfo()
        menuinfo.created.GetCurrentTime()
        
        store = BaseObjectStore(dirpath, 'artemis')
        
        g_dataset = store.register_dataset()
        job_id = store.new_job(g_dataset.uuid)
        store.new_partition(g_dataset.uuid, 'generator')
        # define the schema for the data
        g_table = Table()
        g_table.name = 'generator'
        g_table.uuid = str(uuid.uuid4())
        g_table.info.schema.name = 'csv'
        g_table.info.schema.uuid = str(uuid.uuid4())

        fields = list(itertools.islice(GenCsvLikeArrow.generate_col_names(),20))
        for f in fields:
            field = g_table.info.schema.info.fields.add()
            field.name = f
        
        tinfo = TableObjectInfo()
        tinfo.fields.extend(fields)
        store.register_content(g_table, 
                               tinfo, 
                               dataset_id=g_dataset.uuid,
                               job_id=job_id,
                               partition_key='generator')
       
        config = JobConfigFactory('csvgen', msgmenu,
                                  jobname='arrowproto',
                                  generator_type='csv',
                                  filehandler_type='csv',
                                  nbatches=1,
                                  #num_cols=20,
                                  num_rows=10000,
                                  table_id=g_table.uuid,
                                  linesep='\r\n',
                                  delimiter=",",
                                  max_buffer_size=10485760,
                                  max_malloc=2147483648,
                                  write_csv=True,
                                  output_repo=dirpath,
                                  seed=42
                                  )
        config.configure()
        config.add_algos(mb.algos)
        print(config._msg) 
        configinfo = ConfigObjectInfo()
        configinfo.created.GetCurrentTime()
        
        
        menu_uuid = store.register_content(msgmenu, menuinfo).uuid
        config_uuid = store.register_content(config._msg, configinfo).uuid
        

        dataset = store.register_dataset(menu_id=menu_uuid, config_id=config_uuid)
        job_id = store.new_job(dataset.uuid)
        store.save_store()

        msg = config.job_config
        job = JobInfo_pb()
        job.name = 'arrowproto'
        job.job_id = 'example'
        job.store_id = store.store_uuid
        job.store_name = store.store_name
        job.store_path = dirpath
        job.menu_id = menu_uuid
        job.config_id = config_uuid
        job.dataset_id = dataset.uuid
        job.parentset_id = g_dataset.uuid
        job.job_id = str(job_id) 
        print(job)
        bow = Artemis(job, loglevel='INFO')
        bow.control()
        bow.gate.store.save_store()
        store = BaseObjectStore(dirpath, job.store_name, store_uuid=job.store_id)
        print(bow.gate.store[dataset.uuid].dataset)
        logs = store.list(suffix='log') 
        print(logs)
        url_data = urllib.parse.urlparse(logs[-1].address)
        copyfile(urllib.parse.unquote(url_data.path), 'test.log')
        #nrecords = 0
        #for table in bow._jp.meta.summary.tables:
        #    nrecords += table.num_rows
        #assert(nrecords == 100000)

    def test_proto(self):
        if use_factories_test is True:
            self.factory_example()
        else:
            Singleton.reset(ArtemisGateSvc)
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
    #unittest.main()
    test = ArtemisTestCase()
    test.test_proto()
