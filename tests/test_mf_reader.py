#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.


import unittest
import logging
import uuid
from google.protobuf import text_format

from artemis.core.tree import Tree, Node, Element
from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets
from artemis.core.dag import Sequence, Chain, Menu
from artemis.algorithms.legacyalgo import LegacyDataAlgo
from artemis.artemis import Artemis
from artemis.core.properties import JobProperties
from artemis.tools.mftool import MfTool
from artemis.generators.legacygen import GenMF
from artemis.io.filehandler import FileHandlerTool
from artemis.io.writer import BufferOutputWriter
from artemis.generators.filegen import FileGenerator
import artemis.io.protobuf.artemis_pb2 as artemis_pb2
logging.getLogger().setLevel(logging.INFO)

class Test_MF_Reader(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)

    def tearDown(self):
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
    
    def test_mf_reader(self):
        '''
        This test simply tests the reader function of the code.
        '''

        # Field definitions.
        intconf0 = {'utype':'int', 'length':10}
        intconf1 = {'utype':'uint', 'length':6}
        strconf0 = {'utype':'str', 'length':4}
        # Schema definition for all fields.
        schema = [intconf0, strconf0, intconf1]
        # Test data block.
        block = "012345678aabcd012345012345678babcd012345"\
                 + "012345678cabc 012345012345678dabcd012345"\
                 + "012345678eabcd012345012345678fabcd012345"\
                 + "012345678aabc 012345012345678babcd012345"\
                 + "012345678cabcd012345012345678dabcd012345"\
                 + "012345678eabc 012345012345678fabcd012345"\
                 + "012345678aabcd012345012345678babcd012345"\
                 + "012345678cabc 012345"
        # Show block in unencoded format.
        print('Block: ')
        print(block)
        # Encode in EBCDIC format.
        block = block.encode(encoding='cp500')
        # Show block in encoded format.
        print('Encoded block: ')
        print(block)
        # Create MfTool object. It is configured.
        mfreader = MfTool('reader',ds_schema=schema)
        # Run the reader on the data block.
        mfreader.execute(block)

    def test_mf_gen_read(self):
        '''
        This test takes input from the mf data generator and
        feeds it to the mf data reader.
        '''
        # Field definitions.
        intconf0 = {'utype': 'int', 'length': 10, 'min_val': 0, 'max_val': 10}
        intuconf0 = {'utype': 'uint', 'length': 6, 'min_val': 0, 'max_val': 10}
        strconf0 = {'utype': 'str', 'length': 4}
        # Schema definition.
        schema = [intconf0, intuconf0, strconf0]
        # Size of chunk to create.
        size = 10
        # Create a generator objected, properly configured.
        my_gen = GenMF('test', ds_schema=schema, num_rows=size)
        # Create a data chunk.
        chunk = my_gen.gen_chunk()
        # Create MfTool object, properly configured.
        my_read = MfTool('reader', ds_schema=schema)
        # Read generated data chunk.
        batch = my_read.execute(chunk)
        print("Batch columns %i, rows %i" % (batch.num_columns, batch.num_rows))
        print(batch.schema)
        
    def test_mfartemis(self):
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        prtcfg = ''
        legacyalgo = LegacyDataAlgo('legacyparser', loglevel='INFO')

        legacyChain = Chain("legacychain")
        seqX = Sequence(["initial"], (legacyalgo,), "seqX")
        legacyChain.add(seqX)
        
        testmenu = Menu("test")
        testmenu.add(legacyChain)
        testmenu.generate()
        
        prtcfg = 'arrowmf_proto.dat'
        try:
            msgmenu = testmenu.to_msg()
        except Exception:
            raise
        
        intconf0 = {'utype': 'int', 'length': 10, 'min_val': 0, 'max_val': 10}
        intuconf0 = {'utype': 'uint', 'length': 6, 'min_val': 0, 'max_val': 10}
        strconf0 = {'utype': 'str', 'length': 4}
        # Schema definition.
        # Size of chunk to create.
        # Create a generator objected, properly configured.
        generator = GenMF('generator',
                          column_a=intconf0,
                          column_b=intuconf0,
                          column_c=strconf0,
                          num_rows=10000, nbatches=10)

        msggen = generator.to_msg()
        
        mftool = MfTool('legacytool',
                        column_a=intconf0,
                        column_b=intuconf0,
                        column_c=strconf0)

        mftoolcfg = mftool.to_msg()
        blocksize = mftool.record_size * 100
        filetool = FileHandlerTool('filehandler',
                                   blocksize=blocksize,
                                   skip_header=False,
                                   legacy_data=True,
                                   loglevel='INFO')
        filetoolcfg = filetool.to_msg()

        defaultwriter = BufferOutputWriter('bufferwriter',
                                           BUFFER_MAX_SIZE=10485760,
                                           #BUFFER_MAX_SIZE=2147483648,  
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

        mftoolmsg = msg.tools.add()
        mftoolmsg.CopyFrom(mftoolcfg)
        print(text_format.MessageToString(mftoolmsg))

        try:
            with open(prtcfg, "wb") as f:
                f.write(msg.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write message")
        except Exception:
            raise
        bow = Artemis("mftest", 
                      protomsg=prtcfg,
                      loglevel='INFO',
                      jobname='mftest')
        bow.control()

    def test_mfartemisio(self):
        Singleton.reset(JobProperties)
        Singleton.reset(Tree)
        Singleton.reset(ArrowSets)
        prtcfg = ''
        legacyalgo = LegacyDataAlgo('legacyparser', loglevel='INFO')

        legacyChain = Chain("legacychain")
        seqX = Sequence(["initial"], (legacyalgo,), "seqX")
        legacyChain.add(seqX)
        
        testmenu = Menu("test")
        testmenu.add(legacyChain)
        testmenu.generate()
        
        prtcfg = 'arrowmf_proto.dat'
        try:
            msgmenu = testmenu.to_msg()
        except Exception:
            raise
        
        intconf0 = {'utype': 'int', 'length': 10, 'min_val': 0, 'max_val': 10}
        intuconf0 = {'utype': 'uint', 'length': 6, 'min_val': 0, 'max_val': 10}
        strconf0 = {'utype': 'str', 'length': 4}
        # Schema definition.
        # Size of chunk to create.
        # Create a generator objected, properly configured.
        generator = GenMF('generator',
                          column_a=intconf0,
                          column_b=intuconf0,
                          column_c=strconf0,
                          num_rows=10000, 
                          nbatches=10,
                          suffix='.txt',
                          prefix='testio',
                          path='/tmp')

        generator.write()
        
        
        filegen = FileGenerator('generator',
                                path='/tmp',
                                glob='testio*.txt',
                                nbatches=0)
        msggen = filegen.to_msg()
        mftool = MfTool('legacytool',
                        column_a=intconf0,
                        column_b=intuconf0,
                        column_c=strconf0)

        mftoolcfg = mftool.to_msg()
        blocksize = mftool.record_size * 100
        filetool = FileHandlerTool('filehandler',
                                   blocksize=blocksize,
                                   skip_header=False,
                                   legacy_data=True,
                                   loglevel='INFO')
        filetoolcfg = filetool.to_msg()

        defaultwriter = BufferOutputWriter('bufferwriter',
                                           BUFFER_MAX_SIZE=10485760,
                                           #BUFFER_MAX_SIZE=2147483648,  
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

        mftoolmsg = msg.tools.add()
        mftoolmsg.CopyFrom(mftoolcfg)
        print(text_format.MessageToString(mftoolmsg))

        try:
            with open(prtcfg, "wb") as f:
                f.write(msg.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write message")
        except Exception:
            raise
        bow = Artemis("mftest", 
                      protomsg=prtcfg,
                      loglevel='INFO',
                      jobname='mftest')
        bow.control()

if __name__ == "__main__":
    unittest.main()
