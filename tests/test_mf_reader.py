#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented 
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.


import unittest
import logging

from artemis.core.tree import Tree
from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets
from artemis.artemis import Artemis
from artemis.core.properties import JobProperties
from artemis.tools.mftool import MfTool
from artemis.generators.legacygen import GenMF

from artemis.configurables.factories import MenuFactory, JobConfigFactory
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
        my_gen = GenMF('test', ds_schema=schema, num_rows=size, loglevel='INFO')
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
        
        mb = MenuFactory('legacygen')
        prtcfg = 'arrowmf_proto.dat'
        try:
            msgmenu = mb.build()
        except Exception:
            raise
        intconf0 = {'utype': 'int', 'length': 10, 'min_val': 0, 'max_val': 10}
        intuconf0 = {'utype': 'uint', 'length': 6, 'min_val': 0, 'max_val': 10}
        strconf0 = {'utype': 'str', 'length': 4}
        # Schema definition.
        # Size of chunk to create.
        # Create a generator objected, properly configured.
        
        config = JobConfigFactory('legacygen', msgmenu)
        config.configure(ctype='legacy',
                         nbatches=10,
                         num_rows=10000,
                         delimiter='\r\n',
                         column_a=intconf0,
                         column_b=intuconf0,
                         column_c=strconf0)

        msg = config.job_config
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
        mb = MenuFactory('legacygen')
        prtcfg = 'arrowmf_proto.dat'
        try:
            msgmenu = mb.build()
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
                          path='/tmp',
                          loglevel='INFO')

        generator.write()
        config = JobConfigFactory('legacyio', msgmenu)
        config.configure(ctype='legacy',
                         nbatches=10,
                         delimiter='\r\n',
                         path='/tmp',
                         glob='testio*.txt',
                         column_a=intconf0,
                         column_b=intuconf0,
                         column_c=strconf0)

        msg = config.job_config

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
