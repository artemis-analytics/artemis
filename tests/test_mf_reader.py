#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.


import unittest
import logging
from collections import OrderedDict, namedtuple

import pyarrow as pa

from artemis.tools.mftool import MfTool
from artemis.core.tool import ToolBase

from artemis.generators.legacygen import GenMF

logging.getLogger().setLevel(logging.INFO)

class Test_MF_Reader(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        pass

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


if __name__ == "__main__":
    unittest.main()
