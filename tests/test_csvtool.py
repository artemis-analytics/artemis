#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import unittest
import logging
from google.protobuf import text_format
from artemis.tools.csvtool import CsvTool
from artemis.core.tool import ToolBase

from artemis.generators.csvgen import GenCsvLikeArrow
logging.getLogger().setLevel(logging.INFO)
class CsvToolTestCase(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        pass
    
    def test_to_msg(self):
        tool = CsvTool("tool", block_size=2**16)
        print(text_format.MessageToString(tool.to_msg()))

        newtool = ToolBase.from_msg(logging.getLogger(), tool.to_msg()) 


    def test(self):
        tool = CsvTool("tool", block_size=2**16)
        generator = GenCsvLikeArrow('test')
        data, names, batch = generator.make_random_csv()
        
        length = len(data)
        
        tbatch = tool.execute(data)
        print(batch.schema, batch.num_rows, batch.num_columns)
        print(tbatch.schema, tbatch.num_rows, tbatch.num_columns)
        print(tbatch.to_pydict())
        assert batch.schema == tbatch.schema
        assert batch.num_rows == tbatch.num_rows
        assert batch.num_columns == tbatch.num_columns
        assert batch.to_pydict() == tbatch.to_pydict()
        assert batch.equals(tbatch)

if __name__ == "__main__":
    unittest.main()
