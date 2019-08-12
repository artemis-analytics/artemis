#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.

import unittest
import pyarrow as pa
from random import randint

from artemis.core.tool import ToolBase
from artemis.tools.filtercoltool import FilterColTool

class FilterColTestCase(unittest.TestCase):
    
    def test_basic(self):
        tool = FilterColTool('tool', columns=["b"], invert=False)
        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3]), pa.array([4, 5, 6]), 
                                           pa.array([7, 8, 9])], ["a", "b", "c"])
        tbatch = tool.execute(batch)
        assert batch.num_rows == tbatch.num_rows
        assert tbatch.to_pydict() == {"b": [4, 5, 6]}

    def test_default_invert(self):
        tool = FilterColTool('tool', columns=["b"])
        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3]), pa.array([4, 5, 6]), 
                                           pa.array([7, 8, 9])], ["a", "b", "c"])
        tbatch = tool.execute(batch)
        assert batch.num_rows == tbatch.num_rows
        assert tbatch.to_pydict() == {"b": [4, 5, 6]}

    def test_no_columns(self):
        # Should pass back original list
        tool = FilterColTool('tool')
        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3]), pa.array([4, 5, 6]), 
                                           pa.array([7, 8, 9])], ["a", "b", "c"])
        tbatch = tool.execute(batch)
        assert batch.num_rows == tbatch.num_rows
        assert batch.num_columns == tbatch.num_columns
        assert tbatch.to_pydict() == {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}

    def test_invert_true(self):
        tool = FilterColTool('tool', columns=["b"], invert=True)
        batch = pa.RecordBatch.from_arrays([pa.array([1, 2, 3]), pa.array([4, 5, 6]), 
                                           pa.array([7, 8, 9])], ["a", "b", "c"])
        tbatch = tool.execute(batch)
        assert batch.num_rows == tbatch.num_rows
        assert tbatch.to_pydict() == {"a": [1, 2, 3], "c": [7, 8, 9]}

if __name__ == "__main__":
    unittest.main()
