#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
import pyarrow as pa
from random import randint

from artemis.core.tool import ToolBase

# from artemis.tools.filtercoltool import FilterColTool


def module_exists(module_name, object_name):
    try:
        __import__(module_name, fromlist=[object_name])
    except ImportError:
        return False
    else:
        return True


@unittest.skipUnless(
    module_exists("artemis.tools.filtercoltool", "FilterColTool"),
    "filtercoltool not installed",
)
class FilterColTestCase(unittest.TestCase):
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def test_basic(self):
        from artemis.tools.filtercoltool import FilterColTool

        tool = FilterColTool("tool", columns=["b"], invert=False)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array([4, 5, 6]), pa.array([7, 8, 9])],
            ["a", "b", "c"],
        )
        tbatch = tool.execute(batch)
        assert batch.num_rows == tbatch.num_rows
        assert tbatch.to_pydict() == {"b": [4, 5, 6]}

    def test_default_invert(self):
        from artemis.tools.filtercoltool import FilterColTool

        tool = FilterColTool("tool", columns=["b"])
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array([4, 5, 6]), pa.array([7, 8, 9])],
            ["a", "b", "c"],
        )
        tbatch = tool.execute(batch)
        assert batch.num_rows == tbatch.num_rows
        assert tbatch.to_pydict() == {"b": [4, 5, 6]}

    def test_no_columns(self):
        from artemis.tools.filtercoltool import FilterColTool

        # Should pass back original list
        tool = FilterColTool("tool")
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array([4, 5, 6]), pa.array([7, 8, 9])],
            ["a", "b", "c"],
        )
        tbatch = tool.execute(batch)
        assert batch.num_rows == tbatch.num_rows
        assert batch.num_columns == tbatch.num_columns
        assert tbatch.to_pydict() == {"a": [1, 2, 3], "b": [4, 5, 6], "c": [7, 8, 9]}

    def test_invert_true(self):
        from artemis.tools.filtercoltool import FilterColTool

        tool = FilterColTool("tool", columns=["b"], invert=True)
        batch = pa.RecordBatch.from_arrays(
            [pa.array([1, 2, 3]), pa.array([4, 5, 6]), pa.array([7, 8, 9])],
            ["a", "b", "c"],
        )
        tbatch = tool.execute(batch)
        assert batch.num_rows == tbatch.num_rows
        assert tbatch.to_pydict() == {"a": [1, 2, 3], "c": [7, 8, 9]}


if __name__ == "__main__":
    unittest.main()
