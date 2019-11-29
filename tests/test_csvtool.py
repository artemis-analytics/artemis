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

"""

"""
import unittest
import logging
import tempfile
import uuid
import itertools

import pyarrow as pa
from google.protobuf import text_format

from artemis.tools.csvtool import CsvTool
from artemis.core.tool import ToolBase
from artemis.generators.csvgen import GenCsvLikeArrow
from artemis.meta.cronus import BaseObjectStore
from artemis.io.protobuf.table_pb2 import Table
from artemis.io.protobuf.cronus_pb2 import TableObjectInfo

logging.getLogger().setLevel(logging.INFO)
class CsvToolTestCase(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        pass
    
    def setupStore(self, dirpath):
        store = BaseObjectStore(dirpath, 'artemis')
        g_dataset = store.register_dataset()
        store.new_partition(g_dataset.uuid, 'generator')
        job_id = store.new_job(g_dataset.uuid)
        
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
        id_ = store.register_content(g_table, 
                               tinfo, 
                               dataset_id=g_dataset.uuid,
                               job_id=job_id,
                               partition_key='generator').uuid
        return store, g_dataset.uuid, job_id, id_, fields

    def test_to_msg(self):
        tool = CsvTool("tool", block_size=2**16)
        print(text_format.MessageToString(tool.to_msg()))

        newtool = ToolBase.from_msg(logging.getLogger(), tool.to_msg()) 


    def test(self):
        with tempfile.TemporaryDirectory() as dirpath:
            store, ds_id, job_id, tbl_id, names = self.setupStore(dirpath)
            
            generator = GenCsvLikeArrow('test',
                                        nbatches=1,
                                        table_id=tbl_id)
            generator.gate.meta.parentset_id = ds_id
            generator.gate.meta.job_id = str(job_id)
            generator.gate.store = store
            generator.initialize()
            tool = CsvTool("tool", block_size=2**16)
            data, names, batch = generator.make_random_csv()
            
            length = len(data)
            buf = pa.py_buffer(data) 
            tbatch = tool.execute(buf)
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
