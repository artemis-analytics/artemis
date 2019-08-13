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
import tempfile
import tempfile
import uuid
import itertools

import pandas as pd
import numpy as np
import pyarrow as pa

from artemis.io.writer import BufferOutputWriter 
from artemis.core.tree import Element
from artemis.core.properties import JobProperties
from artemis.meta.cronus import BaseObjectStore
from artemis.io.protobuf.table_pb2 import Table
from artemis.io.protobuf.cronus_pb2 import TableObjectInfo

logging.getLogger().setLevel(logging.INFO)


class WriterTestCase(unittest.TestCase):

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

        return store, g_dataset.uuid, job_id

    def test_writer(self):
        with tempfile.TemporaryDirectory() as dirpath:
            store, ds_id, job_id = self.setupStore(dirpath)
            jp = JobProperties()
            jp.store = store
            jp.meta.dataset_id = ds_id
            jp.meta.job_id = str(job_id)

            nrows = 5
            df = pd.DataFrame({
                'one': np.random.randn(nrows),
                'two': ['foo', np.nan, 'bar', 'bazbaz', 'qux']})
            batch = pa.RecordBatch.from_pandas(df)

            frames = []
            batches = []
            elements = []
            for i in range(5):
                unique_df = df.copy()
                unique_df['one'] = np.random.randn(len(df))
                batch = pa.RecordBatch.from_pandas(unique_df)
                frames.append(unique_df)
                batches.append(batch)
                el = Element(str(i))
                el.add_data(batch)
                elements.append(el)
        
            with tempfile.TemporaryDirectory() as dirpath:
                writer = BufferOutputWriter('test')
                writer.BUFFER_MAX_SIZE = 1024
                writer._fbasename = 'test'
                writer._path = dirpath
                writer._schema = batch.schema
                writer.initialize()
                jp.store.new_partition(jp.meta.dataset_id, 'test')
                writer.write(elements)
                writer._finalize()

    def test_schema(self):
        '''
        Test writer raises ValueError for batch schema mismatch
        Also possible to just let Arrow throw an error
        '''
        nrows = 5
        df = pd.DataFrame({
            'one': np.random.randn(nrows),
            'two': ['foo', np.nan, 'bar', 'bazbaz', 'qux']})
        batch = pa.RecordBatch.from_pandas(df)

        frames = []
        batches = []
        elements = []
        for i in range(5):
            unique_df = df.copy()
            unique_df['one'] = np.random.randn(len(df))
            batch = pa.RecordBatch.from_pandas(unique_df)
            frames.append(unique_df)
            batches.append(batch)
            el = Element(str(i))
            el.add_data(batch)
            elements.append(el)
        
        df = pd.DataFrame({
            'one': np.random.randn(nrows),
            'two': ['foo', np.nan, 'bar', 'bazbaz', 'qux'],
            'three': np.random.randn(nrows)})

        batch = pa.RecordBatch.from_pandas(df)
        el = Element(str(i))
        el.add_data(batch)
        elements.append(el)
    
        with tempfile.TemporaryDirectory() as dirpath:
            writer = BufferOutputWriter('test')
            writer.BUFFER_MAX_SIZE = 1024
            writer._fbasename = 'test'
            writer._path = dirpath
            writer._schema = batch.schema
            writer.initialize()
            with self.assertRaises(ValueError):
                writer.write(elements)
                writer._finalize()


if __name__ == "__main__":
    unittest.main()
