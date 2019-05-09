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

from artemis.io.writer import BufferOutputWriter 
from artemis.core.tree import Element
import pandas as pd
import numpy as np
import pyarrow as pa
logging.getLogger().setLevel(logging.INFO)


class WriterTestCase(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        pass

    def test_writer(self):
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
