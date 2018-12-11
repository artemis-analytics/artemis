#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import unittest
import logging
from artemis.io.writer import BufferOutputWriter 
from artemis.core.tree import Element
import pandas as pd
import numpy as np
import pyarrow as pa
logging.getLogger().setLevel(logging.INFO)


class WritterTestCase(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        pass

    def test(self):
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

        writer = BufferOutputWriter('test')
        writer.BUFFER_MAX_SIZE = 1024
        writer._fbasename = 'test'
        writer._schema = batch.schema
        writer.initialize()

        try:
            writer.write(elements)
            writer._finalize()
        except Exception:
            raise IOError
        

if __name__ == "__main__":
    unittest.main()
