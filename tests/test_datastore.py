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

import pyarrow as pa

import logging
from artemis.core.singleton import Singleton
from artemis.core.datastore import ArrowSets

class DatastoreTest(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        Singleton.reset(ArrowSets)

    def test_create(self):
        self.assertFalse(Singleton.exists(ArrowSets), msg='ArrowSets should not be initialized.')
        my_data = ArrowSets()
        self.assertTrue(Singleton.exists(ArrowSets), msg='ArrowSets should be initialized.')

    def test_book(self):
        my_data = ArrowSets()
        self.assertFalse(my_data.contains('test1'), msg='ArrowSets should not contain test1.')
        my_data.book('test1')
        self.assertTrue(my_data.contains('test1'), msg='ArrowSets should contain test1.')

    def test_add(self):
        my_data = ArrowSets()
        self.assertFalse(my_data.contains('test3'), msg='ArrowSets should not contain test3.')
        my_data.add_to_dict('test3', 9)
        self.assertTrue(my_data.contains('test3'), msg='ArrowSets should contain test3.')
        self.assertEqual(my_data.get_data('test3'), 9, msg='Data should be 9.')

    def test_book_and_add(self):
        my_data = ArrowSets()
        my_data.book('test2')
        self.assertIsNone(my_data.get_data('test2'), msg='ArrowSets should not contain data for test2.')
        my_data.add_to_dict('test2', 5)
        self.assertEqual(my_data.get_data('test2'), 5, msg='Data should be 5.')


    def test_get(self):
        my_data = ArrowSets()
        self.assertFalse(my_data.contains('test4'))
        my_data.add_to_dict('test4', 13)
        self.assertTrue(my_data.contains('test4'))
        my_value = my_data.get_data('test4')
        self.assertEqual(my_value, 13, msg='Data should be 13.')

    def test_arrow(self):
        arr1 = pa.array([1, 2, 3, 4])
        arr2 = pa.array(['test1', 'test2', 'test3', 'test4'])

        data = [arr1, arr2]

        batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1'])

        print('Number of columns: ' + str(batch.num_columns))
        print('Number of rows: ' + str(batch.num_rows))
        print('Schema of batch: ' + str(batch.schema))

        for i_batch in batch:
            print('Batch print: ' + str(i_batch))

        arr3 = pa.array([ 11, 21, 31, 41])
        arr4 = pa.array(['test11','test21','test31','test41'])
        data2 = [arr3, arr4]
        batch2 = pa.RecordBatch.from_arrays(data, ['f0', 'f1'])

        arr5 = pa.array([ 12, 22, 32, 42])
        arr6 = pa.array(['test12','test22','test32','test42'])
        data3 = [arr5, arr6]
        batch3 = pa.RecordBatch.from_arrays(data, ['f0', 'f1'])

        arr7 = pa.array([ 13, 23, 33, 43])
        arr8 = pa.array(['test13','test23','test33','test43'])
        data4 = [arr7, arr8]
        batch4 = pa.RecordBatch.from_arrays(data, ['f0', 'f1'])

        arr9 = pa.array([ 14, 24, 34, 44])
        arr10 = pa.array(['test14','test24','test34','test44'])
        data5 = [arr9, arr10]
        batch5 = pa.RecordBatch.from_arrays(data, ['f0', 'f1'])

        arr11 = pa.array([ 15, 25, 35, 45])
        arr12 = pa.array(['test15','test25','test35','test45'])
        data6 = [arr11, arr12]
        batch6 = pa.RecordBatch.from_arrays(data, ['f0', 'f1'])

        arr13 = pa.array([ 16, 26, 36, 46])
        arr14 = pa.array(['test16','test26','test36','test46'])
        data7 = [arr13, arr14]
        batch7 = pa.RecordBatch.from_arrays(data, ['f0', 'f1'])

        arr15 = pa.array([ 17, 27, 37, 47])
        arr16 = pa.array(['test17','test27','test37','test47'])
        data8 = [arr15, arr16]
        batch8 = pa.RecordBatch.from_arrays(data, ['f0', 'f1'])

        print('Type of multiplied batc: ' + str(type([batch]*5)))

        print('Test of Table.from_batches with hand-constructed list.')
        table = pa.Table.from_batches([batch, batch2, batch3, batch4,
                                       batch5, batch6, batch7, batch8])

        print(table)
        print('Number of rows: ' + str(table.num_rows))

        tables = [table] * 3

        table_all = pa.concat_tables(tables)

        print('Number of rwos of concatenated tables: ' + str(table_all.num_rows))

        print('Number of chunks of column 0: ' + str(table_all[0].data.num_chunks))
        print('Print dta of chunks of column 0: ' + str(table_all[0].data))
        print('Number of chunks of column 1: ' + str(table_all[1].data.num_chunks))
        print('Print data of column1: ' + str(table_all[1].data))

        pandas = table.to_pandas()
        print('Print panadas dataframe: \n' + str(pandas))
