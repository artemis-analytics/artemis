#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Dominic Parent <dominic.parent@canada.ca>
#
# Distributed under terms of the  license.

"""
Arrow PoC code.
"""

import pyarrow as pa
from .singleton import Singleton


class ArrowSets(metaclass=Singleton):
    def __init__(self):
        self.arrow_dict = {}

    def add_to_dict(self, key, batch):
        self.arrow_dict[key] = batch

    def get_data(self, key):
        return self.arrow_dict[key]

    def book(self, key):
        self.arrow_dict[key] = None


def main():
    arr1 = pa.array([1, 2, 3, 4])
    arr2 = pa.array(['test1', 'test2', 'test3', 'test4'])

    data = [arr1, arr2]

    batch = pa.RecordBatch.from_arrays(data, ['f0', 'f1'])

    print('Number of columns: ' + str(batch.num_columns))
    print('Number of rows: ' + str(batch.num_rows))
    print('Schema of batch: ' + str(batch.schema))

    for i_batch in batch:
        print('Batch print: ' + str(i_batch))

    arr3 = pa.array([11, 21, 31, 41])
    arr4 = pa.array(['test11', 'test21', 'test31', 'test41'])
    data2 = [arr3, arr4]
    batch2 = pa.RecordBatch.from_arrays(data2, ['f0', 'f1'])
    arr5 = pa.array([12, 22, 32, 42])
    arr6 = pa.array(['test12', 'test22', 'test32', 'test42'])
    data3 = [arr5, arr6]
    batch3 = pa.RecordBatch.from_arrays(data3, ['f0', 'f1'])
    arr7 = pa.array([13, 23, 33, 43])
    arr8 = pa.array(['test13', 'test23', 'test33', 'test43'])
    data4 = [arr7, arr8]
    batch4 = pa.RecordBatch.from_arrays(data4, ['f0', 'f1'])
    arr9 = pa.array([14, 24, 34, 44])
    arr10 = pa.array(['test14', 'test24', 'test34', 'test44'])
    data5 = [arr9, arr10]
    batch5 = pa.RecordBatch.from_arrays(data5, ['f0', 'f1'])
    arr11 = pa.array([15, 25, 35, 45])
    arr12 = pa.array(['test15', 'test25', 'test35', 'test45'])
    data6 = [arr11, arr12]
    batch6 = pa.RecordBatch.from_arrays(data6, ['f0', 'f1'])
    arr13 = pa.array([16, 26, 36, 46])
    arr14 = pa.array(['test16', 'test26', 'test36', 'test46'])
    data7 = [arr13, arr14]
    batch7 = pa.RecordBatch.from_arrays(data7, ['f0', 'f1'])
    arr15 = pa.array([17, 27, 37, 47])
    arr16 = pa.array(['test17', 'test27', 'test37', 'test47'])
    data8 = [arr15, arr16]
    batch8 = pa.RecordBatch.from_arrays(data8, ['f0', 'f1'])

    print('Type of multiplied batch: ' + str(type([batch] * 5)))

    print('Test of Table.from_batches with hand-constructed list.')
    table = pa.Table.from_batches([batch, batch2, batch3, batch4,
                                   batch5, batch6, batch7, batch8])
    print(table)
    print('Number of rows: ' + str(table.num_rows))

    tables = [table] * 3

    table_all = pa.concat_tables(tables)

    print('Number of rows of concatenated tables: ' + str(table_all.num_rows))

    print('Number of chunks of column 0: ' + str(table_all[0].data.num_chunks))
    print('Print data of chunks of column 0: ' + str(table_all[0].data))
    print('Number of chunks of column 1: ' + str(table_all[1].data.num_chunks))
    print('Print data of column 1: ' + str(table_all[1].data))

    pandas = table.to_pandas()
    print('Print pandas dataframe: \n ' + str(pandas))


if __name__ == "__main__":
    main()
