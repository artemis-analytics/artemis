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
import csv
import io
from ast import literal_eval
import pyarrow as pa
from pyarrow.csv import read_csv, ReadOptions

from artemis.generators.generators import GenCsvLike, GenCsvLikeArrow

logging.getLogger().setLevel(logging.INFO)


class GeneratorTestCase(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        pass

    def test_xgen(self):
        generator = GenCsvLike()
        generator.nchunks = 1
        ichunk = 0
        for chunk in generator.generate():
            print('Test chunk %s' % ichunk)
            ichunk += 1
    
    def chunker(self):
        nbatches = 1
        generator = GenCsvLikeArrow('test')
        for ibatch in range(nbatches):
            yield generator.make_random_csv()

    def test_genarrow(self):
        generator = GenCsvLikeArrow('test')
        nbatches = 1
        for batch in range(nbatches):
            print(generator.make_random_csv())

    def test_chunker(self):
        for batch in self.chunker():
            print(batch)

    def test_batch(self):
        generator = GenCsvLikeArrow('test')
        data, names, batch = generator.make_random_csv()

    def test_read_StringIO(self):
        generator = GenCsvLikeArrow('test')
        # data is byte encoded
        data, names, batch = generator.make_random_csv()
        # Get the StringIO object
        # To be ready to pass to reader
        bytesio = io.BytesIO(data).read().decode()
        stringio = io.StringIO(bytesio)

        for row in csv.reader(stringio):
            print(row)
    
    def test_read_TextIO(self):
        generator = GenCsvLikeArrow('test')
        # csvlike is byte encoded
        data, names, batch = generator.make_random_csv()
        # Get the Text IO object
        # To be ready to pass to reader
        textio = io.TextIOWrapper(io.BytesIO(data))

        for row in csv.reader(textio):
            print(row)

    def test_arrowbuf(self):
        generator = GenCsvLikeArrow('test')
        data, names, batch = generator.make_random_csv()
        # Create the pyarrow buffer, zero-copy view 
        # to the csvbytes objet
        buf = pa.py_buffer(data)
        print('Raw bytes from generator')
        print(data)
        print('PyArrow Buf')
        print(buf)
        print(buf.to_pybytes())
        table = read_csv(buf)
        print(table.schema)

    def test_read_csv(self):
        generator = GenCsvLikeArrow('test')
        data, names, batch = generator.make_random_csv()
        # textio = io.TextIOWrapper(io.BytesIO(data))
        columns = [[] for _ in range(generator.num_cols)]
        
        with io.TextIOWrapper(io.BytesIO(data)) as textio:
            header = next(csv.reader(textio))

            assert(header == names)
            assert(names == batch.schema.names)
            
            for row in csv.reader(textio):
                for i, item in enumerate(row):
                    if item == 'nan':
                        item = 'None'
                    columns[i].append(literal_eval(item))
        
        array = []
        for column in columns:
            array.append(pa.array(column))
        rbatch = pa.RecordBatch.from_arrays(array, header)
        assert(batch.schema.names == names)
        assert(batch.schema == rbatch.schema)
        return rbatch
    
    def test_read_mixed_csv(self):
        generator = GenCsvLikeArrow('test')
        data, names, batch = generator.make_mixed_random_csv()
        textio = io.TextIOWrapper(io.BytesIO(data))
        columns = [[] for _ in range(generator.num_cols)]
        header = next(csv.reader(textio))
        
        assert(header == names)
        assert(names == batch.schema.names)
        
        for row in csv.reader(textio):
            for i, item in enumerate(row):
                if item == 'nan':
                    item = 'None'
                columns[i].append(literal_eval(item))
       
        array = []
        for column in columns:
            array.append(pa.array(column))
        rbatch = pa.RecordBatch.from_arrays(array, header)
        # Relies on the literal type conversion to python types first
        # Arrow then converts a python array of type<x> to pa array of type<x>
        assert(batch.schema.names == names)
        assert(batch.schema == rbatch.schema)
        return rbatch

    def test_pyarrow_read_mixed_csv(self):
        generator = GenCsvLikeArrow('test')
        data, names, batch = generator.make_mixed_random_csv()
        assert(names == batch.schema.names)
        buf = pa.py_buffer(data)
        table = read_csv(buf, ReadOptions())
        assert(len(data) == buf.size)
        try:
            assert(batch.schema == table.schema)
        except AssertionError:
            print("Expected schema")
            print(batch.schema)
            print("Inferred schema")
            print(table.schema)
        return table

    def test_pyarrow_read_csv(self):
        generator = GenCsvLikeArrow('test')
        data, names, batch = generator.make_random_csv()
        assert(names == batch.schema.names)
        buf = pa.py_buffer(data)
        table = read_csv(buf, ReadOptions())
        assert(len(data) == buf.size)
        try:
            assert(batch.schema == table.schema)
        except AssertionError:
            print("Expected schema")
            print(batch.schema)
            print("Inferred schema")
            print(table.schema)
        return table


if __name__ == "__main__":
    unittest.main()



