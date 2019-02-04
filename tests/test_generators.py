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
import tempfile
from ast import literal_eval
import pyarrow as pa
from pyarrow.csv import read_csv, ReadOptions

from artemis.generators.generators import GenCsvLike, GenCsvLikeArrow, FileGenerator

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
    
    def test_write_mixed_csv(self):

        generator = GenCsvLikeArrow('test', 
                                    nbatches=10, 
                                    num_cols=100, 
                                    num_rows=10000)

        data, col_names, batch = generator.make_mixed_random_csv()
        _fname = 'test.dat'
        with pa.OSFile(_fname, 'wb') as sink:
            writer = pa.RecordBatchFileWriter(sink, batch.schema)
            i = 0
            for _ in range(10):
                print("Generating batch ", i)
                data, batch = next(generator.generate())
                writer.write_batch(batch)
                i += 1
            writer.close()
   
    def test_continuous_write(self):
        generator = GenCsvLikeArrow('test', 
                                    nbatches=4, 
                                    num_cols=100, 
                                    num_rows=10000)

        data, col_names, batch = generator.make_mixed_random_csv()
        schema = batch.schema
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, schema)
        i = 0
        ifile = 0
        batch = None
        print("Size allocated ", pa.total_allocated_bytes())
        for data, batch in generator.generate(): 
            print("Generating batch ", i)
            print("Size allocated ", pa.total_allocated_bytes())
            if pa.total_allocated_bytes() < int(20000000):
                print("Writing to buffer ", batch.num_rows)
                writer.write_batch(batch)
            else:
                print("Flush to disk ", pa.total_allocated_bytes())
                _fname = 'test_'+str(ifile)+'.dat'
                buf = sink.getvalue()
                with pa.OSFile(_fname, 'wb') as f:
                    try:
                        f.write(buf)
                    except Exception:
                        print("Bad idea")
                print("Size allocated ", pa.total_allocated_bytes())
                ifile += 1
                # Batch still needs to be written
                sink = pa.BufferOutputStream()
                writer = pa.RecordBatchFileWriter(sink, schema)
                writer.write_batch(batch)
            i += 1
        
        batch = None

        print("Size allocated ", pa.total_allocated_bytes())

    def test_write_buffer_csv(self):

        generator = GenCsvLikeArrow('test', 
                                    nbatches=4, 
                                    num_cols=100, 
                                    num_rows=10000)

        data, col_names, batch = generator.make_mixed_random_csv()
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)
        i = 0
        _sum_size = 0 
        for _ in range(2):
            print("Generating batch ", i)
            data, batch = next(generator.generate())
            writer.write_batch(batch)
            _sum_size += pa.get_record_batch_size(batch)
            i += 1
        batch = None
        print("Size allocated ", pa.total_allocated_bytes())
        print("Sum size serialized ", _sum_size) 
        if pa.total_allocated_bytes() < 20000000:
            for _ in range(2):
                print("Generating batch ", i)
                data, batch = next(generator.generate())
                writer.write_batch(batch)
                _sum_size += pa.get_record_batch_size(batch)
                i += 1
            batch = None
            print("Size allocated ", pa.total_allocated_bytes())
            print("Sum size serialized ", _sum_size) 
        writer.close() 
        try:
            sink.flush()
        except ValueError:
            print("Cannot flush")

        buf = sink.getvalue() 
        print("Size in buffer ", buf.size)
        print("Size allocated ", pa.total_allocated_bytes())
        

        reader = pa.RecordBatchFileReader(pa.BufferReader(buf))
        print(reader.num_record_batches)
        
        with pa.OSFile('test.dat', 'wb') as f:
            try:
                f.write(buf)
            except Exception:
                print("Bad idea")
        
        file_obj = pa.OSFile('test.dat')
        reader = pa.open_file(file_obj)
        print(reader.num_record_batches)
     
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
    
    def test_writecsv(self):
        generator = GenCsvLikeArrow('test', nbatches=3, suffix='.csv', prefix='test', path='/tmp')
        generator.write()

    def test_filegenerator(self):
        generator = GenCsvLikeArrow('test', nbatches=3, suffix='.csv', prefix='test', path='/tmp')
        generator.write()
        generator = FileGenerator('test', path='/tmp', glob='*.csv')
        for item in generator.generate():
            print(item)

        iter_ = generator.generate()
        print(next(iter_))



if __name__ == "__main__":
    unittest.main()


