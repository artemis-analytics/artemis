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
import csv
import io
from ast import literal_eval
import pyarrow as pa
from pyarrow.csv import read_csv, ReadOptions

from artemis.generators.csvgen import GenCsvLike, GenCsvLikeArrow
from artemis.generators.legacygen import GenMF
from artemis.io.filehandler import FileHandlerTool
logging.getLogger().setLevel(logging.INFO)


class ReaderTestCase(unittest.TestCase):

    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        self._handler = FileHandlerTool('tool', delimiter='\r\n')
        self._handler.initialize()

    def tearDown(self):
        pass
     
    def test_read_block(self):
        delimiter = b'\n'
        data = delimiter.join([b'123', b'456', b'789'])
        f = io.BytesIO(data)

        assert self._handler._read_block(f, 1, 2) == b'23'
        assert self._handler._read_block(f, 0, 1, delimiter=b'\n') == b'123\n'
        assert self._handler._read_block(f, 0, 2, delimiter=b'\n') == b'123\n'
        assert self._handler._read_block(f, 0, 3, delimiter=b'\n') == b'123\n'
        assert self._handler._read_block(f, 0, 5, delimiter=b'\n') == b'123\n456\n'
        assert self._handler._read_block(f, 0, 8, delimiter=b'\n') == b'123\n456\n789'
        assert self._handler._read_block(f, 0, 100, delimiter=b'\n') == b'123\n456\n789'
        assert self._handler._read_block(f, 1, 1, delimiter=b'\n') == b''
        assert self._handler._read_block(f, 1, 5, delimiter=b'\n') == b'456\n'
        assert self._handler._read_block(f, 1, 8, delimiter=b'\n') == b'456\n789'

        for ols in [[(0, 3), (3, 3), (6, 3), (9, 2)],
                    [(0, 4), (4, 4), (8, 4)]]:
            out = [self._handler._read_block(f, o, l, b'\n') for o, l in ols]
            assert b"".join(filter(None, out)) == data

    def test_seek_delimiter_endline(self):
        f = io.BytesIO(b'123\n456\n789')

        # if at zero, stay at zero
        self._handler._seek_delimiter(f, b'\n', 5)
        assert f.tell() == 0

        # choose the first block
        for bs in [1, 5, 100]:
            f.seek(1)
            self._handler._seek_delimiter(f, b'\n', blocksize=bs)
            assert f.tell() == 4

        # handle long delimiters well, even with short blocksizes
        f = io.BytesIO(b'123abc456abc789')
        for bs in [1, 2, 3, 4, 5, 6, 10]:
            f.seek(1)
            self._handler._seek_delimiter(f, b'abc', blocksize=bs)
            assert f.tell() == 6

        # End at the end
        f = io.BytesIO(b'123\n456')
        f.seek(5)
        self._handler._seek_delimiter(f, b'\n', 5)
        assert f.tell() == 7  
    
    def test_get_blocks(self):
        generator = GenCsvLikeArrow('test')
        data, names, batch = generator.make_random_csv()
        
        length = len(data)
        
        # IO Buffer bytestream
        #buf = io.BytesIO(data)
        buf = pa.py_buffer(data)
        #file_ = pa.PythonFile(buf, mode='r')
        
        self._handler.prepare(buf)
        
        print("Generated buffer is %s bytes" % length)
        # IO Buffer bytestream
        buf = io.BytesIO(data)
        file_ = pa.PythonFile(buf, mode='r')
        
        blocksum = 0

        blocks = self._handler.blocks
        for blk in blocks:
            blocksum += blk[1]
        try:
            assert blocksum == length
        except AssertionError:
            print(length, blocksum)
        
        # print(offsets)
        # print(lengths)
    
    '''
    def test_readinto(self):
        generator = GenCsvLikeArrow('test')
        data, names, batch = generator.make_random_csv()
        
        length = len(data)
        
        # IO Buffer bytestream
        #buf = io.BytesIO(data)
        #file_ = pa.PythonFile(buf, mode='r')
        buf = pa.py_buffer(data)
        self._handler.prepare(buf)
        
        print("Generated buffer is %s bytes" % length)
        print("Arrow buffer is %s bytes" % buf.size)
        # IO Buffer bytestream
        #buf = io.BytesIO(data)
        #file_ = pa.PythonFile(buf, mode='r')

        blocks = self._handler.blocks
        chunks = [bytearray(block[1]) for block in blocks]
        for i, block in enumerate(blocks):
            self._handler.readinto_block(buf, chunks[i], block[0])
            print(chunks[i].decode())
        print(blocks) 
        #print(offsets)
        #print(lengths)
    
    def test_readinto_large(self):
        generator = GenCsvLikeArrow('test',
                                    nbatches=1, 
                                    num_cols=20, 
                                    num_rows=10000)
        data, names, batch = generator.make_random_csv()
        
        length = len(data)
        print("Generated large file %i" % length)
        # IO Buffer bytestream
        #buf = io.BytesIO(data)
        #file_ = pa.PythonFile(buf, mode='r')
        buf = pa.py_buffer(data)

        self._handler.prepare(buf)
        
        print("Generated buffer is %s bytes" % length)
        # IO Buffer bytestream
        buf = io.BytesIO(data)
        file_ = pa.PythonFile(buf, mode='r')

        # offsets, lengths = self._handler.get_blocks(file_, 6, b'\r\n', off_head)
        blocks = self._handler.blocks
        chunks = [bytearray(block[1]) for block in blocks]
        for i, block in enumerate(blocks):
            self._handler.readinto_block(file_, chunks[i], block[0])
            #print(chunks[i].decode())
        print(blocks)
        print(chunks[-1])
        print("Test last chunk read")
        with io.BytesIO(chunks[-1]) as raw:
            with io.TextIOWrapper(raw) as file_:
                print(file_.read())
        
        print("Read all chunks")
        for chunk in chunks:
            with io.BytesIO(chunk) as raw:
                with io.TextIOWrapper(raw) as file_:
                    reader = csv.reader(file_)
                    try:
                        for row in reader:
                            pass
                    except Exception:
                        print('problem at last chunk')
                        print(chunk)
        print('Completed reading large chunks')
        #print(offsets)
        #print(lengths)
    '''
    def test_prepare_csv(self):
        generator = GenCsvLikeArrow('test',
                                    nbatches=1, 
                                    num_cols=10, 
                                    num_rows=100)
        data, names, batch = generator.make_random_csv()
        handler = FileHandlerTool('tool', delimiter='\r\n', blocksize=100 )
        handler.initialize()
        length = len(data)
        buf = pa.py_buffer(data)
        print(buf.size, pa.total_allocated_bytes())
        reader = handler.prepare_csv(buf)
        print(reader.header) 
        print(type(reader))
        #print(reader.rndblocks)
        #for block in reader.rndblocks:
        #   print(block)  
            #iblock = self.generator.\
            #    random_state.randint(0, len(_finfo.blocks) - 1)
        print(pa.total_allocated_bytes())
        for batch in reader.sampler():
            print(batch.to_pybytes())
        for batch in reader:
            print(batch.to_pybytes())
        reader.close()
    
    def test_prepare_legacy(self):

        intconf0 = {'utype': 'int', 'length': 10, 'min_val': 0, 'max_val': 10}
        intuconf0 = {'utype': 'uint', 'length': 6, 'min_val': 0, 'max_val': 10}
        strconf0 = {'utype': 'str', 'length': 4}
        # Schema definition.
        # Size of chunk to create.
        # Create a generator objected, properly configured.
        generator = GenMF('generator',
                          column_a=intconf0,
                          column_b=intuconf0,
                          column_c=strconf0,
                          num_rows=1000, 
                          nbatches=1,
                          loglevel='INFO')
        data = next(generator.generate())
        handler = FileHandlerTool('tool', legacy_data=True, blocksize=20*100 )
        handler.initialize()
        length = len(data)
        buf = pa.py_buffer(data)
        print(buf.size, pa.total_allocated_bytes())
        reader = handler.prepare_legacy(buf)
        for batch in reader:
            print(batch.to_pybytes())

if __name__ == '__main__':
    unittest.main()

