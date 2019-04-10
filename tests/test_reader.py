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
import io
import pyarrow as pa
from pandas.util.testing import assert_frame_equal

from artemis.generators.csvgen import GenCsvLikeArrow
from artemis.generators.legacygen import GenMF
from artemis.io.filehandler import FileHandlerTool
logging.getLogger().setLevel(logging.INFO)


class ReaderTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_read_block(self):
        handler = FileHandlerTool('tool', delimiter='\r\n')
        handler.initialize()
        delimiter = b'\n'
        data = delimiter.join([b'123', b'456', b'789'])
        f = io.BytesIO(data)

        self.assertEqual(handler._read_block(f, 1, 2), b'23')
        self.assertEqual(handler._read_block(f, 0, 1, delimiter=b'\n'),
                         b'123\n')
        self.assertEqual(handler._read_block(f, 0, 2, delimiter=b'\n'),
                         b'123\n')
        self.assertEqual(handler._read_block(f, 0, 3, delimiter=b'\n'),
                         b'123\n')
        self.assertEqual(handler._read_block(f, 0, 5, delimiter=b'\n'),
                         b'123\n456\n')
        self.assertEqual(handler._read_block(f, 0, 8, delimiter=b'\n'),
                         b'123\n456\n789')
        self.assertEqual(handler._read_block(f, 0, 100, delimiter=b'\n'),
                         b'123\n456\n789')
        self.assertEqual(handler._read_block(f, 1, 1, delimiter=b'\n'),
                         b'')
        self.assertEqual(handler._read_block(f, 1, 5, delimiter=b'\n'),
                         b'456\n')
        self.assertEqual(handler._read_block(f, 1, 8, delimiter=b'\n'),
                         b'456\n789')

        for ols in [[(0, 3), (3, 3), (6, 3), (9, 2)],
                    [(0, 4), (4, 4), (8, 4)]]:
            out = [handler._read_block(f, o, l, b'\n') for o, l in ols]
            self.assertEqual(b"".join(filter(None, out)), data)

    def test_seek_delimiter_endline(self):
        handler = FileHandlerTool('tool', delimiter='\r\n')
        handler.initialize()
        f = io.BytesIO(b'123\n456\n789')

        # if at zero, stay at zero
        handler._seek_delimiter(f, b'\n', 5)
        self.assertEqual(f.tell(), 0)

        # choose the first block
        for bs in [1, 5, 100]:
            f.seek(1)
            handler._seek_delimiter(f, b'\n', blocksize=bs)
            self.assertEqual(f.tell(), 4)

        # handle long delimiters well, even with short blocksizes
        f = io.BytesIO(b'123abc456abc789')
        for bs in [1, 2, 3, 4, 5, 6, 10]:
            f.seek(1)
            handler._seek_delimiter(f, b'abc', blocksize=bs)
            self.assertEqual(f.tell(), 6)

        # End at the end
        f = io.BytesIO(b'123\n456')
        f.seek(5)
        handler._seek_delimiter(f, b'\n', 5)
        self.assertEqual(f.tell(), 7)

    def test_get_blocks(self):
        generator = GenCsvLikeArrow('test')
        data, names, batch = generator.make_random_csv()
        handler = FileHandlerTool('tool', delimiter='\r\n',
                                  header_rows=0, schema=names)
        handler.initialize()
        length = len(data)

        # IO Buffer bytestream
        # buf = io.BytesIO(data)
        # file_ = pa.PythonFile(buf, mode='r')

        buf = pa.py_buffer(data)
        handler.execute(buf)

        blocksum = 0
        blocks = handler.blocks
        for blk in blocks:
            blocksum += blk[1]
        self.assertEqual(blocksum, length)

    def test_execute_csv(self):
        generator = GenCsvLikeArrow('test',
                                    nbatches=1,
                                    num_cols=10,
                                    num_rows=100)
        data, names, batch = generator.make_random_csv()
        handler = FileHandlerTool('tool', linesep='\r\n', blocksize=10000)
        handler.initialize()

        buf = pa.py_buffer(data)
        reader = handler.execute(buf)

        self.assertEqual(len(data), handler.size_bytes)

        rdata = b''
        for batch in reader:
            rdata += batch.to_pybytes()

        self.assertEqual(data, rdata)

    def test_execute_legacy(self):

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
        handler = FileHandlerTool('tool',
                                  filetype='legacy',
                                  blocksize=20*100,
                                  encoding='cp500',
                                  schema=['column_a', 'column_b', 'column_c'])
        handler.initialize()
        buf = pa.py_buffer(data)
        reader = handler.execute(buf)
        num_batches = 0
        for batch in reader:
            num_batches += 1

        self.assertEqual(num_batches, 10)

    def test_prepare_csv(self):
        generator = GenCsvLikeArrow('test',
                                    nbatches=1,
                                    num_cols=10,
                                    num_rows=100)
        data, names, batch = generator.make_random_csv()
        buf = pa.py_buffer(data)
        handler = FileHandlerTool('tool', linesep='\r\n', blocksize=100)
        stream = pa.input_stream(buf)
        handler.prepare_csv(stream)
        self.assertEqual(len(handler.schema), 10)

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
        buf = pa.py_buffer(data)
        handler = FileHandlerTool('tool',
                                  filetype='legacy',
                                  blocksize=20*100,
                                  encoding='cp500',
                                  schema=['column_a', 'column_b', 'column_c'])
        stream = pa.input_stream(buf)
        handler.prepare_legacy(stream)

    def test_ipc(self):
        generator = GenCsvLikeArrow('test',
                                    nbatches=1,
                                    num_cols=10,
                                    num_rows=100)
        data, names, batch = generator.make_random_csv()
        sink = pa.BufferOutputStream()
        writer = pa.RecordBatchFileWriter(sink, batch.schema)
        writer.write(batch)
        writer.close()
        buf = sink.getvalue()
        handler = FileHandlerTool('tool', filetype='ipc')
        handler.initialize()
        handler.prepare_ipc(buf)
        self.assertEqual(len(handler.schema), 10)

        reader = handler.execute(buf)
        rbatch = next(reader)
        self.assertEqual(batch.num_rows, rbatch.num_rows)
        assert_frame_equal(batch.to_pandas(), rbatch.to_pandas())


if __name__ == '__main__':
    unittest.main()

