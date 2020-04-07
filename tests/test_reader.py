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
import io
import tempfile
import uuid
import itertools

from sas7bdat import SAS7BDAT
import pyarrow as pa
from pandas.util.testing import assert_frame_equal

from artemis.generators.csvgen import GenCsvLikeArrow
from artemis.generators.legacygen import GenMF
from artemis.io.filehandler import FileHandlerTool
from cronus.core.cronus import BaseObjectStore
from artemis_format.pymodels.table_pb2 import Table
from artemis_format.pymodels.cronus_pb2 import TableObjectInfo, FileObjectInfo

logging.getLogger().setLevel(logging.INFO)


class ReaderTestCase(unittest.TestCase):
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")
        pass

    def tearDown(self):
        logging.getLogger().setLevel(logging.INFO)

    def setupStore(self, dirpath):
        store = BaseObjectStore(dirpath, "artemis")
        g_dataset = store.register_dataset()
        store.new_partition(g_dataset.uuid, "generator")
        job_id = store.new_job(g_dataset.uuid)

        # define the schema for the data
        g_table = Table()
        g_table.name = "generator"
        g_table.uuid = str(uuid.uuid4())
        g_table.info.schema.name = "csv"
        g_table.info.schema.uuid = str(uuid.uuid4())

        fields = list(itertools.islice(GenCsvLikeArrow.generate_col_names(), 20))
        for f in fields:
            field = g_table.info.schema.info.fields.add()
            field.name = f

        tinfo = TableObjectInfo()
        tinfo.fields.extend(fields)
        id_ = store.register_content(
            g_table,
            tinfo,
            dataset_id=g_dataset.uuid,
            job_id=job_id,
            partition_key="generator",
        ).uuid
        return store, g_dataset.uuid, job_id, id_, fields

    def test_read_block(self):
        handler = FileHandlerTool("tool", delimiter="\r\n")
        handler.initialize()
        delimiter = b"\n"
        data = delimiter.join([b"123", b"456", b"789"])
        f = io.BytesIO(data)

        self.assertEqual(handler._read_block(f, 1, 2), b"23")
        self.assertEqual(handler._read_block(f, 0, 1, delimiter=b"\n"), b"123\n")
        self.assertEqual(handler._read_block(f, 0, 2, delimiter=b"\n"), b"123\n")
        self.assertEqual(handler._read_block(f, 0, 3, delimiter=b"\n"), b"123\n")
        self.assertEqual(handler._read_block(f, 0, 5, delimiter=b"\n"), b"123\n456\n")
        self.assertEqual(
            handler._read_block(f, 0, 8, delimiter=b"\n"), b"123\n456\n789"
        )
        self.assertEqual(
            handler._read_block(f, 0, 100, delimiter=b"\n"), b"123\n456\n789"
        )
        self.assertEqual(handler._read_block(f, 1, 1, delimiter=b"\n"), b"")
        self.assertEqual(handler._read_block(f, 1, 5, delimiter=b"\n"), b"456\n")
        self.assertEqual(handler._read_block(f, 1, 8, delimiter=b"\n"), b"456\n789")

        for ols in [[(0, 3), (3, 3), (6, 3), (9, 2)], [(0, 4), (4, 4), (8, 4)]]:
            out = [handler._read_block(f, o, l, b"\n") for o, l in ols]
            self.assertEqual(b"".join(filter(None, out)), data)

    def test_seek_delimiter_endline(self):
        handler = FileHandlerTool("tool", delimiter="\r\n")
        handler.initialize()
        f = io.BytesIO(b"123\n456\n789")

        # if at zero, stay at zero
        handler._seek_delimiter(f, b"\n", 5)
        self.assertEqual(f.tell(), 0)

        # choose the first block
        for bs in [1, 5, 100]:
            f.seek(1)
            handler._seek_delimiter(f, b"\n", blocksize=bs)
            self.assertEqual(f.tell(), 4)

        # handle long delimiters well, even with short blocksizes
        f = io.BytesIO(b"123abc456abc789")
        for bs in [1, 2, 3, 4, 5, 6, 10]:
            f.seek(1)
            handler._seek_delimiter(f, b"abc", blocksize=bs)
            self.assertEqual(f.tell(), 6)

        # End at the end
        f = io.BytesIO(b"123\n456")
        f.seek(5)
        handler._seek_delimiter(f, b"\n", 5)
        self.assertEqual(f.tell(), 7)

    def test_get_blocks(self):
        with tempfile.TemporaryDirectory() as dirpath:
            store, ds_id, job_id, tbl_id, names = self.setupStore(dirpath)

            generator = GenCsvLikeArrow("test", nbatches=1, table_id=tbl_id)
            generator.gate.meta.parentset_id = ds_id
            generator.gate.meta.job_id = str(job_id)
            generator.gate.store = store
            generator.initialize()
            data, names, batch = generator.make_random_csv()
            handler = FileHandlerTool(
                "tool", delimiter="\r\n", header_rows=0, schema=names
            )
            handler.initialize()
            length = len(data)

            fileinfo = FileObjectInfo()
            fileinfo.type = 1
            fileinfo.partition = "generator"
            job_id = str(job_id)
            id_ = generator.gate.store.register_content(
                data,
                fileinfo,
                dataset_id=generator.gate.meta.parentset_id,
                partition_key="generator",
                job_id=job_id,
            ).uuid
            buf = pa.py_buffer(data)
            generator.gate.store.put(id_, buf)
            reader = handler.execute(id_)

            # IO Buffer bytestream
            # buf = io.BytesIO(data)
            # file_ = pa.PythonFile(buf, mode='r')

            handler.execute(id_)

            blocksum = 0
            blocks = handler.blocks
            for blk in blocks:
                blocksum += blk[1]
            self.assertEqual(blocksum, length)

    def test_execute_csv(self):
        with tempfile.TemporaryDirectory() as dirpath:
            store, ds_id, job_id, tbl_id, names = self.setupStore(dirpath)

            generator = GenCsvLikeArrow("generator", nbatches=1, table_id=tbl_id)
            generator.gate.meta.parentset_id = ds_id
            generator.gate.meta.job_id = str(job_id)
            generator.gate.store = store
            generator.initialize()
            data, names, batch = generator.make_random_csv()
            handler = FileHandlerTool("tool", linesep="\r\n", blocksize=10000)
            handler.initialize()

            buf = pa.py_buffer(data)
            fileinfo = FileObjectInfo()
            fileinfo.type = 1
            fileinfo.partition = "generator"
            job_id = str(job_id)
            id_ = generator.gate.store.register_content(
                data,
                fileinfo,
                dataset_id=generator.gate.meta.parentset_id,
                partition_key="generator",
                job_id=job_id,
            ).uuid
            buf = pa.py_buffer(data)
            generator.gate.store.put(id_, buf)
            reader = handler.execute(id_)

            self.assertEqual(len(data), handler.size_bytes)

            rdata = b""
            for batch in reader:
                rdata += batch.to_pybytes()

            self.assertEqual(data, rdata)

    def test_execute_legacy(self):
        with tempfile.TemporaryDirectory() as dirpath:
            store, ds_id, job_id, tbl_id, names = self.setupStore(dirpath)

            intconf0 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
            intuconf0 = {"utype": "uint", "length": 6, "min_val": 0, "max_val": 10}
            strconf0 = {"utype": "str", "length": 4}
            # Schema definition.
            # Size of chunk to create.
            # Create a generator objected, properly configured.
            generator = GenMF(
                "generator",
                column_a=intconf0,
                column_b=intuconf0,
                column_c=strconf0,
                num_rows=1000,
                nbatches=1,
                loglevel="INFO",
            )
            generator.gate.meta.parentset_id = ds_id
            generator.gate.meta.job_id = str(job_id)
            generator.gate.store = store
            generator.initialize()

            handler = FileHandlerTool(
                "tool",
                filetype="legacy",
                blocksize=20 * 100,
                encoding="cp500",
                schema=["column_a", "column_b", "column_c"],
            )
            handler.initialize()
            data = next(generator.generate())
            buf = pa.py_buffer(data)
            fileinfo = FileObjectInfo()
            fileinfo.type = 2
            fileinfo.partition = "generator"
            job_id = str(job_id)
            id_ = generator.gate.store.register_content(
                data,
                fileinfo,
                dataset_id=generator.gate.meta.parentset_id,
                partition_key="generator",
                job_id=job_id,
            ).uuid
            buf = pa.py_buffer(data)
            generator.gate.store.put(id_, buf)

    # data = next(generator.generate())
    # buf = pa.py_buffer(data)
    # reader = handler.execute(buf)
    # num_batches = 0
    # for batch in reader:
    #     num_batches += 1

    # self.assertEqual(num_batches, 10)

    def test_schema(self):
        """
        Test for schema mismatch in file header
        """
        pass

    def test_prepare_csv(self):
        with tempfile.TemporaryDirectory() as dirpath:
            store, ds_id, job_id, tbl_id, names = self.setupStore(dirpath)

            generator = GenCsvLikeArrow("test", nbatches=1, table_id=tbl_id)
            generator.gate.meta.parentset_id = ds_id
            generator.gate.meta.job_id = str(job_id)
            generator.gate.store = store
            generator.initialize()
            data, names, batch = generator.make_random_csv()
            buf = pa.py_buffer(data)
            handler = FileHandlerTool("tool", linesep="\r\n", blocksize=100)
            stream = pa.input_stream(buf)
            handler.prepare_csv(stream)
            self.assertEqual(len(handler.schema), 20)

    def test_prepare_legacy(self):

        intconf0 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
        intuconf0 = {"utype": "uint", "length": 6, "min_val": 0, "max_val": 10}
        strconf0 = {"utype": "str", "length": 4}
        # Schema definition.
        # Size of chunk to create.
        # Create a generator objected, properly configured.
        generator = GenMF(
            "generator",
            column_a=intconf0,
            column_b=intuconf0,
            column_c=strconf0,
            num_rows=1000,
            nbatches=1,
            loglevel="INFO",
        )
        data = next(generator.generate())
        buf = pa.py_buffer(data)
        handler = FileHandlerTool(
            "tool",
            filetype="legacy",
            blocksize=20 * 100,
            encoding="cp500",
            schema=["column_a", "column_b", "column_c"],
        )
        stream = pa.input_stream(buf)
        handler.prepare_legacy(stream)

    def test_ipc(self):
        with tempfile.TemporaryDirectory() as dirpath:
            store, ds_id, job_id, tbl_id, names = self.setupStore(dirpath)

            generator = GenCsvLikeArrow("generator", nbatches=1, table_id=tbl_id)
            generator.gate.meta.parentset_id = ds_id
            generator.gate.meta.job_id = str(job_id)
            generator.gate.store = store
            generator.initialize()
            data, names, batch = generator.make_random_csv()
            sink = pa.BufferOutputStream()
            writer = pa.RecordBatchFileWriter(sink, batch.schema)
            writer.write(batch)
            writer.close()
            buf = sink.getvalue()
            fileinfo = FileObjectInfo()
            fileinfo.type = 5
            fileinfo.partition = "generator"
            job_id = str(job_id)
            id_ = generator.gate.store.register_content(
                buf,
                fileinfo,
                dataset_id=generator.gate.meta.parentset_id,
                partition_key="generator",
                job_id=job_id,
            ).uuid
            generator.gate.store.put(id_, buf)
            handler = FileHandlerTool("tool", filetype="ipc")
            handler.initialize()
            reader = handler.execute(id_)
            rbatch = next(reader)
            self.assertEqual(len(handler.schema), 20)
            self.assertEqual(batch.num_rows, rbatch.num_rows)
            assert_frame_equal(batch.to_pandas(), rbatch.to_pandas())

    def test_sas(self):
        #
        # Test file obtained from
        # http://www.principlesofeconometrics.com/poe5/poe5sas.html
        #
        with tempfile.TemporaryDirectory() as dirpath:
            store, ds_id, job_id, tbl_id, names = self.setupStore(dirpath)
            handler = FileHandlerTool("tool", filetype="sas7bdat")
            handler.gate.store = store
            path = "tests/data/accidents.sas7bdat"
            fileinfo = FileObjectInfo()
            fileinfo.type = 7
            fileinfo.partition = "generator"
            job_id = str(job_id)
            obj = store.register_content(
                path,
                fileinfo,
                dataset_id=ds_id,
                partition_key="generator",
                job_id=job_id,
            )
            print(store[obj.uuid])
            handler.initialize()
            stream = pa.input_stream(path)
            handler.prepare_sas(stream)
            reader = handler.execute(obj.uuid)
            bdat = SAS7BDAT(path)
            df1 = bdat.to_data_frame()
            batch = next(reader)
            assert_frame_equal(df1, batch.to_pandas())
            batch = next(reader.sampler())
            assert_frame_equal(df1, batch.to_pandas())


if __name__ == "__main__":
    unittest.main()
