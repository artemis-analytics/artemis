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

import unittest
import logging
import tempfile

from artemis.generators.legacygen import GenMF
from artemis.core.algo import AlgoBase
from cronus.core.cronus import BaseObjectStore

logging.getLogger().setLevel(logging.INFO)


class Test_MF_Gen(unittest.TestCase):
    def setUp(self):
        print("================================================")
        print("Beginning new TestCase %s" % self._testMethodName)
        print("================================================")

    def tearDown(self):
        pass

    def test_dev(self):
        """
        Code to test development of generator.
        """
        # Field configuration.
        intconf0 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
        intconf1 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
        intconf2 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
        intconf3 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
        intconf4 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
        intconf5 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
        intconf6 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
        intconf7 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
        intconf8 = {"utype": "int", "length": 10, "min_val": 0, "max_val": 10}
        intconf9 = {"utype": "uint", "length": 10, "min_val": 0, "max_val": 10}
        strconf0 = {"utype": "str", "length": 10}
        strconf1 = {"utype": "str", "length": 10}
        strconf2 = {"utype": "str", "length": 10}
        strconf3 = {"utype": "str", "length": 10}
        strconf4 = {"utype": "str", "length": 10}
        strconf5 = {"utype": "str", "length": 10}
        strconf6 = {"utype": "str", "length": 10}
        strconf7 = {"utype": "str", "length": 10}
        strconf8 = {"utype": "str", "length": 10}
        strconf9 = {"utype": "str", "length": 10}
        # Dataset configuration.
        test_ds = [
            intconf0,
            intconf1,
            strconf0,
            intconf2,
            strconf1,
            strconf2,
            intconf3,
            intconf4,
            intconf5,
            strconf3,
            intconf6,
            strconf4,
            strconf5,
            strconf6,
            intconf7,
            strconf7,
            strconf8,
            intconf8,
            intconf9,
            strconf9,
        ]
        # Number of records.
        size = 10
        # Create GenMF object, properly configured.
        test_gen = GenMF("test", ds_schema=test_ds, num_rows=size, loglevel="INFO")
        # Test for data column generation with different types.
        test_gen.gen_column(intconf0, size)
        test_gen.gen_column(strconf0, size)
        test_gen.gen_column(intconf9, size)
        # Test for entire chunk.
        test_gen.gen_chunk()

    def test_msg(self):
        intconf0 = {
            "utype": "int",
            "length": 10,
            "min_val": 0,
            "max_val": 10,
        }

        test_gen = GenMF("test", column=intconf0, num_rows=10, loglevel="INFO")
        msg = test_gen.to_msg()

        logger = logging.getLogger()
        test_gen2 = AlgoBase.from_msg(logger, msg)

        print(test_gen2.properties.column)
        test_gen2.gen_chunk()

    def test_generate_chunks(self):
        intconf0 = {
            "utype": "int",
            "length": 10,
            "min_val": 0,
            "max_val": 10,
        }
        with tempfile.TemporaryDirectory() as dirpath:
            store = BaseObjectStore(dirpath, "artemis")

            g_dataset = store.register_dataset()
            job_id = store.new_job(g_dataset.uuid)
            store.new_partition(g_dataset.uuid, "generator")

            test_gen = GenMF(
                "generator", column=intconf0, num_rows=10, nbatches=10, loglevel="INFO"
            )
            test_gen.gate.meta.parentset_id = g_dataset.uuid
            test_gen.gate.meta.job_id = str(job_id)
            test_gen.gate.store = store
            test_gen.initialize()

            for chunk in test_gen:
                print("Batch")
                print(chunk)

    def test_meta(self):
        intconf0 = {
            "utype": "int",
            "length": 10,
            "min_val": 0,
            "max_val": 10,
        }
        with tempfile.TemporaryDirectory() as dirpath:
            store = BaseObjectStore(dirpath, "artemis")

            g_dataset = store.register_dataset()
            job_id = store.new_job(g_dataset.uuid)
            store.new_partition(g_dataset.uuid, "generator")
            test_gen = GenMF(
                "generator",
                column=intconf0,
                num_rows=10,
                nbatches=10,
                header="header",
                header_offset=10,
                footer="footer",
                footer_size=10,
                loglevel="INFO",
            )
            test_gen.gate.meta.parentset_id = g_dataset.uuid
            test_gen.gate.meta.job_id = str(job_id)
            test_gen.gate.store = store
            test_gen.initialize()

            chunk = next(test_gen)
            # assert len(chunk) == 120


if __name__ == "__main__":
    unittest.main()
