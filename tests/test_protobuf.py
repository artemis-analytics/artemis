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
import os

import pyarrow as pa

from artemis.core.dag import Sequence, Chain, Menu
from artemis.algorithms.dummyalgo import DummyAlgo1
from artemis.algorithms.csvparseralgo import CsvParserAlgo
from artemis.algorithms.profileralgo import ProfilerAlgo
from artemis.core.singleton import Singleton
from artemis.core.properties import JobProperties
import artemis.io.protobuf.artemis_pb2 as artemis_pb2
from cronus.io.protobuf.menu_pb2 import Menu as Menu_pb2


logging.getLogger().setLevel(logging.INFO)


class ArtemisTestCase(unittest.TestCase):
    
    def setUp(self):
        pass

    def tearDown(self):
        Singleton.reset(JobProperties)

    def test_schema(self):
        fields = [('foo', pa.int32()),
                  ('bar', pa.float32())]
        schema = pa.schema(fields)

        serialized_schema = schema.serialize().to_pybytes()

        msg = artemis_pb2.SchemaInfo()
        msg.arrow_schema = serialized_schema
        
        newschema = pa.ipc.read_schema(pa.py_buffer(msg.arrow_schema))

        assert schema == newschema






if __name__ == '__main__':
    unittest.main()
