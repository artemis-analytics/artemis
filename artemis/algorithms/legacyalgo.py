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
Conversion of legacy data to Arrow RecordBatch
"""


from artemis.core.algo import AlgoBase
from artemis.decorators import timethis
from artemis.utils.utils import range_positive


class LegacyDataAlgo(AlgoBase):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ LegacyAlgo' % self.name)
        # TODO
        # Add any required tools to list of algo properties
        # All tools must be loaded in the ToolStore
        # Check for existence of tool

    def initialize(self):
        self.__logger.info('%s: Initialized LegacyAlgo' % self.name)

    def book(self):
        self.__logger.info("Book")
        bins = [x for x in range_positive(0., 100., 2.)]
        self.gate.hbook.book(self.name, 'time.pyparse',
                             bins, 'ms', timer=True)
        self.gate.hbook.book(self.name, 'time.legacydataparse',
                             bins, 'ms', timer=True)

    def rebook(self):
        pass

    @timethis
    def pyarrow_parsing(self, block):
        try:
            batch = self.get_tool('legacytool').execute(block)
        except Exception:
            raise
        return batch

    @timethis
    def pyarrow_fwfr(self, block):
        try:
            batch = self.get_tool('fwftool').execute(block)
        except Exception:
            raise
        return batch

    def execute(self, element):

        raw_ = element.get_data()
        self.__logger.info("Processing block %s", type(raw_))
        try:
            tbatch, time_ = self.pyarrow_parsing(raw_.to_pybytes())
        except Exception:
            self.__logger.error("PyArrow parsing fails")
            raise

        self.gate.hbook.fill(self.name, 'time.legacydataparse', time_)

        try:
            fbatch, time_ = self.pyarrow_fwfr(raw_)
        except Exception:
            self.__logger.error("PyArrow parsing fails")
            raise
        self.gate.hbook.fill(self.name, 'time.pyparse', time_)

        self.__logger.debug("Arrow schema: %s time: ", tbatch.schema)

        self.__logger.info("First Batch")
        self.__logger.info("Schema %s", tbatch.schema)
        self.__logger.info("Rows %i Columns %i", tbatch.num_rows,
                           tbatch.num_columns)

        self.__logger.info("Second Batch")
        self.__logger.info("Schema %s", fbatch.schema)
        self.__logger.info("Rows %i Columns %i",
                           fbatch.num_rows, fbatch.num_columns)
        # Does this overwrite the existing data for this element?
        element.add_data(tbatch)
        self.__logger.debug("Element Data type %s", type(element.get_data()))

    def finalize(self):
        self.__logger.info("Completed LegacyParsing")
