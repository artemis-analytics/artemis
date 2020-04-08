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
Algorithm that parsers and converts a bytes object of csv data to Arrow record batch
format. Calls a tool that executes the pyarrow csv reader.
"""
from artemis_base.utils.decorators import timethis
from artemis_base.utils.utils import range_positive

from artemis.core.algo import AlgoBase


class CsvParserAlgo(AlgoBase):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info("%s: __init__ CsvParserAlgo" % self.name)
        # TODO
        # Add any required tools to list of algo properties
        # All tools must be loaded in the ToolStore
        # Check for existence of tool

    def initialize(self):
        self.__logger.info("%s: Initialized CsvParserAlgo" % self.name)

    def book(self):
        self.__logger.info("Book")
        bins = [x for x in range_positive(0.0, 100.0, 2.0)]
        self.gate.hbook.book(self.name, "time.pyarrowparse", bins, "ms", timer=True)

    def rebook(self):
        pass

    @timethis
    def pyarrow_parsing(self, block):
        try:
            batch = self.get_tool("csvtool").execute(block)
        except Exception:
            raise
        return batch

    def execute(self, element):

        raw_ = element.get_data()

        try:
            tbatch, time_ = self.pyarrow_parsing(raw_)
        except Exception:
            self.__logger.error("PyArrow parsing fails")
            raise
        self.gate.hbook.fill(self.name, "time.pyarrowparse", time_)

        self.__logger.debug("Arrow schema: %s: ", tbatch.schema)

        #  TODO
        #  Arrow schema validation per batch

        # Does this overwrite the existing data for this element?
        element.add_data(tbatch)
        self.__logger.debug("Element Data type %s", type(element.get_data()))

    def finalize(self):
        self.__logger.info("Completed CsvParsing")
