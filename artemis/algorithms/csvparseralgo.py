#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Algorithm which configures a reader
given a bytes object
"""
from artemis.core.algo import AlgoBase
from artemis.decorators import timethis
from artemis.utils.utils import range_positive


class CsvParserAlgo(AlgoBase):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ CsvParserAlgo' % self.name)
        # TODO
        # Add any required tools to list of algo properties
        # All tools must be loaded in the ToolStore
        # Check for existence of tool

    def initialize(self):
        self.__logger.info('%s: Initialized CsvParserAlgo' % self.name)

    def book(self):
        self.__logger.info("Book")
        bins = [x for x in range_positive(0., 100., 2.)]
        self.gate.hbook.book(self.name, 'time.pyarrowparse',
                             bins, 'ms', timer=True)

    def rebook(self):
        pass

    @timethis
    def pyarrow_parsing(self, block):
        try:
            batch = self.get_tool('csvtool').execute(block)
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
        self.gate.hbook.fill(self.name, 'time.pyarrowparse', time_)

        self.__logger.debug("Arrow schema: %s: ", tbatch.schema)

        #  TODO
        #  Arrow schema validation per batch

        # Does this overwrite the existing data for this element?
        element.add_data(tbatch)
        self.__logger.debug("Element Data type %s", type(element.get_data()))

    def finalize(self):
        self.__logger.info("Completed CsvParsing")
