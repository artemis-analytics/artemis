#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Conversion of legacy data to Arrow RecordBatch
"""


from artemis.core.algo import AlgoBase
from artemis.decorators import timethis
from artemis.utils.utils import range_positive
from artemis.core.tool import ToolStore


class LegacyDataAlgo(AlgoBase):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ LegacyAlgo' % self.name)
        self.__tools = ToolStore()
        # TODO
        # Add any required tools to list of algo properties
        # All tools must be loaded in the ToolStore
        # Check for existence of tool

    def initialize(self):
        self.__logger.info('%s: Initialized LegacyAlgo' % self.name)

    def book(self):
        self.__logger.info("Book")
        bins = [x for x in range_positive(0., 100., 2.)]
        self._jp.hbook.book(self.name, 'time.pyparse',
                            bins, 'ms', timer=True)
        self._jp.hbook.book(self.name, 'time.legacydataparse',
                            bins, 'ms', timer=True)

    def rebook(self):
        pass

    @timethis
    def pyarrow_parsing(self, block):
        try:
            batch = self.__tools.get('legacytool').execute(block)
        except Exception:
            raise
        return batch

    def execute(self, element):

        raw_ = element.get_data().to_pybytes()

        try:
            tbatch, time_ = self.pyarrow_parsing(raw_)
        except Exception:
            self.__logger.error("PyArrow parsing fails")
            raise
        self._jp.hbook.fill(self.name, 'time.legacydataparse', time_)

        self.__logger.debug("Arrow schema: %s time: ", tbatch.schema)

        # Does this overwrite the existing data for this element?
        element.add_data(tbatch)
        self.__logger.debug("Element Data type %s", type(element.get_data()))

    def finalize(self):
        self.__logger.info("Completed LegacyParsing")
