#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the license.

from artemis.core.tool import ToolStore
from artemis.core.algo import AlgoBase
from artemis.decorators import timethis
from artemis.utils.utils import range_positive
# Keep only specified columns from the record batches. 
# Use invert option to remove only specified columns.


class FilterAlgo(AlgoBase):
    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ FilterAlgo' % self.name)
        self.__tools = ToolStore()

    def initialize(self):
        self.__logger.info('%s: Initialized FilterAlgo' % self.name)

    def book(self):
        self.__logger.info("Book")
        bins = [x for x in range_positive(0., 100., 2.)]
        self._jp.hbook.book(self.name, 'time.filtercol',
                            bins, 'ms', timer=True)

    def rebook(self):
        pass

    @timethis
    def filter_columns(self, record_batch):
        return self.__tools.get('filtercoltool').execute(record_batch)

    def execute(self, element):
        fbatch, time_ = self.filter_columns(element.get_data())
        self._jp.hbook.fill(self.name, 'time.filtercol', time_)
        element.add_data(fbatch)

    def finalize(self):
        self.__logger.info("Completed FilterAlgo")
