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
from artemis.core.physt_wrapper import Physt_Wrapper
from artemis.utils.utils import range_positive
from artemis.core.timerstore import TimerSvc
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
        self.hbook = Physt_Wrapper()
        self.__timers = TimerSvc()
        self.__timers.book(self.name, 'legacydataparse')
        bins = [x for x in range_positive(0., 100., 2.)]
        self.hbook.book(self.name, 'time.pyparse', bins, 'ms')
        self.hbook.book(self.name, 'time.legacydataparse', bins, 'ms')

    def rebook(self):

        for key in self.__timers.keys:
            if 'steer' in key:
                continue
            if self.name not in key:
                continue
            self.__logger.info("Rebook %s", key)
            name = key.split('.')[-1]
            avg_, std_ = self.__timers.stats(self.name, name)
            bins = [x for x in range_positive(0., avg_ + 5*std_, 2.)]
            self.hbook.rebook(self.name, 'time.'+name, bins, 'ms')

    @timethis
    def pyarrow_parsing(self, block):
        try:
            batch = self.__tools.get('legacytool').execute(block)
        except Exception:
            raise
        return batch

    def execute(self, element):
        
        _finfo = self._jp.meta.data[-1]
        raw_ = element.get_data()

        try:
            tbatch, time_ = self.pyarrow_parsing(raw_)
        except Exception:
            self.__logger.error("PyArrow parsing fails")
            raise
        self.__timers.fill(self.name, 'legacydataparse', time_)
        self.hbook.fill(self.name, 'time.legacydataparse', time_)

        self.__logger.debug("Arrow schema: %s time: ", tbatch.schema)
        
        _finfo.schema.arrow_schema = tbatch.schema.serialize().to_pybytes()
        # Does this overwrite the existing data for this element?
        element.add_data(tbatch)
        self.__logger.debug("Element Data type %s", type(element.get_data()))

    def finalize(self):
        self.__logger.info("Completed LegacyParsing")
        summary = self._jp.meta.summary

        for key in self.__timers.keys:
            if self.name in key:
                key = key.split('.')[-1]
            else:
                continue
            _name = '.'
            _name = _name.join([self.name, 'time', key])
            if _name == self.name + '.time.' + self.name:
                continue
            self.logger.info("Retrieve %s %s", self.name, 'time.'+key)
            try:
                mu = self.hbook.get_histogram(self.name, 'time.'+key).mean()
                std = self.hbook.get_histogram(self.name, 'time.'+key).std()
                print(mu, std)
            except KeyError:
                self.__logger.error("Cannot retrieve %s ", _name)
            self.__logger.debug("%s timing: %2.4f" % (key, mu))

            # Add to the msg
            msgtime = summary.timers.add()
            msgtime.name = _name
            msgtime.time = mu
            msgtime.std = std
