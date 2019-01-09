#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Algorithm which configures a reader
given a bytes object
"""
from ast import literal_eval
import csv
import io

import pyarrow as pa
from pyarrow.csv import read_csv, ReadOptions

from artemis.core.algo import AlgoBase
from artemis.io.reader import Reader
from artemis.core.properties import JobProperties
from artemis.decorators import timethis
from artemis.core.physt_wrapper import Physt_Wrapper
from artemis.utils.utils import range_positive
from artemis.core.timerstore import TimerSvc


class CsvParserAlgo(AlgoBase):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ CsvParserAlgo' % self.name)
        self.reader = None
        self.jobops = None

    def initialize(self):
        self.reader = Reader()
        self.jobops = JobProperties()
        self.__logger.info('%s: Initialized CsvParserAlgo' % self.name)

    def book(self):
        self.__logger.info("Book")
        self.hbook = Physt_Wrapper()
        self.__timers = TimerSvc()
        self.__timers.book(self.name, 'pyparse')
        self.__timers.book(self.name, 'pyarrowparse')
        bins = [x for x in range_positive(0., 100., 2.)]
        self.hbook.book(self.name, 'time.pyparse', bins, 'ms')
        self.hbook.book(self.name, 'time.pyarrowparse', bins, 'ms')

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
    def py_parsing(self, schema, columns, length, block):
        try:
            with io.TextIOWrapper(io.BytesIO(block)) as file_:
                reader = csv.reader(file_)
                try:
                    next(reader)
                except Exception:
                    self.__logger.error("Cannot read inserted header")
                    raise
                try:
                    for row in reader:
                        # print(row)
                        length += 1
                        for i, item in enumerate(row):
                            if item == 'nan':
                                item = 'None'
                            try:
                                columns[i].append(literal_eval(item))
                            except Exception:
                                self.__logger.error("Line %i row %s" %
                                                    (i, row))
                                raise
                except Exception:
                    self.__logger.error('Error reading line %i' % length)
                    raise
        except IOError:
            raise
        except Exception:
            raise

        array = []
        for column in columns:
            try:
                array.append(pa.array(column))
            except Exception:
                self.__logger.error("Cannot convert list to pyarrow arrow")
                raise
        try:
            rbatch = pa.RecordBatch.from_arrays(array, schema)
        except Exception:
            self.__logger.error("Cannot convert arrays to batch")
            raise
        return rbatch

    @timethis
    def pyarrow_parsing(self, block):
        # create pyarrow buffer from raw bytes
        buf_ = pa.py_buffer(block)
        try:
            table = read_csv(buf_, ReadOptions(block_size=2**25))
        except Exception:
            self.__logger.error("Problem converting csv to table")
            raise
        # We actually want a batch
        # batch can be converted to table but not vice-verse, we get batches
        # Should always be length 1 though (chunksize can be set however)
        batches = table.to_batches()
        if len(batches) != 1:
            self.__logger.error("Table has more than 1 RecordBatches")
            raise Exception

        return batches[-1]

    def execute(self, element):

        raw_ = element.get_data()
        _finfo = self.jobops.meta.data[-1]
        schema = [x.name for x in _finfo.schema.columns]
        self.__logger.debug('Expected header %s' % schema)
        columns = [[] for _ in range(len(schema))]
        length = 0

        try:
            rbatch, time_ = self.py_parsing(schema, columns, length, raw_)
        except Exception:
            self.__logger.error("Python parsing fails")
            raise
        self.__timers.fill(self.name, 'pyparse', time_)
        self.hbook.fill(self.name, 'time.pyparse', time_)

        try:
            tbatch, time_ = self.pyarrow_parsing(raw_)
        except Exception:
            self.__logger.error("PyArrow parsing fails")
            raise
        self.__timers.fill(self.name, 'pyarrowparse', time_)
        self.hbook.fill(self.name, 'time.pyarrowparse', time_)

        if rbatch.equals(tbatch) is False:
            self.__logger.error("Batches not validated")
        else:
            self.__logger.debug("Batches equal %s", rbatch.equals(tbatch))

        self.__logger.debug("Arrow schema: %s time: ", rbatch.schema)
        self.__logger.debug("Arrow schema: %s time: ", tbatch.schema)

        # Does this overwrite the existing data for this element?
        element.add_data(tbatch)
        self.__logger.debug("Element Data type %s", type(element.get_data()))

    def finalize(self):
        self.__logger.info("Completed CsvParsing")
        summary = self.jobops.meta.summary

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
            self.__logger.info("%s timing: %2.4f" % (key, mu))

            # Add to the msg
            msgtime = summary.timers.add()
            msgtime.name = _name
            msgtime.time = mu
            msgtime.std = std
