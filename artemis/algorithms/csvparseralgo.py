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

from artemis.core.algo import AlgoBase
from artemis.io.reader import Reader
from artemis.core.properties import JobProperties


class CsvParserAlgo(AlgoBase):

    def __init__(self, name, **kwargs):
        super().__init__(name, **kwargs)
        self.__logger.info('%s: __init__ CsvParserAlgo' % self.name)
        self.__logger.debug('%s: __init__ CsvParserAlgo' % self.name)
        self.__logger.warning('%s: __init__ CsvParserAlgo' % self.name)
        self.reader = None
        self.jobproperties = None
        print('%s: __init__ CsvParserAlgo' % self.name)

    def initialize(self):
        self.__logger.info(self.__logger)
        self.__logger.info(self._CsvParserAlgo__logger)
        self.reader = Reader()
        self.jobops = JobProperties()
        self.__logger.info('%s: Initialized CsvParserAlgo' % self.name)

    def book(self):
        pass

    def execute(self, payload):

        # header = ['a', 'b']
        fileinfo = list(self.jobops.data['file'].items())[-1]
        self.__logger.info(fileinfo)
        schema = fileinfo[-1]['schema']
        self.__logger.info('Expected header %s' % schema)
        columns = [[] for _ in range(len(schema))]
        length = 0
        try:
            with io.TextIOWrapper(io.BytesIO(payload)) as file_:
                reader = csv.reader(file_)
                # Indent will need to be fixed when you
                # uncomment this code. Sorry, blame flake8.
                # try:
                #    header = next(reader)
                # except IOError:
                #    self.__logger.error('Cannot read header from block')
                #    self.__logger.error(header)
                #    raise

                # if(schema != header):
            #    self.__logger.error('Header from block does not match schema')
                #    self.__logger.error(header)
                #    raise ValueError

                try:
                    for row in reader:
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
            array.append(pa.array(column))
        rbatch = pa.RecordBatch.from_arrays(array, schema)
        self.__logger.info("Arrow schema %s", rbatch.schema)

    def finalize(self):
        pass
