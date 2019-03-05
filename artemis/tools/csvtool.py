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
import io
from ast import literal_eval
import csv

import pyarrow as pa
from pyarrow.csv import read_csv, ReadOptions, ParseOptions

from artemis.decorators import iterable
from artemis.core.tool import ToolBase


@iterable
class CsvToolOptions:

    # Add user-defined options for Artemis.CsvTool
    dummy = 'brain'
    pass


class CsvTool(ToolBase):

    def __init__(self, name, **kwargs):

        # Retrieves the default options from arrow
        # Updates with any user-defined options
        # Create a final dictionary to store all properties
        ropts = self._get_opts(ReadOptions(), **kwargs)
        popts = self._get_opts(ParseOptions(), **kwargs)
        options = {**ropts, **popts, **dict(CsvToolOptions())}
        options.update(kwargs)

        super().__init__(name, **options)
        self.__logger.info(options)
        self._readopts = ReadOptions(**ropts)
        self._parseopts = ParseOptions(**popts)
        self._convertopts = None  # Coming in 0.12
        self.__logger.info('%s: __init__ CsvTool' % self.name)
    
    def _get_opts(self, cls, **kwargs):
        options = {}
        for attr in dir(cls):
            if attr[:2] != '__' and attr != "escape_char":
                options[attr] = getattr(cls, attr)
                if attr in kwargs:
                    options[attr] = kwargs[attr]
        return options

    def initialize(self):
        self.__logger.info("%s properties: %s",
                           self.__class__.__name__,
                           self.properties)

    def execute_pyparsing(self, schema, columns, length, block):
        '''
        Relies on python object inspection to determine type
        Used as a comparison and validation tool for the pyarrow module
        '''
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

    def execute(self, block):
        '''
        Calls the read_csv module from pyarrow

        Parameters
        ----------
        buf: bytearray object containing raw bytes from csv file

        Returns
        ---------
        pyarrow RecordBatch
        '''
        # create pyarrow buffer from raw bytes
        buf_ = pa.py_buffer(block)
        try:
            table = read_csv(buf_,
                             read_options=self._readopts,
                             parse_options=self._parseopts)
        except Exception:
            self.__logger.error("Problem converting csv to table")
            raise
        # We actually want a batch
        # batch can be converted to table
        # but not vice-verse, we get batches
        # Should always be length 1 though (chunksize can be set however)
        batches = table.to_batches()
        self.__logger.debug("Batches %i", len(batches))
        for batch in batches:
            self.__logger.debug("Batch records %i", batch.num_rows)
        if len(batches) != 1:
            self.__logger.error("Table has more than 1 RecordBatches")
            raise Exception

        return batches[-1]
