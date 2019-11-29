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
Generator for csv-like data
"""
import sys
import csv
import string
import io
import random
from array import array

import pyarrow as pa

from artemis.logger import Logger
from artemis.decorators import iterable
from artemis.generators.common import GeneratorBase
from artemis.io.protobuf.cronus_pb2 import FileObjectInfo
from artemis.io.protobuf.table_pb2 import Table


@Logger.logged
class GenCsvLike:
    '''
    Creates data in CSV format and sends bytes.
    Generator tries to guess at payload size
    for total number of floats to generate / column / chunk
    '''
    def __init__(self):
        '''
        Chunk configuration
        Data of ncolumns, of size <size> in <unit>.

        Number of chunks per requent
        nchunks

        Maximum number of requests
        maxchunks

        Up to client to be
        Ready for data
        '''
        self.ncolumns = 10
        self.units = 'm'
        self.size = 10
        self.nchunks = 10
        self.maxrequests = 1  # Equivalent to EOF?
        self._cntr = 0

    def gen_chunk(self, ncolumn, unit, size):
        '''
        Create a chunk of data of ncolumns, of size <size> in <unit>.
        '''
        units = {
                'b': 1,
                'k': 1000,
                'm': 1000000,
                'g': 1000000000,
                'B': 1,
                'K': 1000,
                'M': 1000000,
                'G': 1000000000}

        # Based off tests of random floats from random.random.
        float_size = 20

        # Total number of floats needed according to supplied criteria.
        nfloats = int((size * units[unit] / float_size))
        self.__logger.info("Total number of floats %s" % nfloats)

        # Total number of rows based off number of floats and required columns
        # nrows = int(nfloats / ncolumn)
        #
        chunk = ''
        floats = array('d', (random.random() for i in range(nfloats)))
        csv_rows = []
        csv_row = []
        i = 0
        j = 0
        # Initialize all variables above to avoid null references.
        columns = [[] for col in range(ncolumn)]

        while i < nfloats:
            # Generates list of rows.
            csv_row = []
            j = 0
            while j < ncolumn and i < nfloats:
                # Generates columns in each row.
                csv_row.append(floats[i])
                columns[j].append(floats[i])
                j += 1
                i += 1
            csv_rows.append(csv_row)

        # Use StringIO as an in memory file equivalent
        # (instead of with...open construction).
        output = io.StringIO()
        sio_f_csv = csv.writer(output)
        sio_f_csv.writerows(csv_rows)

        # Encodes the csv file as bytes.
        chunk = bytes(output.getvalue(), encoding='utf_8')
        return chunk

    def generate(self):
        self.__logger.info('Generate')
        self.__logger.info("%s: Producing Data" % (self.__class__.__name__))
        self.__logger.debug("%s: Producing Data" % (self.__class__.__name__))
        i = 0
        mysum = 0
        mysumsize = 0
        while i < self.nchunks:
            getdata = self.gen_chunk(20, 'm', 10)
            # Should be bytes.
            self.__logger.debug('%s: type data: %s' %
                                (self.__class__.__name__, type(getdata)))
            mysumsize += sys.getsizeof(getdata)
            mysum += len(getdata)
            i += 1
            yield getdata

        # Helped to figure out the math for an average float size.
        self.__logger.debug('%s: Average of total: %2.1f' %
                            (self.__class__.__name__, mysum/i))
        # Same as previous.
        self.__logger.debug('%s: Average of size: %2.1f' %
                            (self.__class__.__name__, mysumsize/i))


@iterable
class GenCsvLikeArrowOptions:
    nbatches = 1
    nsamples = 1
    num_cols = 2
    num_rows = 10
    linesep = u'\r\n'
    # seed = 42
    header = True


class GenCsvLikeArrow(GeneratorBase):
    '''
    Arrow-like generator
    see arrow/python/pyarrow/tests/test_csv.py

    tests specific number of rows and columns
    sends a batch rather than Table
    '''
    pa_types = ('int32', 'uint32', 'int64', 'uint64',
                'float32', 'float64')
    # TODO
    # bool type currently failing in pa.csv.read_csv
    # 'bool', 'decimal',
    # 'binary', 'binary10', 'ascii', 'unicode',
    # 'int64 list', 'struct', 'struct from tuples')

    def __init__(self, name, **kwargs):

        options = dict(GenCsvLikeArrowOptions())
        options.update(kwargs)

        super().__init__(name, **options)

        self.table_id = self.properties.table_id
        # self.num_cols = self.properties.num_cols
        self.num_rows = self.properties.num_rows
        self.linesep = self.properties.linesep
        # self.seed = self.properties.seed
        self.header = self.properties.header
        self.nsamples = self.properties.nsamples

        # Build the random columns names once
        # self.col_names = \
        # list(itertools.islice(GenCsvLikeArrow.generate_col_names(),
        #                       self.num_cols))
        self.num_cols = None
        self.col_names = []
        self.types = []

        self.__logger.info("Initialized %s", self.__class__.__name__)
        self.__logger.info("%s properties: %s",
                           self.__class__.__name__,
                           self.properties)

    @property
    def num_batches(self):
        return self._nbatches

    @num_batches.setter
    def num_batches(self, n):
        self._nbatches = n

    @staticmethod
    def generate_col_names():
        letters = string.ascii_lowercase
        # for letter in letters:
        #     yield letter

        for first in letters:
            for second in letters:
                yield first + second

    def initialize(self):
        self.__logger.info("Initialize CsvGenerator")
        table = Table()
        self.gate.store.get(self.table_id, table)
        self.num_cols = len(table.info.schema.info.fields)

        for field in table.info.schema.info.fields:
            self.col_names.append(field.name)
        _tr = len(self.pa_types)-1

        for _ in range(self.num_cols):
            self.types.append(self.pa_types
                              [self._builtin_generator.rnd.randint(0, _tr)])

    def make_random_csv(self):
        # Numpy generates column wise
        # Above we generate row-wise
        # Transpose to rows for csv
        arr = self._builtin_generator.rnd.\
                randint(0, 1000, size=(self.num_cols, self.num_rows))

        # Simulates the write of csv file
        # Encode to bytes for processing, as above
        csv = io.StringIO()
        if self.header is True:
            csv.write(u",".join(self.col_names))
            csv.write(self.linesep)
        for row in arr.T:
            csv.write(u",".join(map(str, row)))
            csv.write(self.linesep)

        # bytes object with unicode encoding
        csv = csv.getvalue().encode()
        columns = [pa.array(a, type=pa.int64()) for a in arr]
        expected = pa.RecordBatch.from_arrays(columns, self.col_names)
        return csv, self.col_names, expected

    def make_mixed_random_csv(self):
        '''
        Use arrow commons builtins to generate
        arrow arrays and push to csv
        precursor to using adds
        '''
        size = self.num_rows

        columns = [[] for _ in range(self.num_cols)]

        for icol in range(self.num_cols):
            ty, data = self._builtin_generator.\
                get_type_and_builtins(self.num_rows, self.types[icol])
            columns[icol] = data

        csv = io.StringIO()
        if self.header is True:
            csv.write(u",".join(self.col_names))
            csv.write(self.linesep)
        for irow in range(size):
            row = []
            for column in columns:
                if column[irow] is None:
                    row.append('nan')
                else:
                    row.append(column[irow])
            csv.write(u",".join(map(str, row)))
            csv.write(self.linesep)

        # bytes object with unicode encoding
        csv = csv.getvalue().encode()
        columns = [pa.array(a) for a in columns]
        expected = pa.RecordBatch.from_arrays(columns, self.col_names)
        return csv, self.col_names, expected

    def generate(self):
        while self._nbatches > 0:
            self.__logger.info("%s: Generating datum " %
                               (self.__class__.__name__))
            data, col_names, batch = self.make_mixed_random_csv()
            self.__logger.debug('%s: type data: %s' %
                                (self.__class__.__name__, type(data)))
            yield data, batch
            self._nbatches -= 1
            self.__logger.debug("Batch %i", self._nbatches)

    def sampler(self):
        while self.nsamples > 0:
            self.__logger.info("%s: Generating datum " %
                               (self.__class__.__name__))
            data, col_names, batch = self.make_mixed_random_csv()
            self.__logger.debug('%s: type data: %s' %
                                (self.__class__.__name__, type(data)))
            fileinfo = FileObjectInfo()
            fileinfo.type = 1
            fileinfo.partition = self.name
            job_id = f"{self.gate.meta.job_id}_sample_{self.nsamples}"
            ds_id = self.gate.meta.parentset_id
            id_ = self.gate.store.register_content(data,
                                                   fileinfo,
                                                   dataset_id=ds_id,
                                                   partition_key=self.name,
                                                   job_id=job_id).uuid
            buf = pa.py_buffer(data)
            self.gate.store.put(id_, buf)
            yield id_
            self.nsamples -= 1
            self.__logger.debug("Batch %i", self.nsamples)

    def __next__(self):
        next(self._batch_iter)
        self.__logger.info("%s: Generating datum " %
                           (self.__class__.__name__))
        data, col_names, batch = self.make_mixed_random_csv()
        self.__logger.debug('%s: type data: %s' %
                            (self.__class__.__name__, type(data)))
        if self.gate is not None:
            self.__logger.info("Register in store")
            fileinfo = FileObjectInfo()
            fileinfo.type = 1
            fileinfo.partition = self.name
            job_id = f"{self.gate.meta.job_id}_batch_{self._batchidx}"
            ds_id = self.gate.meta.parentset_id
            id_ = self.gate.store.register_content(data,
                                                   fileinfo,
                                                   dataset_id=ds_id,
                                                   partition_key=self.name,
                                                   job_id=job_id).uuid
            buf = pa.py_buffer(data)
            self.gate.store.put(id_, buf)
            self._batchidx += 1
            # return buf
            return id_
        else:
            return data

    def write(self):
        self.__logger.info("Batch %i", self._nbatches)
        iter_ = self.generate()
        while True:
            try:
                raw, batch = next(iter_)
            except StopIteration:
                self.__logger.info("Request data: iterator complete")
                break
            except Exception:
                self.__logger.info("Iterator empty")
                raise
            buf = pa.py_buffer(raw)
            fileinfo = FileObjectInfo()
            fileinfo.type = 1
            fileinfo.partition = self.name
            job_id = f"{self.gate.meta.job_id}_batch_{self._batchidx}"
            ds_id = self.gate.meta.parentset_id
            id_ = self.gate.store.register_content(buf,
                                                   fileinfo,
                                                   dataset_id=ds_id,
                                                   partition_key=self.name,
                                                   job_id=job_id).uuid
            self.gate.store.put(id_, buf)
            self._batchidx += 1
