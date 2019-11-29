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

"""
import six
import pyarrow as pa
from sas7bdat import SAS7BDAT

from artemis.logger import Logger
from artemis.errors import AbstractMethodError
from artemis.core.gate import ArtemisGateSvc


class BaseReader():

    def __init__(self):
        self.gate = ArtemisGateSvc()

    def sampler(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        raise AbstractMethodError(self)

    def reset(self):
        pass

    def close(self):
        pass


class ReaderFactory():

    def __new__(cls, reader,
                filepath_or_buffer,
                header,
                header_offset,
                blocks,
                rnd,
                nsamples,
                num_rows):

        if reader == 'csv':
            return CsvReader(filepath_or_buffer,
                             header,
                             header_offset,
                             blocks,
                             rnd,
                             nsamples)
        elif reader == 'legacy':
            return LegacyReader(filepath_or_buffer,
                                header,
                                header_offset,
                                blocks,
                                rnd,
                                nsamples)
        elif reader == 'ipc':
            return ArrowReader(filepath_or_buffer,
                               header,
                               header_offset,
                               blocks,
                               rnd,
                               nsamples)
        elif reader == 'sas7bdat':
            return Sas7bdatReader(filepath_or_buffer,
                                  header,
                                  header_offset,
                                  rnd,
                                  nsamples,
                                  num_rows)
        else:
            raise TypeError


@Logger.logged
class ArrowReader(BaseReader):
    def __init__(self,
                 filepath_or_buffer,
                 header,
                 header_offset,
                 blocks,
                 rnd,
                 nsamples=4
                 ):
        super().__init__()
        # self.reader = pa.ipc.open_file(filepath_or_buffer)
        self.reader = self.gate.store.open(filepath_or_buffer)
        self.header = header
        self.header_offset = header_offset
        self.blocks = blocks
        self.iter_blocks = iter(range(self.reader.num_record_batches))
        self.nsamples = nsamples
        self.rnd = rnd

    def sampler(self):
        rndblocks = iter(self.rnd.choice(self.reader.num_record_batches,
                         self.nsamples))
        for iblock in rndblocks:
            yield self.reader.get_batch(iblock)
        self.__logger.info("Completed sampling")

    def __next__(self):
        try:
            block = next(self.iter_blocks)
        except StopIteration:
            raise
        return self.reader.get_batch(block)

    def close(self):
        self.reader.close()


@Logger.logged
class CsvReader(BaseReader):

    def __init__(self,
                 filepath_or_buffer,
                 header,
                 header_offset,
                 blocks,
                 rnd,
                 nsamples=4
                 ):
        super().__init__()
        self.stream = self.gate.store.open(filepath_or_buffer)
        self.header = header
        self.header_offset = header_offset
        self.blocks = blocks
        self.iter_blocks = iter(blocks)
        self.nsamples = nsamples
        self.rnd = rnd
        self._prepare()

    def _prepare(self):
        if self.header_offset != 0:
            header = self.stream.read(self.header_offset)
            if header != self.header:
                self.__logger.error("Configured header != file header")
                raise ValueError

    def sampler(self):
        rndblocks = iter(self.rnd.choice(len(self.blocks),
                         self.nsamples))
        for iblock in rndblocks:
            block = self.blocks[iblock]
            self.stream.seek(block[0])
            data = self.header
            data += self.stream.read(block[1])
            yield pa.py_buffer(data)
        self.__logger.info("Completed sampling")
        self.stream.seek(self.header_offset)

    def __next__(self):
        try:
            block = next(self.iter_blocks)
        except StopIteration:
            raise

        if self.stream.tell() != block[0]:
            self.__logger.error("Wrong block %i %i",
                                block[0],
                                self.stream.tell())
            raise IOError
        data = self.header
        data += self.stream.read(block[1])
        return pa.py_buffer(data)

    def close(self):
        self.stream.close()


@Logger.logged
class LegacyReader(BaseReader):

    def __init__(self,
                 filepath_or_buffer,
                 header,
                 header_offset,
                 blocks,
                 rnd,
                 nsamples=4
                 ):
        super().__init__()
        # TODO
        # Switch between metastore and buffer ?
        # self.stream = pa.input_stream(filepath_or_buffer)
        self.stream = self.gate.store.open(filepath_or_buffer)
        self.header = header
        self.header_offset = header_offset
        self.blocks = blocks
        self.iter_blocks = iter(blocks)
        self.nsamples = nsamples
        self.rnd = rnd
        self._prepare()

    def _prepare(self):
        header = self.stream.read(self.header_offset)
        if header != self.header:
            raise ValueError

    def sampler(self):
        rndblocks = iter(self.rnd.choice(len(self.blocks),
                         self.nsamples))
        for iblock in rndblocks:
            block = self.blocks[iblock]
            self.stream.seek(block[0])
            yield self.stream.read_buffer(block[1])
        self.__logger.debug("Completed sampling")
        self.stream.seek(self.header_offset)

    def __next__(self):
        try:
            block = next(self.iter_blocks)
        except StopIteration:
            raise

        if self.stream.tell() != block[0]:
            self.__logger.error("Wrong block %i %i",
                                block[0],
                                self.stream.tell())
            raise IOError
        return self.stream.read_buffer(block[1])

    def close(self):
        self.stream.close()


@Logger.logged
class Sas7bdatReader(BaseReader):

    def __init__(self,
                 filepath_or_buffer,
                 header,
                 header_offset,
                 rnd,
                 nsamples=4,
                 num_rows=4095
                 ):
        # Switch between metastore and buffer ?
        # self.stream = pa.input_stream(filepath_or_buffer)
        super().__init__()
        self.stream = self.gate.store.open(filepath_or_buffer)
        self.header = header
        self.header_offset = header_offset
        self.num_rows = num_rows
        self.nsamples = nsamples
        self.rnd = rnd
        self.schema = None
        self.reader = SAS7BDAT(self.__module__,
                               log_level=self.__logger.getEffectiveLevel(),
                               extra_time_format_strings=None,
                               extra_date_format_strings=None,
                               skip_header=False,
                               encoding='utf8',
                               encoding_errors='ignore',
                               align_correction=True,
                               fh=self.stream)  # Use pa.open_stream()
        self.iter_ = self.reader.readlines()
        self._prepare()

    def _prepare(self):
        _props = self.reader.header.properties
        self.schema = self.reader.column_names_strings
        self.header_offset = self.reader.properties.header_length
        #  SASHeader __repr__
        self.header = 'Header:\n%s' % '\n'.join(
            ['\t%s: %s' % (k, v.decode(self.reader.encoding,
                                       self.reader.encoding_errors)
             if isinstance(v, bytes) else v)
             for k, v in sorted(six.iteritems(_props.__dict__))]
            )
        self.header = bytes(self.header, 'utf8')
        self.schema = next(self.iter_)

    def reset(self):
        self.iter_ = self.reader.readlines()
        next(self.iter_)  # Skip header row

    def sampler(self):
        '''
        Requires reading entire SAS
        Otherwise, we'll need to rewrite the underlying reader
        to sample raw bytes correctly

        TODO -- Implement random bytes chunk from SASbdat files
        '''
        self.reset()
        batches = [batch for batch in self]
        rndblocks = iter(self.rnd.choice(len(batches),
                         self.nsamples))
        for iblock in rndblocks:
            yield batches[iblock]
        self.__logger.debug("Completed sampling")
        self.reset()

    def __next__(self):
        data = []
        nrecords = 0
        while nrecords < self.num_rows:
            try:
                row = next(self.iter_)
            except StopIteration:
                break
            data.append(row)
            nrecords += 1
        if nrecords == 0:
            raise StopIteration
        data = zip(*data)  # Transpose python rows to columns
        arrays = [pa.array(arr) for arr in data]
        batch = pa.RecordBatch.from_arrays(arrays, self.schema)
        return batch

    def close(self):
        self.stream.close()
