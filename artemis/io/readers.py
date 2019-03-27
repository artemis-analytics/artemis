#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2019 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""

"""
import pyarrow as pa

from artemis.logger import Logger
from artemis.errors import AbstractMethodError


class BaseReader():

    def sampler(self):
        pass

    def __iter__(self):
        return self

    def __next__(self):
        raise AbstractMethodError(self)

    def close(self):
        pass


class ReaderFactory():

    def __new__(cls, reader,
                filepath_or_buffer,
                header,
                header_offset,
                blocks,
                rnd,
                nsamples):

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
        else:
            raise TypeError


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
        self.stream = pa.input_stream(filepath_or_buffer)
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
        self.stream = pa.input_stream(filepath_or_buffer)
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
