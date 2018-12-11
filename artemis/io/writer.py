#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Dedicated Writer classes to manage output data streams
"""
import pyarrow as pa

from artemis.logger import Logger
from artemis.decorators import timethis


@Logger.logged
class BufferOutputWriter():
    '''
    Manage output data with an in-memory buffer
    buffer is flushed to disk when a max buffer size
    is reached
    Only data sink supported is Arrow::BufferOutputStream
    '''

    def __init__(self, name, **kwargs):
        self.BUFFER_MAX_SIZE = 2147483648  # 2 GB
        self._name = name
        self._cache = None  # cache for a pa.RecordBatch
        self._sink = None  # pa.BufferOutputStream
        self._writer = None  # pa.RecordBatchFileWriter
        self._schema = None  # pa.schema
        self._fbasename = None
        self._sizeof_batches = 0
        self._nbatches = 0
        self._nrecords = 0
        self._filecounter = 0
        self._fname = ''

    def initialize(self):
        self._sink = pa.BufferOutputStream()
        self._writer = pa.RecordBatchFileWriter(self._sink, self._schema)
        self._new_filename()

    def _finalize(self):
        '''
        Close final writer
        Close final buffer
        Gather statistics
        '''
        self.__logger.info("Finalize final file")
        try:
            self._writer.close()
        except Exception:
            self.__logger.error("Cannot close final writer")
            raise
        try:
            self._write_buffer()
        except Exception:
            self.__logger.error("Cannot flush final buffer")
            raise

    def expected_sizeof(self, batch):
        _sum = 0
        _sum = pa.get_record_batch_size(batch)
        _sum += self._sizeof_batches
        return _sum

    def _new_sink(self):
        '''
        return a new BufferOutputStream
        '''
        self.__logger.info("Request new BufferOutputStream")
        self._sink = pa.BufferOutputStream()

    def _new_filename(self):
        self._fname = self._fbasename + \
                      '_' + self._name + \
                      '_' + str(self._filecounter) + '.arrow'

    def _write_buffer(self):
        try:
            buf = self._sink.getvalue()
        except Exception:
            self.__logger.error("Cannot flush stream")
            raise
        with pa.OSFile(self._fname, 'wb') as f:
            try:
                f.write(buf)
            except IOError:
                self.__logger_error("Error writing OSFile %s", self._fname())
                raise

    def _new_writer(self):
        '''
        return a new writer
        requires closing the current writer
        flushing the buffer
        writing the buffer to file
        '''
        self.__logger.info("Finalize file %s", self._fname)
        self.__logger.info("N Batches %i Size %i",
                           self._nbatches, self._sizeof_batches)
        try:
            self._writer.close()
        except Exception:
            self.__logger.error('Cannot close writer')
            raise
        try:
            self._write_buffer()
        except Exception:
            self.__logger.error('Cannot write buffer to disk')
            raise
        self._filecounter += 1
        self._new_filename()
        self._new_sink()
        self._sizeof_batches = 0
        self._nbatches = 0
        self._writer = pa.RecordBatchFileWriter(self._sink, self._schema)

    def _can_write(self, batch):
        _size = self.expected_sizeof(batch)
        if _size > self.BUFFER_MAX_SIZE:
            self.__logger.info("Request new writer")
            self.__logger.info("Current size %i, estimated %i",
                               self._sizeof_batches, _size)
            try:
                self._new_writer()
            except Exception:
                self.__logger.error("Failed to create new writer")
                raise
        else:
            self.__logger.debug("Continue filling buffer")

    @timethis
    def write(self, payload):
        '''
        Manages writing a collection of batches
        caches a batch if beyond the max buffer size

        this should function as a consumer of batches
        RecordBatches are given as a generator to ensure
        all batches are pushed to a buffer
        '''
        for i, element in enumerate(payload):
            self.__logger.info("Processing Element %i", i)
            batch = element.get_data()
            if not isinstance(batch, pa.lib.RecordBatch):
                self.__logger.warning("Batch is of type %s", type(batch))
                continue
            if batch.schema != self._schema:
                self.__logger.warning("Batch ignored, incorrect scema")
                continue
            try:
                self._can_write(batch)
            except Exception:
                self.__logger.error("Failed sizeof check")
                raise
            try:
                self.__logger.info("Write to sink")
                self._writer.write_batch(batch)
            except Exception:
                self.__logger.error("Cannot write a batch")
                raise
            self._nbatches += 1
            self._sizeof_batches += pa.get_record_batch_size(batch)

        return True
