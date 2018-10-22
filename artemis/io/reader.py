#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2017 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Simple class for file handling
Yields csv rows
"""
import csv
import io
import weakref

import pyarrow as pa
from pyarrow.csv import read_csv

from artemis.logger import Logger


@Logger.logged
class Reader():
    '''
    Wrapper class for reading csv file

    Can we create a PythonFile object and a weakreference
    to that FileObject?
    Pass the weakreference, do the reading in an algo

    '''
    def __init__(self, use_pyarrow=False, filename=None):
        self.filename = filename
        self._length = None
        self.read = None
        self._file = None  # pyarrow.PythonFile
        self._wr = None  # Weak reference to the file
        if self.filename is not None:
            self.read = self.read_data_from_file
        elif use_pyarrow is True:
            self.read = self.read_csv_from_bytes
        else:
            self.read = self.read_data_from_bytes
        self.__logger.info("Initialized reader %s", self.read)

    def open_from_buffer(self, data):
        try:
            self._file = pa.PythonFile(io.BytesIO(data), mode='r')
            # do something
            self._wr = weakref.ref(self._file)
        except Exception:
            self.__logger.error("Cannot open file from buffer")
            raise

    def open_from_path(self, path):
        # TODO
        # Figure out the in-memory file buffer first
        with open(path, 'rb') as f:
            try:
                self._file = pa.PythonFile(f, mode='r')
                self._wr = weakref.ref(self._file)
            finally:
                self._file.close()
                self.__logger.info("Complete file read")

    def close(self):
        if self.eof():
            try:
                self._file.close()
                return True
            except Exception:
                self.__logger.error("Cannot close file")
                raise
        else:
            self.__logger.warning("Not at EOF")
            return False

    def reset(self):
        self._file.seek(0)

    def eof(self):
        try:
            while self._file.read(1):
                self._file.seek(-1, 1)
                return False
        except Exception:
            self.__logger.warning("At end of file")
            return True

    @property
    def filehandle(self):
        return self._wr

    def read_as_table(self):
        table = read_csv(self._file)
        return table

    def read_as_csv(self):
        self._length = 0

        try:
            with io.TextIOWrapper(io.BytesIO(self._file)) as file_:
                file_.readline()
                for row in csv.reader(file_):
                    self._length += 1
                    yield row
        except IOError:
            raise
        except Exception:
            raise
        self.__logger.info("Initialized reader %s", self.read)

    def read_data_from_file(self):
        self._length = 0
        try:
            with open(self.filename, 'rU') as file_:
                file_.readline()
                reader = csv.reader(file_)
                for row in reader:
                    self._length += 1
                    yield row
        except IOError:
            raise
        except Exception:
            raise

    def read_data_from_bytes(self, data):
        '''
        implements a TextIOWrapper from raw bytes
        '''
        if isinstance(data, bytes) is False and \
                isinstance(data, bytearray) is False:
            raise TypeError
        self._length = 0

        try:
            with io.TextIOWrapper(io.BytesIO(data)) as file_:
                file_.readline()
                for row in csv.reader(file_):
                    self._length += 1
                    yield row
        except IOError:
            raise
        except Exception:
            raise

    def read_csv_from_bytes(self, data):
        '''
        implements pyarrow csv reader from raw bytes
        '''
        if isinstance(data, bytes) is False and \
                isinstance(data, bytearray) is False:
            raise TypeError

        buf = pa.py_buffer(data)
        table = read_csv(buf)
        return table


@Logger.logged
class FileHandler():

    def __init__(self):
        pass

    @staticmethod
    def seek_delimiter(file_, delimiter, blocksize):
        '''
        Dask-like line delimiter
        to read by bytes and seek to nearest line
        default block_size 2**16 or 64 bytes

        BUG
        Last block is not at EOF???
        '''
        if file_.tell() == 0:
            return

        last = b''
        while True:
            current = file_.read(blocksize)
            if not current:
                return
            full = last + current
            try:
                i = full.index(delimiter)
                file_.seek(file_.tell() - (len(full) - i) + len(delimiter))
                return
            except (OSError, ValueError):
                print("Problem at last seek")
            last = full[-len(delimiter):]

    @staticmethod
    def get_block(file_, offset, length, size, delimiter=None):
        '''
        Dask-like block read of data in bytes
        Returns the length of bytes to read for a block
        starts at last position in file, does not ensure that
        file is already at position after delimiter

        Requries starting offset to be after delimiter

        # TODO, if offset not a delimiter seek to the next one

        # BUG last seek goes past EOF
        '''
        if offset != file_.tell():  # commonly both zero
            file_.seek(offset)

        if not offset and length is None and file_.tell() == 0:
            return file_.read()

        if delimiter:
            # TODO
            # If initial block and not at file start
            # Find the first delimiter?
            start = file_.tell()
            length -= start - offset
            if (start+length) > size:
                length = size - start
            # BUG - No Exception thrown on seek past last byte in object
            try:
                file_.seek(start + length)
                FileHandler.seek_delimiter(file_, delimiter, 2**16)
            except (OSError, ValueError):
                file_.seek(0, 2)
            end = file_.tell()

            offset = start
            length = end - start

        return offset, length

    @staticmethod
    def read_block(file_, offset, length, delimiter=None):
        '''
        Dask-like block read of data in bytes
        Ensures the start point of a block is after a delimiter
        '''
        if offset != file_.tell():  # commonly both zero
            file_.seek(offset)

        if not offset and length is None and file_.tell() == 0:
            return file_.read()

        if delimiter:
            FileHandler.seek_delimiter(file_, delimiter, 2**16)
            start = file_.tell()
            length -= start - offset

            try:
                file_.seek(start + length)
                FileHandler.seek_delimiter(file_, delimiter, 2**16)
            except (OSError, ValueError):
                file_.seek(0, 2)

            end = file_.tell()

            offset = start
            length = end - start

            file_.seek(offset)
        return file_.read(length)

    def create_header(schema):
        linesep = '\r\n'
        csv = io.StringIO()
        csv.write(u",".join(schema))
        csv.write(linesep)

        # bytes object with unicode encoding
        csv = csv.getvalue().encode()
        return bytearray(csv)

    @staticmethod
    def readinto_block(file_, bobj, offset, schema=None):
        '''
        Dask-like block read of data in bytes
        Assumes length of block fixed and preallocated bytearray provided
        Assumes the blocksize and line delimiter already handled

        # Requires inserting header into each block
        '''
        if schema is None:
            if offset != file_.tell():
                file_.seek(offset)
            return file_.readinto(bobj)
        else:
            block_ = FileHandler.create_header(schema)

            if offset != file_.tell():
                file_.seek(offset)
            file_.readinto(bobj)
            block_.extend(bobj)
            # return file_.readinto(bobj)
            return block_

    @staticmethod
    def strip_header(file_, delimiter='\r\n', separator=','):
        '''
        try to read the first line of file
        filehandle, f, is pa.PythonFile
        '''
        if file_.tell() != 0:
            file_.seek(0)

        header = file_.readline()
        offset = file_.tell()  # Do we start of offset, or offset + byte
        meta = header.decode().rstrip(delimiter).split(separator)

        file_.seek(0)
        return header, meta, offset

    @staticmethod
    def get_blocks(file_,
                   blocksize=2**27,
                   delimiter='\r\n',
                   skip_header=False,
                   offset_header=None):
        '''
        creates a generator to return blocks of data from IO bytestream
        '''
        # Seek to end (0 bytes relative to the end)
        file_.seek(0, 2)
        fsize = file_.tell()
        if skip_header:
            file_.seek(offset_header)
        else:
            file_.seek(0)
        pos = file_.tell()
        blocks = []  # tuples of length two (offset, length)
        while file_.tell() < fsize:
            if pos == 0 and offset_header is not None:
                size = blocksize + offset_header
            else:
                size = blocksize
            blocks.append(FileHandler.get_block(file_,
                                                pos,
                                                size,
                                                fsize,
                                                delimiter))
            pos = file_.tell()

        # Seek back to start
        file_.seek(0)
        return blocks
