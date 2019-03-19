#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""
Generic tool for reading raw bytes into Arrow buffer
Aimed for handling ascii encoded files, e.g.
tab delimited or legacy data
Support for:
Chunking data in bytes
Scanning for line delimiter
Extracting meta data from a header
"""
import io
import pathlib
import pyarrow as pa

from artemis.decorators import iterable
from artemis.core.tool import ToolBase
from artemis.generators.common import BuiltinsGenerator
from artemis.io.readers import ReaderFactory 


@iterable
class FileHandlerOptions:
    blocksize = 2**27
    separator = ','
    skip_header = False
    legacy_data = False
    offset_header = 0
    seed = 42
    nsamples = 1


class FileHandlerTool(ToolBase):

    def __init__(self, name, **kwargs):
        options = dict(FileHandlerOptions())
        options.update(kwargs)
        super().__init__(name, **options)
        #  Default delimiter value is None
        #  Force set of delimiter in configuration options
        #  If not set, no finding end of line with delimiter search
        self._delimiter = None
        self._offset_header = None
        self._legacy_data = self.properties.legacy_data
        self.nsamples = self.properties.nsamples
        self.__logger.info('%s: __init__ FileHandlerTool' % self.name)

        if hasattr(self.properties, 'seed'):
            self._builtin_generator = BuiltinsGenerator(self.properties.seed)
        else:
            self._builtin_generator = BuiltinsGenerator()
        #
        self.header = None
        self.header_offset = None
        self.footer_offset = None
        self.footer = None
        self.schema = None
        self.blocks = []  # list of tuples (offset, length)
        self.size = None

    def encode_delimiter(self):
        self._delimiter = bytes(self.properties.delimiter, 'utf8')

    def initialize(self):
        self.__logger.info("%s properties: %s",
                           self.__class__.__name__,
                           self.properties)
        if hasattr(self.properties, 'delimiter'):
            self.encode_delimiter()
        self._offset_header = self.properties.offset_header

    def execute(self, filepath_or_buffer):
        input_type = 'csv'
        if self._legacy_data is True:
            input_type = 'legacy'

        self.__logger.info("Prepare csv file")
        stream = pa.input_stream(filepath_or_buffer)

        if stream.tell() != 0:
            stream.seek(0)

        if input_type == 'csv':
            header = self._readline(stream)
        if input_type == 'legacy':
            header = stream.read(self._offset_header)
        header_offset = stream.tell()
        stream.seek(0, 2)
        fsize = stream.tell()
        
        schema = None
        if input_type == 'csv':
            try:
                schema = header.decode().rstrip(self.properties.delimiter).\
                    split(self.properties.separator)
            except Exception:
                self.__logger.error("Error converting icsv header to list")
                raise
        self.__logger.info("Csv info %s %s %s", header, header_offset, schema)
        if self.header is None:
            self.header = header
            self.header_offset = header_offset
            self.schema = schema
            self.size = fsize
        else:
            if self.header != header:
                self.__logger.error("Cannot validate header")
                self.__logger.error("Original %s", self.header)
                self.__logger.error("New header %s", header)
                raise ValueError
            if self.header_offset != header_offset:
                self.__logger.error("Cannot validate offset")
                self.__logger.error("Original %i", self.header_offset)
                self.__logger.error("New offset %i", header_offset)
                raise ValueError
            if schema != self.schema:
                self.__logger.error("Cannot validate schema")
                self.__logger.error("Original %s", self.schema)
                self.__logger.error("New schema %s", schema)
                raise ValueError

        stream.seek(header_offset)
        pos = stream.tell()
        self.blocks = []
        size = self.properties.blocksize
        while stream.tell() < fsize:

            self.__logger.debug("Current position %i size %i filesize %i",
                                pos, size, fsize)
            self.blocks.append(self._get_block(stream,
                                               pos,
                                               size,
                                               fsize,
                                               self._delimiter))
            pos = stream.tell()

        stream.close()

        if self._legacy_data is True:
            self.blocks[-1] = (self.blocks[-1][0],
                               self.blocks[-1][1] - self._offset_header)
            self.__logger.info("Final block w/o footer %s", self.blocks[-1])

        self.__logger.info("Create Reader n samples %i", self.nsamples)
        return ReaderFactory(input_type, filepath_or_buffer,
                             header,
                             header_offset,
                             self.blocks,
                             self._builtin_generator.rnd,
                             self.nsamples)

    def _create_header(self, schema):
        linesep = self.properties.delimiter
        csv = io.StringIO()
        csv.write(u",".join(schema))
        csv.write(linesep)

        # bytes object with unicode encoding
        csv = csv.getvalue().encode()
        return bytearray(csv)

    def _readline(self, stream, size=-1):
        '''
        Using pyarrow input_stream
        use cpython _pyio readline
        '''
        if size is None:
            size = -1
        else:
            try:
                size_index = size.__index__
            except AttributeError:
                raise TypeError(f"{size!r} is not an integer")
            else:
                size = size_index()
        res = bytearray()
        while size < 0 or len(res) < size:
            b = stream.read(1)
            if not b:
                break
            res += b
            if res.endswith(b"\n"):
                break
        return bytes(res)

    def _seek_delimiter(self, file_, delimiter, blocksize):
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

    def _get_block(self, file_, offset, length, size, delimiter=None):
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
                self._seek_delimiter(file_, delimiter, 2**16)
            except (OSError, ValueError):
                file_.seek(0, 2)
            end = file_.tell()

            offset = start
            length = end - start
        else:
            self.__logger.debug("Seek to block offset %i length %i size %i",
                                offset, length, size)

            start = file_.tell()
            length -= start - offset
            if (start+length) > size:
                length = size - start
            # BUG - No Exception thrown on seek past last byte in object
            try:
                file_.seek(start + length)
            except (OSError, ValueError):
                file_.seek(0, 2)
            end = file_.tell()

            offset = start
            length = end - start

        return offset, length

    def _read_block(self, file_, offset, length, delimiter=None):
        '''
        Dask-like block read of data in bytes
        Ensures the start point of a block is after a delimiter
        '''
        if offset != file_.tell():  # commonly both zero
            file_.seek(offset)

        if not offset and length is None and file_.tell() == 0:
            return file_.read()

        if delimiter:
            self._seek_delimiter(file_, delimiter, 2**16)
            start = file_.tell()
            length -= start - offset

            try:
                file_.seek(start + length)
                self._seek_delimiter(file_, delimiter, 2**16)
            except (OSError, ValueError):
                file_.seek(0, 2)

            end = file_.tell()

            offset = start
            length = end - start

            file_.seek(offset)
        return file_.read(length)


class FileFactory():
    '''
    Some ideas taken from github.com/claudep/tabimport
    Abstract away the stream type
    Assumes everything is a file read in as bytes
    '''
    def __new__(cls, datafile):
        format = cls._sniff_format(datafile)

        if format == 'raw':
            return io.BytesIO(datafile)
        elif format == 'file':
            return open(datafile, 'rb')
        elif format == 'path':
            return open(datafile, 'rb')

    @classmethod
    def _sniff_format(cls, dfile):
        if isinstance(dfile, str):
            # format = dfile.rsplit('.', 1)[-1]
            format = 'file'
        elif isinstance(dfile, bytes):
            format = 'raw'
        elif isinstance(dfile, pathlib.PosixPath):
            format = 'path'
        return format
