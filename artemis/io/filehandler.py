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
    delimiter = ','
    linesep = '\r\n'
    header_offset = 0
    header = ''
    footer_offset = 0
    footer = ''
    seed = 42
    nsamples = 1
    filetype = 'csv'
    encoding = 'utf8'
    schema = []
    header_rows = 1


class FileHandlerTool(ToolBase):

    def __init__(self, name, **kwargs):
        options = dict(FileHandlerOptions())
        options.update(kwargs)
        super().__init__(name, **options)
        
        self.encoding = self.properties.encoding
        
        self.delimiter = self.properties.delimiter
        self.linesep = self.properties.linesep
        self.header = bytes(self.properties.header, self.encoding)
        self.footer = bytes(self.properties.header, self.encoding)
        
        self.header_offset = self.properties.header_offset
        self.footer_offset = self.properties.footer_offset
        self.schema = self.properties.schema
        self.header_rows = self.properties.header_rows

        self.nsamples = self.properties.nsamples
        self.filetype = self.properties.filetype
        self.blocksize = self.properties.blocksize

        self.__logger.info('%s: __init__ FileHandlerTool' % self.name)

        if hasattr(self.properties, 'seed'):
            self._builtin_generator = BuiltinsGenerator(self.properties.seed)
        else:
            self._builtin_generator = BuiltinsGenerator()
        #
        self.blocks = []  # list of tuples (offset, length)
        self.size = None
        self.cache_header = None
        self.cache_schema = None

    def initialize(self):
        self.__logger.info("%s properties: %s",
                           self.__class__.__name__,
                           self.properties)
    
    def prepare_csv(self, stream):
        '''
        '''
        self.__logger.info("%s %s", self.linesep, self.delimiter)
        if self.header_rows == 0:
            # Requires schema
            if len(self.schema) == 0:
                self.__logger.error("File does not include header, provide schema")
                raise ValueError
            header = self._create_header(self.schema)
        elif self.header_rows == 1:
            header = self._readline(stream)
            header_offset = stream.tell()
            schema = header.decode().rstrip(self.linesep).\
                split(self.delimiter)
            self.__logger.info("Schema %s", schema)
            # User supplied schema, update header
            if self.schema:
                self.__logger.info("Updating schema %s", self.schema)
                if len(self.schema) != len(schema):
                    self.__logger.error("User schema does not conform to file schema")
                    self.__logger.error("Schema %s", schema)
                    self.__logger.error("Cached %s", self.schema)
                    raise ValueError
                schema = self.schema
            header = self._create_header(schema)
        else:
            # TODO
            # Store multiline header
            # Replace with user defined schema
            header = b''
            for row in self.header_rows:
                header += self._readline(stream)
            header_offset = stream.tell()
            # Replace multiline header with user-defined schema 
            if self.schema:
                header = self._create_schema(schema)
        
        self.header = header
        self.header_offset = header_offset
        self.schema = schema
        self.__logger.info("Stream info header: %s Offset: %s Schema: %s", 
                           self.header, 
                           self.header_offset, 
                           self.schema)
    
    def prepare_legacy(self, stream):
        '''
        Assumes schema is supplied to the parser tool
        '''
        self.header = stream.read(self.header_offset)

    def prepare(self, stream):
        if stream.tell() != 0:
            stream.seek(0)
        if self.filetype == 'csv':
            self.prepare_csv(stream) 
        elif self.filetype == 'legacy':
            self.prepare_legacy(stream) 
        else:
            self.__logger.error("Unknown filetype")
            raise TypeError
        stream.seek(0, 2)
        self.size = stream.tell()
        stream.seek(self.header_offset)
        self.__logger.info("Stream info header: %s Offset: %s Schema: %s Size: %i", 
                           self.header, 
                           self.header_offset, 
                           self.schema,
                           self.size)

    def validate(self):
        '''
       
        '''
        pass

    def execute(self, filepath_or_buffer):

        self.__logger.info("Prepare csv file")
        stream = pa.input_stream(filepath_or_buffer)

        self.prepare(stream)
        pos = stream.tell()
        self.blocks = []
        while stream.tell() < self.size:

            self.__logger.info("Current position %i size %i filesize %i",
                                pos, self.blocksize, self.size)
            self.blocks.append(self._get_block(stream,
                                               pos,
                                               self.blocksize,
                                               self.size,
                                               bytes(self.linesep, self.encoding)))
            pos = stream.tell()

        stream.close()

        if self.filetype == 'legacy':
            self.blocks[-1] = (self.blocks[-1][0],
                               self.blocks[-1][1] - self.header_offset)
            self.__logger.info("Final block w/o footer %s", self.blocks[-1])

        self.__logger.info("Create Reader n samples %i", self.nsamples)
        self.__logger.info("header %s", self.header)
        self.__logger.info("Schema %s", self.schema)
        return ReaderFactory(self.filetype, filepath_or_buffer,
                             self.header,
                             self.header_offset,
                             self.blocks,
                             self._builtin_generator.rnd,
                             self.nsamples)

    def _create_header(self, schema):
        #linesep = self.linesep.decode(self.encoding)
        #delimiter = self.delimiter.decode(self.encoding)
        csv = io.StringIO()
        csv.write(u",".join(schema))
        csv.write(self.linesep)

        # bytes object with unicode encoding
        csv = csv.getvalue().encode()
        self.__logger.info(csv)
        return bytes(csv)

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
