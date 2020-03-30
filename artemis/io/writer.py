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
#
# This module contains code from the Pandas project
#
#    BSD 3-Clause License
#
#    Copyright (c) 2008-2012, AQR Capital Management, LLC, Lambda Foundry, Inc. and
#    PyData Development Team All rights reserved.

"""
Writer classes to manage output data streams to collect record batches into Arrow,
Parquet or Csv file formats.
"""
import urllib
import uuid

import pyarrow as pa

from artemis.core.algo import IOAlgoBase
from artemis.logger import Logger
from artemis.decorators import timethis, iterable
from artemis.core.gate import ArtemisGateSvc

from artemis.io.protobuf.cronus_pb2 import FileObjectInfo, TableObjectInfo
from artemis.io.protobuf.table_pb2 import Table


@iterable
class BufferOutputOptions:
    BUFFER_MAX_SIZE = 2147483648  # 2 GB
    write_csv = True


@Logger.logged
class BufferOutputWriter(IOAlgoBase):
    """
    Manage output data with an in-memory buffer
    buffer is flushed to disk when a max buffer size
    is reached
    Only data sink supported is Arrow::BufferOutputStream
    """

    def __init__(self, name, **kwargs):
        options = dict(BufferOutputOptions())
        options.update(kwargs)
        super().__init__(name, **options)

        self.BUFFER_MAX_SIZE = self.properties.BUFFER_MAX_SIZE
        self._write_csv = self.properties.write_csv
        self._cache = None  # cache for a pa.RecordBatch
        self._buffer = None  # in-memory buffer
        self._sink = None  # pa.BufferOutputStream
        self._writer = None  # pa.RecordBatchFileWriter
        self._schema = None  # pa.schema

        self._sizeof_batches = 0
        self._nbatches = 0  # batches per file
        self._nrecords = 0  # records per file
        self._ncolumns = 0  # columns per file
        self._total_records = 0  # total records written
        self._total_batches = 0  # total number of batches written
        self._filecounter = 0  # total files
        self._fname = ""
        self._finfo = []  # Store list of metadata info objects
        self.gate = None

    @property
    def total_records(self):
        return self._total_records

    @property
    def total_batches(self):
        return self._total_batches

    @property
    def total_files(self):
        return self._filecounter

    def initialize(self):
        self.__logger.info("Initialize writer")
        self.__logger.info(self.properties)
        self.gate = ArtemisGateSvc()
        self._buffer = None
        self._sink = pa.BufferOutputStream()
        self._writer = pa.RecordBatchFileWriter(self._sink, self._schema)

    def flush(self):
        """If all else fails, clear everything
        """
        self.__logger.error("Flushing buffer %s", self.name)
        self._writer = None
        self._sink = None
        self._buffer = None

    def _validate_metainfo(self):
        """
        Validate payload in Arrow table
        - number batches in file
        - number of rows
        - number of columns
        - schema
        """
        try:
            reader = pa.ipc.open_file(self._buffer)
        except Exception:
            raise
        self.__logger.info("Batches in file %i", reader.num_record_batches)
        if self._nbatches != reader.num_record_batches:
            self.__logger.error(
                "Num batches: counter %i payload %i",
                self._nbatches,
                reader.num_record_batches,
            )
            raise ValueError

        sum_records = 0
        for ibatch in range(reader.num_record_batches):
            sum_records += reader.get_batch(ibatch).num_rows

        if self._nrecords != sum_records:
            self.__logger.error(
                "Num records: counter %i payload %i", self._nrecords, sum_records
            )
            raise ValueError

        self._total_records += self._nrecords
        self._total_batches += self._nbatches

    def _finalize_file(self):
        """
        """
        try:
            self._writer.close()
        except Exception:
            self.__logger.error("Cannot close writer")
            raise
        try:
            self._write_buffer()
        except Exception:
            self.__logger.error("Cannot write buffer to disk")
            raise
        try:
            self._write_file()
        except Exception:
            self.__logger.error("Cannot write buffer to disk")
            raise
        try:
            self._validate_metainfo()
        except Exception:
            self.__logger.error("Problem validating metadata")
            raise
        try:
            self._reset()
        except Exception:
            self.__logger.error("Problem resetting internals for next stream")
            raise

    def _finalize(self):
        """
        Close final writer
        Close final buffer
        Gather statistics
        """
        self.__logger.info("Finalize final file %s", self._fname)
        self.__logger.info("Number of batches %i" % self._nbatches)
        self.__logger.info("Number of records %i ", self._nrecords)
        if self._nbatches == 0:
            self.__logger.info("No batches")
            self._writer.close()
            return True

        try:
            self._finalize_file()
        except Exception:
            raise

        return True

    def expected_sizeof(self, batch):
        _sum = 0
        _sum = pa.get_record_batch_size(batch)
        _sum += self._sizeof_batches
        return _sum

    def _reset(self):
        """
        reset for new stream
        """
        self._filecounter += 1
        self._new_sink()
        self._sizeof_batches = 0
        self._nbatches = 0
        self._nrecords = 0

    def _new_sink(self):
        """
        return a new BufferOutputStream
        """
        self.__logger.info("Request new BufferOutputStream")
        self._buffer = None  # Clear the buffer cache
        self._sink = pa.BufferOutputStream()

    def _write_buffer(self):
        try:
            self._buffer = self._sink.getvalue()
            self.__logger.info("Size of buffer %i", self._buffer.size)
        except Exception:
            self.__logger.error("Cannot flush stream")
            raise

    def _build_table_from_file(self, file_id):
        """
        build a table schema from inferred file schema

        Parameters
        ----------
        file_id : uuid

        """
        ds_id = self.gate.store[file_id].parent_uuid
        pkey = self.gate.store[file_id].file.partition
        job_id = self.gate.meta.job_id
        raw_schema = self._schema.serialize().to_pybytes()
        raw_schema_size = len(raw_schema)
        self.__logger.info(
            "Writing Table to DS %s partition %s job %s", ds_id, pkey, job_id
        )

        table = Table()
        table.uuid = str(uuid.uuid4())
        table.name = (
            f"{ds_id}.job_{job_id}.part_{pkey}.file_{file_id}.{table.uuid}.table.pb"
        )

        tinfo = TableObjectInfo()
        for f in self._schema:
            field = table.info.schema.info.fields.add()
            field.name = f.name
            field.info.type = str(f.type)
            tinfo.fields.append(field.name)

        table.info.schema.info.aux.raw_header_size_bytes = raw_schema_size
        table.info.schema.info.aux.raw_header = raw_schema

        self.register_content(
            table, tinfo, dataset_id=ds_id, partition_key=pkey, job_id=job_id
        )

    def _write_file(self):
        fileinfo = FileObjectInfo()
        fileinfo.type = 5
        fileinfo.aux.num_rows = self._nrecords
        fileinfo.aux.num_columns = self._ncolumns
        fileinfo.aux.num_batches = self._nbatches
        p_key = self.name.split("_")[-1]
        ds_id = self.gate.meta.dataset_id
        job_id = self.gate.meta.job_id
        self.logger.info("Partitions %s", self.gate.store.list_partitions(ds_id))
        self.__logger.info(
            "Writing arrow to DS %s partition %s job %s", ds_id, p_key, job_id
        )
        fileinfo.partition = p_key
        try:
            id_ = self.register_content(
                self._buffer,
                fileinfo,
                dataset_id=ds_id,
                job_id=job_id,
                partition_key=p_key,
            ).uuid
        except Exception:
            self.__logger.error("Fail to register buffer to store")
            raise
        self.__logger.info("Writing to store id: %s", id_)
        self.gate.store.put(id_, self._buffer)
        self._build_table_from_file(id_)

        if self._write_csv is True:
            fileinfo = FileObjectInfo()
            fileinfo.type = 1
            fileinfo.aux.num_rows = self._nrecords
            fileinfo.aux.num_columns = self._ncolumns
            fileinfo.aux.num_batches = self._nbatches
            partition_key = self.name.split("_")[-1]
            self.__logger.info(
                "Writing CSV DS: %s Partition: %s Job: %s", ds_id, partition_key, job_id
            )
            try:
                obj = self.register_content(
                    self._buffer,
                    fileinfo,
                    dataset_id=ds_id,
                    job_id=job_id,
                    partition_key=p_key,
                )
                address = obj.address
            except Exception:
                self.__logger.error("Cannot register csv file")
            urldata = urllib.parse.urlparse(address)
            path = urllib.parse.unquote(urldata.path)
            BufferOutputWriter.to_csv(self._buffer, path)

    def _new_writer(self):
        """
        return a new writer
        requires closing the current writer
        flushing the buffer
        writing the buffer to file
        """
        self.__logger.info("Finalize file %s", self._fname)
        self.__logger.info("N Batches %i Size %i", self._nbatches, self._sizeof_batches)

        try:
            self._finalize_file()
        except Exception:
            raise

        self._writer = pa.RecordBatchFileWriter(self._sink, self._schema)

    def _can_write(self, batch):
        _size = self.expected_sizeof(batch)
        if _size > self.BUFFER_MAX_SIZE:
            self.__logger.info("Request new writer")
            self.__logger.info(
                "Current size %i, estimated %i", self._sizeof_batches, _size
            )
            try:
                self._new_writer()
            except Exception:
                self.__logger.error("Failed to create new writer")
                raise
        else:
            self.__logger.debug("Continue filling buffer")

    @timethis
    def write(self, payload):
        """
        Manages writing a collection of batches
        caches a batch if beyond the max buffer size

        this should function as a consumer of batches
        RecordBatches are given as a generator to ensure
        all batches are pushed to a buffer
        """
        for i, element in enumerate(payload):
            self.__logger.debug("Processing Element %i", i)
            batch = element.get_data()
            if not isinstance(batch, pa.lib.RecordBatch):
                self.__logger.warning("Batch is of type %s", type(batch))
                continue
            if batch.schema != self._schema:
                self.__logger.error("Batch error, incorrect schema")
                if len(batch.schema) != len(self._schema):
                    self.__logger.error("mismatch in number of fields")
                else:
                    for icol, col in enumerate(self._schema):
                        if col != batch.schema[icol]:
                            self.__logger.error("Current field %s", batch.schema[icol])
                            self.__logger.error("Expected field %s", col)
                raise ValueError
            try:
                self._can_write(batch)
            except Exception as e:
                self.__logger.error("Failed sizeof check")
                raise e
            try:
                self.__logger.debug("Write to sink")
                self._ncolumns = batch.num_columns
                self._nrecords += batch.num_rows
                self._nbatches += 1
                self._sizeof_batches += pa.get_record_batch_size(batch)
                self.__logger.debug(
                    "Records %i Batches %i size %i",
                    self._nrecords,
                    self._nbatches,
                    self._sizeof_batches,
                )
                self._writer.write_batch(batch)
            except Exception:
                self.__logger.error("Cannot write a batch")
                raise
        self.__logger.debug(
            "Records %i Batches %i size %i",
            self._nrecords,
            self._nbatches,
            self._sizeof_batches,
        )
        return True

    @staticmethod
    def to_csv(
        buf,
        path_or_buf=None,
        sep=",",
        na_rep="",
        float_format=None,
        columns=None,
        header=True,
        index=False,
        index_label=None,
        mode="w",
        encoding=None,
        compression=None,
        quoting=None,
        quotechar='"',
        line_terminator="\n",
        chunksize=None,
        date_format=None,
        doublequote=True,
        escapechar=None,
        decimal=".",
    ):
        """ Write DataFrame to a comma-separated values (csv) file. Obtained from
        pandas.core.frame.

        Parameters
        ----------
        buf : pyarrow.buffer
            arrow buffer of a RecordBatchFile
        path_or_buf : string or file handle, default None
            File path or object, if None is provided the result is returned as
            a string.
        sep : character, default ``,``
            Field delimiter for the output file.
        na_rep : string, default ``''``
            Missing data representation
        float_format : string, default None
            Format string for floating point numbers
        columns : sequence, optional
            Columns to write
        header : boolean or list of string, default True
            Write out the column names. If a list of strings is given it is
            assumed to be aliases for the column names
        index : boolean, default True
            Write row names (index)
        index_label : string or sequence, or False, default None
            Column label for index column(s) if desired. If None is given, and
            ``header`` and ``index`` are True, then the index names are used. A
            sequence should be given if the DataFrame uses MultiIndex.  If
            False do not print fields for index names. Use index_label=False
            for easier importing in R
        mode : str
            Python write mode, default 'w'
        encoding : string, optional
            A string representing the encoding to use in the output file,
            defaults to 'ascii' on Python 2 and 'utf-8' on Python 3.
        compression : string, optional
            A string representing the compression to use in the output file.
            Allowed values are 'gzip', 'bz2', 'zip', 'xz'. This input is only
            used when the first argument is a filename.
        line_terminator : string, default ``'\n'``
            The newline character or character sequence to use in the output
            file
        quoting : optional constant from csv module
            defaults to csv.QUOTE_MINIMAL. If you have set a `float_format`
            then floats are converted to strings and thus csv.QUOTE_NONNUMERIC
            will treat them as non-numeric
        quotechar : string (length 1), default ``'\"'``
            character used to quote fields
        doublequote : boolean, default True
            Control quoting of `quotechar` inside a field
        escapechar : string (length 1), default None
            character used to escape `sep` and `quotechar` when appropriate
        chunksize : int or None
            rows to write at a time
        date_format : string, default None
            Format string for datetime objects
        decimal: string, default '.'
            Character recognized as decimal separator. E.g. use ',' for
            European data

        Returns
        -------
        bytes

        """

        frame = pa.ipc.open_file(buf).read_pandas()

        from pandas.io.formats.csvs import CSVFormatter

        formatter = CSVFormatter(
            frame,
            path_or_buf,
            line_terminator=line_terminator,
            sep=sep,
            encoding=encoding,
            compression=compression,
            quoting=quoting,
            na_rep=na_rep,
            float_format=float_format,
            cols=columns,
            header=header,
            index=index,
            index_label=index_label,
            mode=mode,
            chunksize=chunksize,
            quotechar=quotechar,
            # tupleize_cols=tupleize_cols,
            date_format=date_format,
            doublequote=doublequote,
            escapechar=escapechar,
            decimal=decimal,
        )
        formatter.save()

        if path_or_buf is None:
            return formatter.path_or_buf.getvalue()
