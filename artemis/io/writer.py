"""
Dedicated Writer classes to manage output data streams
"""
import pyarrow as pa

from artemis.core.tool import ToolBase
from artemis.logger import Logger
from artemis.decorators import timethis


@Logger.logged
class BufferOutputWriter(ToolBase):
    '''
    Manage output data with an in-memory buffer
    buffer is flushed to disk when a max buffer size
    is reached
    Only data sink supported is Arrow::BufferOutputStream
    '''

    def __init__(self, name, **kwargs):
        defaults = self._set_defaults()
        # Override the defaults from the kwargs
        for key in kwargs:
            defaults[key] = kwargs[key]
        super().__init__(name, **defaults)

        self.BUFFER_MAX_SIZE = 2147483648  # 2 GB
        # self._name = name
        self._write_csv = True

        self._cache = None  # cache for a pa.RecordBatch
        self._sink = None  # pa.BufferOutputStream
        self._writer = None  # pa.RecordBatchFileWriter
        self._schema = None  # pa.schema
        self._fbasename = None
        self._sizeof_batches = 0
        self._nbatches = 0  # batches per file
        self._nrecords = 0  # total record count
        self._filecounter = 0
        self._fname = ''

    def _set_defaults(self):
        defaults = {'BUFFER_MAX_SIZE': 2147483648,  # 2 GB
                    'write_csv': True}

        return defaults

    @property
    def total_records(self):
        return self._nrecords

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
        self.__logger.info("Batchs in final file %i" % self._nbatches)
        if self._nbatches == 0:
            self.__logger.info("No batches")
            self._writer.close()
            return True
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
        return True

    @staticmethod
    def to_csv(buf, path_or_buf=None, sep=",", na_rep='', float_format=None,
               columns=None, header=True, index=False, index_label=None,
               mode='w', encoding=None, compression=None, quoting=None,
               quotechar='"', line_terminator='\n', chunksize=None,
               tupleize_cols=None, date_format=None, doublequote=True,
               escapechar=None, decimal='.'):
        r"""Write DataFrame to a comma-separated values (csv) file

        Obtained from pandas.core.frame
        Parameters
        ----------
        buf : arrow buffer of a RecordBatchFile
        path_or_buf : string or file handle, default None
            File path or object, if None is provided the result is returned as
            a string.
        sep : character, default ','
            Field delimiter for the output file.
        na_rep : string, default ''
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
            `header` and `index` are True, then the index names are used. A
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
        quotechar : string (length 1), default '\"'
            character used to quote fields
        doublequote : boolean, default True
            Control quoting of `quotechar` inside a field
        escapechar : string (length 1), default None
            character used to escape `sep` and `quotechar` when appropriate
        chunksize : int or None
            rows to write at a time
        tupleize_cols : boolean, default False
            .. deprecated:: 0.21.0
               This argument will be removed and will always write each row
               of the multi-index as a separate row in the CSV file.

            Write MultiIndex columns as a list of tuples (if True) or in
            the new, expanded format, where each MultiIndex column is a row
            in the CSV (if False).
        date_format : string, default None
            Format string for datetime objects
        decimal: string, default '.'
            Character recognized as decimal separator. E.g. use ',' for
            European data

        """

        '''
        if tupleize_cols is not None:
            warnings.warn("The 'tupleize_cols' parameter is deprecated and "
                          "will be removed in a future version",
                          FutureWarning, stacklevel=2)
        else:
            tupleize_cols = False
        '''
        # Convert table to dataframe
        # use_threads can be enabled
        # frame = table.to_pandas(use_threads=False)
        frame = pa.open_file(buf).read_pandas()

        from pandas.io.formats.csvs import CSVFormatter
        formatter = CSVFormatter(frame, path_or_buf,
                                 line_terminator=line_terminator, sep=sep,
                                 encoding=encoding,
                                 compression=compression, quoting=quoting,
                                 na_rep=na_rep, float_format=float_format,
                                 cols=columns, header=header, index=index,
                                 index_label=index_label, mode=mode,
                                 chunksize=chunksize, quotechar=quotechar,
                                 tupleize_cols=tupleize_cols,
                                 date_format=date_format,
                                 doublequote=doublequote,
                                 escapechar=escapechar, decimal=decimal)
        formatter.save()

        if path_or_buf is None:
            return formatter.path_or_buf.getvalue()

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
                      '_' + self.name + \
                      '_' + str(self._filecounter) + '.arrow'

    def _write_buffer(self):
        try:
            buf = self._sink.getvalue()
            self.__logger.info("Size of buffer %i", buf.size)
        except Exception:
            self.__logger.error("Cannot flush stream")
            raise
        with pa.OSFile(self._fname, 'wb') as f:
            try:
                f.write(buf)
            except IOError:
                self.__logger_error("Error writing OSFile %s", self._fname())
                raise
        if self._write_csv is True:
            BufferOutputWriter.to_csv(buf, self._fname + '.csv')

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
            self.__logger.debug("Processing Element %i", i)
            batch = element.get_data()
            self._nrecords += batch.num_rows
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
                self.__logger.debug("Write to sink")
                self._writer.write_batch(batch)
            except Exception:
                self.__logger.error("Cannot write a batch")
                raise
            self._nbatches += 1
            self._sizeof_batches += pa.get_record_batch_size(batch)

        return True
