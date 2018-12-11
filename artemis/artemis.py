#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Class for managing and running the job
owns the stores needed by the user algorithms
    histograms
    timers
    objects
"""
# Python libraries
# import logging
from pprint import pformat
import sys
import io
import random

# Externals
import pyarrow as pa
# import pyarrow.parquet as pq

# Framework
from artemis.logger import Logger
# from artemis.exceptions import NullDataError

# Core
from artemis.core.properties import Properties
from artemis.core.properties import JobProperties
from artemis.core.steering import Steering
from artemis.core.tree import Tree
from artemis.core.algo import AlgoBase
from artemis.core.timerstore import TimerSvc

# Data generators
# from artemis.generators.generators import GenCsvLikeArrow

# IO
from artemis.io.reader import FileHandler
from artemis.io.writer import BufferOutputWriter
from artemis.core.physt_wrapper import Physt_Wrapper

# Protobuf
# from artemis.io.protobuf.artemis_pb2 import JobConfig as JobConfig_pb
# from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb
# from artemis.io.protobuf.artemis_pb2 import JobState as JobState_pb
import artemis.io.protobuf.artemis_pb2 as artemis_pb2

# Utils
from artemis.utils.utils import bytes_to_mb, range_positive
from google.protobuf import text_format
from artemis.decorators import timethis


@Logger.logged
class Artemis():

    def __init__(self, name, **kwargs):

        #######################################################################
        # Properties
        self.properties = Properties()
        self.jobops = JobProperties()

        # Set defaults if not configured
        self.jobops.meta.name = name
        self.jobops.meta.started.GetCurrentTime()
        self.jobops.meta.state = artemis_pb2.JOB_STARTING

        # Define the internal objects for Artemis
        self.steer = None
        self.generator = None
        self.data_handler = None
        self._raw = None
        self._schema = {}
        self._buffer_streams = {}

        # List of timer histos for easy access
        self.__timers = TimerSvc()

        # Temporary placeholders to managing event loop
        self.max_requests = 0
        self._requestcntr = 0
        self.processed = 0

        _defaults = self._set_defaults()
        # Override the defaults from the kwargs
        for key in kwargs:
            _defaults[key] = kwargs[key]

        for key in _defaults:
            self.properties.add_property(key, _defaults[key])
        #######################################################################

        #######################################################################
        # Logging
        Logger.configure(self, **kwargs)
        #######################################################################

        self._filehandler = FileHandler()
        self.max_requests = self.properties.max_requests

    def _set_defaults(self):
        # Temporary hard-coded values to get something running
        defaults = {'max_requests': 2}
        return defaults

    @property
    def datum(self):
        '''
        datum represents a raw input source
        file
        memoryview
        db source
        table ...
        gets updated in the event loop, so acts as a temporary datastore
        '''
        return self._raw

    @datum.setter
    def datum(self, raw):
        self._raw = raw

    def control(self):
        '''
        Stateful Job processing via pytransitions
        '''
        self.jobops.meta.state = artemis_pb2.JOB_RUNNING
        self._launch()

        # Configure Artemis job
        try:
            self._configure()
        except Exception as e:
            self.logger.error('Caught error in configure')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        try:
            self._lock()
        except Exception as e:
            self.logger.error('Caught error in lock')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        try:
            self._initialize()
        except Exception as e:
            self.logger.error('Caught error in initialize')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        # Book
        # Histograms
        # Timers
        try:
            self._book()
        except Exception as e:
            self.logger.error("Cannot book")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        # TODO
        # Add exception handling
        try:
            self._run()
        except Exception as e:
            self.logger.error("Unexcepted error caught in run")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        # TODO
        # Add exception handling
        self._finalize()

    def _launch(self):
        self.logger.info('Artemis is ready')

    def _configure(self):
        '''
        Configure global job dependencies
        such as DB connections
        Create the histogram store
        '''
        self.__logger.info('Configure')
        self.__logger.info('Job Properties %s',
                           pformat(self.properties.to_dict()))
        if hasattr(self.properties, 'protomsg'):
            _msgcfg = self.jobops.meta.config
            try:
                with open(self.properties.protomsg, 'rb') as f:
                    _msgcfg.ParseFromString(f.read())
            except IOError:
                self.__logger.error("Cannot read collections")
            except Exception:
                self.__logger.error('Cannot parse msg')
                raise
        else:
            self.__logger.error("Configuration not provided")
            raise AttributeError
        self.__logger.info(text_format.MessageToString(_msgcfg))

        # Set up histogram store
        self.hbook = Physt_Wrapper()
        self.__logger.info("Hbook reference count: %i",
                           sys.getrefcount(self.hbook))

        # Create Steering instance
        self.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)

        # Configure the generator
        try:
            self._gen_config()
        except Exception:
            self.__logger.error("Cannot configure generator")
            raise

    def _gen_config(self):
        self.__logger.info('Loading generator from protomsg')
        _msggen = self.jobops.meta.config.input.generator.config
        self.__logger.info(text_format.MessageToString(_msggen))
        try:
            self.generator = AlgoBase.from_msg(self.__logger, _msggen)
        except Exception:
            self.__logger.info("Failed to load generator from protomsg")
            raise
        try:
            self.generator.initialize()
        except Exception:
            self.__logger.error("Cannot initialize algo %s" % 'generator')
            raise
        self.__logger.debug("from_dict: instance {}".
                            format(self.generator.to_dict()))

        # Data Handler is just the generator function which returns a generator
        try:
            self.data_handler = self.generator.generate
        except TypeError:
            self.__logger.error("Cannot set generator")
            raise

    def _lock(self):
        '''
        Lock all properties before initialize
        '''
        # TODO
        # Exceptions?
        self.__logger.info("{}: Lock".format('artemis'))
        self.properties.lock = True
        self.jobops.meta.properties.CopyFrom(self.properties.to_msg())
        try:
            self.steer.lock()
        except Exception:
            self.__logger("cannot lock steering")
            raise

    def _initialize(self):
        self.__logger.info("{}: Initialize".format('artemis'))
        try:
            self.steer.initialize()
        except Exception:
            self.__logger.error('Cannot initialize Steering')
            raise

    def _book(self):
        self.__logger.info("{}: Book".format('artemis'))
        self.hbook.book('artemis', 'counts', range(10))
        bins = [x for x in range_positive(0., 10., 0.1)]

        # Payload and block distributions
        self.hbook.book('artemis', 'payload', bins, 'MB')
        self.hbook.book('artemis', 'nblocks', range(100), 'n')
        self.hbook.book('artemis', 'blocksize', bins, 'MB')

        # Timing plots
        bins = [x for x in range_positive(0., 1000., 2.)]
        self.hbook.book('artemis', 'time.prepblks', bins, 'ms')
        self.hbook.book('artemis', 'time.prepschema', bins, 'ms')
        self.hbook.book('artemis', 'time.execute', bins, 'ms')
        self.hbook.book('artemis', 'time.collect', bins, 'ms')

        # TODO
        # Think of better way to loop over list of timers
        self.__timers.book('artemis', 'prepblks')
        self.__timers.book('artemis', 'prepschema')
        self.__timers.book('artemis', 'execute')
        self.__timers.book('artemis', 'collect')

        try:
            self.steer.book()
        except Exception:
            self.__logger.error('Cannot book Steering')
            raise

    def _rebook(self):
        '''
        Rebook histograms for timers or profiles
        after random sampling of data chunk
        '''
        self.hbook.rebook_all(excludes=['artemis.time.prepblks',
                                        'artemis.time.prepschema'])
        try:
            self.steer.rebook()
        except Exception:
            raise

        _finfo = self.jobops.meta.data[-1]
        avg_, std_ = self.__timers.stats('artemis', 'execute')
        factor = 2 * len(_finfo.blocks)

        bins = [x for x in range_positive(0., avg_*factor, 2.)]
        self.hbook.rebook('artemis', 'time.execute', bins, 'ms')

    @timethis
    def _collect(self):
        '''
        Collect all batches from the leaves
        Occurs after single input source is chunked
        Each chunked converted to a batch
        Batches on leaves collected
        Input file -> Output Arrow RecordBatches
        '''
        _tree = Tree()  # Singleton!

        _use_buffer = True
        _use_osfile = False
        _use_parquet = False
        _use_csv = False
        _msgcfg = self.jobops.meta.config

        # Artemis can have multiple output formats
        # ideally, async io
        # each writer must have a dedicated to config
        try:
            for w in _msgcfg.writers:
                if w.HasField('csvwriter'):
                    self.__logger.info("Write to csv")
                    _use_csv = True
                elif w.HasField('osfilewriter'):
                    self.__logger.info("Write to recordbatch file")
                    _use_osfile = True
                elif w.HasField('parquetwriter'):
                    self.__logger.info("Write to parquet")
                    _use_parquet = True
                else:
                    self.__logger.info("No writers defined, default to buffer")
                    _use_buffer = True

        except Exception:
            self.__logger.warning('No writers defined')
            _use_buffer = True

        # TODO
        # Improve creation of output file names with metadata
        _fbasename = self.jobops.meta.name + \
            '_batches_' + str(self._requestcntr)

        for leaf in _tree.leaves:
            self.__logger.info("Leave node %s", leaf)
            node = _tree.get_node_by_key(leaf)
            els = node.payload
            self.__logger.info('Batches of leaf %s', len(els))
            if isinstance(els[-1].get_data(), pa.lib.RecordBatch):
                self.__logger.info("RecordBatch")
                self.__logger.info("Allocated %i", pa.total_allocated_bytes())
                _schema_batch = els[-1].get_data().schema
                # TODO
                # Get the pyarrow schema as early as possible
                # Store/retrieive from the metastore
                # Do not assume each file has same schema!!!!

                # TODO
                # Configure to use in-memory buffer
                # Output NativeFile
                # Output Parquet
                # Enforce schema check
                try:
                    self._filehandler.write(els,
                                            _fbasename,
                                            _schema_batch,
                                            _use_buffer,
                                            _use_osfile,
                                            _use_parquet,
                                            _use_csv)
                except Exception:
                    self.__logger.error("Error in writer")
                    raise

                try:
                    self._buffer_streams[node.key]._schema = _schema_batch
                    self._buffer_streams[node.key].write(els)
                except Exception:
                    self.__logger.error("Error in buffer writer")
                    raise

                self.__logger.info("Allocated after write %i",
                                   pa.total_allocated_bytes())
            else:
                self.__logger.info("%s", type(els[-1].get_data()))

        return True

    def _run(self):
        '''
        Event Loop
        '''
        self.__logger.info("artemis: Run")
        self.__logger.debug('artemis: Count at run call %s' %
                            str(self._requestcntr))
        self.__logger.info("artemis: Run: pyarrow malloc %i",
                           pa.total_allocated_bytes())
        _tree = Tree()  # Singleton!
        while (self._requestcntr < self.max_requests):
            self.__logger.info("artemis: request %i malloc %i",
                               self._requestcntr,
                               pa.total_allocated_bytes())
            self.hbook.fill('artemis', 'counts', self._requestcntr)
            try:
                self._prepare()
            except Exception:
                self.__logger.error("Failed data prep")
                raise
            # TODO
            # Make number of samples configurable
            for _ in range(10):
                try:
                    result_, time_ = self._execute_sampler()
                    self.__timers.fill('artemis', 'execute', time_)
                    self.__logger.info("Sampler execute time %2.2f", time_)
                except Exception:
                    self.__logger.error("Problem executing sample")
                    raise
            try:
                self._rebook()
            except Exception:
                self.__logger.error("Cannot rebook")
                raise

            self.__logger.info("artemis: sampleing complete malloc %i",
                               pa.total_allocated_bytes())

            # Configure the output data streams
            # Each stream associated with leaf in Tree
            # Store consistent Arrow Table for each process chain

            for leaf in _tree.leaves:
                self.__logger.info("Leave node %s", leaf)
                node = _tree.get_node_by_key(leaf)
                key = node.key
                _last = node.payload[-1].get_data()
                if isinstance(_last, pa.lib.RecordBatch):
                    self.__logger.info("RecordBatch")
                    self.__logger.info("Allocated %i",
                                       pa.total_allocated_bytes())
                    self._buffer_streams[key] = BufferOutputWriter(key)
                    self._buffer_streams[key]._fbasename = \
                        self.jobops.meta.name
                    self._buffer_streams[key]._schema = _last.schema
                    self._buffer_streams[key].initialize()

            try:
                Tree().flush()
            except Exception:
                self.__logger("Problem flushing")
                raise

            self.__logger.info("artemis: flush before execute %i",
                               pa.total_allocated_bytes())

            try:
                result_, time_ = self._execute()
            except Exception:
                self.__logger.error("Problem executing")
                raise

            self.__logger.info("artemis: execute complete malloc %i",
                               pa.total_allocated_bytes())
            self.hbook.fill('artemis', 'time.execute', time_)
            self.__logger.debug('Count after process_data %s' %
                                str(self._requestcntr))
            # TODO: Insert collect for datastore/nodes/tree.
            # TODO: Test memory release.
            try:
                result_, time_ = self._collect()
            except Exception:
                self.__logger.error("Problem collecting")
                raise
            self.hbook.fill('artemis', 'time.collect', time_)

            # Clear memory, tree, raw datum...
            self.datum = None
            try:
                Tree().flush()
            except Exception:
                self.__logger("Problem flushing")
                raise

    def _check_requests(self):
        self.__logger.info('Remaining data check. Status coming up.')
        return self._requestcntr < self.max_requests

    def _requests_count(self):
        self._requestcntr += 1

    def _request_data(self):
        try:
            raw, batch = next(self.data_handler())
        except Exception:
            self.__logger.debug("Iterator empty")
            raise
        # Add fileinfo to message
        # TODO
        # Create file UUID, check UUID when creating block info???
        _finfo = self.jobops.meta.data.add()
        _finfo.name = 'file_' + str(self._requestcntr)

        # Update the raw metadata
        _rinfo = _finfo.raw
        _rinfo.size_bytes = len(raw)

        # Update datum input count
        self._requests_count()

        # Return the raw bytes
        return raw

    def _request_block(self, file_, block_id, meta):
        '''
        Return a block of raw bytes for processing
        Access random blocks
        requies passing the meta (python), needs to moved to proto
        '''
        block = self.jobops.meta.data[-1].blocks[block_id]
        _chunk = bytearray(block.range.size_bytes)

        chunk = self._filehandler.readinto_block(file_,
                                                 _chunk,
                                                 block.range.offset_bytes,
                                                 meta)
        return chunk

    @timethis
    def _prepare_schema(self, file_):
        '''
        Strips the header information (if requested)

        Returns a python list for reading back chunk
        '''
        _finfo = self.jobops.meta.data[-1]
        header, meta, off_head = self._filehandler.strip_header(file_)
        _finfo.schema.size_bytes = off_head
        _finfo.schema.header = header
        for col in meta:
            a_col = _finfo.schema.columns.add()
            a_col.name = col
        return meta  # should be removed and accessed through metastore

    @timethis
    def _prepare_blocks(self, file_, offset):
        '''
        file_ is pyarrow PythonFile
        offset is header offset in bytes
        For each raw byte input
        define the block length and offset.
        Update the FileInfo msg for each block
        Need to place a check to ensure
        the correct FileInfo instance is used
        '''
        _parser = self.jobops.meta.config.parser
        if _parser.HasField("csvparser"):
            blocks = self._filehandler.\
                    get_blocks(file_,
                               _parser.csvparser.block_size,
                               bytes(_parser.csvparser.delimiter, 'utf8'),
                               _parser.csvparser.skip_header,
                               offset)

        else:
            self.__logger.error("Csv Parser not available")
            raise AttributeError

        # Prepare the block meta data
        # FileInfo should already be available
        # Get last FileInfo
        _finfo = self.jobops.meta.data[-1]
        for block in blocks:
            msg = _finfo.blocks.add()
            msg.range.offset_bytes = block[0]
            msg.range.size_bytes = block[1]
        return True

    def _prepare_datum(self):
        '''
        Prepare the input datum (file) for chunk processing
        '''
        # Request the data via the getter
        # TODO
        # Validate that we get the right data back!
        try:
            raw = self.datum
        except Exception:
            self.__logger.debug("Data generator completed file batches")
            raise

        # requestdata prepares the input and adds the FileInfo msg
        # Get the last in list
        _finfo = self.jobops.meta.data[-1]

        stream = io.BytesIO(raw)

        file_ = pa.PythonFile(stream, mode='r')

        # prepare the schema information for the file
        try:
            meta, time_ = self._prepare_schema(file_)
            self.hbook.fill('artemis', 'time.prepschema', time_)
        except Exception:
            self.__logger.error("Problem obtaining schema")
            raise

        # seek past header
        file_.seek(_finfo.schema.size_bytes)

        # Obtain the block information for the file
        try:
            results_, time_ = self._prepare_blocks(file_,
                                                   _finfo.schema.size_bytes)
            self.hbook.fill('artemis', 'time.prepblks', time_)
        except Exception:
            self.__logger.error("Unable to create blocks")
            raise

        # Monitoring
        self.hbook.fill('artemis', 'payload', bytes_to_mb(len(raw)))
        self.hbook.fill('artemis', 'nblocks', len(_finfo.blocks))

        self.__logger.info("Blocks")
        self.__logger.info("Size in bytes %2.3f in MB %2.3f" %
                           (len(raw), bytes_to_mb(len(raw))))

        _finfo.processed.size_bytes = _finfo.schema.size_bytes

        try:
            file_.close()
        except Exception:
            self.__logger.error("Problem closing file")
            raise
        try:
            stream.close()
        except Exception:
            self.__logger.error("Problem closing stream")
            raise

    def _prepare(self):
        '''
        Requests the input data from the data handler
        calls all data preparation methods
        '''
        try:
            self.datum = self._request_data()
        except Exception:
            raise

        try:
            self._prepare_datum()
        except Exception:
            self.__logger.error("failed to prepare the datum")
            raise

    @timethis
    def _execute_sampler(self):
        '''
        Random chunk sampling processing
        '''

        # Get the last in list
        _finfo = self.jobops.meta.data[-1]

        meta = []
        for column in _finfo.schema.columns:
            meta.append(column.name)

        stream = io.BytesIO(self.datum)

        file_ = pa.PythonFile(stream, mode='r')

        # Select a random block
        # TODO add a configurable seed
        # Use a single instance of random
        # should be configured at job start
        iblock = random.randint(0, len(_finfo.blocks) - 1)
        self.__logger.info("Selected random block %i with size %2.2f",
                           iblock, _finfo.blocks[iblock].range.size_bytes)

        try:
            chunk = self._request_block(file_, iblock, meta)
        except Exception:
            self.__logger.error("Error requesting block")
            raise
        try:
            self.steer.execute(chunk)  # Make chunk immutable
        except Exception:
            raise

        try:
            file_.close()
        except Exception:
            self.__logger.error("Problem closing file")
            raise
        try:
            stream.close()
        except Exception:
            self.__logger.error("Problem closing stream")
            raise
        return True

    @timethis
    def _execute(self):
        '''
        Execute called for each input datum (e.g. a file)
        File preprocessing
            obtain the file schema information
            scan the file and create byte blocks
            update all the metadata
        Block processing
            loop over all blocks from file input
            retrieve raw bytes from file
            pass raw data to steering to process block
        '''

        # requestdata prepares the input and adds the FileInfo msg
        # Get the last in list
        _finfo = self.jobops.meta.data[-1]

        meta = []
        for column in _finfo.schema.columns:
            meta.append(column.name)

        stream = io.BytesIO(self.datum)

        file_ = pa.PythonFile(stream, mode='r')

        # Execute steering over all blocks from raw input
        for i, block in enumerate(_finfo.blocks):
            self.hbook.fill('artemis', 'blocksize',
                            bytes_to_mb(block.range.size_bytes))
            try:
                chunk = self._request_block(file_, i, meta)
            except Exception:
                self.__logger.error("Error requesting block")
                raise
            try:
                self.steer.execute(chunk)  # Make chunk immutable
            except Exception:
                raise
            self.__logger.info("Chunk size %i, block size %i" %
                               (len(chunk), block.range.size_bytes))
            _finfo.processed.size_bytes += block.range.size_bytes
            self.processed += len(chunk)

        self.__logger.info('Processed %2.1f' % self.processed)

        if _finfo.processed.size_bytes != _finfo.raw.size_bytes:
            self.__logger.error("Processing payload not complete")
            raise IOError

        try:
            file_.close()
        except Exception:
            self.__logger.error("Problem closing file")
            raise
        try:
            stream.close()
        except Exception:
            self.__logger.error("Problem closing stream")
            raise
        return True

    def _finalize(self):
        self.__logger.info("Finalizing Artemis job %s" %
                           self.jobops.meta.name)
        summary = self.jobops.meta.summary
        try:
            self.steer.finalize()
        except Exception:
            self.__logger.error("Steer finalize fails")
            raise

        mu_payload = self.hbook.get_histogram('artemis', 'payload').mean()
        mu_blocksize = self.hbook.get_histogram('artemis', 'blocksize').mean()
        self.__logger.info("Mean payload %2.2f MB" % mu_payload)
        self.__logger.info("Mean blocksize %2.2f MB" % mu_blocksize)

        for key in self.__timers.keys:
            if 'artemis' not in key:
                continue
            name = key.split('.')[-1]
            mu = self.hbook.get_histogram('artemis', 'time.'+name).mean()
            std = self.hbook.get_histogram('artemis', 'time.'+name).std()
            self.__logger.info("%s timing: %2.4f" % (key, mu))

            # Add to the msg
            msgtime = summary.timers.add()
            msgtime.name = key
            msgtime.time = mu
            msgtime.std = std

        summary.collection.CopyFrom(self.hbook.to_message())
        jobinfoname = self.jobops.meta.name + '_meta.dat'
        self.jobops.meta.state = artemis_pb2.JOB_SUCCESS
        self.jobops.meta.finished.GetCurrentTime()
        try:
            with open(jobinfoname, "wb") as f:
                f.write(self.jobops.meta.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
        except Exception:
            raise

        for key in self._buffer_streams:
            try:
                self._buffer_streams[key]._finalize()
            except Exception:
                self.__logger.error("Finalize buffer stream fails %s", key)
                raise

        self.__logger.info("Processed file summary")
        for f in self.jobops.meta.data:
            self.__logger.info(text_format.MessageToString(f))

        self.__logger.info("Timer Summary")
        for t in self.jobops.meta.summary.timers:
            self.__logger.info(text_format.MessageToString(t))

    def abort(self, *args, **kwargs):
        self.jobops.meta.state = artemis_pb2.JOB_ABORT
        self.__logger.error("Artemis has been triggered to Abort")
        self.__logger.error("Reason %s" % args[0])
        self.jobops.meta.finished.GetCurrentTime()
        jobinfoname = self.jobname + '_meta.dat'
        try:
            with open(jobinfoname, "wb") as f:
                f.write(self.jobops.meta.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
        except Exception:
            raise
