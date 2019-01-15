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
import sys
import io

# Externals
import pyarrow as pa

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
from artemis.core.datastore import ArrowSets
from artemis.core.tool import ToolStore

# IO
from artemis.core.physt_wrapper import Physt_Wrapper

# Protobuf
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

        # Define internal properties
        self.MALLOC_MAX_SIZE = 0
        self._ndatum_samples = 0
        self._nchunk_samples = 0

        # List of timer histos for easy access
        self.__timers = TimerSvc()
        self.__tools = ToolStore()

        for key in kwargs:
            self.properties.add_property(key, kwargs[key])
        #######################################################################

        #######################################################################
        # Logging
        Logger.configure(self, **kwargs)
        #######################################################################

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

        # In order to set the output buffer schema
        # we need to get a schema first
        # In order to set the timing histograms, we need to profile
        # sample first
        # Artemis should always run in a sampling mode first
        # Make number of samples configurable
        try:
            self._sample_chunks()
        except Exception:
            raise

        try:
            self._rebook()
        except Exception:
            self.__logger.error("Cannot rebook")
            raise

        try:
            self._init_buffers()
        except Exception:
            raise
        # Clear all memory and raw data
        try:
            Tree().flush()
            self.datum = None
        except Exception:
            self.__logger("Problem flushing")
            raise

        self.__logger.info("artemis: sampleing complete malloc %i",
                           pa.total_allocated_bytes())
        try:
            self._run()
        except Exception as e:
            self.logger.error("Unexcepted error caught in run")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        try:
            self._finalize()
        except Exception:
            raise

        try:
            self._finalize_buffer()
        except Exception:
            self.__logger.error("Error finalizing buffers")
            raise

    def _launch(self):
        self.logger.info('Artemis is ready')

    def _set_defaults(self):
        # Default values that are part of the metadata
        # Values not set in metadata are updated here
        # Hard-coding of default job parameters
        #
        # Summary information such as counters are initialized
        # Summary information must be reset if rebook called
        # Maximum memory allocation in Arrow to trigger flush
        # Sampler settings

        _summary = self.jobops.meta.summary
        _summary.processed_bytes = 0
        _summary.processed_ndatums = 0

        _msgcfg = self.jobops.meta.config
        if _msgcfg.max_malloc_size_bytes:
            self.MALLOC_MAX_SIZE = _msgcfg.max_malloc_size_bytes
        else:
            self.__logger.info("Setting default memory allocation 2GB")
            self.MALLOC_MAX_SIZE = 2147483648
        if _msgcfg.HasField('sampler'):
            if _msgcfg.sampler.ndatums:
                self._ndatum_samples = _msgcfg.sampler.ndatums
            else:
                self._ndatum_samples = 1
            if _msgcfg.sampler.nchunks:
                self._nchunk_samples = _msgcfg.sampler.nchunks
            else:
                self._nchunk_samples = 10
        else:
            self._ndatum_samples = 1
            self._nchunk_samples = 10

    def _configure(self):
        '''
        Configure global job dependencies
        such as DB connections
        Create the histogram store
        '''
        self.__logger.info('Configure')
        self.__logger.info("%s properties: %s",
                           self.__class__.__name__,
                           self.properties)
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

        try:
            self._set_defaults()
        except ValueError:
            self.__logger.info(text_format.MessageToString(_msgcfg))
            raise
        except Exception:
            raise

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

        # Add tools
        for toolcfg in _msgcfg.tools:
            self.__logger.info("Add Tool %s", toolcfg.name)
            self.__tools.add(self.__logger, toolcfg)
            if toolcfg.name == 'filehandler':
                self.__tools.get('filehandler').initialize()

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

        # Data Handler is just the generator function which returns a generator
        try:
            self.data_handler = self.generator.generate()
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
        self.__logger.info("Rebook")
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

        if self.generator:
            self.generator.num_batches = self.generator.properties.nbatches
            self.data_handler = self.generator.generate()
            self.__logger.info("Expected batches to generate %i",
                               self.generator.num_batches)

        # Reset all meta data needed for processing all job info
        _summary = self.jobops.meta.summary
        _summary.processed_bytes = 0
        _summary.processed_ndatums = 0
        del self.jobops.meta.data[:]  # Remove all the fileinfo msgs

    def _check_malloc(self):
        '''
        Check total allocated memory in Arrow
        and call collect
        Collect does not ensure the file flushed
        Tuning on total allocated memory and the max output buffer
        size before spill
        '''
        if pa.total_allocated_bytes() > self.MALLOC_MAX_SIZE:
            # TODO: Insert collect for datastore/nodes/tree.
            # TODO: Test memory release.
            # TODO: Add histogram for number of forced collects
            self.__logger.info("COLLECT: Total memory reached")
            try:
                result_, time_ = self._collect()
            except Exception:
                self.__logger.error("Problem collecting")
                raise
            self.__logger.info("Allocated %i", pa.total_allocated_bytes())
            self.hbook.fill('artemis', 'time.collect', time_)

    @timethis
    def _collect(self):
        '''
        Collect all batches from the leaves
        Occurs after single input source is chunked
        Each chunked converted to a batch
        Batches on leaves collected
        Input file -> Output Arrow RecordBatches
        '''
        self.__logger.info("artemis: collect: pyarrow malloc %i",
                           pa.total_allocated_bytes())
        _tree = Tree()  # Singleton!

        self.__logger.info("Leaves %s", _tree.leaves)
        for leaf in _tree.leaves:
            self.__logger.info("Leaf node %s", leaf)
            node = _tree.get_node_by_key(leaf)
            els = node.payload
            self.__logger.info('Batches of leaf %s', len(els))
            _name = "writer_"+node.key
            if isinstance(els[-1].get_data(), pa.lib.RecordBatch):
                self.__logger.info("RecordBatch")
                self.__logger.info("Allocated %i", pa.total_allocated_bytes())
                _schema_batch = els[-1].get_data().schema
                # TODO
                # Get the pyarrow schema as early as possible
                # Store/retrieive from the metastore
                # Do not assume each file has same schema!!!!
                try:
                    self.__tools.get("writer_"+node.key)._schema = \
                        _schema_batch
                    self.__tools.get("writer_"+node.key).write(els)
                    self.__logger.info("Records %i Batches %i",
                                       self.__tools.get(_name)._nrecords,
                                       self.__tools.get(_name)._nbatches)
                except Exception:
                    self.__logger.error("Error in buffer writer")
                    raise
            else:
                self.__logger.info("%s", type(els[-1].get_data()))

        # Batches serialized, clear the tree to flush memory
        try:
            Tree().flush()
        except Exception:
            self.__logger("Problem flushing")
            raise

        self.__logger.info("Allocated after write %i",
                           pa.total_allocated_bytes())
        self.__logger.info
        return True

    def _init_buffers(self):
        '''
        Create new buffer output stream and writers
        '''
        self.__logger.info("artemis: _init_buffers")
        # Configure the output data streams
        # Each stream associated with leaf in Tree
        # Store consistent Arrow Table for each process chain
        # Currently the tree does not enforce a leaf to write out
        # record batches, so we could get spurious output buffers

        # Buffer stream requires a fixed pyArrow schema!
        _tree = Tree()  # Singleton!
        _msgcfg = self.jobops.meta.config
        _wrtcfg = None
        for toolcfg in _msgcfg.tools:
            if toolcfg.name == "bufferwriter":
                _wrtcfg = toolcfg

        try:
            for leaf in _tree.leaves:
                self.__logger.info("Leave node %s", leaf)
                node = _tree.get_node_by_key(leaf)
                key = node.key
                _last = node.payload[-1].get_data()
                if isinstance(_last, pa.lib.RecordBatch):
                    _wrtcfg.name = "writer_" + key
                    self.__logger.info("Add Tool %s", _wrtcfg.name)
                    self.__tools.add(self.__logger, _wrtcfg)
                    self.__tools.get(_wrtcfg.name)._schema = _last.schema
                    self.__tools.get(_wrtcfg.name)._fbasename = \
                        self.jobops.meta.name
                    self.__tools.get(_wrtcfg.name).initialize()
        except Exception:
            self.__logger.error("Problem creating output streams")
            raise

        # Batches serialized, clear the tree
        try:
            Tree().flush()
        except Exception:
            self.__logger("Problem flushing")
            raise

    def _sample_chunks(self):
        '''
        Random sampling of chunks from a datum
        process a few to extract schema, check for errors, get
        timing profile
        '''
        self.__logger.info("Sample chunks for preprocess profiling")
        try:
            self._prepare()
        except Exception:
            self.__logger.error("Failed data prep")
            raise

        for _ in range(self._nchunk_samples):
            try:
                result_, time_ = self._execute_sampler()
                self.__timers.fill('artemis', 'execute', time_)
                self.__logger.debug("Sampler execute time %2.2f", time_)
            except Exception:
                self.__logger.error("Problem executing sample")
                raise

    def _run(self):
        '''
        Event Loop
        Prepare an input datum
        Process each chunk, passing to Steering
        After all chunks processed, collect to output buffers
        Clear all data, move to next datum
        '''
        self.__logger.info("artemis: Run")
        self.__logger.debug('artemis: Count at run call %i',
                            self.jobops.meta.summary.processed_ndatums)

        self.__logger.info("artemis: Run: pyarrow malloc %i",
                           pa.total_allocated_bytes())
        while True:
            self.__logger.info("artemis: request %i malloc %i",
                               self.jobops.meta.summary.processed_ndatums,
                               pa.total_allocated_bytes())
            self.hbook.fill('artemis', 'counts',
                            self.jobops.meta.summary.processed_ndatums)
            try:
                self._prepare()
            except StopIteration:
                self.__logger.info("Quit run")
                self.datum = None
                break
            except Exception:
                self.__logger.error("Failed data prep")
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

            # Reset the raw datum, processing complete
            self.datum = None

    def _request_data(self):
        try:
            raw, batch = next(self.data_handler)
        except StopIteration:
            self.__logger.info("Request data: iterator complete")
            raise
        except Exception:
            self.__logger.info("Iterator empty")
            raise

        # Add fileinfo to message
        # TODO
        # Create file UUID, check UUID when creating block info???
        _finfo = self.jobops.meta.data.add()
        _finfo.name = 'file_' + str(self.jobops.meta.summary.processed_ndatums)

        # Update the raw metadata
        _rinfo = _finfo.raw
        _rinfo.size_bytes = len(raw)

        # Update datum input count
        self.jobops.meta.summary.processed_ndatums += 1

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

        chunk = self.__tools.get("filehandler").\
            readinto_block(file_,
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
        header, meta, off_head = \
            self.__tools.get("filehandler").prepare(file_)
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
        blocks = self.__tools.get("filehandler").execute(file_)

        # Prepare the block meta data
        # FileInfo should already be available
        # Get last FileInfo
        _finfo = self.jobops.meta.data[-1]
        for i, block in enumerate(blocks):
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
        except StopIteration:
            self.__logger.info("Processing complete: StopIteration")
            raise
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
        if self.generator:
            iblock = self.generator.\
                random_state.randint(0, len(_finfo.blocks) - 1)
            self.__logger.debug("Selected random block %i with size %2.2f",
                                iblock,
                                _finfo.blocks[iblock].range.size_bytes)
        else:
            self.__logger.error("Generator not configured, abort sampling")
            raise ValueError

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
            self.__logger.debug("Chunk size %i, block size %i",
                                len(chunk),
                                block.range.size_bytes)
            _finfo.processed.size_bytes += block.range.size_bytes
            self.jobops.meta.summary.processed_bytes += len(chunk)
            chunk = None

            # Now check whether to fill buffers and flush tree
            self._check_malloc()

        self.__logger.info('Processed %i' %
                           self.jobops.meta.summary.processed_bytes)

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

    def _finalize_buffer(self):
        '''
        Ensure the data store is empty
        Spill any remaining arrow buffers to disk
        '''
        # Ensure all data has been sent to buffer
        _store = ArrowSets()
        if _store.is_empty() is False:
            self.__logger.info("Collecting remaining data")
            try:
                result_, time_ = self._collect()
            except Exception:
                self.__logger.error("Problem collecting")
                raise
            self.__logger.info("Allocated %i", pa.total_allocated_bytes())
            self.hbook.fill('artemis', 'time.collect', time_)
        # Spill any remaining buffers to disk
        # Set the output file metadata
        summary = self.jobops.meta.summary
        _wnames = []
        for leaf in Tree().leaves:
            self.__logger.info("Leave node %s", leaf)
            node = Tree().get_node_by_key(leaf)
            key = node.key
            _wnames.append("writer_" + node.key)

        for key in _wnames:
            try:
                writer = self.__tools.get(key)
            except KeyError:
                continue
            try:
                writer._finalize()
            except Exception:
                self.__logger.error("Finalize buffer stream fails %s", key)
                raise

            self.__logger.info("File summary statistics")
            for table in writer._finfo:
                tableinfo = summary.tables.add()
                tableinfo.CopyFrom(table)
                self.__logger.info(text_format.MessageToString(tableinfo))

            self.__logger.info("Dataset summary statistics")
            self.__logger.info("%s Records: %i Batches: %i Files: %i",
                               writer.name,
                               writer.total_records,
                               writer.total_batches,
                               writer.total_files)

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

        for key in self.__timers.keys:
            if 'artemis' not in key:
                continue
            name = key.split('.')[-1]
            mu = self.hbook.get_histogram('artemis', 'time.'+name).mean()
            std = self.hbook.get_histogram('artemis', 'time.'+name).std()

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

        self.__logger.info("Job Summary")
        self.__logger.info("=================================")
        self.__logger.info("Processed file summary")
        for f in self.jobops.meta.data:
            self.__logger.info(text_format.MessageToString(f))

        self.__logger.info("Processed data summary")
        self.__logger.info("Mean payload %2.2f MB", mu_payload)
        self.__logger.info("Mean blocksize %2.2f MB", mu_blocksize)
        self.__logger.info("Total datums requested %i",
                           summary.processed_ndatums)
        self.__logger.info("Total bytes processed %i",
                           summary.processed_bytes)

        self.__logger.info("Timer Summary")
        for t in self.jobops.meta.summary.timers:
            self.__logger.info("%s: %2.2f +/- %2.2f", t.name, t.time, t.std)
        self.__logger.info("=================================")

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
