#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
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
import os
import uuid
import pathlib

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
from artemis.io.filehandler import FileFactory

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
        self._jp = JobProperties()

        # Set defaults if not configured
        self._jp.meta.name = name
        self._jp.meta.job_id = str(uuid.uuid4())
        self._jp.meta.started.GetCurrentTime()
        self._update_state(artemis_pb2.JOB_STARTING)
        self._job_id = None
        self._path = ''

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

        # TODO
        # Improve consistency of setting path, filesnames, etc...
        self._set_job_id()

        # Required for setting the log file
        if 'jobname' not in kwargs.keys():
            kwargs['jobname'] = self._job_id

        for key in kwargs:
            self.properties.add_property(key, kwargs[key])
        #######################################################################
        self._set_path()
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

    @property
    def job_id(self):
        return self._job_id

    @datum.setter
    def datum(self, raw):
        self._raw = raw

    def control(self):
        '''
        Stateful Job processing via pytransitions
        '''
        self._jp.meta.state = artemis_pb2.JOB_RUNNING
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
        except Exception as e:
            self.logger.error('Caught error in sample_chunks')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        try:
            self._rebook()
        except Exception as e:
            self.logger.error('Caught error in rebook')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        try:
            self._init_buffers()
        except Exception as e:
            self.logger.error('Caught error in init_buffers')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        # Clear all memory and raw data
        try:
            Tree().flush()
            self.datum = None
        except Exception as e:
            self.logger.error('Caught error in Tree.flush')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

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
        except Exception as e:
            self.logger.error("Unexcepted error caught in finalize")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        try:
            self._finalize_buffer()
        except Exception as e:
            self.logger.error("Unexcepted error caught in finalizing errors")
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

    def _set_path(self, **kwargs):
        if hasattr(self.properties, 'path'):
            if os.path.exists(self.properties.path) is False:
                raise IOError
            self._path = os.path.abspath(self.properties.path)
        else:
            self._path = ''

    def _set_job_id(self):
        '''
        Creates unique basename for output data
        '''
        _bname = self._jp.meta.name
        _id = self._jp.meta.job_id
        self._job_id = _bname + '-' + _id

    def _update_state(self, state):
        self._jp.meta.state = state

    def _get_raw_size(self, raw):
        '''
        Given raw data payload
        determine size
        Supports
        bytes (when running simulation)
        pathlib.PosixPath (when obtaining files from FileGenerator)
        '''
        if isinstance(raw, bytes):
            return len(raw)
        elif isinstance(raw, pathlib.PosixPath):
            return os.path.getsize(raw)
        else:
            raise TypeError

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

        _summary = self._jp.meta.summary
        _summary.processed_bytes = 0
        _summary.processed_ndatums = 0

        _msgcfg = self._jp.meta.config
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
        self.__logger.info("Job ID %s", self._job_id)
        self.__logger.info("Job path %s", self._path)
        if hasattr(self.properties, 'protomsg'):
            _msgcfg = self._jp.meta.config
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
        _msggen = self._jp.meta.config.input.generator.config
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
        self._jp.meta.properties.CopyFrom(self.properties.to_msg())
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

        _finfo = self._jp.meta.data[-1]
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
        _summary = self._jp.meta.summary
        _summary.processed_bytes = 0
        _summary.processed_ndatums = 0
        del self._jp.meta.data[:]  # Remove all the fileinfo msgs

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
        _msgcfg = self._jp.meta.config
        _wrtcfg = None
        for toolcfg in _msgcfg.tools:
            if toolcfg.name == "bufferwriter":
                _wrtcfg = toolcfg

        try:
            for leaf in _tree.leaves:
                self.__logger.info("Leave node %s", leaf)
                node = _tree.get_node_by_key(leaf)
                key = node.key
                try:
                    _last = node.payload[-1].get_data()
                except IndexError:
                    self.__logger.error("Cannot retrieve payload! %s", key)
                    raise

                if isinstance(_last, pa.lib.RecordBatch):
                    _wrtcfg.name = "writer_" + key
                    self.__logger.info("Add Tool %s", _wrtcfg.name)
                    self.__tools.add(self.__logger, _wrtcfg)
                    self.__tools.get(_wrtcfg.name)._schema = _last.schema
                    self.__tools.get(_wrtcfg.name)._fbasename = self._job_id
                    self.__tools.get(_wrtcfg.name)._path = self._path
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
        self.__logger.info("Sample nchunks for preprocess profiling %i",
                           self._nchunk_samples)
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
                            self._jp.meta.summary.processed_ndatums)

        self.__logger.info("artemis: Run: pyarrow malloc %i",
                           pa.total_allocated_bytes())
        while True:
            self.__logger.info("artemis: request %i malloc %i",
                               self._jp.meta.summary.processed_ndatums,
                               pa.total_allocated_bytes())
            self.hbook.fill('artemis', 'counts',
                            self._jp.meta.summary.processed_ndatums)
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
        except ValueError:
            #  Occurs when receiving a 1tuple from generator
            try:
                raw = next(self.data_handler)
            except StopIteration:
                self.__logger.info("Request data: iterator complete")
                raise
        except TypeError:
            #  Occurs when receiving a file path
            try:
                raw = next(self.data_handler)
            except StopIteration:
                self.__logger.info("Request data: iterator complete")
                raise

        # Add fileinfo to message
        # TODO
        # Create file UUID, check UUID when creating block info???
        _finfo = self._jp.meta.data.add()
        _finfo.name = 'file_' + str(self._jp.meta.summary.processed_ndatums)

        # Update the raw metadata
        _rinfo = _finfo.raw
        try:
            _rinfo.size_bytes = self._get_raw_size(raw)
        except TypeError:
            self.__logger.warning("Cannot determine type from raw datum")
            _rinfo.size_bytes = 0

        # Update datum input count
        self._jp.meta.summary.processed_ndatums += 1

        # Return the raw bytes
        return raw

    def _request_block(self, file_, block_id, meta):
        '''
        Return a block of raw bytes for processing
        Access random blocks
        requies passing the meta (python), needs to moved to proto
        '''
        block = self._jp.meta.data[-1].blocks[block_id]
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
        #  TODO
        #  Improve file preparation for different input types
        _finfo = self._jp.meta.data[-1]
        try:
            header, meta, off_head = \
                self.__tools.get("filehandler").prepare(file_)
            _finfo.schema.size_bytes = off_head
            _finfo.schema.header = header
            for col in meta:
                a_col = _finfo.schema.columns.add()
                a_col.name = col
        except UnicodeDecodeError:
            self.__logger.warning("Input data type is not utf8")
            # Assume for now no header in file
            _finfo.schema.size_bytes = 0
            _finfo.schema.header = b''
            meta = self.__tools.get("legacytool").columns
            for col in meta:
                a_col = _finfo.schema.columns.add()
                a_col.name = col
        except Exception:
            self.__logger.error("Unknown error at file preparation")
            raise

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
        _finfo = self._jp.meta.data[-1]
        for i, block in enumerate(blocks):
            msg = _finfo.blocks.add()
            msg.range.offset_bytes = block[0]
            msg.range.size_bytes = block[1]
            self.__logger.debug(msg)
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
        _finfo = self._jp.meta.data[-1]

        stream = FileFactory(raw)

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
        try:
            _fsize = self._get_raw_size(raw)
        except TypeError:
            self.__logger.warning("Cannot determine type from raw datum")
            _fsize = 0

        self.hbook.fill('artemis', 'payload', bytes_to_mb(_fsize))
        self.hbook.fill('artemis', 'nblocks', len(_finfo.blocks))

        self.__logger.info("Blocks")
        self.__logger.info("Size in bytes %2.3f in MB %2.3f" %
                           (_fsize, bytes_to_mb(_fsize)))

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
        _finfo = self._jp.meta.data[-1]

        meta = []
        for column in _finfo.schema.columns:
            meta.append(column.name)

        stream = FileFactory(self.datum)

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
        _finfo = self._jp.meta.data[-1]

        meta = []
        for column in _finfo.schema.columns:
            meta.append(column.name)

        stream = FileFactory(self.datum)

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
            self._jp.meta.summary.processed_bytes += len(chunk)
            chunk = None

            # Now check whether to fill buffers and flush tree
            self._check_malloc()

        self.__logger.info('Processed %i' %
                           self._jp.meta.summary.processed_bytes)

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

    def _finalize_jobstate(self, state):
        self._update_state(state)
        self._jp.meta.finished.GetCurrentTime()
        duration = self._jp.meta.summary.job_time
        duration.seconds = self._jp.meta.finished.seconds -\
            self._jp.meta.started.seconds
        duration.nanos = self._jp.meta.finished.nanos -\
            self._jp.meta.started.nanos
        if duration.seconds < 0 and duration.nanos > 0:
            duration.seconds += 1
            duration.nanos -= 1000000000
        elif duration.seconds > 0 and duration.nanos < 0:
            duration.seconds -= 1
            duration.nanos += 1000000000

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
        summary = self._jp.meta.summary
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
                           self._jp.meta.name)
        summary = self._jp.meta.summary

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
        jobinfoname = self._job_id + '_meta.dat'
        if hasattr(self.properties, 'path'):
            jobinfoname = os.path.join(self._path, jobinfoname)
        self._finalize_jobstate(artemis_pb2.JOB_SUCCESS)

        try:
            with open(jobinfoname, "wb") as f:
                f.write(self._jp.meta.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
        except Exception:
            raise

        self.__logger.info("Job Summary")
        self.__logger.info("=================================")
        self.__logger.info("Job %s", self._jp.meta.name)
        self.__logger.info("Job id %s", self._jp.meta.job_id)
        self.__logger.info("Total job time %s",
                           text_format.MessageToString(
                                self._jp.meta.summary.job_time))
        self.__logger.info("Processed file summary")
        for f in self._jp.meta.data:
            self.__logger.info(text_format.MessageToString(f))

        self.__logger.info("Processed data summary")
        self.__logger.info("Mean payload %2.2f MB", mu_payload)
        self.__logger.info("Mean blocksize %2.2f MB", mu_blocksize)
        self.__logger.info("Total datums requested %i",
                           summary.processed_ndatums)
        self.__logger.info("Total bytes processed %i",
                           summary.processed_bytes)

        self.__logger.debug("Timer Summary")
        for t in self._jp.meta.summary.timers:
            self.__logger.debug("%s: %2.2f +/- %2.2f", t.name, t.time, t.std)
        self.__logger.info("This is a test of your greater survival")
        self.__logger.info("=================================")

    def abort(self, *args, **kwargs):
        self._jp.meta.state = artemis_pb2.JOB_ABORT
        self.__logger.error("Artemis has been triggered to Abort")
        self.__logger.error("Reason %s" % args[0])
        self._jp.meta.finished.GetCurrentTime()
        jobinfoname = self.jobname + '_meta.dat'
        try:
            with open(jobinfoname, "wb") as f:
                f.write(self._jp.meta.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
        except Exception:
            raise
