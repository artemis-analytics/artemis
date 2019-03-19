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
import pathlib
import traceback

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

# Generators
from artemis.generators.filegen import FileGenerator
from artemis.generators.csvgen import GenCsvLikeArrow
from artemis.generators.legacygen import GenMF


class ArtemisFactory:
    '''
    Factory class
    Update the configuration message from JobInfo message
    Allows for configuration to be static or predefined
    Reused for new datasets with different input and output data repos

    Requires updating
    bufferwriter tool with output repo path
    file generator tool with input repo path
    also replace a data generator with a file generator
    '''
    def __new__(cls, jobinfo, loglevel='INFO'):
        dirpath = jobinfo.output.repo

        if jobinfo.HasField('input'):
            if jobinfo.input.HasField('atom'):
                inpath = jobinfo.input.atom.repo
                glob = jobinfo.input.atom.glob
                generator = jobinfo.config.input.generator
                if generator.config.klass == 'FileGenerator':
                    for p in generator.config.properties.property:
                        if p.name == 'path':
                            p.value = inpath
                        if p.name == 'glob':
                            p.value = glob

        for tool in jobinfo.config.tools:
            if tool.name == 'bufferwriter':
                for p in tool.properties.property:
                    if p.name == 'path':
                        p.value = dirpath

        return Artemis(jobinfo, loglevel=loglevel)


@Logger.logged
class Artemis():

    def __init__(self, jobinfo, **kwargs):
        self.properties = Properties()
        self._jp = JobProperties()
        self._jp.meta.CopyFrom(jobinfo)

        # TODO
        # Validate the metadata
        #
        self._job_id = self._jp.meta.name + '-' + self._jp.meta.job_id

        # Logging
        Logger.configure(self,
                         jobname=self._job_id,
                         path=self._jp.meta.output.repo,
                         loglevel=kwargs['loglevel'])
        #######################################################################
        # Initialize summary info in meta data
        self._jp.meta.started.GetCurrentTime()
        self._update_state(artemis_pb2.JOB_STARTING)
        self._jp.meta.summary.processed_bytes = 0
        self._jp.meta.summary.processed_ndatums = 0

        # Define internal properties from job configuration
        self._path = self._jp.meta.output.repo
        self.MALLOC_MAX_SIZE = self._jp.meta.config.max_malloc_size_bytes
        self._ndatum_samples = self._jp.meta.config.sampler.ndatums
        self._nchunk_samples = self._jp.meta.config.sampler.nchunks

        # Define the internal objects for Artemis
        self.steer = None
        self.generator = None
        self.data_handler = None
        self.reader = None
        self._raw = None
        self._schema = {}

        # List of timer histos for easy access
        self.__timers = TimerSvc()
        self.__tools = ToolStore()

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
        if isinstance(raw, pa.lib.Buffer):
            return raw.size
        elif isinstance(raw, pathlib.PosixPath):
            return os.path.getsize(raw)
        else:
            raise TypeError

    def _launch(self):
        self.logger.info('Artemis is ready')

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
        if hasattr(self._jp.meta, 'config') is False:
            self.__logger.error("Configuration not provided")
            raise AttributeError

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
        for toolcfg in self._jp.meta.config.tools:
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

                # TODO
                # Properly configure the properties in the job config
                # This is a workaround which overwrites any set properties
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
        self.__logger.info("Generator type %s", type(self.generator))
        if isinstance(self.generator, GenCsvLikeArrow):
            self.__logger.debug("Expect bytes from GenCsvLikeArrow")
            try:
                data, batch = next(self.data_handler)
                raw = pa.py_buffer(data)
            except StopIteration:
                self.__logger.info("Request data: iterator complete")
                raise
        elif isinstance(self.generator, GenMF):
            #  Occurs when receiving a 1tuple from generator
            self.__logger.debug("expect bytes from GenMF")
            try:
                data = next(self.data_handler)
                raw = pa.py_buffer(data)
            except StopIteration:
                self.__logger.info("Request data: iterator complete")
                raise
        elif isinstance(self.generator, FileGenerator):
            self.__logger.info("Expect filepath")
            try:
                raw = next(self.data_handler)
            except StopIteration:
                self.__logger.info("Request data: iterator complete")
                raise

        else:
            self.__logger.error("Unknown data handler type %s",
                                type(self.generator))
            raise TypeError


        # Return the raw bytes
        return raw
    
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
            self.__logger.error("Failed to request data")
            raise

        handler = self.__tools.get("filehandler")
        try:
            self.reader = handler.execute(self.datum)
        except Exception:
            self.__logger.error("Failed to prepare file")
            raise
        
        # Add fileinfo to message
        # TODO
        # Create file UUID, check UUID when creating block info???
        _finfo = self._jp.meta.data.add()
        _finfo.name = 'file_' + str(self._jp.meta.summary.processed_ndatums)

        # Update the raw metadata
        _rinfo = _finfo.raw
        try:
            _rinfo.size_bytes = self._get_raw_size(self.datum)
        except TypeError:
            self.__logger.warning("Cannot determine type from raw datum")
            _rinfo.size_bytes = 0
        self.__logger.info("Payload size %i", _rinfo.size_bytes)
        # Update datum input count
        self._jp.meta.summary.processed_ndatums += 1
           
        _finfo.schema.size_bytes = handler.header_offset
        _finfo.schema.header = handler.header
        self.__logger.info("Updating meta data from handler")
        if handler.schema is not None:
            for col in handler.schema:
                a_col = _finfo.schema.columns.add()
                a_col.name = col
        
        for i, block in enumerate(handler.blocks):
            msg = _finfo.blocks.add()
            msg.range.offset_bytes = block[0]
            msg.range.size_bytes = block[1]
            self.__logger.debug(msg)

    @timethis
    def _execute_sampler(self):
        '''
        Random chunk sampling processing
        ''' 

        for batch in self.reader.sampler():
            self.steer.execute(batch)

    @timethis
    def _execute(self):
        '''
        '''
        _finfo = self._jp.meta.data[-1]
        for batch in self.reader:
            self.hbook.fill('artemis', 'blocksize',
                            bytes_to_mb(batch.size))
            try:
                self.steer.execute(batch)
            except Exception:
                raise
            _finfo.processed.size_bytes += batch.size 
            self._jp.meta.summary.processed_bytes += batch.size 
            self._check_malloc()
        
        self.__logger.info('Processed %i' %
                           self._jp.meta.summary.processed_bytes)
        
        # Need to account for header in batch size
        # if _finfo.processed.size_bytes != _finfo.raw.size_bytes:
        #     self.__logger.error("Processing payload not complete")
        #    raise IOError




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

    def _flush_buffer(self):
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
                writer.flush()
            except Exception:
                self.__logger.error("Flush buffer stream fails %s", key)
                raise

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
        try:
            self._finalize_buffer()
        except Exception:
            self.__logger.error("Cannot finalize buffer")
            try:
                self._flush_buffer()
            except Exception as e:
                error_message = traceback.format_exc()
                self.__logger.error("Flush fails. Reason: %s", e)
                self.__logger.error(error_message)

        self._jp.meta.finished.GetCurrentTime()
        jobinfoname = self._job_id + '_meta.dat'
        try:
            with open(jobinfoname, "wb") as f:
                f.write(self._jp.meta.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
        except Exception as e:
            error_message = traceback.format_exc()
            self.__logger.error("Meta data write fails. Reason: %s", e)
            self.__logger.error(error_message)
