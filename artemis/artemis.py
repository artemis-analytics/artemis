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
from artemis.core.tool import ToolStore

# IO
from artemis.io.collector import Collector
from artemis.core.physt_wrapper import Physt_Wrapper

# Protobuf
import artemis.io.protobuf.artemis_pb2 as artemis_pb2

# Utils
from artemis.utils.utils import bytes_to_mb, range_positive
from google.protobuf import text_format
from artemis.decorators import timethis


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
        self.file_handler = None
        self.reader = None
        self.collector = None
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
            self.collector.initialize()
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
        self.collector = Collector('collector',
                                   max_malloc=self.MALLOC_MAX_SIZE,
                                   job_id=self._job_id,
                                   path=self._path,
                                   loglevel=Logger.CONFIGURED_LEVEL)

        # Configure the data handler
        _msggen = self._jp.meta.config.input.generator.config
        try:
            self.data_handler = AlgoBase.from_msg(self.__logger, _msggen)
        except Exception:
            self.__logger.info("Failed to load generator from protomsg")
            raise

        # Add tools
        for toolcfg in self._jp.meta.config.tools:
            self.__logger.info("Add Tool %s", toolcfg.name)
            self.__tools.add(self.__logger, toolcfg)

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

        try:
            self.data_handler.initialize()
        except Exception:
            self.__logger.error("Cannot initialize algo %s" % 'generator')
            raise

        for toolcfg in self._jp.meta.config.tools:
            if toolcfg.name == "bufferwriter":
                continue
            try:
                self.__tools.get(toolcfg.name).initialize()
            except Exception:
                self.__logger.error("Cannot initialize %s", toolcfg.name)

        self.file_handler = self.__tools.get("filehandler")

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

        self.__logger.info("artemis: allocated before reset %i",
                           pa.total_allocated_bytes())
        self.data_handler.reset()
        self.__logger.info("artemis: allocated after reset %i",
                           pa.total_allocated_bytes())

        # Reset all meta data needed for processing all job info
        _summary = self._jp.meta.summary
        _summary.processed_bytes = 0
        _summary.processed_ndatums = 0
        del self._jp.meta.data[:]  # Remove all the fileinfo msgs

    def _sample_chunks(self):
        '''
        Random sampling of chunks from a datum
        process a few to extract schema, check for errors, get
        timing profile
        '''
        self.__logger.info("Sample nchunks for preprocess profiling %i",
                           self._nchunk_samples)

        for datum in self.data_handler.sampler():
            if isinstance(datum, bytes):
                datum = pa.py_buffer(datum)

            try:
                self.reader = self.file_handler.execute(datum)
            except Exception:
                self.__logger.error("Failed to prepare file")
                raise

            try:
                self._prepare()
            except Exception:
                self.__logger.error("Failed data prep")
                raise

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
        for datum in self.data_handler:
            if isinstance(datum, bytes):
                datum = pa.py_buffer(datum)

            self.hbook.fill('artemis', 'counts',
                            self._jp.meta.summary.processed_ndatums)
            try:
                self.reader = self.file_handler.execute(datum)
            except Exception:
                self.__logger.error("Failed to prepare file")
                raise
            try:
                self._prepare()
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

    def _prepare(self):
        '''
        Requests the input data from the data handler
        calls all data preparation methods
        '''

        # Add fileinfo to message
        # TODO
        # Create file UUID, check UUID when creating block info???
        _finfo = self._jp.meta.data.add()
        _finfo.name = 'file_' + str(self._jp.meta.summary.processed_ndatums)

        # Update the raw metadata
        _finfo.raw.size_bytes = self.file_handler.size_bytes
        self.__logger.info("Payload size %i", _finfo.raw.size_bytes)

        # Update datum input count
        self._jp.meta.summary.processed_ndatums += 1

        _finfo.schema.size_bytes = self.file_handler.header_offset
        _finfo.schema.header = self.file_handler.header
        self.__logger.info("Updating meta data from handler")
        self.__logger.debug("File schema %s", self.file_handler.schema)
        if self.file_handler.schema is not None:
            for col in self.file_handler.schema:
                a_col = _finfo.schema.columns.add()
                a_col.name = col

        for i, block in enumerate(self.file_handler.blocks):
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
            self.collector.execute()

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
        self.__logger.info("Total job time %i seconds",
                           self._jp.meta.summary.job_time.seconds)
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

        try:
            self.collector.finalize()
        except Exception:
            self.__logger.error("Collector fails to finalize buffer")
            raise

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
