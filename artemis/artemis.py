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
import logging
from pprint import pformat
import sys
import importlib
import json
from collections import OrderedDict
import io

# Externals
import pyarrow as pa

# Framework
from artemis.logger import Logger
from artemis.exceptions import NullDataError

# Core
from artemis.core.properties import Properties
from artemis.core.properties import JobProperties
from artemis.core.steering import Steering
from artemis.core.tree import Tree
from artemis.core.algo import AlgoBase

# Data generators
from artemis.generators.generators import GenCsvLikeArrow

# IO
from artemis.io.reader import FileHandler
from artemis.core.physt_wrapper import Physt_Wrapper

# Protobuf
from artemis.io.protobuf.artemis_pb2 import JobConfig as JobConfig_pb
from artemis.io.protobuf.artemis_pb2 import JobInfo as JobInfo_pb

# Utils
from artemis.utils.utils import bytes_to_mb, range_positive


@Logger.logged
class Artemis():

    def __init__(self, name, **kwargs):

        # Set defaults if not configured
        self.jobname = name
        self.jobinfo = JobInfo_pb()
        self.jobinfo.name = name
        self.jobinfo.started.GetCurrentTime()

        # Set jobname to name of artemis instance
        # Update kwargs
        if 'jobname' not in kwargs:
            kwargs['jobname'] = name

        # Set menu configuration file to default testmenu.json
        # Update kwargs
        if 'menu' not in kwargs:
            kwargs['menu'] = 'testmenu.json'

        # Set the output global job configuration file
        self.meta_filename = self.jobname + '_meta.json'

        self.steer = None
        self._menu = None
        self._meta_dict = None

        self.generator = None
        self.data_handler = None

        if 'generator' not in kwargs:
            gencfg = self.jobname + '_gencfg.json'
            self.generator = GenCsvLikeArrow('generator',
                                             nbatches=1,
                                             num_cols=20,
                                             num_rows=10000)
            try:
                with open(gencfg, 'x') as ofile:
                    json.dump(self.generator.to_dict(), ofile, indent=4)
            except Exception:
                self.__logger.error("Cannot dump the generator config")

            self.generator = None
            kwargs['generator'] = gencfg

        #######################################################################
        # Properties
        self.properties = Properties()
        self.jobops = JobProperties()

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

        # Temporary placeholders to managing event loop
        self.num_chunks = self.properties.num_chunks
        self.max_requests = self.properties.max_requests
        self._chunkcntr = 0
        self._requestcntr = 0
        self.processed = 0
        self.data = None
        self.payload = None

    @property
    def jobname(self):
        return self._jobname

    @jobname.setter
    def jobname(self, value):
        self._jobname = value

    @property
    def menu(self):
        '''
        Menu is a collection of dictionaries
        Execution graph according to output node
        Parent-children node relationships
        Properties of algorithms
        '''
        return self._menu

    @menu.setter
    def menu(self, config):
        self._menu = config

    def _set_defaults(self):
        # Temporary hard-coded values to get something running
        defaults = {'num_chunks': 2,
                    'max_requests': 2,
                    'blocksize': 2**27,
                    'delimiter': '\r\n',
                    'skip_header': False,
                    'offset_header': None}
        return defaults

    def control(self):
        '''
        Stateful Job processing via pytransitions
        '''
        self._launch()

        # Configure Artemis job
        try:
            self._configure()
        except Exception as e:
            self.logger.error('Caught error in configure')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        # Initialize all algorithms
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
        self._lock()
        # TODO
        # Add exception handling
        self._to_dict()
        try:
            self._to_json()
        except Exception as e:
            self.logger.error('Caught error in initialize')
            self.__logger.error("Reason: %s" % e)
            self.abort(e)
            return False

        # TODO
        # Add exception handling
        self._run()

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
        self.hbook = Physt_Wrapper()
        self.__logger.info("Hbook reference count: %i",
                           sys.getrefcount(self.hbook))
        self.__logger.info('Job Properties')
        self.__logger.info('Properties %s', pformat(self.properties.to_dict()))

        # Obtain the menu configuration
        try:
            with open(self.properties.menu, 'r') as ifile:
                self.menu = json.load(ifile, object_pairs_hook=OrderedDict)
        except IOError as e:
            self.__logger.error("Cannot open file: %s", self.properties.menu)
            self.__logger.error('I/O({0}: {1})'.format(e.errno, e.strerror))
            # Propagate the expection up to Artemis::control()
            raise
        except Exception as e:
            self.__logger.error("Unknow expection")
            self.__logger.error("Reason: %s" % e)
            # Propagate the expection up to Artemis::control()
            raise

        if not self.menu:
            self.__logger.error("Menu dictionary is null")
            raise NullDataError('Null menu')
        elif(logging.getLogger().isEnabledFor(logging.DEBUG) or
             self.__logger.isEnabledFor(logging.DEBUG)):
            self.__logger.debug(pformat(self.menu))

        # Fill the JobProperties
        self.jobops.data['job'] = self.properties.to_dict()
        self.jobops.data['menu'] = self.menu

        if hasattr(self.properties, 'protomsg'):
            try:
                self.jobops.data['protomsg'] = JobConfig_pb()
            except Exception:
                self.__logger.error('Cannot create JobConfig msg')
            try:
                with open(self.properties.protomsg, 'rb') as f:
                    self.jobops.data['protomsg'].\
                            ParseFromString(f.read())
            except IOError:
                self.__logger.error("Cannot read collections")
            except Exception:
                self.__logger.error('Cannot parse msg')
                raise
            # Add the menu message back to jobops men
            try:
                self.jobops.data['menu']['protomsg'] = \
                        self.jobops.data['protomsg'].menu
            except Exception:
                self.__logger.error('Cannot set menu msg')

        self.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)

        # Configure the generator
        try:
            self._gen_config()
        except Exception:
            self.__logger.error("Cannot configure generator")
            raise

        return True

    def _gen_config(self):
        self.__logger.info('Load the generator')
        if hasattr(self.properties, 'protomsg'):
            self.__logger.info('Loading generator from protomsg')
            _msggen = self.jobops.data['protomsg'].input.generator.config
            try:
                self.generator = AlgoBase.from_msg(self.__logger, _msggen)
            except Exception:
                self.__logger.info("Failed to load generator from protomsg")
                raise
        else:
            self.__logger.info('Loading from json')
            try:
                with open(self.properties.generator, 'r') as ifile:
                    gencfg = json.load(ifile, object_pairs_hook=OrderedDict)
            except IOError as e:
                self.__logger.error("Cannot open file: %s",
                                    self.properties.generator)
                self.__logger.error('I/O({0}: {1})'.format(e.errno,
                                                           e.strerror))
                # Propagate the expection up to Artemis::control()
                raise
            except Exception as e:
                self.__logger.error("Unknow expection")
                self.__logger.error("Reason: %s" % e)
                # Propagate the expection up to Artemis::control()
                raise
            try:
                self.generator = AlgoBase.load(self.__logger, **gencfg)
            except Exception:
                self.__logger.error('Error loading the generator')
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
        # Add the generator to the jobproperties
        # self.jobops.data['generator'] = gencfg

    def _initialize(self):
        self.__logger.info("{}: Initialize".format('artemis'))
        try:
            self.steer.initialize()
        except Exception:
            self.__logger.error('Cannot initialize Steering')
            raise
        self.jobops.data['steer'] = self.steer.to_dict()

    def _lock(self):
        '''
        Lock all properties before initialize
        '''
        # TODO
        # Exceptions?
        self.__logger.info("{}: Lock".format('artemis'))
        self.properties.lock = True
        self.steer.lock()

    def _book(self):
        self.__logger.info("{}: Book".format('artemis'))
        self.hbook.book('artemis', 'counts', range(10))
        bins = [x for x in range_positive(0., 10., 0.1)]
        self.hbook.book('artemis', 'payload', bins, 'MB')
        self.hbook.book('artemis', 'nblocks', range(100), 'n')
        self.hbook.book('artemis', 'blocksize', bins, 'MB')
        self.steer.book()

    def _set_meta_environment(self, meta_filename):
        self.meta_filename = meta_filename
        # print(event.kwargs)
        # self.meta_filename = event.kwargs.get('meta_filename', None)

    def _to_dict(self):
        '''
        Dictionary of job configuration
        '''
        # TODO
        # Exceptions?
        self._meta_dict = OrderedDict()
        self._meta_dict['job'] = self.properties.to_dict()
        # Menu is already stored as OrderedDict
        self._meta_dict['menu'] = self.menu
        self._meta_dict['steer'] = self.steer.to_dict()

        self.__logger.debug(pformat(self._meta_dict))

    def _to_json(self):
        if 'protomsg' in self._meta_dict['menu']:
            del self._meta_dict['menu']['protomsg']
        try:
            with open(self.meta_filename, 'x') as ofile:
                json.dump(self._meta_dict, ofile, indent=4)
        except IOError as e:
            self.__logger.error('I/O Error({0}: {1})'.
                                format(e.errno, e.strerror))
            raise
        except TypeError:
            self.__logger.error('TypeError: %s', pformat(self._meta_dict))
            raise
        except Exception:
            self.__logger.error('Unknown error')
            raise

    def parse_from_json(self, filename):
        with open(filename, 'r') as ifile:
            data = json.load(ifile, object_pairs_hook=OrderedDict)
        if data:
            self.from_dict(data)
        else:
            self.__logger.error("parse_from_json: Problem with config file")

    def from_dict(self, config):
        '''
        Conversion from job to a configuration
        '''
        if 'job' not in config:
            self.__logger.error('from_dict: job properties not stored')
            return False
        if 'steer' not in config:
            self.__logger.error('from_dict: steer properties not stored')
            return False
        if 'menu' not in config:
            self.__logger.error('from_dict: menu not stored')
            return False
        for p in config['job']:
            self.properties.add_property(p, config['job'][p])
        for p in config['steer']:
            self.steer.properties.add_property(p, config['job'][p])
        menu = OrderedDict()
        for item in config['menu']:
            algos = []
            # print(config['menu'][item])
            if len(config['menu'][item].keys()) == 0:
                continue
            for algo in config['menu'][item]:
                # print(algo)
                try:
                    module = importlib.import_module(
                            config['menu'][item][algo]['module']
                            )
                except ImportError:
                    print('Unable to load module ',
                          config['menu'][item][algo]['module'])
                    return False
                class_ = getattr(module, config['menu'][item][algo]['class'])
                self.__logger.debug("from_dict: %s" % (algo))
                self.__logger.debug(pformat(
                                    config['menu'][item][algo]['properties']
                                    ))

                # Update algorithm logging level
                if 'loglevel' not in config['menu'][item][algo]['properties']:
                    config['menu'][item][algo]['properties']['loglevel'] = \
                            Logger.CONFIGURED_LEVEL

                instance = class_(algo,
                                  **config['menu'][item][algo]['properties']
                                  )
                algos.append(instance)
                self.__logger.debug("from_dict: instance {}".format(instance))
            menu[item] = tuple(algos)
        self.menu = menu
        return True

    def _run(self):
        '''
        Event Loop
        '''
        self.__logger.info("artemis: Run")
        self.__logger.debug('artemis: Count at run call %s' %
                            str(self._requestcntr))
        # while (self._requestcntr < self.max_requests):
        #     self._request_data()
        #     self.__logger.debug('Count after get_data %s' %
        #                         str(self._requestcntr))
        #     self._execute()
        #     self.__logger.debug('Count after process_data %s' %
        #                         str(self._requestcntr))
        while (self._requestcntr < self.max_requests):
            try:
                self.hbook.fill('artemis', 'counts', self._requestcntr)
                self._super_execute()
                self.__logger.debug('Count after process_data %s' %
                                    str(self._requestcntr))
                # TODO: Insert collect for datastore/nodes/tree.
                # TODO: Test memory release.
                Tree().flush()
            except Exception:
                self.__logger.error("Problem executing")
                raise

    def _finalize(self):
        # print("Hbook refernce count: ", sys.getrefcount(self.hbook))
        # print(self.hbook)]
        self.__logger.info("Finalizing Artemis job %s" % self.jobname)
        self.jobops.data['results'] = OrderedDict()
        self.steer.finalize()
        mu_payload = self.hbook.get_histogram('artemis', 'payload').mean()
        mu_blocksize = self.hbook.get_histogram('artemis', 'blocksize').mean()
        self.__logger.info("Mean payload %2.2f MB" % mu_payload)
        self.__logger.info("Mean blocksize %2.2f MB" % mu_blocksize)
        self.jobops.data['results']['artemis.payload'] = mu_payload
        self.jobops.data['results']['artemis.blocksize'] = mu_blocksize

        collections = self.hbook.to_message()
        self.jobinfo.summary.collection.CopyFrom(collections)
        colname = self.jobname + '_hist.dat'
        jobinfoname = self.jobname + '_info.dat'
        try:
            with open(colname, "wb") as f:
                f.write(collections.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
        except Exception:
            raise
        try:
            with open(jobinfoname, "wb") as f:
                f.write(self.jobinfo.SerializeToString())
        except IOError:
            self.__logger.error("Cannot write hbook")
        except Exception:
            raise

        self.__logger.info(pformat(self.jobops.data))
        postname = self.jobname + '_results.json'
        if 'protomsg' in self.jobops.data:
            del self.jobops.data['protomsg']
        try:
            with open(postname, 'x') as ofile:
                json.dump(self.jobops.data, ofile, indent=4)
        except IOError as e:
            self.__logger.error('I/O Error({0}: {1})'.
                                format(e.errno, e.strerror))

        self.jobinfo.finished.GetCurrentTime()

    def _check_requests(self):
        self.__logger.info('Remaining data check. Status coming up.')
        return self._requestcntr < self.max_requests

    def _requests_count(self):
        self._requestcntr += 1

    def _request_data(self):
        self.data = self.data_handler()
        self._requests_count()

    def _request_datum(self):
        self.__logger.debug('{}: request_datum: chunk: {} requests: {}'.format(
                            'artemis', self._chunkcntr, self._requestcntr))
        try:
            # Request the next chunk to process
            self.payload = next(self.data)
            self.__logger.info("Received data %s", len(self.payload))
            if self.payload is None:
                self.__logger.error("generator is Null")
                raise NullDataError('empty payload')
        except Exception:
            self.__logger.debug("Data handler empty, make request")
            return False
        else:
            self._chunkcntr += 1
            return True

    def _execute(self):
        # TODO data request should send total payload size
        # self.__logger.debug("Payload size %2.1f" % sys.getsizeof(self.data))
        # TODO
        # Exception handling for Steering
        while self._request_datum():
            self.__logger.info("Steering payload %s", len(self.payload))
            try:
                self.steer.execute(self.payload)
            except Exception:
                raise
            self.processed += sys.getsizeof(self.payload)
        # TODO should check total payload matches processed payload
        if(self.__logger.isEnabledFor(logging.INFO)):
            self.__logger.debug('Processed %2.1f' % self.processed)

    def _super_execute(self):
        try:
            raw = next(self.data_handler())
            self._requests_count()
        except Exception:
            self.__logger.debug("Data generator completed file batches")
            raise
        self.jobops.data['file'] = OrderedDict()

        raw_size = len(raw)

        stream = io.BytesIO(raw)

        file_ = pa.PythonFile(stream, mode='r')

        header, meta, off_head = self._filehandler.strip_header(file_)

        metadict = OrderedDict()
        metadict['payload'] = raw_size
        metadict['schema'] = meta
        self.jobops.data['file']['file_'+str(self._requestcntr)] = metadict
        # seek past header
        file_.seek(off_head)

        blocks = self._filehandler.get_blocks(file_,
                                              self.properties.blocksize,
                                              bytes(self.properties.delimiter,
                                                    'utf8'),
                                              self.properties.skip_header,
                                              off_head)
        self.__logger.info("Blocks")
        print(blocks)
        self.hbook.fill('artemis', 'payload', bytes_to_mb(raw_size))
        self.hbook.fill('artemis', 'nblocks', len(blocks))
        self.__logger.info("Size in bytes %2.3f in MB %2.3f" %
                           (raw_size, bytes_to_mb(raw_size)))
        proc_size = off_head
        for block in blocks:
            _chunk = bytearray(block[1])  # Mutable, readinto bytearray
            self.hbook.fill('artemis', 'blocksize',
                            bytes_to_mb(len(_chunk)))

            chunk = self._filehandler.readinto_block(file_,
                                                     _chunk,
                                                     block[0],
                                                     meta)
            try:
                self.steer.execute(chunk)  # Make chunk immutable
            except Exception:
                raise
            self.__logger.info("Chunk size %i, block size %i" %
                               (len(chunk), block[1]))
            proc_size += len(_chunk)
            self.processed += len(chunk)

        self.__logger.info('Payload %i, processed %i' % (raw_size, proc_size))
        self.__logger.info('Processed %2.1f' % self.processed)

        if proc_size != raw_size:
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

    def abort(self, *args, **kwargs):
        self.__logger.error("Artemis has been triggered to Abort")
        self.__logger.error("Reason %s" % args[0])
