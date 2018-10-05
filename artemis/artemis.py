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

# Externals

# Framework
from artemis.logger import Logger
from artemis.exceptions import NullDataError

# Core
from artemis.core.properties import Properties
from artemis.core.properties import JobProperties
from artemis.core.steering import Steering

# Data generators
from artemis.generators.generators import GenCsvLike


@Logger.logged
class Artemis():

    def __init__(self, name, **kwargs):

        # Set defaults if not configured
        self.jobname = name

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

        self.hbook = dict()
        self.steer = None
        self._menu = None
        self._meta_dict = None

        #######################################################################
        # Properties
        self.properties = Properties()
        self.jobops = JobProperties()
        for key in kwargs:
            self.properties.add_property(key, kwargs[key])
        #######################################################################

        #######################################################################
        # Logging
        Logger.configure(self, **kwargs)
        #######################################################################

        # Data generator class instance
        self.generator = GenCsvLike()

        # Data Handler is just the generator function which returns a generator
        self.data_handler = self.generator.generate

        # Temporary placeholders to managing event loop
        self.num_chunks = 0
        self._chunkcntr = 0
        self.max_requests = 0
        self._requestcntr = -1
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
        '''
        self.__logger.info('Configure')
        self.hbook = dict()
        self.__logger.info("Hbook reference count: %i",
                           sys.getrefcount(self.hbook))

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

        self.steer = Steering('steer', loglevel=Logger.CONFIGURED_LEVEL)

        # TODO
        # For framework level classes, use module __name__ for logger
        # getChild from artemis.artemis logger and set log level?

        # Temporary hard-coded values to get something running
        self.num_chunks = 2
        self.max_requests = 2
        self.processed = 0
        return True

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
        self.hbook["job_counts"] = "counts"
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
        while (self._requestcntr < self.max_requests):
            self._request_data()
            self.__logger.debug('Count after get_data %s' %
                                str(self._requestcntr))
            self._execute()
            self.__logger.debug('Count after process_data %s' %
                                str(self._requestcntr))

    def _finalize(self):
        print("Hbook refernce count: ", sys.getrefcount(self.hbook))
        print(self.hbook)

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
        self.__logger.debug("Payload size %2.1f" % sys.getsizeof(self.data))
        # TODO
        # Exception handling for Steering
        while self._request_datum():
            self.steer.execute(self.payload)
            self.processed += sys.getsizeof(self.payload)
        # TODO should check total payload matches processed payload
        if(self.__logger.isEnabledFor(logging.DEBUG)):
            self.__logger.debug('Processed %2.1f' % self.processed)

    def abort(self, *args, **kwargs):
        self.__logger.error("Artemis has been triggered to Abort")
        self.__logger.error("Reason %s" % args[0])
