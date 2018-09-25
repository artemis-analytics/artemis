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
from pprint import pprint, pformat
import sys
import importlib
import json
from collections import OrderedDict

# Externals
from transitions import Machine

# Framework
from artemis.logger import Logger

from artemis.core.properties import Properties
from artemis.core.steering import Steering

# Data generators
from artemis.generators.generators import GenCsvLike


@Logger.logged
class Artemis():
    
    # Global state,
    states = ['quiescent',
              'start',
              'stop',
              'configuration',
              'initialization',
              'lock',
              'meta',
              'book',
              'run',
              'execution',
              'end',
              'abort',
              'error',
              'finalization']
    
    transitions = [
            {'trigger': 'launch',
                'source': 'quiescent',
                'dest': 'start',
                'after': '_launch'},
            {'trigger': 'configure',
                'source': 'start',
                'dest': 'configuration',
                'after': '_configure'},
            {'trigger': 'initialize',
                'source': 'configuration',
                'dest': 'initialization',
                'after': '_initialize'},
            {'trigger': 'lock_properties',
                'source': 'initialization',
                'dest': 'lock',
                'after': '_lock'},
            {'trigger': 'metastore',
                'source': 'lock',
                'dest': 'meta',
                'prepare': ['_to_dict'],
                # 'before': '_set_meta_environment',
                'after': '_to_json'},
            {'trigger': 'book_job',
                'source': 'meta',
                'dest': 'book',
                'after': '_book'},
            {'trigger': 'run_job',
                'source': 'book',
                'dest': 'run',
                'after': '_run'},
            # {'trigger': 'finalize',
            #     'source': 'run',
            #     'dest': 'finalization',
            #     'after': '_finalize'},
            {'trigger': 'request_data',
                'source': 'run',
                'dest': 'execution',
                'after': '_request_data',
                'conditions': '_check_requests'},
            {'trigger': 'execute',
                'source': 'execution',
                'dest': 'run',
                'after': ['_execute', '_requests_count']},
            {'trigger': 'no_data',
                'source': 'run',
                'dest': 'end',
                'after': 'run_out'},
            # Can we have a Trigger that is also a Sate?
            {'trigger': 'error', 
                'source': '*', 'dest': 'error', 
                'after': 'proc_error'}
            ]

    def __init__(self, name, **kwargs):
        self.jobname = name
        if 'jobname' not in kwargs:
            kwargs['jobname'] = name
        self.meta_filename = self.jobname + '_meta.json'

        self.hbook = dict()
        self.steer = None
        self._menu = None
        self._meta_dict = None
        
        ############################################################################
        # Properties 
        self.properties = Properties()
        for key in kwargs:
            self.properties.add_property(key, kwargs[key])
        ############################################################################

        ############################################################################
        # Logging
        Logger.configure(self, **kwargs) 
        ############################################################################
        
        # Data generator class instance
        self.generator = GenCsvLike()
        
        # Data Handler is just the generator function which returns a generator
        self.data_handler = self.generator.generate  
        
        ############################################################################
        # Initialize the State Machine
        # Artmetis' Soul
        
        self.machine = Machine(model=self,
                               states=Artemis.states,
                               transitions=Artemis.transitions,
                               #send_event=True,
                               initial='quiescent')
        ############################################################################
        
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
        return self._menu

    @menu.setter
    def menu(self, config):
        self._menu = config
    
    def control(self):
        '''
        Stateful Job processing via pytransitions
        '''
        self.launch()
        self.configure()
        self.initialize()
        self.lock_properties()
        self.metastore()
        self.book_job()
        self.run_job()
        self.no_data()
        self.error()

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
        self.steer.initialize(self)
    
    def _lock(self):
        '''
        Lock all properties before initialize
        '''
        self.__logger.info("{}: Lock".format('artemis'))
        self.properties.lock = True
        self.steer.properties.lock = True
        for key in self._menu:
            algos = self._menu[key].algos
            for algo in algos:
                if isinstance(algo, str):
                    self.__logger.debug("{}: Lock {}".format('artemis', algo))
                else:
                    algo.properties.lock = True
    
    def _book(self):
        self.__logger.info("{}: Book".format('artemis'))
        self.hbook["job_counts"] = "counts"
        self.steer.book()
   
    def _set_meta_environment(self, meta_filename):
        self.meta_filename = meta_filename
        #print(event.kwargs)
        #self.meta_filename = event.kwargs.get('meta_filename', None)
        
    def _to_dict(self):
        '''
        Dictionary of job configuration
        '''
        self._meta_dict = OrderedDict()
        self._meta_dict['job'] = OrderedDict()
        props = self.properties.properties
        for item in props:
            self._meta_dict['job'][item] = props[item]
        props = self.steer.properties.properties
        self._meta_dict['steer'] = OrderedDict()
        for item in props:
            self._meta_dict['steer'][item] = props[item]
        self._meta_dict['menu'] = OrderedDict()
        for key in self._menu:
            self._meta_dict['menu'][key] = OrderedDict()
            algos = self._menu[key].algos
            for algo in algos:
                if isinstance(algo, str):
                    continue
                self._meta_dict['menu'][key][algo.name] = OrderedDict()
                self._meta_dict['menu'][key][algo.name]['class'] = \
                    algo.__class__.__name__
                self._meta_dict['menu'][key][algo.name]['module'] = \
                    algo.__module__
                props = algo.properties.properties
                self._meta_dict['menu'][key][algo.name]['properties'] = \
                    OrderedDict()
                for item in props:
                    #TODO: Retain the type information of the value
                    self._meta_dict['menu'][key][algo.name]['properties'][item] = \
                        props[item]
        self.__logger.debug(pformat(self._meta_dict))
    
    def _to_json(self):
        try:
            with open(self.meta_filename, 'x') as ofile:
                json.dump(self._meta_dict, ofile, indent=4)
        except IOError as e:
            print('I/O Error({0}: {1})'.format(e.errno, e.strerror))
            return False
        except:
            print('Unexpected error:', sys.exc_info()[0])
            return False
        return True
    
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
                self.__logger.debug("from_dict: {}".format(algo)) 
                self.__logger.debug(pformat(config['menu'][item][algo]['properties']))

                # Update the logging level of the algorithms if loglevel not set
                # This ensures that the user-defined algos get the artemis level logging
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
        self.__logger.info("{}: Run".format('artemis'))
        self.__logger.debug('{} Testing count after run_job {}'.format('artemis',str(self._requestcntr)))
        while (self._requestcntr < self.max_requests):
            self.request_data()
            self.__logger.debug('{} Testing count after get_data {}'.format('artemis',str(self._requestcntr)))
            self.execute()
            self.__logger.debug('{} Testing count after process_data {}'.format('artemis',str(self._requestcntr)))

    def _finalize(self):
        print("Hbook refernce count: ", sys.getrefcount(self.hbook))
        print(self.hbook)
    
    def _check_requests(self):
        print('Remaining data check. Status coming up.')
        return self._requestcntr < self.max_requests

    def _requests_count(self):
        print(self.state)
        self._requestcntr += 1

    def _request_data(self):
        print(self.state)
        self.data = self.data_handler()
    
    def _request_datum(self):
        self.__logger.debug('{}: request_datum: chunk: {} requests: {}'.format(
                            'artemis', self._chunkcntr, self._requestcntr))
        try:
            self.payload = next(self.data)   # Request the next chunk to process
            if self.payload is None:
                self.__logger.error("request_datum:generator returned but object is None")    
                                    
        except:
            self.__logger.debug("request_datum: Data handler empty, make request")
            return False 
        else:
            self._chunkcntr += 1
            return True

    def _execute(self):
        if(self.__logger.isEnabledFor(logging.DEBUG)):
            self.__logger.debug('Execute: State {}'.format('artemis', self.state))
        # TODO data request should send total payload size
        self.__logger.debug("Processing event size %2.1f", sys.getsizeof(self.data))
        while self._request_datum():
            self.steer.execute(self.payload)
            self.processed += sys.getsizeof(self.payload)
        # TODO should check total payload matches processed payload    
        if(self.__logger.isEnabledFor(logging.DEBUG)):
            self.__logger.debug('Processed %2.1f', self.processed)

    def run_out(self):
        print(self.state)
        pass

    def proc_error(self):
        print(self.state)


