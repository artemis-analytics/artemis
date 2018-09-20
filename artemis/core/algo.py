#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2018 Ryan Mackenzie White <ryan.white4@canada.ca>
#
# Distributed under terms of the  license.

"""
Algorithms
"""

import sys
import logging

from artemis.core.properties import Properties


# TODO Create an interface class to AlgoBase to expose the run, finalize methods to framework
# Interface IAlgoBase class to expose the methods to the framework (apparently, I should not write a framework, see Fluent Python ... I am bored but probably getting paid)
# Concrete implementation of interface with AlgoBase
# Concrete base class provides the mixins or other ABCs
# Likely we want to provide the Job class instance to retrieve 
# job.histbook
# job.timers
# job.objectstore
# Inherited classes for user-defined methods MyAlgo

def logged(obj):
    '''
    Taken from autologging.py
    Create a decorator to add logging to a class
    '''
    
    # Default use module name for logger
    # If AlgoBase use mro to set name
    logger_name = obj.__module__
    logger_attribute_name = '_' + obj.__name__ + '__logger'
    setattr(obj, logger_attribute_name, logging.getLogger(logger_name))
    
    return obj

class AbcAlgoBase(type):
    '''
    https://stackoverflow.com/questions/29069655/python-logging-with-a-common-logger-class-mixin-and-class-inheritance

    Logger for the Base class and each derived class.
    Not for instances though
    To identify logging from different configurations pass the instance name (attribute)
    '''
    def __init__(cls, *args, **kwargs):
        super().__init__(*args, **kwargs)
         
        # Explicit name mangling
        logger_attribute_name = '_' + cls.__name__ + '__logger'

        # Logger name derived accounting for inheritance for the bonus marks
        logger_name = '.'.join([c.__name__ for c in cls.mro()[-2::-1]])

        setattr(cls, logger_attribute_name, logging.getLogger(logger_name))

class AlgoBase(metaclass=AbcAlgoBase):
     
    def __init__(self, name, **kwargs):
        '''
        Access the Base logger directly through
        self.__logger
        Derived class use the classmethods for info, debug, warn, error
        All formatting, loglevel checks, etc... can be done through the classmethods

        Can we use staticmethods in artemis to make uniform formatting of info, debug, warn, error?
        '''
        self.__logger.info('__init__ AlgoBase')
        # name will be mangled to _AlgoBase__name
        self.__name = name
        self.properties = Properties()
        for key in kwargs:
            self.properties.add_property(key, kwargs[key])
        
        # Check kwargs for loglevel, which overrides root logger level setting
        if 'loglevel' in kwargs:
            numeric_level = logging.INFO
            if isinstance(self.properties.loglevel, int):
                numeric_level = self.properties.loglevel
            else:    
                numeric_level = getattr(logging, 
                                        self.properties.loglevel.upper(), None)
                if not isinstance(numeric_level, int):
                    raise ValueError('Invalid log level: %s' % self.properties.loglevel)
            self.setLogLevel(numeric_level)
        else:
            # Set the effective level from the root logger
            self.setLogLevel(logging.getLogger().getEffectiveLevel())
        
    def __init_subclass__(cls, **kwargs):
        '''
        See PEP 487
        Essentially acts as a class method decorator
        '''
        super().__init_subclass__(**kwargs)
    
    @classmethod
    def setLogLevel(cls, level):
        getattr(cls, '_' + cls.__name__ + '__logger').setLevel(level)
    
    @property
    def name(self):
        return self.__name
    
    @property
    def hbook(self):
        return self._hbook

    @hbook.setter
    def hbook(self, hbook):
        self._hbook = hbook
    
    def lock(self):
        '''
        Lock all properties for algorithm
        '''
        self.properties.lock = True

    def initialize(self):
        '''
        Framework initialize
        '''
        pass

    def book(self):
        '''
        Book histograms
        '''
        pass 
    
    def execute(self, payload):
        '''
        Algo always accepts the output Node on a graph
        Data is accessed via the Parent.payload
        '''
        pass 
   
    def finalize(self):
        '''
        report timings, counters, etc..
        '''
        pass 


