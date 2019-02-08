#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © Her Majesty the Queen in Right of Canada, as represented
# by the Minister of Statistics Canada, 2019.
#
# Distributed under terms of the  license.

"""

"""
import logging
import os


class Logger():
    '''
    class to retain common logging properties
    '''

    FMT = '%(name)s - %(funcName)s - %(levelname)s - %(message)s'
    DEFAULT_LEVEL = logging.INFO
    CONFIGURED_LEVEL = DEFAULT_LEVEL

    @staticmethod
    def loglevel(**kwargs):
        # Check kwargs for loglevel, which overrides root logger level setting
        numeric_level = Logger.DEFAULT_LEVEL
        if 'loglevel' in kwargs:
            if isinstance(kwargs['loglevel'], int):
                numeric_level = kwargs['loglevel']
            else:
                numeric_level = getattr(logging,
                                        kwargs['loglevel'].upper(), None)
                if not isinstance(numeric_level, int):
                    raise ValueError('Invalid log level: %s' %
                                     kwargs['loglevel'])
        else:
            # Set the effective level from the root logger
            numeric_level = logging.getLogger().getEffectiveLevel()
            # print('Root logger level ', numeric_level)

        Logger.CONFIGURED_LEVEL = numeric_level

        return numeric_level

    @staticmethod
    def logfilehandler(**kwargs):
        if 'jobname' not in kwargs:
            name = 'test'
        else:
            name = kwargs['jobname']
        print(kwargs)
        print(name)
        logging_fname = name + '.log'
        if 'path' in kwargs:
            logging_fname = os.path.join(kwargs['path'], logging_fname)
        fh = logging.FileHandler(logging_fname, 'w')
        fh.setFormatter(logging.Formatter(Logger.FMT))
        fh.setLevel(Logger.CONFIGURED_LEVEL)
        logging.getLogger().addHandler(fh)

    @staticmethod
    def logged(obj):
        '''
        Taken from autologging.py
        Create a decorator to add logging to a class
        '''
        # Default use module name for logger
        # If AlgoBase use mro to set name
        logger_name = obj.__module__
        logger_attribute_name = '_' + obj.__name__ + '__logger'

        def fget(obj): return getattr(obj, logger_attribute_name)

        # add the getter property to cls
        setattr(obj, 'logger', property(fget))

        setattr(obj, logger_attribute_name, logging.getLogger(logger_name))

        return obj

    @staticmethod
    def setloglevel(obj, **kwargs):
        '''
        all loggers have hidden name __logger
        '''
        # level = Logger.loglevel(**kwargs)
        _logname = '_' + obj.__class__.__name__ + '__logger'
        logging.getLogger().debug('Setting the log level for %s' %
                                  obj.__class__.__name__)
        getattr(obj, _logname).setLevel(Logger.CONFIGURED_LEVEL)

    @staticmethod
    def setexternals():
        pass
        # logging.getLogger('transitions').setLevel(Logger.CONFIGURED_LEVEL)

    @staticmethod
    def configure(obj, **kwargs):
        Logger.loglevel(**kwargs)
        Logger.setloglevel(obj, **kwargs)
        if obj.__class__.__name__ == 'Artemis':
            Logger.logfilehandler(**kwargs)
            Logger.setexternals()
